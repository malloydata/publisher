// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import {
   buildScenario,
   rowDifference,
   disposeScenarios,
   freshDuckDB,
   type R4Fixture,
   type R4Query,
   type Row,
} from "./harness";

/**
 * The #7098 fixture, served after the v1 cutover: no serve shape exists, so
 * every stored answer is the served copy's (or a leg's). Every answer must
 * equal live. (Before the retirement it also ran through today's storage
 * serve shape, and with PUBLISHER_MATERIALIZE on beside it.)
 */
for (const mode of ["cutover"] as const) {
   const servedCopy = true;
   describe(`[${mode}]`, () => {
      let restoreSettings: () => void;
      beforeAll(() => {
         restoreSettings = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               ...(servedCopy ? { PUBLISHER_MATERIALIZE: "true" } : {}),
            },
         }).restore;
      });
      afterAll(async () => {
         restoreSettings();
         await disposeScenarios();
      });

      /** Served must equal live, and must have read a stored table. */
      async function expectServedMatchesLive(
         fixture: R4Fixture,
         query: R4Query,
         /** In served-copy mode, the served copy (not the serve shape) answers. */
         viaServedCopy = false,
      ) {
         const run = await fixture.run(query);
         if (servedCopy && viaServedCopy) {
            expect(run.servedVia).toBe("served_copy");
         }
         if (query.expectLive) expect(run.servedFrom).not.toBe("storage");
         else expect(run.servedFrom).toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         // After the cutover, also by the strict comparison
         // (`answer_difference.ts`: a string stays a string, an integer and a
         // bigint exact), which must find nothing.
         if (mode === "cutover") {
            expect(strictRowDifference(run.live, run.served)).toBeUndefined();
         }
         return run;
      }

      describe("R4: a persisted rollup and its extension", () => {
         let fixture: R4Fixture;
         beforeAll(async () => {
            fixture = await buildScenario({
               files: {
                  "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum(), order_count is count()
}

source: daily_ext is daily extend {
  dimension: big_day is total > 100
  measure: grand_total is total.sum()
}
`,
               },
               seedSql: [
                  `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'east', 100.5),
               (2, DATE '2026-01-01', 'west', 50.25),
               (3, DATE '2026-01-02', 'east', 200.0),
               (4, DATE '2026-01-02', 'east', 25.125),
               (5, DATE '2026-01-03', 'west', 300.0)
             ) AS t(id, order_date, region, amount)`,
               ],
            });
         });

         it("binds the persisted source", () => {
            expect(fixture.bindings.map((b) => b.sourceName)).toContain(
               "daily",
            );
         });

         it("matches live grouped by a stored dimension", async () => {
            await expectServedMatchesLive(
               fixture,
               {
                  name: "by date",
                  malloy:
                     "run: daily -> { group_by: order_date; aggregate: t is total.sum() }",
               },
               true,
            );
         });

         it("matches live under a filter", async () => {
            await expectServedMatchesLive(
               fixture,
               {
                  name: "east only",
                  malloy:
                     "run: daily -> { where: region = 'east'; aggregate: t is total.sum(), n is order_count.sum() }",
               },
               true,
            );
         });

         it("matches live through the extension's dimension and measure", async () => {
            await expectServedMatchesLive(
               fixture,
               {
                  name: "big days",
                  malloy:
                     "run: daily_ext -> { group_by: big_day; aggregate: grand_total }",
               },
               true,
            );
         });
      });

      describe("R4: child counts through a join_many with childless parents", () => {
         // Flights 5 and 6 have no segments: the case malloydata/malloy#1762 is about.
         const seedSql = [
            `CREATE TABLE flights AS SELECT * FROM (VALUES
         (1, 'AA'), (2, 'AA'), (3, 'UA'), (4, 'UA'), (5, 'AA'), (6, 'UA')
       ) AS t(id, carrier)`,
            `CREATE TABLE segments AS SELECT * FROM (VALUES
         (10, 1, 100), (11, 1, 200), (12, 2, 300), (13, 3, 400), (14, 4, 500)
       ) AS t(seg_id, flight_id, miles)`,
         ];
         const model = `##! experimental.persistence
#@ persist name="segs" storage="lake"
source: segs is duckdb.table('segments') -> { select: * } extend {
  primary_key: seg_id
  measure: seg_count is count()
}

#@ persist name="flight_rows" storage="lake"
source: flights is duckdb.table('flights') -> { select: * } extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
}
`;
         const byCarrier: R4Query = {
            name: "segments per carrier",
            malloy:
               "run: flights -> { group_by: carrier; aggregate: seg_count is segs.count(), miles is segs.miles.sum() }",
            truthSql: `SELECT f.carrier, count(s.seg_id) AS seg_count, sum(s.miles) AS miles
                 FROM flights f LEFT JOIN segments s ON s.flight_id = f.id GROUP BY 1`,
         };
         let fixture: R4Fixture;
         beforeAll(async () => {
            fixture = await buildScenario({
               files: { "m.malloy": model },
               seedSql,
            });
         });

         const pick =
            (...columns: string[]) =>
            (rows: Row[]) =>
               rows.map((r) =>
                  Object.fromEntries(columns.map((c) => [c, r[c]])),
               );
         const miles = pick("carrier", "miles");
         const counts = pick("carrier", "seg_count");

         it("serves the sum from storage, equal to live and to the truth", async () => {
            const run = await fixture.run(byCarrier);
            expect(run.servedFrom).toBe("storage");
            expect(
               rowDifference(miles(run.live), miles(run.served)),
            ).toBeUndefined();
            expect(
               rowDifference(miles(run.truth ?? []), miles(run.served)),
            ).toBeUndefined();
         });

         // Since Malloy 0.0.438 (malloydata/malloy#3208, fixing #1762) live
         // counts a childless flight as no segments, so live, the served copy
         // (which compiles the author's own join) and the serve shape all
         // answer the truth.
         it("serves the right child count from storage", async () => {
            const run = await fixture.run(byCarrier);
            expect(
               rowDifference(counts(run.truth ?? []), counts(run.served)),
            ).toBeUndefined();
         });

         it("live child count matches the truth (malloydata/malloy#1762)", async () => {
            const run = await fixture.run(byCarrier);
            expect(
               rowDifference(counts(run.truth ?? []), counts(run.live)),
            ).toBeUndefined();
         });

         it("served child count equals live (malloydata/malloy#1762)", async () => {
            const run = await fixture.run(byCarrier);
            expect(
               rowDifference(counts(run.live), counts(run.served)),
            ).toBeUndefined();
         });
      });

      // ms2data/service#6982, the phase 0 probe: two sources from different origin
      // connections, both stored in one destination, joined. Malloy refuses the join
      // as authored (malloydata/malloy#3032); over the destination it is a join on
      // one connection. This checks the second half by hand, before phase 3 builds
      // the served copy that does it.
      describe("R4 probe: a join across origin connections, over their destination", () => {
         const ROOT = "file:///r4probe/";
         const origin = (name: string): DuckDBConnection => freshDuckDB(name);

         async function compile(
            connections: Map<string, DuckDBConnection>,
            model: string,
            query: string,
         ) {
            const runtime = new Runtime({
               urlReader: new InMemoryURLReader(
                  new Map([[`${ROOT}m.malloy`, model]]),
               ),
               connections: new FixedConnectionMap(
                  connections,
                  [...connections.keys()][0],
               ),
            });
            return runtime
               .loadModel(new URL(`${ROOT}m.malloy`), {
                  importBaseURL: new URL(ROOT),
               })
               .loadQuery(query);
         }

         const accounts = `SELECT * FROM (VALUES (1, 'acme'), (2, 'globex'), (3, 'initech')) AS t(id, account_name)`;
         const usage = `SELECT * FROM (VALUES (1, 5), (1, 7), (2, 1)) AS t(account_id, events)`;
         const query =
            "run: account_usage -> { group_by: account_name; aggregate: total_events is u.events.sum(), usage_rows is u.count() }";

         it("is refused as authored, across two connections", async () => {
            const crm = origin("crm");
            const product = origin("product");
            await crm.runSQL(`CREATE TABLE accounts AS ${accounts}`);
            await product.runSQL(`CREATE TABLE usage AS ${usage}`);
            const authored = `
source: accounts is crm.table('accounts') extend { primary_key: id }
source: usage is product.table('usage')
source: account_usage is accounts extend { join_many: u is usage on u.account_id = id }
`;
            const q = await compile(
               new Map([
                  ["crm", crm],
                  ["product", product],
               ]),
               authored,
               query,
            );
            await expect(q.getSQL()).rejects.toThrow(/connection/i);
         });

         it("runs over the destination and matches a hand-computed answer", async () => {
            // Both sides stored in the destination, as their builds would write them.
            const lake = origin("lake");
            await lake.runSQL(`CREATE TABLE accounts_s AS ${accounts}`);
            await lake.runSQL(`CREATE TABLE usage_s AS ${usage}`);
            const servedCopy = `
source: accounts is lake.table('accounts_s') extend { primary_key: id }
source: usage is lake.table('usage_s')
source: account_usage is accounts extend { join_many: u is usage on u.account_id = id }
`;
            const q = await compile(
               new Map([["lake", lake]]),
               servedCopy,
               query,
            );
            const rows = (await q.run()).data.toObject() as Row[];
            // initech has no usage: Malloy's sum of no rows is 0. Only the sum is
            // compared here; the count case is above.
            expect(
               rowDifference(
                  [
                     { account_name: "acme", total_events: 12 },
                     { account_name: "globex", total_events: 1 },
                     { account_name: "initech", total_events: 0 },
                  ],
                  rows.map((r) => ({
                     account_name: r.account_name,
                     total_events: r.total_events,
                  })),
               ),
            ).toBeUndefined();
         });
      });

      // Rolling windows (the plan's "Rolling windows" section, scenario S25): a
      // current-time `where:` is evaluated at build with `now` written as a literal,
      // and re-applied at every read. Over the serving window [b, b + interval), the
      // literal must make the stored set the WIDEST the window needs, or a read comes
      // up short. This hand-builds the tables each literal would write and pins each
      // read by writing its time into the query; rolling_window.component.spec.ts
      // runs the real build and served copy at a pinned clock.
      describe("R4 probe: rolling windows keep the widest set over the serving window", () => {
         const ROOT = "file:///r4window/";
         const B = "2026-11-10 00:00:00"; // build start
         // Interval: one day. Reads at b, mid-interval, and just before b + interval.
         const READS = [
            "2026-11-10 00:00:00",
            "2026-11-10 12:00:00",
            "2026-11-10 23:59:59",
         ];
         let duckdb: DuckDBConnection;
         let runtime: Runtime;

         beforeAll(async () => {
            duckdb = freshDuckDB("duckdb");
            // One event an hour, from well before b - 30 days to after b + 1 day.
            await duckdb.runSQL(`CREATE TABLE events AS
         SELECT ts, row_number() OVER (ORDER BY ts) AS amount
         FROM range(TIMESTAMP '${B}' - INTERVAL 36 DAY, TIMESTAMP '${B}' + INTERVAL 2 DAY, INTERVAL 1 HOUR) t(ts)`);
            const build = (table: string, where: string) =>
               duckdb.runSQL(
                  `CREATE TABLE ${table} AS SELECT * FROM events WHERE ${where}`,
               );
            // Lower bound, "the last 30 days".
            await build(
               "lower_widest",
               `ts >= TIMESTAMP '${B}' - INTERVAL 30 DAY`,
            );
            await build(
               "lower_s25",
               `ts >= TIMESTAMP '${B}' + INTERVAL 1 DAY - INTERVAL 30 DAY`,
            );
            // Upper bound, "older than 30 days".
            await build(
               "upper_widest",
               `ts < TIMESTAMP '${B}' + INTERVAL 1 DAY - INTERVAL 30 DAY`,
            );
            await build(
               "upper_narrow",
               `ts < TIMESTAMP '${B}' - INTERVAL 30 DAY`,
            );
            const model = [
               "events",
               "lower_widest",
               "lower_s25",
               "upper_widest",
               "upper_narrow",
            ]
               .map((t) => `source: ${t} is duckdb.table('${t}')`)
               .join("\n");
            runtime = new Runtime({
               urlReader: new InMemoryURLReader(
                  new Map([[`${ROOT}m.malloy`, model]]),
               ),
               connections: new FixedConnectionMap(
                  new Map([["duckdb", duckdb]]),
                  "duckdb",
               ),
            });
         });

         /** `source` read at `at`, with the window's term re-applied at that time. */
         async function read(
            source: string,
            term: ">=" | "<",
            at: string,
         ): Promise<Row[]> {
            const query = `run: ${source} -> {
        where: ts ${term} @${at} - 30 days
        aggregate: n is count(), total is amount.sum()
      }`;
            const result = await runtime
               .loadModel(new URL(`${ROOT}m.malloy`), {
                  importBaseURL: new URL(ROOT),
               })
               .loadQuery(query)
               .run();
            return result.data.toObject() as Row[];
         }

         for (const at of READS) {
            it(`a lower bound built with now = b matches live at ${at}`, async () => {
               expect(
                  rowDifference(
                     await read("events", ">=", at),
                     await read("lower_widest", ">=", at),
                  ),
               ).toBeUndefined();
            });
            it(`an upper bound built with now = b + interval matches live at ${at}`, async () => {
               expect(
                  rowDifference(
                     await read("events", "<", at),
                     await read("upper_widest", "<", at),
                  ),
               ).toBeUndefined();
            });
         }

         it("a lower bound built as S25 first expected (now = b + 1 day) comes up short mid-interval", async () => {
            const at = READS[1];
            const [live] = await read("events", ">=", at);
            const [served] = await read("lower_s25", ">=", at);
            expect(Number(served.n)).toBeLessThan(Number(live.n));
         });

         it("an upper bound built with now = b comes up short after b", async () => {
            const at = READS[1];
            const [live] = await read("events", "<", at);
            const [served] = await read("upper_narrow", "<", at);
            expect(Number(served.n)).toBeLessThan(Number(live.n));
         });
      });

      // ms2data/service#6642: a stored column's type is whatever the warehouse
      // returned, which need not be the type Malloy declares. Each column below is
      // one way that happens: DuckDB answers `date + 1 month` with a timestamp (as
      // Postgres answers a month truncation), a `sql_date` over a timestamp keeps the
      // timestamp, a `sql_boolean` over an integer stays an integer, and a
      // `sql_number` over text stays text.
      describe("R4: stored column types and the types Malloy declares", () => {
         const model = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="typed" storage="lake"
source: typed is ev -> {
  select:
    id, amt, ts, tz, d,
    d_next is d + 1 month,
    month_start is sql_date("date_trunc('month', \${ts})"),
    ts_utc is sql_timestamp("\${tz}"),
    flag is sql_boolean("CASE WHEN \${id} > 2 THEN 1 ELSE 0 END"),
    amount_text is sql_number("CAST(\${amt} AS VARCHAR)")
}
`;
         // Times late in the UTC day, so a query time zone moves them across a day.
         const seedSql = [
            `CREATE TABLE ev AS SELECT * FROM (VALUES
         (1, 10.125::DECIMAL(18,3), TIMESTAMP '2026-01-15 23:30:00', TIMESTAMPTZ '2026-01-15 23:30:00+00', DATE '2026-01-31'),
         (2, 20.250::DECIMAL(18,3), TIMESTAMP '2026-02-03 01:00:00', TIMESTAMPTZ '2026-02-03 01:00:00+00', DATE '2026-02-28'),
         (3, 5.500::DECIMAL(18,3), TIMESTAMP '2026-02-28 12:00:00', TIMESTAMPTZ '2026-02-28 12:00:00+00', DATE '2026-03-01'),
         (4, 0.001::DECIMAL(18,3), TIMESTAMP '2026-03-01 00:00:00', TIMESTAMPTZ '2026-03-01 00:00:00+00', DATE '2026-03-31')
       ) AS t(id, amt, ts, tz, d)`,
         ];
         const decimal: R4Query = {
            name: "decimal sum, average and ratio",
            malloy:
               "run: typed -> { aggregate: s is amt.sum(), a is amt.avg(), per is amt.sum() / count() }",
            truthSql:
               // Raw DuckDB returns a DECIMAL as text; DOUBLE compares as a number.
               "SELECT sum(amt)::DOUBLE AS s, avg(amt)::DOUBLE AS a, (sum(amt) / count(*))::DOUBLE AS per FROM ev",
         };
         const dateVsDate: R4Query = {
            name: "a date compared with a date",
            malloy:
               "run: typed -> { where: d_next > d; aggregate: n is count() }",
         };
         const dateInZone: R4Query = {
            name: "a date grouped under a query time zone",
            malloy:
               "run: typed -> { timezone: 'America/Los_Angeles'; group_by: d_next, month_start; aggregate: n is count() }",
         };
         const truncatedDate: R4Query = {
            name: "a warehouse timestamp declared a date",
            malloy:
               "run: typed -> { where: month_start = @2026-02-01; group_by: month_start; aggregate: s is amt.sum() }",
         };
         const flag: R4Query = {
            name: "an integer declared a boolean",
            malloy: "run: typed -> { group_by: flag; aggregate: n is count() }",
         };
         const timestamps: R4Query[] = [
            {
               name: "timestamptz by day",
               malloy:
                  "run: typed -> { group_by: on_day is tz.day; aggregate: n is count() }",
            },
            {
               name: "timestamptz by day under a query time zone",
               malloy:
                  "run: typed -> { timezone: 'America/Los_Angeles'; group_by: on_day is tz.day; aggregate: n is count() }",
            },
            {
               name: "a stored timestamptz declared a timestamp, by day under a query time zone",
               malloy:
                  "run: typed -> { timezone: 'America/Los_Angeles'; group_by: on_day is ts_utc.day; aggregate: n is count() }",
            },
            {
               name: "a stored timestamptz declared a timestamp, filtered",
               malloy:
                  "run: typed -> { where: ts_utc >= @2026-02-03 01:00:00; aggregate: n is count() }",
            },
            {
               name: "a timestamp by hour",
               malloy:
                  "run: typed -> { group_by: h is ts.hour; aggregate: n is count() }",
            },
         ];
         const numberAsText: R4Query = {
            name: "a number the warehouse returned as text",
            malloy:
               "run: typed -> { group_by: amount_text; aggregate: n is count() }",
            expectLive: true,
         };

         describe("as built now, casting to the declared types", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: { "m.malloy": model },
                  seedSql,
               });
            });

            it("stores the castable columns as their declared types, and leaves the rest", () => {
               const types = Object.fromEntries(
                  fixture.bindings[0].schema.map((c) => [c.name, c.type]),
               );
               expect(types).toMatchObject({
                  d_next: "DATE",
                  month_start: "DATE",
                  flag: "BOOLEAN",
                  amt: "DECIMAL(18,3)",
                  ts: "TIMESTAMP",
                  tz: "TIMESTAMP WITH TIME ZONE",
                  ts_utc: "TIMESTAMP WITH TIME ZONE",
                  amount_text: "VARCHAR",
               });
            });

            it("reports only the column no cast can make faithful", () => {
               expect(fixture.typeMismatches()).toMatchObject([
                  {
                     sourceName: "typed",
                     mismatches: [
                        {
                           column: "amount_text",
                           storedType: "VARCHAR",
                           declaredType: "number",
                           handling: "live",
                        },
                     ],
                  },
               ]);
            });

            it("serves a decimal equal to live and to the truth", async () => {
               const run = await expectServedMatchesLive(fixture, decimal);
               expect(
                  rowDifference(run.truth ?? [], run.served),
               ).toBeUndefined();
            });

            for (const query of [dateVsDate, dateInZone, truncatedDate, flag]) {
               it(`serves ${query.name} from storage, equal to live`, async () => {
                  await expectServedMatchesLive(fixture, query);
               });
            }

            for (const query of timestamps) {
               it(`serves ${query.name} from storage, equal to live`, async () => {
                  await expectServedMatchesLive(fixture, query);
               });
            }

            it("answers a query reading the text-typed number live", async () => {
               await expectServedMatchesLive(fixture, numberAsText);
            });
         });

         // A table an older build wrote, before the cast: the serve side must not
         // serve its mistyped columns, and must still serve the rest.
         describe("as an older build stored it, uncast", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: { "m.malloy": model },
                  seedSql,
                  uncast: true,
               });
            });

            it("reports each column whose stored type does not serve as declared", () => {
               const [only] = fixture.typeMismatches();
               expect(
                  only.mismatches.map((m) => [
                     m.column,
                     m.storedType,
                     m.handling,
                  ]),
               ).toEqual([
                  ["d_next", "TIMESTAMP", "cast"],
                  ["month_start", "TIMESTAMP", "cast"],
                  ["flag", "INTEGER", "cast"],
                  ["amount_text", "VARCHAR", "live"],
               ]);
            });

            // Read through a cast to the declared type, so the old table still
            // serves them, equal to live.
            for (const query of [dateVsDate, dateInZone, truncatedDate, flag]) {
               it(`serves ${query.name} from storage through a cast, equal to live`, async () => {
                  await expectServedMatchesLive(fixture, query);
               });
            }

            it("answers the text-typed number live", async () => {
               await expectServedMatchesLive(fixture, numberAsText);
            });

            it("still serves the columns stored faithfully", async () => {
               await expectServedMatchesLive(fixture, decimal);
               for (const query of timestamps) {
                  await expectServedMatchesLive(fixture, query);
               }
            });
         });
      });

      // The review of ms2data/service#6642: a mistyped column must not take the rest
      // of the shape with it, and an instant must not be dated in the wrong zone.
      describe("R4: a mistyped column stays local to its source", () => {
         const seedSql = [
            `CREATE TABLE ev AS SELECT * FROM (VALUES
         (1, 10.5::DOUBLE, TIMESTAMPTZ '2026-01-15 16:30:00+00', DATE '2026-01-31'),
         (2, 20.25::DOUBLE, TIMESTAMPTZ '2026-02-03 01:00:00+00', DATE '2026-02-28'),
         (3, 5.5::DOUBLE, TIMESTAMPTZ '2026-02-28 15:30:00+00', DATE '2026-03-01')
       ) AS t(id, amt, tz, d)`,
         ];
         const header = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')
`;
         const other = `
#@ persist name="other" storage="lake"
source: other is ev -> { select: id, amt } extend { measure: total is amt.sum() }
`;

         describe("a refinement over a column no cast makes faithful", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: {
                     "m.malloy":
                        header +
                        `
#@ persist name="typed2" storage="lake"
source: typed2 is ev -> {
  select: id, d, d_next is d + 1 month, amount_text is sql_number("CAST(\${amt} AS VARCHAR)")
} extend {
  dimension: next_month is d_next.month
  dimension: amount_twice is amount_text * 2
}
` +
                        other,
                  },
                  seedSql,
                  uncast: true,
               });
            });

            it("withholds only that source, which answers live", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "the source with the mistyped column",
                  malloy:
                     "run: typed2 -> { group_by: next_month; aggregate: n is count() }",
                  expectLive: true,
               });
            });

            it("keeps serving every other source from storage, whole", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "another stored source in the same model",
                  malloy: "run: other -> { group_by: id; aggregate: total }",
               });
            });
         });

         describe("a refinement over a column a cast makes faithful", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: {
                     "m.malloy":
                        header +
                        `
#@ persist name="typed3" storage="lake"
source: typed3 is ev -> { select: id, amt, d, d_next is d + 1 month } extend {
  dimension: next_month is d_next.month
}
` +
                        other,
                  },
                  seedSql,
                  uncast: true,
               });
            });

            it("serves the refinement from storage through the cast, equal to live", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "a dimension over a date an older build stored as a timestamp",
                  malloy:
                     "run: typed3 -> { where: d_next > d; group_by: next_month; aggregate: s is amt.sum() }",
               });
            });

            it("serves the other source from storage too", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "another stored source in the same model",
                  malloy: "run: other -> { aggregate: total }",
               });
            });
         });

         describe("a date taken from an instant under a query time zone", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: {
                     "m.malloy":
                        header +
                        `
#@ persist name="tok" storage="lake"
source: tok is ev -> {
  timezone: 'Asia/Tokyo'
  select: id, amt, day_of is sql_date("\${tz}")
}
`,
                  },
                  seedSql,
               });
            });

            it("is not cast to a date at build", () => {
               const types = Object.fromEntries(
                  fixture.bindings[0].schema.map((c) => [c.name, c.type]),
               );
               expect(types.day_of).toBe("TIMESTAMP WITH TIME ZONE");
            });

            it("answers a query reading it live, equal to live", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "the instant's date",
                  malloy:
                     "run: tok -> { group_by: day_of; aggregate: n is count() }",
                  expectLive: true,
               });
            });

            it("serves a query not reading it from storage", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "the rest of the source",
                  malloy: "run: tok -> { aggregate: s is amt.sum() }",
               });
            });
         });
      });

      // Sources built on a bound source, over a table an older build typed: a join
      // into it, and a source extending it, see its columns through the read cast.
      // A source extending it that names a column no cast makes faithful takes that
      // one base off storage, and nothing else.
      describe("R4: sources built on a mistyped stored source", () => {
         const seedSql = [
            `CREATE TABLE ev AS SELECT * FROM (VALUES
         (1, 10.5::DOUBLE, DATE '2026-01-31'),
         (2, 20.25::DOUBLE, DATE '2026-02-28'),
         (3, 5.5::DOUBLE, DATE '2026-03-01')
       ) AS t(id, amt, d)`,
         ];
         const header = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="other" storage="lake"
source: other is ev -> { select: id, amt } extend { measure: total is amt.sum() }
source: other_ext is other extend { dimension: big is amt > 10 }
`;
         const typed = (extra: string) => `
#@ persist name="typed" storage="lake"
source: typed is ev -> {
  select: id, amt, d, d_next is d + 1 month, flag is sql_boolean("CASE WHEN \${id} > 1 THEN 1 ELSE 0 END")${extra}
}
`;

         describe("a join into it, and a source extending it, over cast columns", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: {
                     "m.malloy":
                        header +
                        typed("") +
                        `source: typed_date is typed extend { dimension: nm is d_next.month }
source: other_join is other extend { join_one: typed on typed.id = id }
`,
                  },
                  seedSql,
                  uncast: true,
               });
            });

            it("reads the cast columns through it", () => {
               expect(
                  fixture
                     .typeMismatches()[0]
                     .mismatches.map((m) => [m.column, m.handling]),
               ).toEqual([
                  ["d_next", "cast"],
                  ["flag", "cast"],
               ]);
            });

            for (const query of [
               {
                  name: "a source extending it, over a cast date",
                  malloy:
                     "run: typed_date -> { where: d_next > d; group_by: nm; aggregate: n is count() }",
               },
               {
                  name: "a join into it, grouped by a cast boolean",
                  malloy:
                     "run: other_join -> { group_by: typed.flag; aggregate: total }",
               },
               {
                  name: "a join into it, filtered on a cast date",
                  malloy:
                     "run: other_join -> { where: typed.d_next > @2026-03-01; aggregate: total }",
               },
            ]) {
               it(`serves ${query.name} from storage, equal to live`, async () => {
                  await expectServedMatchesLive(fixture, query);
               });
            }
         });

         describe("a source extending it that names a column no cast makes faithful", () => {
            let fixture: R4Fixture;
            beforeAll(async () => {
               fixture = await buildScenario({
                  files: {
                     "m.malloy":
                        header +
                        typed(
                           `, amount_text is sql_number("CAST(\${amt} AS VARCHAR)")`,
                        ) +
                        `source: typed_text is typed extend { dimension: tagged is concat(amount_text, 'x') }
`,
                  },
                  seedSql,
                  uncast: true,
               });
            });

            it("answers the base's queries live, even those not reading the column", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "the base, over a cast boolean",
                  malloy:
                     "run: typed -> { where: flag; aggregate: n is count() }",
                  expectLive: true,
               });
            });

            it("keeps every other stored source, and sources extending them, on storage", async () => {
               await expectServedMatchesLive(fixture, {
                  name: "another stored source",
                  malloy: "run: other -> { aggregate: total }",
               });
               await expectServedMatchesLive(fixture, {
                  name: "a source extending another stored source",
                  malloy:
                     "run: other_ext -> { group_by: big; aggregate: total }",
               });
            });
         });
      });
   });
}

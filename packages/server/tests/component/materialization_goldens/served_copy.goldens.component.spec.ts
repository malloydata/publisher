// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Served-copy goldens the package goldens cannot reach, because each needs a
 * stored table that differs from what today's build writes:
 *
 *  - containment, per re-root kind: one source's stored table no longer fits
 *    its own text (a stand-in for a re-root bug), and the served copy must
 *    trace the compile failure to that re-root alone, leave it out with a
 *    coded reason, and keep every other source's table;
 *  - the cast plan, per case: a column read through a faithful cast, a column
 *    left out so a query reading it answers live, and a big integer read as
 *    its native type.
 *
 * Each golden holds the files generated while serving, the served copy's
 * problems, and, per query, where it was answered from and whether it equals
 * live. Regenerate with UPDATE_GOLDENS=1 (see golden_files.ts).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Scenario,
} from "../r4/harness";
import { GeneratedFiles, recordingOverlays } from "./capture";
import {
   expectGolden,
   removeGoldens,
   unclaimedGoldens,
   UPDATING,
} from "./golden_files";
import { Normalizer } from "./normalize";

/** The area this spec owns under the goldens root. */
const SERVED_COPY_AREA = "served_copy";

interface ServedCopyCase {
   name: string;
   scenario: R4Scenario;
   /** Stored tables made to differ from what the build wrote, by source. */
   tamper?: Record<string, (column: { name: string; type: string }) => string>;
   queries: { name: string; malloy: string }[];
}

const EVENTS_SEED = [
   `CREATE TABLE events AS
      SELECT ts, CAST(row_number() OVER (ORDER BY ts) AS INTEGER) AS amount,
             CASE WHEN hour(ts) % 2 = 0 THEN 'acme' ELSE 'globex' END AS org,
             CAST(hour(ts) % 3 AS INTEGER) AS customer_id
      FROM range(TIMESTAMP '2026-11-01 00:00:00', TIMESTAMP '2026-11-03 00:00:00', INTERVAL 1 HOUR) t(ts)`,
   `CREATE TABLE customers AS
      SELECT CAST(i AS INTEGER) AS id, 'segment_' || i AS segment FROM range(3) t(i)`,
];

/** `amount` read back as text: its source's own `amount.sum()` no longer compiles. */
const AMOUNT_AS_TEXT = (c: { name: string; type: string }) =>
   c.name === "amount" ? "VARCHAR" : c.type;

const V1_CONTAINMENT_MODEL = `##! experimental { persistence }

source: events is duckdb.table('events')

#@ persist storage="lake"
source: p_kept is events -> { select: org, amount } extend {
  measure: total is amount.sum()
}

#@ persist storage="lake"
source: p_broken is events -> { select: org, amount } extend {
  measure: total is amount.sum()
}
`;

const CONTAINMENT_MODEL = `

source: events is duckdb.table('events')
source: customers is duckdb.table('customers') extend { primary_key: id }

#(materialize) storage="lake"
source: m_kept is events extend {
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: m_broken is events extend {
  measure: total is amount.sum()
}

#(materialize) storage="lake" inline=["c"]
source: i_broken is events extend {
  join_one: c is customers with customer_id
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: e_broken is m_kept extend {
  measure: top is amount.max()
}
`;

const CAST_SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 9007199254740993::BIGINT, TIMESTAMP '2026-01-15 23:30:00', TIMESTAMPTZ '2026-01-15 23:30:00+00', '007', '2026-01-31', 1, TIMESTAMP '2026-01-15 00:00:00', TIMESTAMPTZ '2026-01-15 00:00:00+00'),
      (2, -9007199254740993::BIGINT, TIMESTAMP '2026-02-03 01:00:00', TIMESTAMPTZ '2026-02-03 01:00:00+00', '7', '2026-02-28', 0, TIMESTAMP '2026-02-03 00:00:00', TIMESTAMPTZ '2026-02-03 00:00:00+00'),
      (3, NULL::BIGINT, NULL::TIMESTAMP, NULL::TIMESTAMPTZ, NULL, NULL, NULL, NULL, NULL)
   ) AS t(id, big, ts, tz, code, d_text, flag_int, dts, dtz)`,
];

const CAST_MODEL = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="typed" storage="lake"
source: typed is ev -> {
  select:
    id, big, ts, tz, code,
    d_from_ts is sql_date("\${dts}"),
    d_from_tz is sql_date("\${dtz}"),
    d_from_text is sql_date("\${d_text}"),
    flag is sql_boolean("\${flag_int}"),
    code_num is sql_number("\${code}")
}

#(materialize) storage="lake"
source: wide is ev extend {
  measure: n is count()
}
`;

const CASES: ServedCopyCase[] = [
   {
      name: "containment, a v1 table",
      scenario: {
         files: { "m.malloy": V1_CONTAINMENT_MODEL },
         seedSql: EVENTS_SEED,
      },
      tamper: { p_broken: AMOUNT_AS_TEXT },
      queries: [
         {
            name: "the kept re-root",
            malloy: "run: p_kept -> { group_by: org; aggregate: total }",
         },
         {
            name: "the broken re-root",
            malloy: "run: p_broken -> { group_by: org; aggregate: total }",
         },
      ],
   },
   {
      name: "containment per materialized re-root kind",
      scenario: {
         files: { "m.malloy": CONTAINMENT_MODEL },
         seedSql: EVENTS_SEED,
         materialize: true,
      },
      tamper: {
         m_broken: AMOUNT_AS_TEXT,
         i_broken: AMOUNT_AS_TEXT,
         e_broken: AMOUNT_AS_TEXT,
      },
      queries: [
         {
            name: "the kept re-root",
            malloy: "run: m_kept -> { group_by: org; aggregate: total }",
         },
         {
            name: "the broken re-root",
            malloy: "run: m_broken -> { group_by: org; aggregate: total }",
         },
         {
            name: "an inlined join's broken re-root",
            malloy:
               "run: i_broken -> { group_by: c.segment; aggregate: total }",
         },
         {
            name: "an extension table's broken re-root",
            malloy: "run: e_broken -> { group_by: org; aggregate: top }",
         },
      ],
   },
   {
      name: "cast plan",
      // An older build's table: every column as the warehouse typed it.
      scenario: {
         files: { "m.malloy": CAST_MODEL },
         seedSql: CAST_SEED,
         uncast: true,
      },
      queries: [
         {
            name: "a faithful cast: a boolean from an integer",
            malloy: "run: typed -> { group_by: flag; aggregate: n is count() }",
         },
         {
            name: "a faithful cast: a date from a timestamp",
            malloy:
               "run: typed -> { group_by: d_from_ts; aggregate: n is count() }",
         },
         {
            name: "a faithful cast: a date from an instant",
            malloy:
               "run: typed -> { group_by: d_from_tz; aggregate: n is count() }",
         },
         {
            name: "a left-out column: a date stored as text",
            malloy:
               "run: typed -> { group_by: d_from_text; aggregate: n is count() }",
         },
         {
            name: "a left-out column: a number stored as text",
            malloy:
               "run: typed -> { group_by: code_num; aggregate: n is count() }",
         },
         {
            name: "a big integer, persisted",
            malloy: "run: typed -> { group_by: id, big; order_by: id }",
         },
         {
            name: "a big integer, materialized",
            malloy: "run: wide -> { group_by: id, big; order_by: id }",
         },
         {
            name: "a query reading no cast column",
            malloy: "run: typed -> { group_by: code; aggregate: n is count() }",
         },
      ],
   },
];

/** The golden's base path for a case, under the area. */
function goldenPath(c: ServedCopyCase): string {
   return `${SERVED_COPY_AREA}/${c.name.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}`;
}

const claimed = new Set<string>();
let restore: () => void;

beforeAll(() => {
   restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});

afterAll(async () => {
   restore();
   await disposeScenarios();
});

describe("served-copy goldens", () => {
   for (const c of CASES) {
      describe(c.name, () => {
         let fixture: R4Fixture;
         const served = new GeneratedFiles();
         beforeAll(async () => {
            fixture = await buildScenario(c.scenario);
            for (const [source, retype] of Object.entries(c.tamper ?? {})) {
               const bound = fixture.bindings.filter(
                  (b) => b.sourceName === source,
               );
               expect({
                  source,
                  bound: bound.map((b) => b.sourceName),
               }).toEqual({ source, bound: [source] });
               // In place: the fixture's runs read the same binding objects.
               (bound[0] as { schema: (typeof bound)[0]["schema"] }).schema =
                  bound[0].schema.map((col) => ({
                     ...col,
                     type: retype(col),
                  }));
            }
         }, 180_000);

         it("matches its golden", async () => {
            const normalizer = new Normalizer().addRoot(fixture.packagePath);
            const problems = await recordingOverlays(served, () =>
               fixture.servedCopyProblems(),
            );
            const queries: unknown[] = [];
            for (const q of c.queries) {
               try {
                  const run = await recordingOverlays(served, () =>
                     fixture.run({ name: q.name, malloy: q.malloy }),
                  );
                  queries.push({
                     query: q.name,
                     servedFrom: run.servedFrom,
                     servedVia: run.servedVia ?? null,
                     equalToLive:
                        rowDifference(run.live, run.served) === undefined,
                     fieldTypes: run.servedFields.map((f) => [
                        f.name,
                        (f.type as { type?: string }).type ?? f.type,
                     ]),
                  });
               } catch (error) {
                  queries.push({
                     query: q.name,
                     error: (error instanceof Error
                        ? error.message
                        : String(error)
                     ).split("\n")[0],
                  });
               }
            }
            const base = goldenPath(c);
            claimed.add(`${base}.serve.json`);
            claimed.add(`${base}.served.txt`);
            expectGolden(
               `${base}.serve.json`,
               normalizer.json({
                  problems: problems.map((p) => ({
                     sourceName: p.sourceName,
                     reason: p.reason,
                     ...(p.join ? { join: p.join } : {}),
                  })),
                  queries,
               }),
            );
            expectGolden(
               `${base}.served.txt`,
               served.render(
                  normalizer,
                  `${c.name}: files generated while serving`,
               ),
            );
         }, 180_000);
      });
   }

   it("leaves no golden of a case that is gone", () => {
      const stale = unclaimedGoldens(SERVED_COPY_AREA, claimed);
      if (UPDATING) removeGoldens(stale);
      else expect(stale).toEqual([]);
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P3/P4: a `#(rollup)` rollup in a `storage=` destination read past a
 * row-level gate, through the real package path (plan, bind, router,
 * `govern()`), with the gated-sources switch off (G3: the base's own gates)
 * and on (any gate proven on the grain).
 *
 * The rollup's variant redeclares the queried source over the stored rollup
 * in an overlay of its declaring file, with its gates written on it, so
 * `govern()` grafts and re-proves them there. A gate column must be a grain
 * dimension, stored exactly, and read in no other zone than the build's; a
 * query that fails any of these answers live.
 *
 * The matrix, every caller against live, interleaved and repeated:
 *
 * - callers: two tenants admitted, one admitted to no rows, one missing the
 *   given, and one a lock refuses;
 * - gates: a column, a computed dimension, a day truncation, a big integer
 *   past 2^53, an internal field, a lock beside a row filter, a lock beside
 *   a given-reading `where:`, an inherited
 *   row filter, an extension's own row filter on the grain and off it, a
 *   source with its own `timezone:`, and a TIMESTAMPTZ column;
 * - six query zones over the day gate;
 * - query stages: a pipeline, a nest, a named view and a `where:`;
 * - renames and joins: a caller's `rename:` and `except:` of the gate column,
 *   a caller join, a renamed extension, and a gated parent joining an
 *   ungated source's `storage=` rollup (g-mixed, answered live here).
 *
 * Every outcome must equal live's: the same rows, or a denial where live
 * denies. The answers a G3 rollup should give are checked to come from it.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference } from "./harness";

const P = ` storage="${LAKE}"`;

const MODEL = `##! experimental { givens access_modifiers composite_sources }
given: REGION :: string
given: REG :: string
given: ON_DAY :: timestamp
given: ACCT :: number
given: ROLE :: string
given: CAT :: string
given: AT :: timestamp
given: EXCL :: string

source: orders_raw is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DOUBLE) AS amount, category,
         region, CAST(acct AS BIGINT) AS acct, CAST(ts AS TIMESTAMP) AS ts,
         CAST(ts AS TIMESTAMPTZ) AS tsz
  FROM (VALUES
    (1, 10.0, 'A', 'east', 9007199254740993, '2026-01-04 04:00:00'),
    (2, 20.0, 'A', 'west', 9007199254740992, '2026-01-04 20:00:00'),
    (3, 30.0, 'B', 'east', 9007199254740993, '2026-01-05 06:00:00'),
    (4,  5.0, 'B', 'west', 9007199254740992, '2026-01-05 23:30:00'),
    (5, 12.5, 'C', 'west', 7, '2026-01-06 00:15:00'),
    (6,  7.5, 'C', 'east', 7, '2026-01-06 12:00:00')
  ) AS t(id, amount, category, region, acct, ts)
""") extend { primary_key: id }

// Ungated, so the destination always holds a rollup.
source: open is orders_raw extend {
  #(rollup) grain=["category"]${P}
  measure: open_total is amount.sum()
}

#(access_filter) region = $REGION
source: by_region is orders_raw extend {
  #(rollup) grain=["category", "region"]${P}
  measure: total is amount.sum()
  view: by_category is { group_by: category; aggregate: total }
}

#(access_filter) reg = $REG
source: by_upper is orders_raw extend {
  dimension: reg is upper(region)
  #(rollup) grain=["category", "reg"]${P}
  measure: total is amount.sum()
}

#(access_filter) dday = $ON_DAY
source: by_day is orders_raw extend {
  dimension: dday is ts.day
  #(rollup) grain=["category", "dday"]${P}
  measure: total is amount.sum()
}

#(access_filter) acct = $ACCT
source: by_acct is orders_raw extend {
  #(rollup) grain=["category", "acct"]${P}
  measure: total is amount.sum()
}

#(access_filter) tenant = $REGION
source: by_internal is orders_raw extend {
  internal dimension: tenant is region
  #(rollup) grain=["category", "tenant"]${P}
  measure: total is amount.sum()
}

#(authorize) 'admin' = $ROLE
#(access_filter) region = $REGION
source: by_lock_filter is orders_raw extend {
  #(rollup) grain=["category", "region"]${P}
  measure: total is amount.sum()
  #(rollup) grain=["category", "region"]${P}
  measure: lf_n is count()
}

// A lock beside a given-reading where: of the base's own: the where:'s
// column is read like a gate column.
#(authorize) 'admin' = $ROLE
source: by_lock_where is orders_raw extend {
  where: category = $CAT
  #(rollup) grain=["category"]${P}
  measure: total is amount.sum()
}

#(access_filter) region = $REGION
source: gated_base is orders_raw
source: by_inherited is gated_base extend {
  #(rollup) grain=["category", "region"]${P}
  measure: total is amount.sum()
  #(rollup) grain=["category", "region"]${P}
  measure: inh_n is count()
}

// Extensions of a rollup base (row 22): one adding a row filter on a grain
// field, one off the grain, and one renaming the gate column.
#(access_filter) category = $CAT
source: ext_on_grain is by_region extend {}
#(access_filter) acct = $ACCT
source: ext_off_grain is by_region extend {}
source: ext_renamed is by_region extend { rename: area is region }

// A source with its own time zone: never read from its rollup.
#(access_filter) dday = $ON_DAY
source: by_zoned_source is orders_raw extend {
  timezone: 'America/New_York'
  dimension: dday is ts.day
  #(rollup) grain=["category", "dday"]${P}
  measure: total is amount.sum()
}

// A gate over a column stored with its zone: answered live.
#(access_filter) at_z = $AT
source: by_tstz is orders_raw extend {
  dimension: at_z is tsz
  #(rollup) grain=["category", "at_z"]${P}
  measure: total is amount.sum()
}

// g-mixed: a gated parent joining an ungated source's storage= rollup.
source: lines is duckdb.sql("""
  SELECT CAST(order_id AS BIGINT) AS order_id, CAST(qty AS DOUBLE) AS qty
  FROM (VALUES (1, 2.0), (1, 3.0), (2, 4.0), (3, 1.0), (5, 6.0), (6, 2.5))
    AS t(order_id, qty)
""") extend {
  #(rollup) grain=["order_id"]${P}
  measure: qty_total is qty.sum()
}
#(access_filter) region = $REGION
source: gated_parent is orders_raw extend {
  join_many: lines on lines.order_id = id
}

// A column the source compares case-insensitively: a gate or a scope over
// it is never read from the byte-exact copy (#1880's comparison rule).
source: orders_ci is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DOUBLE) AS amount, category,
         region COLLATE NOCASE AS region
  FROM (VALUES (1, 10.0, 'A', 'east'), (2, 20.0, 'A', 'East'), (3, 30.0, 'B', 'EAST'),
               (4, 5.0, 'B', 'west'), (5, 1.0, 'C', 'West')) AS t(id, amount, category, region)
""") extend { primary_key: id }
#(access_filter) region = $REGION
source: ci_eq is orders_ci extend {
  #(rollup) grain=["category", "region"]${P}
  measure: total is amount.sum()
}
#(access_filter) category = $CAT
source: ci_where_filter is orders_ci extend {
  where: region != $EXCL
  #(rollup) grain=["category", "region"]${P}
  measure: total is amount.sum()
}

// A composite with its own row filter: refused live, so never built.
#(access_filter) region = $REGION
source: comp_own is compose(orders_raw, orders_raw extend { dimension: x is 1 }) extend {
  #(rollup) grain=["category", "region"]${P}
  measure: total is amount.sum()
}

source: region_names is duckdb.sql("""
  SELECT * FROM (VALUES ('east', 'East'), ('west', 'West')) AS t(code, label)
""") extend { primary_key: code }
`;

type Givens = Record<string, string | number | bigint>;

const REGION_CALLERS: Record<string, Givens> = {
   east: { REGION: "east", ROLE: "admin" },
   west: { REGION: "west", ROLE: "admin" },
   "no rows": { REGION: "north", ROLE: "admin" },
   "missing given": { ROLE: "admin" },
   "the lock refuses": { REGION: "east", ROLE: "viewer" },
};

const ZONES = [
   "",
   "timezone: 'UTC'; ",
   "timezone: 'America/New_York'; ",
   "timezone: 'Asia/Kolkata'; ",
   "timezone: 'Pacific/Chatham'; ",
   "timezone: 'Pacific/Kiritimati'; ",
   "timezone: 'America/Adak'; ",
];

interface Case {
   name: string;
   malloy: string;
   callers: Record<string, Givens>;
   /** With the switch off and on: whether an admitted caller is answered from the rollup. */
   fromRollup?: { off: boolean; on: boolean; caller: string };
}

const byCat = (s: string) =>
   `run: ${s} -> { group_by: category; aggregate: total; order_by: category }`;

const CASES: Case[] = [
   {
      name: "a column",
      malloy: byCat("by_region"),
      callers: REGION_CALLERS,
      fromRollup: { off: true, on: true, caller: "east" },
   },
   {
      name: "a column, grouped by the gate column",
      malloy:
         "run: by_region -> { group_by: region, category; aggregate: total; order_by: category }",
      callers: REGION_CALLERS,
      fromRollup: { off: true, on: true, caller: "west" },
   },
   {
      name: "a column, a pipeline",
      malloy:
         "run: by_region -> { group_by: category; aggregate: total } -> { aggregate: grand is total.sum() }",
      callers: REGION_CALLERS,
   },
   {
      name: "a column, a nest",
      malloy:
         "run: by_region -> { group_by: category; nest: n is { aggregate: total }; order_by: category }",
      callers: REGION_CALLERS,
   },
   {
      name: "a column, a named view",
      malloy: "run: by_region -> by_category",
      callers: REGION_CALLERS,
   },
   {
      name: "a column, a where:",
      malloy:
         "run: by_region -> { where: category != 'C'; group_by: category; aggregate: total; order_by: category }",
      callers: REGION_CALLERS,
   },
   {
      name: "a caller rename: of the gate column",
      malloy:
         "run: by_region extend { rename: r2 is region } -> { group_by: category; aggregate: total }",
      callers: REGION_CALLERS,
   },
   {
      name: "a caller except: of the gate column",
      malloy:
         "run: by_region extend { except: region } -> { group_by: category; aggregate: total }",
      callers: REGION_CALLERS,
   },
   {
      name: "a caller join",
      malloy:
         "run: by_region extend { join_one: rn is region_names on region = rn.code } -> { group_by: rn.label; aggregate: total }",
      callers: REGION_CALLERS,
   },
   {
      name: "a computed dimension",
      malloy: byCat("by_upper"),
      callers: {
         EAST: { REG: "EAST" },
         WEST: { REG: "WEST" },
         "no rows": { REG: "east" },
         "missing given": {},
      },
      fromRollup: { off: true, on: true, caller: "EAST" },
   },
   ...ZONES.map((tz) => ({
      name: `a day truncation${tz ? `, ${tz.trim()}` : ""}`,
      malloy: `run: by_day -> { ${tz}group_by: category; aggregate: total; order_by: category }`,
      callers: {
         "jan 4": { ON_DAY: "2026-01-04 00:00:00" },
         "jan 5": { ON_DAY: "2026-01-05 00:00:00" },
         "jan 6": { ON_DAY: "2026-01-06 00:00:00" },
         "no rows": { ON_DAY: "2025-06-01 00:00:00" },
         "missing given": {},
      },
      ...(tz === ""
         ? { fromRollup: { off: true, on: true, caller: "jan 5" } }
         : {}),
   })),
   ...ZONES.map((tz) => ({
      name: `a day truncation grouped by the day${tz ? `, ${tz.trim()}` : ""}`,
      malloy: `run: by_day -> { ${tz}group_by: dday, category; aggregate: total; order_by: category }`,
      callers: {
         "jan 4": { ON_DAY: "2026-01-04 00:00:00" },
         "jan 6": { ON_DAY: "2026-01-06 00:00:00" },
      },
   })),
   {
      name: "a big integer past 2^53",
      malloy: byCat("by_acct"),
      callers: {
         "...993": { ACCT: 9007199254740993n },
         "...992": { ACCT: 9007199254740992n },
         seven: { ACCT: 7 },
         "no rows": { ACCT: 8 },
         "missing given": {},
      },
      fromRollup: { off: true, on: true, caller: "...993" },
   },
   {
      name: "an internal gate field",
      malloy: byCat("by_internal"),
      callers: REGION_CALLERS,
      fromRollup: { off: true, on: true, caller: "east" },
   },
   {
      name: "a lock beside a row filter",
      malloy: byCat("by_lock_filter"),
      callers: REGION_CALLERS,
      fromRollup: { off: true, on: true, caller: "east" },
   },
   {
      name: "a lock beside a given-reading where:",
      malloy: byCat("by_lock_where"),
      callers: {
         "admin A": { ROLE: "admin", CAT: "A" },
         "admin B": { ROLE: "admin", CAT: "B" },
         "no rows": { ROLE: "admin", CAT: "Z" },
         "the lock refuses": { ROLE: "viewer", CAT: "A" },
         "missing given": { ROLE: "admin" },
      },
      fromRollup: { off: true, on: true, caller: "admin A" },
   },
   {
      name: "an inherited row filter",
      malloy: byCat("by_inherited"),
      callers: REGION_CALLERS,
      fromRollup: { off: false, on: true, caller: "east" },
   },
   {
      name: "an extension's own row filter on a grain field",
      malloy: byCat("ext_on_grain"),
      callers: {
         "east A": { REGION: "east", CAT: "A" },
         "west B": { REGION: "west", CAT: "B" },
         "no rows": { REGION: "east", CAT: "Z" },
         "missing given": { REGION: "east" },
      },
   },
   {
      name: "an extension's own row filter off the grain",
      malloy: byCat("ext_off_grain"),
      callers: {
         "east 7": { REGION: "east", ACCT: 7 },
         "missing given": { REGION: "east" },
      },
      fromRollup: { off: false, on: false, caller: "east 7" },
   },
   {
      name: "an extension renaming the gate column",
      malloy: byCat("ext_renamed"),
      callers: REGION_CALLERS,
   },
   ...ZONES.slice(0, 3).map((tz) => ({
      name: `a source with its own time zone${tz ? `, ${tz.trim()}` : ""}`,
      malloy: `run: by_zoned_source -> { ${tz}group_by: category; aggregate: total; order_by: category }`,
      callers: {
         "jan 4": { ON_DAY: "2026-01-04 00:00:00" },
         "jan 5": { ON_DAY: "2026-01-05 00:00:00" },
         "missing given": {},
      },
      fromRollup: { off: false, on: false, caller: "jan 4" },
   })),
   {
      name: "a TIMESTAMPTZ gate column",
      malloy: byCat("by_tstz"),
      callers: {
         "jan 4 04:00": { AT: "2026-01-04 04:00:00" },
         "jan 5 06:00": { AT: "2026-01-05 06:00:00" },
         "no rows": { AT: "2025-01-01 00:00:00" },
         "missing given": {},
      },
      fromRollup: { off: false, on: false, caller: "jan 4 04:00" },
   },
   {
      name: "a NOCASE gate",
      malloy:
         "run: ci_eq -> { group_by: category, region; aggregate: total; order_by: category }",
      callers: {
         east: { REGION: "east" },
         EAST: { REGION: "EAST" },
         West: { REGION: "West" },
      },
      fromRollup: { off: false, on: false, caller: "east" },
   },
   {
      name: "a NOCASE where: beside a gate",
      malloy:
         "run: ci_where_filter -> { group_by: category, region; aggregate: total; order_by: category }",
      callers: {
         "A excl east": { CAT: "A", EXCL: "east" },
         "B excl east": { CAT: "B", EXCL: "east" },
      },
      fromRollup: { off: false, on: false, caller: "A excl east" },
   },
   {
      name: "a composite with its own row filter",
      malloy: byCat("comp_own"),
      callers: { east: { REGION: "east" }, west: { REGION: "west" } },
   },
   {
      name: "g-mixed: a gated parent joining an ungated source's storage= rollup",
      malloy:
         "run: gated_parent -> { group_by: category; aggregate: q is lines.qty_total; order_by: category }",
      callers: REGION_CALLERS,
   },
];

function show(o: Outcome): string {
   return JSON.stringify(o, (_k, v) =>
      typeof v === "bigint" ? `${v}n` : (v as unknown),
   );
}

function difference(live: Outcome, served: Outcome): string | undefined {
   if ("error" in live || "error" in served) {
      return "error" in live && "error" in served && live.error === served.error
         ? undefined
         : `live ${show(live)} against served ${show(served)}`;
   }
   return rowDifference(live.answer.rows, served.answer.rows);
}

/** Every comparison the matrix made, for the report. */
const tally = { compared: 0, differences: 0, fromRollup: 0, denied: 0 };

for (const gatedSources of [false, true]) {
   describe(`P3/P4: storage= rollups past a row-level gate, every caller equal to live (gated sources ${gatedSources ? "on" : "off"})`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome[]>();
      const key = (c: Case, caller: string) => `${c.name}\u0000${caller}`;

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({ "m.malloy": MODEL });
         for (const c of CASES) {
            for (const [caller, givens] of Object.entries(c.callers)) {
               live.set(
                  key(c, caller),
                  await loader.run("m.malloy", c.malloy, givens),
               );
            }
         }
         fixture = await loader.bind({ includeRollups: true });
         // Interleaved and repeated across callers.
         for (const round of [0, 1]) {
            for (const c of CASES) {
               const callers = Object.keys(c.callers);
               for (const caller of round === 0
                  ? callers
                  : [...callers].reverse()) {
                  const outcomes = served.get(key(c, caller)) ?? [];
                  outcomes.push(
                     await fixture.run("m.malloy", c.malloy, c.callers[caller]),
                  );
                  served.set(key(c, caller), outcomes);
               }
            }
         }
      }, 900_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
         if (process.env.P3P4_TALLY) {
            // eslint-disable-next-line no-console
            console.log(
               `P3P4_TALLY gs=${gatedSources} ${JSON.stringify(tally)}`,
            );
         }
      });

      it("builds every storage= rollup it should, and no other", () => {
         expect(fixture.buildErrors).toEqual({});
      });

      for (const c of CASES) {
         it(`${c.name}: every caller gets live's answer`, () => {
            for (const caller of Object.keys(c.callers)) {
               const want = live.get(key(c, caller))!;
               for (const got of served.get(key(c, caller)) ?? []) {
                  const diff = difference(want, got);
                  tally.compared++;
                  if (diff !== undefined) tally.differences++;
                  if ("error" in got) tally.denied++;
                  else if (got.answer.servedBy === "rollup") tally.fromRollup++;
                  expect(
                     diff,
                     `${caller}: ${diff ?? ""}\n${show(got)}`,
                  ).toBeUndefined();
               }
            }
         });
         if (c.fromRollup) {
            const want = gatedSources ? c.fromRollup.on : c.fromRollup.off;
            const caller = c.fromRollup.caller;
            it(`${c.name}: an admitted caller is answered ${want ? "from the rollup" : "live"}`, () => {
               const got = served.get(key(c, caller))![0];
               expect("answer" in got, show(got)).toBe(true);
               if (!("answer" in got)) return;
               if (want) expect(got.answer.servedBy).toBe("rollup");
               else expect(got.answer.servedBy).not.toBe("rollup");
            });
         }
      }
   });
}

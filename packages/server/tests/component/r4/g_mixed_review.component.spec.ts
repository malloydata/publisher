// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Regressions for the question-14 review of PR #1854 (g-mixed): three ways
 * a gated entry point over an ungated `storage=` table answered rows live
 * does not, each closed by #1855's gate checks, which g-mixed reads.
 *
 * - H1: a lock-only composite over an ungated `storage=` member (`D …`):
 *   served only from the member live resolves to.
 * - H2: a `DECIMAL` (or `BIGINT`) gate column stored as a `DOUBLE` (`A …`,
 *   `B …`, with the table altered after its build): never read past the
 *   gate.
 * - H3: a stage computed in an inline zoned base (`C …`), and a stored day
 *   under a caller's zone (`C2 …`): never read past the gate.
 *
 * Every caller, interleaved, gets live's answer or live's denial, with the
 * gate columns stored as built and altered to `DOUBLE`.
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

const TRIPS =
   "d,cat,amt,dur,org\n" +
   "2017-01-02,a,1.5,10,1\n" +
   "2017-03-02,b,2.5,4000,2\n" +
   "2018-01-02,a,3.5,5000,1\n" +
   "2017-05-05,c,-1,7200,2\n" +
   "2017-06-01,b,4.0,3700,1\n" +
   "2017-07-01,a,0.5,9000,2\n";

const MODEL = `##! experimental { givens persistence access_modifiers composite_sources }
given: ACCT :: number
given: ON_DAY :: timestamp
given: ORG :: number
given: CAT :: string
given: CATS :: string[]
given: ROLE :: string

source: accounts is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(acct AS DECIMAL(20,0)) AS acct,
         CAST(acct AS BIGINT) AS acct_i, region, CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    (1, '9007199254740992', 'east', 10),
    (2, '9007199254740992', 'east', 20),
    (3, '9007199254740993', 'west', 1000),
    (4, '9007199254740993', 'west', 2000),
    (5, '9007199254740995', 'north', 7)
  ) AS t(id, acct, region, amount)
""") extend { primary_key: id }

source: ev is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, tenant, CAST(ts AS TIMESTAMP) AS ts,
         CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    (1, 'a', '2026-01-04 04:00:00', 1),
    (2, 'a', '2026-01-04 20:00:00', 2),
    (3, 'b', '2026-01-05 06:00:00', 1000),
    (4, 'b', '2026-01-05 07:00:00', 2000),
    (5, 'c', '2026-01-06 12:00:00', 50)
  ) AS t(id, tenant, ts, amount)
""") extend { primary_key: id }

// A: DECIMAL gate column, stored DOUBLE (the unconstrained-numeric gap).
#(materialize) name="u_dec" storage="${LAKE}"
source: u_dec is accounts -> { where: id > -1; select: id, acct, region, amount } extend { measure: n is count() }
#(access_filter) acct = $ACCT
source: g_dec is u_dec extend {}

// B: BIGINT gate column stored DOUBLE, read under a rename (interim misses),
// and the plain control (interim catches).
#(materialize) name="u_big" storage="${LAKE}"
source: u_big is accounts -> { where: id > -2; select: id, acct_i, region, amount } extend { measure: n is count() }
#(access_filter) acct_i = $ACCT
source: g_big is u_big extend {}

// C: a stage computed in an inline base's zone; gated extension; no query zone.
#@ persist name="u_srctz" storage="${LAKE}"
source: u_srctz is ev extend { timezone: 'America/Los_Angeles' } -> { where: id > -7; select: id, tenant, amount, ts, dday is ts.day }
#(access_filter) dday = $ON_DAY
source: g_srctz is u_srctz extend {}
#(materialize) name="u_srctz_mz" storage="${LAKE}"
source: u_srctz_mz is ev extend { timezone: 'America/Los_Angeles' } -> { where: id > -9; select: id, tenant, amount, ts, dday is ts.day }
#(access_filter) dday = $ON_DAY
source: g_srctz_mz is u_srctz_mz extend {}
#(materialize) name="u_day_mz" storage="${LAKE}"
source: u_day_mz is ev -> { where: id > -10; select: id, tenant, amount, ts, dday is ts.day }
#(access_filter) dday = $ON_DAY
source: g_day_mz is u_day_mz extend {}
// C2: plain stage day, gated extension (zoned query covered by interim).
#@ persist name="u_day" storage="${LAKE}"
source: u_day is ev -> { where: id > -8; select: id, tenant, amount, ts, dday is ts.day }
#(access_filter) dday = $ON_DAY
source: g_day is u_day extend {}

// D: composite (L4) over an ungated storage= member, gated by a row filter.
source: trips is duckdb.table('data/trips.csv')
#(materialize) name="u_y" storage="${LAKE}"
source: y is trips extend { where: amt > 0 }
#(authorize) 'analyst' = $ROLE
source: comp_gate_org is compose(y extend { where: org = $ORG }, y)
#(authorize) 'analyst' = $ROLE
source: comp_gate_cat is compose(y extend { where: cat = 'a' }, y)
`;

const RMODEL = `##! experimental { givens persistence access_modifiers }
import "m.malloy"
#(access_filter) a = $ACCT
source: g_big_ren is u_big extend { rename: a is acct_i }
`;
const ALTER: Record<string, string> = { u_dec: "acct", u_big: "acct_i" };

interface Q {
   name: string;
   malloy: string;
   callers: Record<string, Record<string, unknown>>;
   file?: string;
}

const ACCTS = {
   "992": { ACCT: "9007199254740992" },
   "993": { ACCT: "9007199254740993" },
   "994-none": { ACCT: "9007199254740994" },
   missing: {},
};
const DAYS = {
   d4: { ON_DAY: "2026-01-04 00:00:00" },
   d5: { ON_DAY: "2026-01-05 00:00:00" },
   d6: { ON_DAY: "2026-01-06 00:00:00" },
   missing: {},
};
const ORGS = {
   org1: { ORG: 1, ROLE: "analyst" },
   org2: { ORG: 2, ROLE: "analyst" },
   org99: { ORG: 99, ROLE: "analyst" },
   viewer: { ORG: 1, ROLE: "viewer" },
   missing: {},
};

const QUERIES: Q[] = [
   {
      name: "A g_dec",
      malloy: "run: g_dec -> { select: id, region; order_by: id }",
      callers: ACCTS,
   },
   {
      name: "A g_dec agg",
      malloy: "run: g_dec -> { aggregate: n }",
      callers: ACCTS,
   },
   {
      file: "r.malloy",
      name: "B g_big_ren",
      malloy: "run: g_big_ren -> { select: id, region; order_by: id }",
      callers: ACCTS,
   },
   {
      name: "B g_big",
      malloy: "run: g_big -> { select: id, region; order_by: id }",
      callers: ACCTS,
   },
   {
      name: "C g_srctz",
      malloy: "run: g_srctz -> { select: id, tenant; order_by: id }",
      callers: DAYS,
   },
   {
      name: "C g_srctz_mz",
      malloy: "run: g_srctz_mz -> { select: id, tenant; order_by: id }",
      callers: DAYS,
   },
   {
      name: "C2 g_day_mz LA",
      malloy:
         "run: g_day_mz -> { timezone: 'America/Los_Angeles'; select: id, tenant; order_by: id }",
      callers: DAYS,
   },
   {
      name: "C2 g_day noz",
      malloy: "run: g_day -> { select: id, tenant; order_by: id }",
      callers: DAYS,
   },
   {
      name: "C2 g_day LA",
      malloy:
         "run: g_day -> { timezone: 'America/Los_Angeles'; select: id, tenant; order_by: id }",
      callers: DAYS,
   },
   {
      name: "C2 g_day nested tz",
      malloy:
         "run: g_day -> { group_by: id, tenant } -> { timezone: 'America/Los_Angeles'; select: id, tenant; order_by: id }",
      callers: DAYS,
   },
   ...["comp_gate_org", "comp_gate_cat"].flatMap((s) => [
      {
         name: `D ${s} cat`,
         malloy: `run: ${s} -> { group_by: cat; aggregate: n is count(); order_by: cat }`,
         callers: ORGS,
      },
      {
         name: `D ${s} tot`,
         malloy: `run: ${s} -> { aggregate: n is count(), t is amt.sum() }`,
         callers: ORGS,
      },
      {
         name: `D ${s} org`,
         malloy: `run: ${s} -> { group_by: org; aggregate: n is count(); order_by: org }`,
         callers: ORGS,
      },
   ]),
];

function show(o: Outcome): string {
   return JSON.stringify(o, (_k, v) =>
      typeof v === "bigint" ? `${v}n` : (v as unknown),
   );
}

function difference(live: Outcome, served: Outcome): string | undefined {
   if ("error" in live || "error" in served) {
      return "error" in live && "error" in served
         ? undefined
         : `live ${show(live)} against served ${show(served)}`;
   }
   return rowDifference(live.answer.rows, served.answer.rows);
}

for (const alter of [false, true])
   describe(`question-14 review of #1854: H1 to H3, gate columns ${alter ? "altered to DOUBLE" : "as built"}`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome[]>();
      const key = (q: string, c: string) => `${q}\u0000${c}`;

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({
            "m.malloy": MODEL,
            "r.malloy": RMODEL,
            "data/trips.csv": TRIPS,
         });
         for (const q of QUERIES)
            for (const c of Object.keys(q.callers))
               live.set(
                  key(q.malloy, c),
                  await loader.run(
                     q.file ?? "m.malloy",
                     q.malloy,
                     q.callers[c] as never,
                  ),
               );
         fixture = await loader.bind();
         if (alter) {
            for (const entry of Object.values(fixture.stored)) {
               const name = String(entry.sourceName);
               const col = ALTER[name];
               if (!col) continue;
               const table = `"${String(entry.physicalTableName).replace(/"/g, '""')}"`;
               await loader.lake.runSQL(
                  `ALTER TABLE ${table} ALTER COLUMN ${col} TYPE DOUBLE`,
               );
               entry.schema = (
                  entry.schema as { name: string; type: string }[]
               ).map((c) => (c.name === col ? { ...c, type: "DOUBLE" } : c));
            }
            await fixture.rebind();
         }
         for (const q of QUERIES) {
            const cs = Object.keys(q.callers);
            for (const c of [...cs, ...cs.reverse()]) {
               const list = served.get(key(q.malloy, c)) ?? [];
               list.push(
                  await fixture.run(
                     q.file ?? "m.malloy",
                     q.malloy,
                     q.callers[c] as never,
                  ),
               );
               served.set(key(q.malloy, c), list);
            }
         }
      }, 900_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("builds every stored source", () => {
         expect(fixture.buildErrors).toEqual({});
      });

      for (const q of QUERIES) {
         it(`${q.name}: every caller gets live's answer`, () => {
            const lines: string[] = [];
            for (const c of Object.keys(q.callers)) {
               const want = live.get(key(q.malloy, c))!;
               for (const got of served.get(key(q.malloy, c)) ?? []) {
                  const via =
                     "answer" in got
                        ? `${got.answer.servedBy}/${got.answer.via ?? ""}/${got.answer.servedSkip?.reason ?? ""}:${(got.answer.servedSkip?.detail ?? "").replace(/\n/g, " | ").slice(0, 200)}`
                        : "error";
                  const diff = difference(want, got);
                  lines.push(
                     `${c} via=${via} ${diff ? "DIFF " + diff : "ok"} live=${show(want).slice(0, 200)} served=${show(got).slice(0, 300)}`,
                  );
               }
            }
            expect(lines.filter((l) => l.includes("DIFF"))).toEqual([]);
         });
      }
   });

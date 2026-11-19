// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A relative time filter string (`ts ~ f'last 7 days'`) under v2
 * `#(materialize)`, `#(rollup)` and a persisted view, at a pinned clock that
 * then moves. The filter compiles to no `now`, but its SQL reads the clock
 * (DuckDB's `LOCALTIMESTAMP`), so a table built with it would hold the
 * window as of the build. Each route treats it as `now`: a `#(materialize)`
 * `where:` holding one is not built, a dimension over one is computed at read,
 * a rollup over a base that filters with one is not built, and a persisted
 * view that filters with one is not built. Every answer equals live at both
 * clocks. An absolute filter string still builds and serves from its table.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'a', 10.0, TIMESTAMP '2024-12-01 08:00:00'),
      (2, 'a', 20.0, TIMESTAMP '2025-01-10 08:00:00'),
      (3, 'b', 5.0, TIMESTAMP '2025-01-12 08:00:00'),
      (4, 'b', 7.5, TIMESTAMP '2025-01-20 08:00:00'),
      (5, 'a', 2.5, TIMESTAMP '2025-01-22 08:00:00')
    ) AS t(id, g, amt, ts)`,
];

/** The build's clock: the last 7 days keep rows 2 and 3. */
const BUILT_AT = "2025-01-15 00:00:00";
/** Later: the same window keeps rows 4 and 5. */
const LATER = "2025-01-25 00:00:00";

const files = {
   "m.malloy": `source: ev is duckdb.table('ev')

#(materialize) storage="lake"
source: m_last7 is ev extend {
  where: ts ~ f'last 7 days'
  measure: n is count()
}

#(materialize) storage="lake"
source: m_dim is ev extend {
  dimension: this_week is ts ~ f'last 7 days'
  measure: n is count()
}

#(materialize) storage="lake"
source: m_2025 is ev extend {
  where: ts ~ f'2025'
  measure: n is count()
}

source: r_base is ev extend {
  where: ts ~ f'last 7 days'
  #(rollup) grain=["g"]
  measure: total is amt.sum()
}

source: r_2025 is ev extend {
  where: ts ~ f'2025'
  #(rollup) grain=["g"]
  measure: total is amt.sum()
}

source: v is ev extend {
  measure: n is count()
  #@ persist storage="lake"
  view: last7 is { where: ts ~ f'last 7 days'; group_by: g; aggregate: n }
  #@ persist storage="lake"
  view: in2025 is { where: ts ~ f'2025'; group_by: g; aggregate: n }
}
`,
};

describe("a relative filter string under v2 materialization", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files,
         seedSql,
         materialize: true,
         preaggregate: true,
         persistedViews: true,
         clock: BUILT_AT,
      });
   }, 300_000);
   afterAll(async () => {
      await disposeScenarios();
   });

   it("refuses the rollup over a base that filters with one, and builds the absolute one", () => {
      expect(fixture.rollupRefusals).toEqual([
         { reason: "current_time", sourceName: "r_base" },
      ]);
   });

   it("refuses the persisted view that filters with one, and builds the absolute one", () => {
      expect(fixture.viewRefusals.some((r) => r.includes("current time"))).toBe(
         true,
      );
      expect(fixture.viewRefusals.length).toBe(1);
   });

   const cases: [string, string, string, boolean][] = [
      [
         "a #(materialize) where:",
         "m_last7",
         "run: m_last7 -> { group_by: g; aggregate: n; order_by: g }",
         false,
      ],
      [
         "a #(materialize) dimension, computed at read",
         "m_dim",
         "run: m_dim -> { group_by: this_week; aggregate: n; order_by: this_week }",
         true,
      ],
      [
         "a rollup over a base that filters with one",
         "r_base",
         "run: r_base -> { group_by: g; aggregate: total; order_by: g }",
         false,
      ],
      ["a persisted view", "v", "run: v -> last7", false],
   ];

   for (const at of [BUILT_AT, LATER]) {
      for (const [what, src, malloy, stored] of cases) {
         it(`${what} (${src}) equals live at ${at}`, async () => {
            fixture.setClock(at);
            const run = await fixture.run({ name: `${src} at ${at}`, malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (stored) {
               expect(run.servedFrom).toBe("storage");
            } else {
               expect(run.servedFrom).not.toBe("storage");
               expect(run.servedBy?.kind).not.toBe("rollup");
            }
         });
      }
   }

   it("answers the window at each read's clock, not the build's", async () => {
      const ids = async (at: string) => {
         fixture.setClock(at);
         const run = await fixture.run({
            name: `m_last7 ids at ${at}`,
            malloy: "run: m_last7 -> { group_by: id; order_by: id }",
         });
         return run.served.map((r) => Number(r.id));
      };
      expect(await ids(BUILT_AT)).toEqual([2, 3]);
      expect(await ids(LATER)).toEqual([4, 5]);
   });

   it("an absolute filter string builds and serves from its table", async () => {
      fixture.setClock(LATER);
      for (const [src, malloy] of [
         [
            "m_2025",
            "run: m_2025 -> { group_by: g; aggregate: n; order_by: g }",
         ],
         ["v.in2025", "run: v -> in2025"],
      ] as const) {
         const run = await fixture.run({ name: src, malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).toBe("storage");
      }
      const rollup = await fixture.run({
         name: "r_2025",
         malloy:
            "run: r_2025 -> { group_by: g; aggregate: total; order_by: g }",
      });
      expect(rowDifference(rollup.live, rollup.served)).toBeUndefined();
      expect(rollup.servedBy?.kind).toBe("rollup");
   });
});

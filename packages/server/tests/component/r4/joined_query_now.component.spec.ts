// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A join to a query-shaped source whose query reads the current time, under
 * v2 `#(materialize)` and `#(preaggregate)`, at a pinned clock that then
 * moves. Every value read through the join depends on `now`, so a dimension
 * that reads it is computed at read, never stored, and a rollup measure that
 * reads it is not built, nor is a rollup grain that reads it, nor a
 * `#(materialize)` whose `inline=` names the join (its stored columns would
 * hold the join's rows as of the build); each answer equals live at both
 * clocks. Before the shared current-time walk read a joined source's query,
 * the rollup grain and the inlined join stored the build's rows and answered
 * them after the clock moved.
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
      (2, 'a', 20.0, TIMESTAMP '2024-12-20 08:00:00'),
      (3, 'b', 5.0, TIMESTAMP '2025-01-05 08:00:00'),
      (4, 'b', 7.5, TIMESTAMP '2025-01-10 08:00:00'),
      (5, 'a', 2.5, TIMESTAMP '2025-01-14 08:00:00')
    ) AS t(id, g, amt, ts)`,
];

/** The build's clock: a 30-day window keeps rows 2 to 5. */
const BUILT_AT = "2025-01-15 00:00:00";
/** Later: the same window keeps rows 3 to 5. */
const LATER = "2025-01-25 00:00:00";

const BASE = `source: ev is duckdb.table('ev')
source: recent_ids is ev -> { where: ts >= now - 30 days; select: id }
`;

function sumN(rows: Record<string, unknown>[], key: string): number {
   return rows.reduce((n, r) => n + Number(r[key] ?? 0), 0);
}

describe("#(materialize): a join to a query-shaped source that reads now", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `${BASE}
#(materialize) storage="lake"
source: m is ev extend {
  join_one: r is recent_ids on r.id = id
  dimension: in_window is r.id is not null
  measure: n is count()
}

#(materialize) storage="lake" inline=["r"]
source: mi is ev extend {
  join_one: r is recent_ids on r.id = id
  dimension: in_window is r.id is not null
  measure: n is count()
}
`,
         },
         seedSql,
         materialize: true,
         clock: BUILT_AT,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   for (const [src, at, inWindow] of [
      ["m", BUILT_AT, 4],
      ["m", LATER, 3],
      ["mi", BUILT_AT, 4],
      ["mi", LATER, 3],
      ["mi_ids", BUILT_AT, 4],
      ["mi_ids", LATER, 3],
   ] as const) {
      it(`${src}: a value read through the join, read at ${at}, equals live`, async () => {
         fixture.setClock(at);
         const query =
            src === "mi_ids"
               ? "run: mi -> { where: r.id is not null; group_by: in_window; aggregate: n; order_by: in_window }"
               : `run: ${src} -> { group_by: in_window; aggregate: n; order_by: in_window }`;
         const run = await fixture.run({
            name: `${src} at ${at}`,
            malloy: query,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         // `mi` is refused for its inline= and answers live (a query
         // through `m`'s join is answered live as before).
         if (src !== "m") expect(run.servedFrom).not.toBe("storage");
         expect(
            sumN(
               run.live.filter((r) => r.in_window === true),
               "n",
            ),
         ).toBe(inWindow);
      });
   }
});

describe("#(preaggregate): a measure read through a join to a query-shaped source that reads now", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `${BASE}
source: p is ev extend {
  join_one: r is recent_ids on r.id = id
  #(preaggregate) grain=["g"]
  measure: in_window is count(r.id)
  dimension: in_w is r.id is not null
  #(preaggregate) grain=["g"]
  #(preaggregate) grain=["in_w"]
  measure: total is amt.sum()
}
`,
         },
         seedSql,
         preaggregate: true,
         clock: BUILT_AT,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   for (const [at, inWindow] of [
      [BUILT_AT, 4],
      [LATER, 3],
   ] as const) {
      it(`the join-reading measure is not built and answers live at ${at}`, async () => {
         fixture.setClock(at);
         const run = await fixture.run({
            name: `p at ${at}`,
            malloy:
               "run: p -> { group_by: g; aggregate: in_window; order_by: g }",
         });
         expect(run.servedBy?.kind).not.toBe("rollup");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(sumN(run.live, "in_window")).toBe(inWindow);
      });
   }

   for (const at of [BUILT_AT, LATER]) {
      it(`a grain read through the join answers live at ${at}`, async () => {
         fixture.setClock(at);
         const run = await fixture.run({
            name: `p by in_w at ${at}`,
            malloy:
               "run: p -> { group_by: in_w; aggregate: total; order_by: in_w }",
         });
         expect(run.servedBy?.kind).not.toBe("rollup");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("its neighbour, which reads no time, is still served from its rollup", async () => {
      fixture.setClock(LATER);
      const run = await fixture.run({
         name: "total",
         malloy: "run: p -> { group_by: g; aggregate: total; order_by: g }",
      });
      expect(run.servedBy?.kind).toBe("rollup");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

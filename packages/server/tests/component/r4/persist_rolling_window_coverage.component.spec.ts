// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a rolling window under a v1 `#@ persist storage=`, at a pinned clock
 * whose window cuts the rows in two. A window in the extend block is
 * read-time: re-applied on every read. It is read at the build's clock, equal
 * to live and served, and again after the clock moves, still equal to live,
 * since its term is re-applied over the stored rows.
 *
 * A window in the persisted query would be baked into the build, so a package
 * refuses that source at plan (`current_time`) and answers it live; this
 * harness builds every persist source without the plan's gates, so that shape
 * is covered through the real package path instead
 * (v1_persist_current_time.component.spec.ts, and the conformance shape
 * "persist with a rolling window").
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
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

const MODEL = `##! experimental { persistence }
#@ persist name="read_t" storage="lake"
source: read_time is duckdb.table('ev') -> {
  select: id, g, amt, ts
} extend {
  where: ts >= now - 30 days
  measure: total is amt.sum()
}
`;

describe("R4: a rolling window under v1 #@ persist storage=, at a pinned clock", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql,
         clock: BUILT_AT,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("read_time: at the build's clock, served and equal to live", async () => {
      fixture.setClock(BUILT_AT);
      const run = await fixture.run({
         name: "read_time",
         malloy:
            "run: read_time -> { group_by: g; aggregate: total, n is count(); order_by: g }",
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(strictRowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe("served_copy");
      // The window cut the rows: four of five.
      expect(run.live.reduce((n, r) => n + Number(r.n), 0)).toBe(4);
   });

   it("read_time: after the clock moves, the window is re-applied, served and equal to live", async () => {
      fixture.setClock(LATER);
      const run = await fixture.run({
         name: "read_time later",
         malloy:
            "run: read_time -> { group_by: g; aggregate: total, n is count(); order_by: g }",
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe("served_copy");
      expect(run.live.reduce((n, r) => n + Number(r.n), 0)).toBe(3);
   });
});

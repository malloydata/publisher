// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a rolling window under a v1 `#@ persist storage=`, at a pinned clock
 * whose window cuts the rows in two. A window in the persisted query is baked
 * into the build: the table holds the rows of the build's clock, and so does
 * live at that clock. A window in the extend block is read-time: re-applied
 * on every read. Each is read at the build's clock, equal to live and served;
 * the extend-block window is read again after the clock moves, still equal to
 * live, since its term is re-applied over the stored rows.
 *
 * Known gap: the body window read after the clock moves is still served, and
 * answers the build's rows rather than live's (skipped with its reason unless
 * CONFORMANCE_GAPS=run). v2's rolling window re-applies its term at read.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const RUN_GAPS = process.env.CONFORMANCE_GAPS === "run";

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
#@ persist name="body_t" storage="lake"
source: body is duckdb.table('ev') -> {
  where: ts >= now - 30 days
  select: id, g, amt, ts
} extend { measure: total is amt.sum() }

#@ persist name="read_t" storage="lake"
source: read_time is duckdb.table('ev') -> {
  select: id, g, amt, ts
} extend {
  where: ts >= now - 30 days
  measure: total is amt.sum()
}
`;

describe("R4: a rolling window under v1 #@ persist storage=, at a pinned clock", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql,
         clock: BUILT_AT,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const source of ["body", "read_time"]) {
      it(`${source}: at the build's clock, served and equal to live`, async () => {
         fixture.setClock(BUILT_AT);
         const run = await fixture.run({
            name: source,
            malloy: `run: ${source} -> { group_by: g; aggregate: total, n is count(); order_by: g }`,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(strictRowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBe("served_copy");
         // The window cut the rows: four of five.
         expect(run.live.reduce((n, r) => n + Number(r.n), 0)).toBe(4);
      });
   }

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

   const bodyLater = async () => {
      fixture.setClock(LATER);
      const run = await fixture.run({
         name: "body later",
         malloy:
            "run: body -> { group_by: g; aggregate: total, n is count(); order_by: g }",
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   };
   const bodyLaterTitle = "body: after the clock moves, equal to live";
   if (RUN_GAPS) {
      it(bodyLaterTitle, bodyLater);
   } else {
      it.skip(
         `${bodyLaterTitle} [gap wrong: a window in the persisted query is baked into the table at build, and the served copy still answers from it after the clock moves: 4 rows where live keeps 3]`,
         bodyLater,
      );
   }
});

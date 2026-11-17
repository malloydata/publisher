// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Under the v1 cutover's default, a query on a colocated
 * persisted source through its join to another persisted source is answered
 * by the served copy from both tables, and the path report must name the
 * queried source's own table. It named the first table the served copy read,
 * in name order, so a fact queried through its `customer_dim` join was
 * reported as served from `customer_dim`'s table, and the fact's own table
 * appeared nowhere (`off` named the fact's table).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
} from "../../../src/test_helpers/conformance/package_runner";
import { renderFiles, SHAPES } from "./conformance/shapes";

const SHAPE = "joined persisted source from another file";
const FACTS_ALONE = "run: order_facts -> { aggregate: t is amount.sum() }";
const DIMS_ALONE = "run: c_only -> { aggregate: n is count() }";
const THROUGH_JOIN =
   "run: order_facts -> { group_by: c.segment; aggregate: t is amount.sum() }";

describe("the path report names the queried source's table through a join", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   let dispose: () => Promise<void>;

   beforeAll(async () => {
      // The default: the v1 cutover on, PUBLISHER_MATERIALIZE unset.
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      await freshLoadPool();
      const shape = SHAPES.find((s) => s.name === SHAPE)!;
      const files = renderFiles(shape, "colocated");
      // The joined source on the surface too, to read its table's id.
      files["index.malloy"] = `import "facts.malloy"
import "dims.malloy"
source: c_only is customer_dim
export { order_facts, c_only }
`;
      const loader = await loadFixture({ ...DATA_FILES, ...files });
      dispose = loader.dispose;
      fixture = await loader.bind();
   }, 120_000);

   afterAll(async () => {
      await dispose?.();
      restore?.();
      await freshLoadPool();
   });

   async function servedBy(query: string) {
      const model = fixture.pkg.getModel("index.malloy")!;
      const result = await model.runQuery(unrecordedAudit(), { query });
      return result.path?.servedBy as
         | { kind: string; tableId?: string; tier?: string; via?: string }
         | undefined;
   }

   it("names the fact's own table, not the joined dimension's", async () => {
      const facts = await servedBy(FACTS_ALONE);
      const dims = await servedBy(DIMS_ALONE);
      expect(facts?.kind).toBe("materialized");
      expect(dims?.kind).toBe("materialized");
      expect(facts?.tableId).toBeDefined();
      expect(dims?.tableId).toBeDefined();
      expect(facts?.tableId).not.toBe(dims?.tableId);

      const joined = await servedBy(THROUGH_JOIN);
      expect(joined).toEqual({
         kind: "materialized",
         tier: "database",
         tableId: facts!.tableId,
         via: "served_copy",
      });
   });
});

/**
 * P2-32 (F-11): a query on a source whose join reaches a source with two
 * rollups declines both, each for its own reason, and the path report gives
 * each rollup that reason. It stamped the first rollup's reason on both: the
 * `[start_day]` grain's `GRAIN_TOO_COARSE` on the `[start_station_id]`
 * rollup, whose own reason is the join's key (`COUNT_SEMANTICS`).
 */
describe("P2-32: each rollup read through a join carries its own reason", () => {
   const join = (grains: string[]) => `##! experimental.persistence
source: trips is duckdb.sql("""
  SELECT * FROM (VALUES (DATE '2020-01-01', 1), (DATE '2020-01-01', 2), (DATE '2020-01-02', 2), (DATE '2020-01-03', 3)) AS t(start_day, start_station_id)
""") extend {
${grains.map((g) => `  #(preaggregate) grain=["${g}"]`).join("\n")}
  measure: trip_count is count()
}
source: stations_j is duckdb.sql("""
  SELECT * FROM (VALUES (1, 1, 10), (2, 2, 10), (3, 3, 20)) AS t(station_id, station_key, region_id)
""") extend {
  primary_key: station_id
  join_many: t is trips on station_key = t.start_station_id
}
`;
   const QUERY =
      "run: stations_j -> { group_by: region_id; aggregate: t.trip_count; order_by: region_id }";

   for (const grains of [
      ["start_day", "start_station_id"],
      ["start_station_id", "start_day"],
   ]) {
      it(`declared ${grains.join(", then ")}`, async () => {
         const restore = installSettings({ env: {} }).restore;
         await freshLoadPool();
         const loader = await loadFixture({ "join.malloy": join(grains) });
         try {
            const live = await loader.run("join.malloy", QUERY);
            const bound = await loader.bind({ includeRollups: true });
            const model = bound.pkg.getModel("join.malloy")!;
            const result = await model.runQuery(unrecordedAudit(), {
               query: QUERY,
            });
            if ("error" in live) throw new Error(live.error);
            expect(result.compactResult).toEqual(live.answer.rows as never);
            expect(result.path?.servedBy.kind).toBe("live");
            // Each rollup by the grain its table's name carries.
            const grainOf = (tableId: string | undefined) => {
               const table = bound.tables[tableId ?? ""] ?? "";
               return grains.find((g) => table.includes(`__rollup__${g}__`));
            };
            const reasons = Object.fromEntries(
               (result.path?.considered ?? [])
                  .filter((c) => c.kind === "rollup")
                  .map((c) => [grainOf(c.tableId), c.reason]),
            );
            expect(reasons).toEqual({
               start_day: "GRAIN_TOO_COARSE",
               start_station_id: "COUNT_SEMANTICS",
            });
         } finally {
            await loader.dispose();
            restore();
            await freshLoadPool();
         }
      }, 120_000);
   }
});

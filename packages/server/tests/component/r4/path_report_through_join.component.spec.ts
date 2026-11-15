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
} from "./conformance/package_harness";
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
         | { kind: string; tableId?: string; via?: string }
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
         tableId: facts!.tableId,
         via: "served_copy",
      });
   });
});

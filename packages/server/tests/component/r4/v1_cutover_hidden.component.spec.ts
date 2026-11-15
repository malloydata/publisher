// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's G2 (materialization v2, phase 3 pull request 6): a v1
 * colocated `#@ persist` table holds its source's hidden columns (Malloy built
 * it from the source's own query), so the served copy binds them with their
 * access modifiers written again (P3). A measure over an `internal` column is
 * served from the table, and the column stays exactly as hidden as live: a
 * caller can neither group by it nor read a `private` measure.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "./conformance/package_harness";
import { renderFiles, SHAPES } from "./conformance/shapes";
import { rowDifference } from "./harness";

const SHAPE = SHAPES.find(
   (s) =>
      s.name === "joined persisted source with hidden fields, from an index",
)!;

const REFUSED = [
   [
      "an internal column grouped by",
      "run: customer_dim -> { group_by: home_region }",
   ],
   ["a private measure", "run: customer_dim -> { aggregate: west_rows }"],
   [
      "an internal column through the join",
      "run: order_facts -> { group_by: c.home_region }",
   ],
];

const modelOf = (q: string) =>
   q.includes("order_facts") ? "index.malloy" : "dims.malloy";

describe("v1 cutover: a colocated table's hidden columns (G2)", () => {
   let restore: () => void = () => {};
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();
   let dispose: () => Promise<void>;

   beforeAll(async () => {
      await freshLoadPool();
      restore = withSwitch("cutover");
      const loader = await loadFixture({
         ...DATA_FILES,
         ...renderFiles(SHAPE, "colocated"),
      });
      dispose = loader.dispose;
      // Live: before any table is bound.
      for (const [, q] of REFUSED) live.set(q, await loader.run(modelOf(q), q));
      fixture = await loader.bind();
   });
   afterAll(async () => {
      restore();
      await dispose?.();
      await freshLoadPool();
   });

   for (const [name, q] of REFUSED) {
      it(`refuses ${name}, as live does`, async () => {
         expect("error" in live.get(q)!).toBe(true);
         const out = await fixture.run(modelOf(q), q);
         expect("error" in out).toBe(true);
      });
   }

   it("serves a public measure over the internal column from the table, equal to live", async () => {
      const q = "run: customer_dim -> { group_by: segment; aggregate: west_n }";
      const served = await fixture.run("dims.malloy", q);
      if ("error" in served) throw new Error(served.error);
      expect(served.answer.servedBy).toBe("materialized");
      expect(served.answer.via).toBe("served_copy");
      // Live, with the switch on: the cutover never reads Malloy's manifest.
      restore();
      restore = withSwitch("off");
      await fixture.rebind();
      const today = await fixture.run("dims.malloy", q);
      restore();
      restore = withSwitch("cutover");
      await fixture.rebind();
      if ("error" in today) throw new Error(today.error);
      expect(
         rowDifference(today.answer.rows, served.answer.rows),
      ).toBeUndefined();
   });
});

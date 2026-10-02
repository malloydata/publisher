// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The join topology: for each source, every source it reaches by joins, with
 * the join names that get there. Built from a package the real compiler
 * produced (see get_context_join_fixture), because the thing to prove is that
 * the compiled join entry names its target source and alias.
 */

import { describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   JOIN_FIXTURE_MODEL_PATH,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import {
   getPackageIndex,
   JOIN_TOPOLOGY_MAX_DEPTH,
   sourceContextKey,
} from "./get_context_tool";

async function indexOfFixture() {
   const { pkg } = await compileJoinFixture();
   return getPackageIndex(storeServing(pkg), "env", "pkg");
}

/** "path -> target" strings for one root, sorted, so a test reads as a table. */
function reachesOf(
   topology: Awaited<ReturnType<typeof indexOfFixture>>["topology"],
   root: string,
): string[] {
   return (topology.get(sourceContextKey(JOIN_FIXTURE_MODEL_PATH, root)) ?? [])
      .map((r) => `${r.path.join(".")} -> ${r.targetSource} (${r.fanout})`)
      .sort();
}

describe("join topology", () => {
   it("records every join path with its alias and target source", async () => {
      const { topology } = await indexOfFixture();
      // ord joins cust twice (buyer, seller), and reg once; reg joins cust
      // (many), and cust joins country. Depth 3 is region_r.c.origin.
      expect(reachesOf(topology, "ord")).toEqual([
         "buyer -> cust (one)",
         "buyer.origin -> country (one)",
         "region_r -> reg (one)",
         "region_r.c -> cust (many)",
         "region_r.c.origin -> country (many)",
         "seller -> cust (one)",
         "seller.origin -> country (one)",
      ]);
   });

   it("keeps role-played joins of one source apart by alias", async () => {
      const { topology } = await indexOfFixture();
      const buyers = (
         topology.get(sourceContextKey(JOIN_FIXTURE_MODEL_PATH, "ord")) ?? []
      ).filter((r) => r.targetSource === "cust");
      expect(buyers.map((r) => r.path.join("."))).toEqual([
         "buyer",
         "seller",
         "region_r.c",
      ]);
   });

   it("lets two roots reach the same source", async () => {
      const { topology } = await indexOfFixture();
      expect(reachesOf(topology, "inv")).toEqual([
         "customer -> cust (one)",
         "customer.origin -> country (one)",
      ]);
      expect(reachesOf(topology, "reg")).toEqual([
         "c -> cust (many)",
         "c.origin -> country (many)",
      ]);
   });

   it("has no entry for a source that joins nothing", async () => {
      const { topology } = await indexOfFixture();
      expect(reachesOf(topology, "country")).toEqual([]);
   });

   it("reaches deeper than the index copies do", async () => {
      const index = await indexOfFixture();
      const depthOf = (path: string) => path.split(".").length;
      const deepest = Math.max(
         ...[...index.topology.values()].flatMap((rs) =>
            rs.map((r) => r.path.length),
         ),
      );
      expect(deepest).toBe(3);
      expect(deepest).toBeLessThanOrEqual(JOIN_TOPOLOGY_MAX_DEPTH);
      // The index stops at two joins; the topology does not.
      const indexedDepth = Math.max(
         ...index.retrievalEntities
            .filter((e) => e.joinPath)
            .map((e) => depthOf(e.joinPath as string)),
      );
      expect(indexedDepth).toBe(2);
      expect(
         index.retrievalEntities.some((e) =>
            e.name.startsWith("region_r.c.origin."),
         ),
      ).toBe(false);
   });

   it("leaves out joined copies from the direct entities", async () => {
      const index = await indexOfFixture();
      expect(index.directEntities.some((e) => e.joinPath)).toBe(false);
      expect(index.retrievalEntities.length).toBeGreaterThan(
         index.directEntities.length,
      );
      // Same entities, same order, minus the copies.
      expect(index.directEntities).toEqual(
         index.retrievalEntities.filter((e) => !e.joinPath),
      );
   });

   it("is empty when the model has no compiled IR to read targets from", async () => {
      const { pkg } = await compileJoinFixture();
      const model = (
         pkg as { getModel: (p: string) => Record<string, unknown> }
      ).getModel(JOIN_FIXTURE_MODEL_PATH);
      delete model.getModelDef;
      const { topology } = await getPackageIndex(
         storeServing(pkg),
         "env",
         "no-ir",
      );
      expect(topology.size).toBe(0);
   });
});

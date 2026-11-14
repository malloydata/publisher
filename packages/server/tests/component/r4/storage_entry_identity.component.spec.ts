// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` manifest entry serves a source only when it was built from that
 * source's definition. An unversioned package keeps its name when its author
 * edits a `#@ persist` source, and its manifest (the local store's latest run,
 * restored at startup, or a host's) still holds the table the old definition
 * built, under the same `sourceName` and the old content address. Bound by
 * name alone, the edited source answers from the old definition's rows: a
 * wrong answer that compiles, since the columns did not change.
 *
 * Two packages, the second an edit of the first: the first is built, and its
 * entries (and lake) are handed to the second, as a restart after the edit
 * hands them. The second must answer live.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { MalloyConfig, FixedConnectionMap } from "@malloydata/malloy";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   LAKE,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";
import { rowDifference } from "./harness";

const model = (status: string) => `import "raw.malloy"

#@ persist name="orders_by_status" storage="lake"
source: orders is orders_raw -> {
  where: status = '${status}'
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
  measure: n is count()
}
`;

const files = (status: string) => ({
   ...DATA_FILES,
   "raw.malloy": RAW_MODEL,
   "m.malloy": model(status),
});

const QUERY =
   "run: orders -> { group_by: status; aggregate: total, n; order_by: status }";

describe("a storage= entry built from another definition of a same-named source", () => {
   const disposers: (() => Promise<void>)[] = [];
   let restore: (() => void) | undefined;
   afterEach(async () => {
      restore?.();
      for (const d of disposers.splice(0)) await d();
      await freshLoadPool();
   });

   it("is not served to the edited source, which answers live", async () => {
      restore = withSwitch("cutover");
      await freshLoadPool();

      const before = await loadFixture(files("shipped"));
      disposers.push(before.dispose);
      const built = await before.bind();
      const entries = Object.values(built.stored);
      expect(entries).toHaveLength(1);
      // The old definition's table serves the old definition.
      const old = await before.run("m.malloy", QUERY);
      if ("error" in old) throw new Error(old.error);
      expect(old.answer.servedBy).toBe("materialized");

      const after = await loadFixture(files("placed"));
      disposers.push(after.dispose);
      const live = await after.run("m.malloy", QUERY);
      if ("error" in live) throw new Error(live.error);
      expect(live.answer.rows.length).toBeGreaterThan(0);

      // The edited package reads the first one's lake, as its own destination
      // of the same name, and is handed the first one's manifest.
      const lake = new MalloyConfig({ connections: {} });
      lake.wrapConnections(
         () => new FixedConnectionMap(new Map([[LAKE, before.lake]]), LAKE),
      );
      after.pkg.setServeDestinationConfig(() => lake);
      after.pkg.bindColocatedServeManifest({});
      after.pkg.bindStorageServeBindings(built.stored as never);
      await after.pkg.getModel("m.malloy")?.prepareServedCopy();

      // Same name, another address.
      const plan = Object.values(
         (
            after.pkg as unknown as {
               materialization: {
                  buildPlan?: {
                     sources: Record<
                        string,
                        { name?: string; sourceEntityId?: string }
                     >;
                  };
               };
            }
         ).materialization.buildPlan?.sources ?? {},
      ).find((s) => s.name === "orders");
      expect(plan?.sourceEntityId).toBeDefined();
      expect(plan?.sourceEntityId).not.toBe(entries[0].sourceEntityId);

      const out = await after.run("m.malloy", QUERY);
      if ("error" in out) throw new Error(out.error);
      expect(rowDifference(live.answer.rows, out.answer.rows)).toBeUndefined();
      expect(out.answer.servedBy).toBe("live");
   }, 300_000);
});

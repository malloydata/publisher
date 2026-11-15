// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A v1 `#@ persist` source declared by refinement alone (`high_value is raw
 * extend { where: … }`) stores its root's rows. Under the v1 cutover the
 * served copy re-roots it on its own table, its own `extend` run kept, only
 * when its base chain carries no filter and no gate: then nothing the base
 * applies is lost, and the answer equals live. A base with a `where:` of its
 * own keeps the source as written, so it answers live.
 */
import { afterAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES } from "./conformance/data";
import { freshLoadPool, loadFixture } from "./conformance/package_harness";
import { rowDifference } from "./harness";

const QUERY =
   "run: high_value -> { aggregate: total is amount.sum(), n is count() }";

async function servedAndLive(raw: string) {
   const restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
   await freshLoadPool();
   const loader = await loadFixture({
      ...DATA_FILES,
      "m.malloy": `##! experimental.persistence
source: raw is ${raw}
#@ persist name="fsc_high_value"
source: high_value is raw extend {
  where: amount >= 100
}
`,
   });
   const live = await loader.run("m.malloy", QUERY);
   const fixture = await loader.bind();
   try {
      const served = await fixture.run("m.malloy", QUERY);
      if ("error" in live) throw new Error(live.error);
      if ("error" in served) throw new Error(served.error);
      return { live: live.answer, served: served.answer };
   } finally {
      await fixture.dispose();
      restore();
   }
}

describe("a v1 persisted source declared by refinement", () => {
   afterAll(async () => {
      await freshLoadPool();
   });

   it("is answered from its own table when its base applies nothing", async () => {
      const { live, served } = await servedAndLive(
         `duckdb.sql("SELECT id, amount FROM read_csv_auto('data/orders.csv')")`,
      );
      expect(served.servedBy).toBe("materialized");
      expect(served.via).toBe("served_copy");
      expect(rowDifference(live.rows, served.rows)).toBeUndefined();
   }, 120_000);

   it("stays live when its base filters rows of its own", async () => {
      const { live, served } = await servedAndLive(
         `duckdb.sql("SELECT id, amount FROM read_csv_auto('data/orders.csv')") extend { where: id > 3 }`,
      );
      expect(served.servedBy ?? "live").toBe("live");
      expect(rowDifference(live.rows, served.rows)).toBeUndefined();
   }, 120_000);
});

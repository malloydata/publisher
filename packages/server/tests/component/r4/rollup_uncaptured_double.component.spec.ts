// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database rollup in a DuckLake origin whose build captured no native
 * types (a catalog read that failed), its BIGINT sum past 2^53 held in
 * Parquet as a DOUBLE (F-13). The bind's column read marks the column
 * `storedAsDouble`; with no capture to compare against, that mark was
 * ignored and the DOUBLE's rounded digits were served as exact. Found by
 * strict-origin mode (conformance/strict_origin.ts). Served must equal live,
 * compared as strings.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence }
source: typed is duckdb.sql("""
  SELECT * FROM (VALUES
    (CAST('9007199254740993' AS BIGINT), 'a'),
    (CAST('9007199254740991' AS BIGINT), 'a'),
    (CAST('-9007199254740993' AS BIGINT), 'b')
  ) AS t(big, g)
""") extend {
  #(rollup) grain=["g"]
  measure: big_sum is big.sum()
}
`;

const QUERY = "run: typed -> { group_by: g; aggregate: big_sum; order_by: g }";

function asStrings(o: Outcome): string {
   if ("error" in o) return `error: ${o.error}`;
   return JSON.stringify(o.answer.rows, (_k, v) =>
      typeof v === "bigint" ? v.toString() : (v as unknown),
   );
}

describe("a DuckLake rollup whose build captured no native types", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   let live: Outcome;

   beforeAll(async () => {
      restore = withSwitch("on", { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" });
      await freshLoadPool();
      const loader = await loadFixture(
         { "m.malloy": MODEL },
         { lake: "parquet" },
      );
      live = await loader.run("m.malloy", QUERY);
      fixture = await loader.bind({
         includeRollups: true,
         captureNoNativeSchema: true,
      });
   });

   afterAll(async () => {
      await fixture?.dispose();
      restore?.();
      await freshLoadPool();
   });

   it("answers the BIGINT sum with live's digits, never the DOUBLE's", async () => {
      expect(asStrings(live)).toContain("-9007199254740993");
      const served = await fixture.run("m.malloy", QUERY);
      expect(asStrings(served)).toBe(asStrings(live));
   });
});

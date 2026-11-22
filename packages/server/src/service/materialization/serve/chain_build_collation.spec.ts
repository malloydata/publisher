// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `chainBuildCollationRefusal` over a DuckDB source: a chained `storage=`
 * build computed over its stored upstream compares strings byte-exact, so it
 * may stack only where what it groups, compares or orders of a string its
 * source compares byte-exact too. A column under `NOCASE` does not; a build
 * that compares no string, or one over a byte-exact column, may stack. The
 * Postgres cases run end to end in
 * tests/component/r4/stacked_storage_collation.component.spec.ts.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import { chainBuildCollationRefusal } from "./gate_collation";

const rows = (collate: string) => `duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, v${collate} AS v
  FROM (VALUES (1, 'apple'), (2, 'Apple'), (3, 'banana')) AS t(id, v)
""")`;

const MODEL = `
source: nocase is ${rows(" COLLATE NOCASE")} -> { select: id, v }
source: nocase_group is nocase -> { group_by: v; aggregate: n is count() }
source: nocase_where is nocase -> { where: v = 'apple'; select: id }
source: nocase_number is nocase -> { group_by: odd is id % 2; aggregate: n is count() }
source: bytes is ${rows("")} -> { select: id, v } extend {
  measure: lo is min(v)
}
source: bytes_group is bytes -> { group_by: v; aggregate: n is count() }
`;

describe("chainBuildCollationRefusal over a DuckDB source", () => {
   const connection = new DuckDBConnection("duckdb");
   const runtime = new SingleConnectionRuntime({ connection } as never);
   const model = runtime.loadModel(MODEL);
   const load = (query: string) => model.loadQuery(query);
   /** The upstreams stored, by sourceID, as a run names them. */
   let stored: Set<string> | undefined;
   const storedIDs = async () => {
      if (stored) return stored;
      const m = await model.getModel();
      const contents = (
         m as unknown as {
            _modelDef: { contents: Record<string, { sourceID?: string }> };
         }
      )._modelDef.contents;
      stored = new Set(["nocase", "bytes"].map((n) => contents[n].sourceID!));
      return stored;
   };
   const refusal = async (
      downstream: string,
      rollup?: { baseName: string; grain: string[]; measures: string[] },
   ) =>
      chainBuildCollationRefusal({
         load,
         downstream,
         ...(rollup ? { rollup } : {}),
         stored: await storedIDs(),
      });

   afterAll(async () => {
      await connection.close();
   });

   it("refuses a build that groups or compares a NOCASE string", async () => {
      expect(await refusal("nocase_group")).toBeString();
      expect(await refusal("nocase_where")).toBeString();
   });

   it("stacks a build over a NOCASE source that compares no string", async () => {
      expect(await refusal("nocase_number")).toBeUndefined();
   });

   it("stacks a build that groups a byte-exact string", async () => {
      expect(await refusal("bytes_group")).toBeUndefined();
   });

   it("judges a rollup by its grain and its measures", async () => {
      expect(
         await refusal("nocase_rollup", {
            baseName: "nocase",
            grain: ["v"],
            measures: [],
         }),
      ).toBeString();
      expect(
         await refusal("bytes_rollup", {
            baseName: "bytes",
            grain: ["v"],
            measures: ["lo"],
         }),
      ).toBeUndefined();
   });

   it("refuses a downstream it cannot find", async () => {
      expect(await refusal("no_such_source")).toBeString();
   });
});

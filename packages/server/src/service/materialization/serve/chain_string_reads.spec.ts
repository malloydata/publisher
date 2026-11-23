// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `chainBuildStringReads` over a DuckDB source, judged against the stored
 * upstreams' contracts (`stringReadsRefusal`): a chained `storage=` build
 * computed over its stored upstream compares strings byte-exact, so it may
 * stack only where each string it groups, compares or orders is recorded as
 * compared there as live compares it. The upstream over a `NOCASE` column
 * records it `none`; a build that compares no string, or one over a
 * byte-exact column (`exact`), may stack. The Postgres cases run end to end
 * in tests/component/r4/stacked_storage_collation.component.spec.ts.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import type { ColumnContract } from "../domain/column_contract";
import { stringReadsRefusal } from "../domain/string_reads";
import { chainBuildStringReads } from "./chain_string_reads";

/** An upstream's contract, `v` compared as `compare`. */
const contract = (compare: "exact" | "equality" | "none"): ColumnContract => ({
   v: 1,
   dialect: "duckdb",
   columns: [
      {
         name: "id",
         nativeType: "BIGINT",
         collation: null,
         compare: "exact",
         gateExact: true,
      },
      {
         name: "v",
         nativeType: "VARCHAR",
         collation: "binary",
         compare,
         gateExact: true,
      },
   ],
});

const rows = (collate: string) => `duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, v${collate} AS v
  FROM (VALUES (1, 'apple'), (2, 'Apple'), (3, 'banana')) AS t(id, v)
""")`;

const MODEL = `
source: nocase is ${rows(" COLLATE NOCASE")} -> { select: id, v }
source: nocase_group is nocase -> { group_by: v; aggregate: n is count() }
source: nocase_where is nocase -> { where: v = 'apple'; select: id }
source: nocase_mf is nocase -> { aggregate: n is count() { where: v = 'apple' } }
source: nocase_only is nocase extend { where: v = 'apple' }
source: nocase_base_where is nocase_only -> { aggregate: n is count() }
source: nocase_number is nocase -> { group_by: odd is id % 2; aggregate: n is count() }
source: bytes is ${rows("")} -> { select: id, v } extend {
  measure: lo is min(v)
}
source: bytes_group is bytes -> { group_by: v; aggregate: n is count() }
`;

describe("chainBuildStringReads over a DuckDB source", () => {
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
   ) => {
      const stored = [...(await storedIDs())];
      const reads = await chainBuildStringReads(
         load,
         rollup ? { rollup, stored } : { downstream, stored },
      );
      // The upstream over a NOCASE column records it compared unlike live.
      const base = rollup?.baseName ?? downstream;
      return stringReadsRefusal(reads, [
         contract(base.startsWith("nocase") ? "none" : "exact"),
      ]);
   };

   afterAll(async () => {
      await connection.close();
   });

   it("refuses a build that groups or compares a NOCASE string", async () => {
      expect(await refusal("nocase_group")).toBeString();
      expect(await refusal("nocase_where")).toBeString();
      // A filtered measure's filter, and its base's own `where:`.
      expect(await refusal("nocase_mf")).toBeString();
      expect(await refusal("nocase_base_where")).toBeString();
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

   it("refuses a build whose ordered string is recorded equality only", async () => {
      const stored = [...(await storedIDs())];
      const reads = await chainBuildStringReads(load, {
         rollup: { baseName: "bytes", grain: [], measures: ["lo"] },
         stored,
      });
      expect(stringReadsRefusal(reads, [contract("equality")])).toBeString();
      expect(stringReadsRefusal(reads, [contract("exact")])).toBeUndefined();
   });

   it("refuses over an upstream with no contract", async () => {
      const stored = [...(await storedIDs())];
      const reads = await chainBuildStringReads(load, {
         downstream: "bytes_group",
         stored,
      });
      expect(stringReadsRefusal(reads, [undefined])).toBeString();
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { Binding } from "../domain/binding";
import { nativeSchemaNotices } from "./native_schema_notices";

/**
 * The `stored-native-schema-missing` notice names the stored column whose
 * big-integer read lacks its captured type. A v1 table stores its query's
 * output names, so when the source's renames swap two names the notice must
 * name the column that holds the BIGINT sum (`total`), not the one the sum's
 * field is now called (`n`, which holds the count).
 */
describe("nativeSchemaNotices for a swapped v1 database-tier big integer", () => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   let modelDef: unknown;
   beforeAll(async () => {
      await duckdb.runSQL(
         "CREATE TABLE raw AS SELECT 'a' AS sub, 1::BIGINT AS v",
      );
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([
               [
                  "file:///m.malloy",
                  `##! experimental.persistence
source: raw is duckdb.table('raw')
#@ persist name="sw_c"
source: sw is raw -> { group_by: sub; aggregate: total is v.sum(), n is count() } extend {
  rename: tmp is total
  rename: total is n
  rename: n is tmp
}
`,
               ],
            ]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      const model = await runtime
         .loadModel(new URL("file:///m.malloy"))
         .getModel();
      modelDef = (model as unknown as { _modelDef: unknown })._modelDef;
   });
   afterAll(async () => {
      await duckdb.close();
   });

   it("names the stored sum column, not the count's", () => {
      // The table as a v1 build stores it: the query's output names, with
      // no warehouse type captured (an entry from before the capture).
      const binding: Binding = {
         kind: "persist",
         tier: "database",
         sourceName: "sw",
         destinationName: "duckdb",
         virtualHandle: "addr_sw",
         tablePath: "sw_c",
         schema: [
            { name: "sub", type: "VARCHAR" },
            { name: "total", type: "HUGEINT" },
            { name: "n", type: "BIGINT" },
         ],
      };
      const notices = nativeSchemaNotices([binding], modelDef, {});
      expect(notices).toHaveLength(1);
      expect(notices[0]).toMatchObject({
         sourceName: "sw",
         leftOut: ["total"],
      });
   });
});

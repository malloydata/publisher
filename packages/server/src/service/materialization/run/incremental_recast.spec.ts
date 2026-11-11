// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { plannedSources } from "../plan/build_plan";
import { columnsARebuildWouldRecast } from "./incremental_build";

// ms2data/service#6642: a delta INSERTs into the stored table's columns, so a
// table an older build typed is only corrected by a re-seed's CTAS. The
// refresh asks for one when a rebuild would cast a stored column.
describe("columnsARebuildWouldRecast", () => {
   const duckdb = new DuckDBConnection("duckdb");
   let source: Awaited<ReturnType<typeof compile>>;

   async function compile(model: string) {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([["file:///m.malloy", model]]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      const compiled = await runtime
         .loadModel(new URL("file:///m.malloy"))
         .getModel();
      const [only] = plannedSources(await runtime.getBuildTargets(compiled));
      return only;
   }

   beforeAll(async () => {
      await duckdb.runSQL(
         "CREATE TABLE ev AS SELECT 1 AS id, DATE '2026-01-31' AS d, 1 AS flag_int",
      );
      source = await compile(`##! experimental { persistence sql_functions }
#@ persist name="t" storage="lake"
source: t is duckdb.table('ev') -> {
  select: id, d_next is d + 1 month, flag is sql_boolean("\${flag_int} = 1")
}
`);
   });

   afterAll(async () => {
      await duckdb.close();
   });

   it("names the columns an older build stored as another type", async () => {
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE old_t AS SELECT 1 AS id, TIMESTAMP '2026-02-28 00:00:00' AS d_next, 1 AS flag",
      );
      const recast = await columnsARebuildWouldRecast(duckdb, "old_t", source);
      expect(recast.map((m) => [m.column, m.storedType])).toEqual([
         ["d_next", "TIMESTAMP"],
         ["flag", "INTEGER"],
      ]);
   });

   it("names none for a table that stores the declared types", async () => {
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE new_t AS SELECT 1 AS id, DATE '2026-02-28' AS d_next, true AS flag",
      );
      expect(await columnsARebuildWouldRecast(duckdb, "new_t", source)).toEqual(
         [],
      );
   });

   it("names none when the table cannot be described", async () => {
      expect(
         await columnsARebuildWouldRecast(duckdb, "no_such_table", source),
      ).toEqual([]);
   });
});

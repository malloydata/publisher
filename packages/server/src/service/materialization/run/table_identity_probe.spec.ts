// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { sameTableProbe } from "./table_ownership";
import type { TableSiteContext } from "./table_site";

/**
 * sameTableProbe on a real DuckDB, reached under two connection names, as
 * two places whose configs cannot be told apart reach one table: the probe
 * asks the table itself, and puts its comment back.
 */
describe("whether two places hold one table, asked of the table", () => {
   let duckdb: DuckDBConnection;
   let context: TableSiteContext;
   const at = (connectionName: string, physicalTableName: string) => ({
      connectionName,
      physicalTableName,
   });
   const comment = async (table: string) =>
      (
         (
            await duckdb.runSQL(
               `SELECT comment FROM duckdb_tables() WHERE table_name = '${table}'`,
            )
         ).rows as { comment: string | null }[]
      )[0]?.comment ?? null;

   beforeEach(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE t__p0123abcd AS SELECT 1 AS x",
      );
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE u__p0123abcd AS SELECT 2 AS x",
      );
      await duckdb.runSQL("COMMENT ON TABLE t__p0123abcd IS 'kept as it was'");
      // Two names, one database: what a renamed or aliased place looks like.
      context = {
         connectionFor: () => duckdb,
         destinationFor: () => undefined,
         environmentPath: "/",
      };
   });
   afterEach(async () => {
      await duckdb.close();
   });

   it("is the same table reached twice: true, and the comment is put back", async () => {
      expect(
         await sameTableProbe(
            at("old_place", "t__p0123abcd"),
            at("new_place", "t__p0123abcd"),
            context,
         ),
      ).toBe(true);
      expect(await comment("t__p0123abcd")).toBe("kept as it was");
   });

   it("is another table: false", async () => {
      expect(
         await sameTableProbe(
            at("old_place", "t__p0123abcd"),
            at("new_place", "u__p0123abcd"),
            context,
         ),
      ).toBe(false);
      expect(await comment("t__p0123abcd")).toBe("kept as it was");
   });

   it("cannot find the old table, or reach its place: unknown", async () => {
      expect(
         await sameTableProbe(
            at("old_place", "missing__p0123abcd"),
            at("new_place", "t__p0123abcd"),
            context,
         ),
      ).toBeUndefined();
      expect(
         await sameTableProbe(
            at("old_place", "t__p0123abcd"),
            at("new_place", "t__p0123abcd"),
            { ...context, connectionFor: () => undefined },
         ),
      ).toBeUndefined();
   });
});

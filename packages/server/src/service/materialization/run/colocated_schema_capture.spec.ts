// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { capturedColocatedSchema } from "./colocated_build_strategy";

/**
 * The v1 cutover's G1: a colocated build captures the columns it wrote, read
 * back through the connection's own schema reader in a `storage=` entry's
 * spelling, so the served copy binds the table with them.
 */
describe("capturedColocatedSchema", () => {
   let connection: DuckDBConnection;
   beforeAll(async () => {
      connection = new DuckDBConnection("duckdb", ":memory:");
      await connection.runSQL(
         `CREATE TABLE daily AS SELECT 'east' AS region, DATE '2026-01-01' AS d, 3::BIGINT AS n, 1.5::DOUBLE AS total, [{'s': 'x'}] AS nest`,
      );
   });
   afterAll(async () => {
      await connection.close();
   });

   it("reads the built table's columns", async () => {
      expect(
         await capturedColocatedSchema({
            connection,
            physicalTableName: "daily",
            dialect: "duckdb",
            sourceName: "daily",
         }),
      ).toEqual([
         { name: "region", type: "VARCHAR" },
         { name: "d", type: "DATE" },
         { name: "n", type: "BIGINT" },
         { name: "total", type: "DOUBLE" },
         { name: "nest", type: 'STRUCT("s" VARCHAR)[]' },
      ]);
   });

   it("reads a table whose name needs quoting, as the build created it", async () => {
      await connection.runSQL(`CREATE TABLE "Daily Mixed" AS SELECT 1 AS n`);
      expect(
         await capturedColocatedSchema({
            connection,
            physicalTableName: "Daily Mixed",
            dialect: "duckdb",
            sourceName: "daily",
         }),
      ).toEqual([{ name: "n", type: "BIGINT" }]);
   });

   it("is undefined when the table cannot be read, so the bind derives the columns", async () => {
      expect(
         await capturedColocatedSchema({
            connection,
            physicalTableName: "missing_table",
            dialect: "duckdb",
            sourceName: "missing",
         }),
      ).toBeUndefined();
      expect(
         await capturedColocatedSchema({
            connection: {
               fetchSchemaForTables: async () => {
                  throw new Error("unreachable warehouse");
               },
            },
            physicalTableName: "daily",
            dialect: "duckdb",
            sourceName: "daily",
         }),
      ).toBeUndefined();
   });
});

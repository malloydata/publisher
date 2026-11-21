// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { describe, expect, it } from "bun:test";
import {
   managedCatalogRefusal,
   managedTablePathRefusal,
   SYSTEM_RELATION_PREFIXES,
} from "./managed_catalog_scope";

/**
 * What a managed DuckLake connection named `lake` may name (the catalog's own
 * `main` schema), decided on DuckDB's parse before anything runs. Each refused
 * shape is one DuckLake answers from catalog metadata without opening a file
 * (managed_catalog_scope_postgres.spec.ts proves that on a real catalog).
 */
describe("managed catalog scope", () => {
   it.each([
      ["a bare table", "SELECT * FROM orders"],
      ["main.t", "SELECT count(*) FROM main.orders"],
      ["<alias>.t", "SELECT count(*) FROM lake.orders"],
      ["<alias>.main.t", "SELECT count(*) FROM LAKE.Main.orders"],
      ["a CTE", "WITH x AS (SELECT 1 AS n) SELECT * FROM x"],
      [
         "a join and a subquery",
         "SELECT * FROM orders o JOIN (SELECT * FROM main.items) i ON true",
      ],
      ["a value-only table function", "SELECT * FROM range(3), unnest([1, 2])"],
      [
         "ordinary scalar functions",
         "SELECT upper(s), date_trunc('day', d) FROM orders",
      ],
      ["time travel on a main table", "SELECT * FROM orders AT (VERSION => 1)"],
   ])("admits %s", async (_label, sql) => {
      expect(await managedCatalogRefusal(sql, "lake")).toBeUndefined();
   });

   it.each([
      ["another schema", "SELECT count(*) FROM materialized.gated"],
      [
         "another schema, qualified",
         "SELECT max(x) FROM lake.materialized.gated",
      ],
      [
         "another schema in a scalar subquery",
         "SELECT (SELECT count(*) FROM materialized.gated)",
      ],
      ["another catalog", "SELECT * FROM memory.main.t"],
      ["the temp catalog", "SELECT * FROM temp.main.t"],
      [
         "the metadata catalog",
         "SELECT * FROM __ducklake_metadata_lake.ducklake_table_column_stats",
      ],
      ["information_schema", "SELECT * FROM information_schema.columns"],
      ["pg_catalog, qualified", "SELECT * FROM pg_catalog.pg_class"],
      ["pg_class, bare", "SELECT relname, reltuples FROM pg_class"],
      ["duckdb_tables, bare", "SELECT * FROM duckdb_tables"],
      ["duckdb_columns()", "SELECT * FROM duckdb_columns()"],
      [
         "duckdb_tables(), qualified",
         "SELECT * FROM system.main.duckdb_tables()",
      ],
      ["pragma_table_info()", "SELECT * FROM pragma_table_info('gated')"],
      ["a file reader", "SELECT * FROM read_parquet('x.parquet')"],
      ["DESCRIBE", "DESCRIBE materialized.gated"],
      ["DESCRIBE of a main table", "DESCRIBE orders"],
      ["SHOW ALL TABLES", "SHOW ALL TABLES"],
      ["SUMMARIZE", "SUMMARIZE orders"],
      ["a qualified function", "SELECT pg_catalog.pg_typeof(1)"],
      ["a catalog function", "SELECT current_schemas(true)"],
      ["a statement that is not a SELECT", "PRAGMA table_info('gated')"],
      ["SQL that does not parse", "SELECT FROM WHERE"],
   ])("refuses %s", async (_label, sql) => {
      expect(await managedCatalogRefusal(sql, "lake")).toBeString();
   });

   it("reads a .table() path as Malloy places it", async () => {
      expect(await managedTablePathRefusal("orders", "lake")).toBeUndefined();
      expect(
         await managedTablePathRefusal("lake.main.orders", "lake"),
      ).toBeUndefined();
      expect(await managedTablePathRefusal("materialized.gated", "lake")).toBe(
         "'materialized.gated'",
      );
      expect(
         await managedTablePathRefusal("lake.materialized.gated", "lake"),
      ).toBe("'lake.materialized.gated'");
   });

   it("covers every relation in DuckDB's system catalog with a refused prefix", async () => {
      // A bare name falls through the search path to these; a DuckDB upgrade
      // that adds one under another prefix fails here.
      const instance = await DuckDBInstance.create(":memory:");
      const connection = await instance.connect();
      try {
         const reader = await connection.runAndReadAll(
            `SELECT lower(view_name) AS n FROM duckdb_views()
               WHERE database_name = 'system' AND schema_name IN ('main', 'pg_catalog')
             UNION
             SELECT lower(table_name) FROM duckdb_tables()
               WHERE database_name = 'system' AND schema_name IN ('main', 'pg_catalog')`,
         );
         const names = reader.getRowObjectsJson().map((row) => String(row.n));
         expect(names.length).toBeGreaterThan(10);
         const uncovered = names.filter(
            (name) => !SYSTEM_RELATION_PREFIXES.some((p) => name.startsWith(p)),
         );
         expect(uncovered).toEqual([]);
      } finally {
         connection.closeSync();
         instance.closeSync();
      }
   });
});

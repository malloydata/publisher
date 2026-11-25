// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { describe, expect, it } from "bun:test";
import { managedSchemas } from "./connection_type";
import {
   managedCatalogRefusal,
   scrubCatalogSuggestions,
   withoutCatalogSuggestions,
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
      // A name `main` lacks falls through to DuckDB's own catalog views,
      // however it is qualified, quoted or cased.
      ["main.duckdb_tables", "SELECT * FROM main.duckdb_tables"],
      ["MAIN.DuckDB_Columns, quoted", 'SELECT * FROM "MAIN"."DuckDB_Columns"'],
      ["main.sqlite_master", "SELECT * FROM main.sqlite_master"],
      ["main.pg_class", "SELECT * FROM Main.PG_CLASS"],
      ["<alias>.duckdb_tables", "SELECT * FROM lake.duckdb_tables"],
      [
         "<alias>.main.duckdb_tables",
         'SELECT * FROM "lake"."main"."duckdb_tables"',
      ],
      ["system.main.duckdb_tables", "SELECT * FROM system.main.duckdb_tables"],
      [
         "system.pg_catalog.pg_class",
         "SELECT * FROM system.pg_catalog.pg_class",
      ],
      [
         "main.duckdb_tables in sql_number",
         "SELECT (SELECT max(estimated_size) FROM main.duckdb_tables)",
      ],
      // Replacement scans: a quoted path, glob or URL reads a file.
      ["a quoted file", "SELECT * FROM 'gated.parquet'"],
      [
         "a quoted glob",
         "SELECT count(*) FROM '/data/materialized/**/*.parquet'",
      ],
      ["a quoted URL", `SELECT * FROM "s3://bucket/materialized/x.parquet"`],
      ["a path under main.", `SELECT * FROM main."materialized/x.parquet"`],
      ["a dotted name", `SELECT * FROM "x.csv"`],
      ["format_type", "SELECT format_type(23, NULL)"],
      ["get_block_size", "SELECT get_block_size('lake')"],
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

   describe("with readableSchemas listing stripe", () => {
      const schemas = managedSchemas(["stripe", "main", "stripe"]);

      it("is main plus the normalized list", () => {
         expect([...schemas]).toEqual(["main", "stripe"]);
         expect([...managedSchemas()]).toEqual(["main"]);
      });

      it.each([
         ["main, as before", "SELECT * FROM orders"],
         ["a listed schema, bare-qualified", "SELECT * FROM stripe.charges"],
         [
            "a listed schema, alias-qualified",
            "SELECT * FROM lake.stripe.charges",
         ],
         ["a listed schema, cased", 'SELECT * FROM "LAKE"."Stripe".charges'],
         [
            "a listed schema in a scalar subquery",
            "SELECT (SELECT count(*) FROM stripe.charges)",
         ],
      ])("admits %s", async (_label, sql) => {
         expect(
            await managedCatalogRefusal(sql, "lake", schemas),
         ).toBeUndefined();
      });

      it.each([
         ["an unlisted schema", "SELECT * FROM materialized.t"],
         ["an unlisted schema, qualified", "SELECT * FROM lake.materialized.t"],
         [
            "a catalog view in a listed schema",
            "SELECT * FROM stripe.duckdb_tables",
         ],
         [
            "a catalog view in a listed schema, qualified",
            "SELECT * FROM lake.stripe.pg_class",
         ],
         ["a function in a listed schema", "SELECT stripe.upper('x')"],
         ["a path under a listed schema", `SELECT * FROM stripe."x.parquet"`],
         ["DESCRIBE of a listed schema's table", "DESCRIBE stripe.charges"],
         [
            "a listed schema of another catalog",
            "SELECT * FROM memory.stripe.t",
         ],
      ])("refuses %s", async (_label, sql) => {
         expect(await managedCatalogRefusal(sql, "lake", schemas)).toBeString();
      });

      it("reads a .table() path as Malloy places it", async () => {
         expect(
            await managedTablePathRefusal("stripe.charges", "lake", schemas),
         ).toBeUndefined();
         expect(
            await managedTablePathRefusal(
               "lake.stripe.charges",
               "lake",
               schemas,
            ),
         ).toBeUndefined();
         expect(
            await managedTablePathRefusal("materialized.t", "lake", schemas),
         ).toBe("'materialized.t'");
         // The default is main alone, so a caller that forgets the set fails closed.
         expect(await managedTablePathRefusal("stripe.charges", "lake")).toBe(
            "'stripe.charges'",
         );
      });
   });

   it("covers every relation DuckDB's search path reaches with a refused prefix", async () => {
      // A name the catalog's `main` lacks falls through to these, however it
      // is qualified. Listed with the extensions a managed connection loads; a
      // DuckDB or extension upgrade that adds one under another prefix fails
      // here, and so does one the check would admit qualified.
      const instance = await DuckDBInstance.create(":memory:");
      const connection = await instance.connect();
      try {
         for (const extension of [
            "ducklake",
            "postgres",
            "aws",
            "httpfs",
            "json",
         ]) {
            await connection.run(`INSTALL ${extension}; LOAD ${extension};`);
         }
         const reader = await connection.runAndReadAll(
            `SELECT database_name AS c, schema_name AS s, lower(view_name) AS n FROM duckdb_views()
               WHERE database_name IN ('system', 'temp')
             UNION
             SELECT database_name, schema_name, lower(table_name) FROM duckdb_tables()
               WHERE database_name IN ('system', 'temp')`,
         );
         const rows = reader.getRowObjectsJson().map((row) => ({
            c: String(row.c),
            s: String(row.s),
            n: String(row.n),
         }));
         expect(rows.length).toBeGreaterThan(10);
         const searchPath = rows.filter(
            (row) => row.s === "main" || row.s === "pg_catalog",
         );
         const uncovered = searchPath.filter(
            (row) => !SYSTEM_RELATION_PREFIXES.some((p) => row.n.startsWith(p)),
         );
         expect(uncovered).toEqual([]);
         for (const row of rows) {
            const qualified = [
               `SELECT * FROM ${row.s}.${row.n}`,
               `SELECT * FROM ${row.c}.${row.s}.${row.n}`,
            ];
            // information_schema is not on the search path: reached only
            // qualified, by its schema.
            const forms =
               row.s === "information_schema"
                  ? qualified
                  : [
                       `SELECT * FROM ${row.n}`,
                       `SELECT * FROM main.${row.n}`,
                       `SELECT * FROM lake.main.${row.n}`,
                       ...qualified,
                    ];
            for (const sql of forms) {
               expect([sql, await managedCatalogRefusal(sql, "lake")]).toEqual([
                  sql,
                  expect.any(String),
               ]);
            }
         }
      } finally {
         connection.closeSync();
         instance.closeSync();
      }
   });
   it("drops DuckDB's near-name suggestion and keeps the rest of the error", () => {
      const raw =
         'Catalog Error: Table with name gatd does not exist!\nDid you mean "materialized.gated"?\n\nLINE 1: SELECT * FROM gatd';
      expect(withoutCatalogSuggestions(raw)).toBe(
         "Catalog Error: Table with name gatd does not exist!\n\nLINE 1: SELECT * FROM gatd",
      );
      const error = new Error(raw);
      expect(scrubCatalogSuggestions(error)).toBe(error);
      expect(error.message).not.toContain("materialized");
      expect(error.stack).not.toContain("materialized");
      const frozen = Object.freeze(new Error(raw));
      const replaced = scrubCatalogSuggestions(frozen) as Error;
      expect(replaced.message).not.toContain("materialized");
   });
});

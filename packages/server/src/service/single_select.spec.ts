// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../errors";
import { assertSingleSelect, classifySelect } from "./single_select";

describe("classifySelect", () => {
   it.each([
      ["a SELECT", "SELECT 1"],
      ["a CTE", "WITH a AS (SELECT 1 AS x) SELECT * FROM a"],
      ["a trailing semicolon", "SELECT 1;"],
      [
         "a catalog-qualified table it never binds",
         "SELECT * FROM lake.main.orders LIMIT 5",
      ],
   ])("counts %s as one SELECT", async (_label, sql) => {
      expect(await classifySelect(sql)).toEqual({ selects: 1 });
   });

   it("counts each statement of a script", async () => {
      expect(await classifySelect("SELECT 1; SELECT 2")).toEqual({
         selects: 2,
      });
   });

   it.each([
      ["a CREATE SECRET", "CREATE SECRET s (TYPE s3, PROVIDER config)"],
      ["a SET", "SET threads = 1"],
      ["an ATTACH", "ATTACH ':memory:' AS other"],
      ["a COPY", "COPY (SELECT 1) TO 'out.csv'"],
      ["a SELECT followed by a statement", "SELECT 1; SET threads = 1"],
      ["text that does not parse", "SELEC 1"],
   ])("refuses %s", async (_label, sql) => {
      expect(await classifySelect(sql)).toHaveProperty("refused");
   });
});

describe("assertSingleSelect", () => {
   it("lets one SELECT through", async () => {
      await assertSingleSelect("sales", "SELECT 1");
   });

   it.each([
      ["SET threads = 1", "it is not a SELECT"],
      ["SELECT 1; SELECT 2", "it holds 2 statements"],
      ["", "it holds no statement"],
   ])("refuses %p with a 400 naming the database and why", async (sql, why) => {
      const refused = assertSingleSelect("sales", sql);
      await expect(refused).rejects.toBeInstanceOf(BadRequestError);
      await expect(refused).rejects.toThrow(`'sales' is a managed database`);
      await expect(refused).rejects.toThrow(why);
   });

   it.each([
      ["SELECT * FROM duckdb_databases()", "duckdb_databases"],
      ["SELECT value FROM duckdb_settings()", "duckdb_settings"],
      ["SELECT * FROM postgres_query('lake', 'SELECT 1')", "postgres_query"],
      [
         "SELECT * FROM __ducklake_metadata_lake.ducklake_metadata",
         "__ducklake_metadata_lake",
      ],
      ["SELECT 1 -- duckdb_secrets()", "duckdb_secrets"],
      ["SELECT * FROM pragma_database_list", "pragma_database_list"],
      ['SELECT * FROM "Pragma_Database_List"()', "pragma_database_list"],
      ["SELECT * FROM pragma_show('t')", "pragma_show"],
      ["SELECT current_setting('allowed_paths') AS p", "current_setting"],
      ["SELECT * FROM duckdb_variables()", "duckdb_variables"],
      ["SELECT getvariable('v') AS v", "getvariable"],
      ["SELECT * FROM load_aws_credentials()", "load_aws_credentials"],
      ["SELECT * FROM ducklake_settings('lake')", "ducklake_settings"],
      ["SELECT * FROM ducklake_options('lake')", "ducklake_options"],
      ["SELECT * FROM ducklake_snapshots('lake')", "ducklake_snapshots"],
      ["SELECT * FROM ducklake_list_files('lake', 't')", "ducklake_list_files"],
      [
         "SELECT * FROM \"ducklake_some_future_fn\" ('lake')",
         "ducklake_some_future_fn",
      ],
      [
         "SELECT * FROM ducklake_some_future_fn /* c */ ('lake')",
         "ducklake_some_future_fn",
      ],
      [
         "SELECT * FROM ducklake_set_commit_message('lake', 'a', 'b')",
         "ducklake_set_commit_message",
      ],
      [
         "SELECT * FROM ducklake_expire_snapshots('lake', dry_run => true)",
         "ducklake_expire_snapshots",
      ],
      [
         "SELECT * FROM ducklake_cleanup_old_files('lake', dry_run => true)",
         "ducklake_cleanup_old_files",
      ],
      [
         "SELECT * FROM ducklake_delete_orphaned_files('lake', dry_run => true)",
         "ducklake_delete_orphaned_files",
      ],
      [
         "SELECT * FROM ducklake_set_option('lake', 'k', 'v')",
         "ducklake_set_option",
      ],
   ])(
      "refuses introspection %p with a 400 naming the database and the name",
      async (sql, name) => {
         const refused = assertSingleSelect("sales", sql);
         await expect(refused).rejects.toBeInstanceOf(BadRequestError);
         await expect(refused).rejects.toThrow(`'sales' is a managed database`);
         await expect(refused).rejects.toThrow(`may not name '${name}'`);
      },
   );

   it("lets a catalog or table whose name starts like DuckLake's functions through", async () => {
      await assertSingleSelect(
         "sales",
         "SELECT * FROM ducklake_prod.main.orders",
      );
      await assertSingleSelect(
         "sales",
         "SELECT ducklake_id FROM lake.main.orders",
      );
   });
});

describe("assertSingleSelect judges the text as the DuckDB connector will run it", () => {
   // @malloydata/db-duckdb splits raw SQL on its statement separator wherever
   // it appears and runs every part, while DuckDB's parser reads the separator
   // as the start of a line comment. So the gate refuses the separator itself.
   const separated: [string, string][] = [
      [
         "a statement placed after it on the same line",
         "SELECT 1 AS one -- hack: split on this SET memory_limit='256MB'",
      ],
      [
         "a statement placed after it on the next line",
         "SELECT 1 AS one -- hack: split on this\nSELECT 2 AS two",
      ],
      ["it inside a string literal", "SELECT '-- hack: split on this' AS s"],
      ["it inside a block comment", "SELECT 1 /* -- hack: split on this */"],
      ["it alone, trailing a SELECT", "SELECT 1 -- hack: split on this"],
   ];

   for (const kind of [
      "managed database",
      "locked-down DuckDB connection",
   ] as const) {
      it.each(separated)(
         `refuses %s with a 400 on a ${kind}, without echoing the SQL`,
         async (_label, sql) => {
            const refused = assertSingleSelect("sales", sql, kind);
            await expect(refused).rejects.toBeInstanceOf(BadRequestError);
            await expect(refused).rejects.toThrow(`'sales' is a ${kind}`);
            await expect(refused).rejects.toThrow("statement separator");
            await expect(refused).rejects.not.toThrow("memory_limit");
         },
      );
   }

   it("still lets a plain SELECT, and a comment that is not the separator, through", async () => {
      await assertSingleSelect("sales", "SELECT 1 AS one");
      await assertSingleSelect("sales", "SELECT 1 AS one -- hack: a note");
      await assertSingleSelect(
         "sales",
         "SELECT 1 AS one",
         "locked-down DuckDB connection",
      );
   });

   it("still refuses a semicolon-separated script", async () => {
      await expect(
         assertSingleSelect("sales", "SELECT 1; SET memory_limit='256MB'"),
      ).rejects.toThrow("runs only as a single SELECT");
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   NAME_VIEW_MARK,
   nameViewComment,
   nameViewDialectSupported,
   nameViewDropSql,
   nameViewOwner,
   nameViewProbeSql,
   nameViewReplaceDdl,
   nameViewSlotOf,
   NameViewUnsupportedError,
   parseNameViewComment,
   sqlString,
} from "./name_view_sql";

/**
 * The `name=` view's SQL, per dialect: the comment mark, the probe that reads
 * what holds a name, the DDL that points the view, and its drop. Every
 * identifier goes through the shared quoting helpers.
 */

const OWNER = nameViewOwner("env", "pkg");
const markOf = (target: string, owner = OWNER) =>
   nameViewComment({ target, owner });

describe("the name= view's comment mark", () => {
   it("names the package that made the view and the table it reads, and reads back", () => {
      const comment = markOf("analytics.flights_g7");
      expect(comment).toBe(
         `${NAME_VIEW_MARK} owner=env%2Fpkg target=analytics.flights_g7`,
      );
      expect(parseNameViewComment(comment)).toEqual({
         target: "analytics.flights_g7",
         owner: "env/pkg",
      });
   });

   it("carries no quote, backslash or space whatever the table or package is called", () => {
      const target = `my-proj.ds.it's a "t"\\x`;
      const owner = `it's "my" env/a\\b c`;
      const comment = markOf(target, owner);
      const values = comment.slice(NAME_VIEW_MARK.length + 1);
      expect(values.split(" ")).toHaveLength(2);
      expect(values).not.toMatch(/['"\\]/);
      expect(parseNameViewComment(comment)).toEqual({ target, owner });
   });

   it("reads BigQuery's TABLE_OPTIONS spelling, a double-quoted literal", () => {
      expect(parseNameViewComment(`"${markOf("ds.t")}"`)).toEqual({
         target: "ds.t",
         owner: OWNER,
      });
   });

   it("is not any other comment", () => {
      for (const comment of [
         null,
         undefined,
         42,
         "",
         "a user's view",
         NAME_VIEW_MARK,
         `${NAME_VIEW_MARK} target=`,
         `${NAME_VIEW_MARK} target=t`,
         `${NAME_VIEW_MARK} owner=o`,
         `${NAME_VIEW_MARK} owner=o target=`,
         `${NAME_VIEW_MARK} owner=o target=t extra=x`,
         `${NAME_VIEW_MARK} owner=o owner=p`,
         `${NAME_VIEW_MARK} owner=o target=%E0%A4%A`,
         `${NAME_VIEW_MARK}x owner=o target=t`,
         `${NAME_VIEW_MARK}  owner=o target=t`,
         `prefix ${markOf("t")}`,
      ]) {
         expect(parseNameViewComment(comment)).toBeUndefined();
      }
   });
});

describe("string literals per dialect", () => {
   it("escapes the quote as each dialect reads it", () => {
      expect(sqlString(`a'b`, "duckdb")).toBe(`'a''b'`);
      expect(sqlString(`a'b`, "postgres")).toBe(`'a''b'`);
      expect(sqlString(`a'b\\`, "standardsql")).toBe(`'a\\'b\\\\'`);
      expect(sqlString(`a'b\\`, "databricks")).toBe(`'a\\'b\\\\'`);
      expect(sqlString(`a'b\\`, "snowflake")).toBe(`'a''b\\\\'`);
      expect(sqlString(`a'b`, "sqlserver")).toBe(`N'a''b'`);
      expect(sqlString(`a'b`, "trino")).toBe(`'a''b'`);
   });
});

describe("which dialects can hold a marked view", () => {
   it("every dialect that can comment a view and read it back", () => {
      for (const dialect of [
         "duckdb",
         "postgres",
         "standardsql",
         "snowflake",
         "databricks",
         "trino",
         "sqlserver",
      ]) {
         expect(nameViewDialectSupported(dialect)).toBe(true);
      }
   });

   it("not MySQL (no view comment) nor Presto (no COMMENT on CREATE VIEW)", () => {
      for (const dialect of ["mysql", "presto", "unknown"]) {
         expect(nameViewDialectSupported(dialect)).toBe(false);
         expect(() => nameViewProbeSql(["v"], dialect)).toThrow(
            NameViewUnsupportedError,
         );
         expect(() =>
            nameViewReplaceDdl(
               ["v"],
               "t",
               { target: "t", owner: OWNER },
               dialect,
            ),
         ).toThrow(NameViewUnsupportedError);
         expect(() => nameViewDropSql(["v"], dialect)).toThrow(
            NameViewUnsupportedError,
         );
      }
   });
});

describe("pointing the view, per dialect", () => {
   const mark = (dialect: string) => sqlString(markOf("s.t_g2"), dialect);

   it("DuckDB (and MotherDuck, DuckLake): view and mark in one transaction", () => {
      const ddl = nameViewReplaceDdl(
         ["s", "v"],
         `"s"."t_g2"`,
         { target: "s.t_g2", owner: OWNER },
         "duckdb",
      );
      expect(ddl.primary).toBe(
         [
            "BEGIN TRANSACTION;",
            `CREATE OR REPLACE VIEW "s"."v" AS SELECT * FROM "s"."t_g2";`,
            `COMMENT ON VIEW "s"."v" IS ${mark("duckdb")};`,
            "COMMIT;",
         ].join("\n"),
      );
      expect(ddl.rollback).toBe("ROLLBACK");
      expect(ddl.fallback).toBeUndefined();
   });

   it("Postgres: CREATE OR REPLACE, else drop and make again, each one implicit transaction", () => {
      const ddl = nameViewReplaceDdl(
         ["s", "v"],
         `"s"."t_g2"`,
         { target: "s.t_g2", owner: OWNER },
         "postgres",
      );
      expect(ddl.primary).toBe(
         [
            `CREATE OR REPLACE VIEW "s"."v" AS SELECT * FROM "s"."t_g2";`,
            `COMMENT ON VIEW "s"."v" IS ${mark("postgres")};`,
         ].join("\n"),
      );
      expect(ddl.fallback).toBe(
         [
            `DROP VIEW IF EXISTS "s"."v";`,
            `CREATE VIEW "s"."v" AS SELECT * FROM "s"."t_g2";`,
            `COMMENT ON VIEW "s"."v" IS ${mark("postgres")};`,
         ].join("\n"),
      );
      expect(ddl.rollback).toBeUndefined();
   });

   it("BigQuery: one statement, the mark as the description", () => {
      const ddl = nameViewReplaceDdl(
         ["my-proj", "ds", "v"],
         "`my-proj`.`ds`.`t_g2`",
         { target: "s.t_g2", owner: OWNER },
         "standardsql",
      );
      expect(ddl.primary).toBe(
         "CREATE OR REPLACE VIEW `my-proj`.`ds`.`v` " +
            `OPTIONS(description = ${mark("standardsql")}) ` +
            "AS SELECT * FROM `my-proj`.`ds`.`t_g2`",
      );
   });

   it("Snowflake, Databricks and Trino: one statement with COMMENT", () => {
      expect(
         nameViewReplaceDdl(
            ["DB", "S", "V"],
            `"DB"."S"."T"`,
            { target: "s.t_g2", owner: OWNER },
            "snowflake",
         ).primary,
      ).toBe(
         `CREATE OR REPLACE VIEW "DB"."S"."V" COMMENT = ${mark("snowflake")} AS SELECT * FROM "DB"."S"."T"`,
      );
      expect(
         nameViewReplaceDdl(
            ["c", "s", "v"],
            "`c`.`s`.`t`",
            { target: "s.t_g2", owner: OWNER },
            "databricks",
         ).primary,
      ).toBe(
         `CREATE OR REPLACE VIEW \`c\`.\`s\`.\`v\` COMMENT ${mark("databricks")} AS SELECT * FROM \`c\`.\`s\`.\`t\``,
      );
      expect(
         nameViewReplaceDdl(
            ["c", "s", "v"],
            `"c"."s"."t"`,
            { target: "s.t_g2", owner: OWNER },
            "trino",
         ).primary,
      ).toBe(
         `CREATE OR REPLACE VIEW "c"."s"."v" COMMENT ${mark("trino")} AS SELECT * FROM "c"."s"."t"`,
      );
   });

   it("SQL Server: CREATE OR ALTER through EXEC and its MS_Description, in one transaction", () => {
      const ddl = nameViewReplaceDdl(
         ["dbo", "v"],
         `"dbo"."t_g2"`,
         { target: "s.t_g2", owner: OWNER },
         "sqlserver",
      );
      expect(ddl.primary).toContain("SET XACT_ABORT ON;");
      expect(ddl.primary).toContain(`DECLARE @schema sysname = N'dbo';`);
      expect(ddl.primary).toContain(
         `EXEC(N'CREATE OR ALTER VIEW "dbo"."v" AS SELECT * FROM "dbo"."t_g2"');`,
      );
      expect(ddl.primary).toContain(
         `EXEC sys.sp_updateextendedproperty @name = N'MS_Description', @value = ${mark("sqlserver")}`,
      );
      expect(ddl.primary).toContain("EXEC sys.sp_addextendedproperty");
      expect(ddl.primary).toContain(
         `@level1type = N'VIEW', @level1name = N'v'`,
      );
      expect(ddl.primary.trim().endsWith("COMMIT TRANSACTION;")).toBe(true);
      // A bare name is in the caller's default schema.
      expect(
         nameViewReplaceDdl(
            ["v"],
            `"t"`,
            { target: "t", owner: OWNER },
            "sqlserver",
         ).primary,
      ).toContain("DECLARE @schema sysname = SCHEMA_NAME();");
      // CREATE VIEW takes no database.
      expect(() =>
         nameViewReplaceDdl(
            ["db", "dbo", "v"],
            "t",
            { target: "t", owner: OWNER },
            "sqlserver",
         ),
      ).toThrow(NameViewUnsupportedError);
   });

   it("quotes a name that needs it", () => {
      const ddl = nameViewReplaceDdl(
         ["s", `we"ird`],
         `"s"."t"`,
         { target: "s.t", owner: OWNER },
         "postgres",
      );
      expect(ddl.primary).toContain(`VIEW "s"."we""ird" AS`);
   });
});

describe("dropping the view", () => {
   it("is DROP VIEW, never DROP TABLE, in every dialect", () => {
      expect(nameViewDropSql(["s", "v"], "duckdb")).toBe(
         `DROP VIEW IF EXISTS "s"."v"`,
      );
      expect(nameViewDropSql(["p", "d", "v"], "standardsql")).toBe(
         "DROP VIEW IF EXISTS `p`.`d`.`v`",
      );
      expect(nameViewDropSql(["s", "v"], "sqlserver")).toBe(
         `DROP VIEW IF EXISTS "s"."v"`,
      );
   });
});

describe("probing what holds a name, per dialect", () => {
   it("DuckDB: views and tables, case-insensitively, by catalog or schema", () => {
      const sql = nameViewProbeSql(["lake", "v"], "duckdb");
      expect(sql).toContain("FROM duckdb_views() WHERE NOT internal");
      expect(sql).toContain("FROM duckdb_tables()");
      expect(sql).toContain(
         "(lower(database_name) = lower('lake') AND schema_name = 'main')",
      );
      expect(sql).toContain(
         "(database_name = current_database() AND lower(schema_name) = lower('lake'))",
      );
      expect(nameViewProbeSql(["v"], "duckdb")).toContain(
         "schema_name = current_schema()",
      );
      expect(nameViewProbeSql(["c", "s", "v"], "duckdb")).toContain(
         "lower(database_name) = lower('c') AND lower(schema_name) = lower('s')",
      );
   });

   it("Postgres: to_regclass over the quoted path", () => {
      expect(nameViewProbeSql(["s", "v"], "postgres")).toContain(
         `WHERE c.oid = to_regclass('"s"."v"')`,
      );
   });

   it("BigQuery: the dataset's INFORMATION_SCHEMA, and only a dataset-qualified name", () => {
      const sql = nameViewProbeSql(["my-proj", "ds", "v"], "standardsql");
      expect(sql).toContain(
         "FROM `my-proj`.`ds`.INFORMATION_SCHEMA.TABLES t LEFT JOIN `my-proj`.`ds`.INFORMATION_SCHEMA.TABLE_OPTIONS o",
      );
      expect(sql).toContain("WHERE t.table_name = 'v'");
      expect(() => nameViewProbeSql(["v"], "standardsql")).toThrow(
         NameViewUnsupportedError,
      );
   });

   it("Snowflake: exact case, in the named database's INFORMATION_SCHEMA", () => {
      expect(nameViewProbeSql(["DB", "S", "v"], "snowflake")).toBe(
         `SELECT TABLE_TYPE AS "kind", COMMENT AS "comment" FROM "DB".INFORMATION_SCHEMA.TABLES ` +
            `WHERE TABLE_SCHEMA = 'S' AND TABLE_NAME = 'v'`,
      );
      expect(nameViewProbeSql(["v"], "snowflake")).toContain(
         "TABLE_SCHEMA = CURRENT_SCHEMA()",
      );
   });

   it("Databricks and Trino: lower-cased names", () => {
      expect(nameViewProbeSql(["c", "S", "V"], "databricks")).toBe(
         "SELECT table_type AS kind, comment FROM `c`.information_schema.tables " +
            "WHERE table_schema = lower('S') AND table_name = lower('V')",
      );
      expect(nameViewProbeSql(["V"], "databricks")).toContain(
         "table_schema = current_schema()",
      );
      const trino = nameViewProbeSql(["S", "V"], "trino");
      expect(trino).toContain("FROM information_schema.tables t");
      expect(trino).toContain("LEFT JOIN system.metadata.table_comments c");
      expect(trino).toContain("t.table_schema = lower('S')");
   });

   it("SQL Server: the object and its MS_Description", () => {
      const sql = nameViewProbeSql(["dbo", "v"], "sqlserver");
      expect(sql).toContain(`WHERE o.object_id = OBJECT_ID(N'"dbo"."v"')`);
      expect(sql).toContain("ep.name = N'MS_Description'");
   });
});

describe("reading what holds a name", () => {
   it("nothing", () => {
      expect(nameViewSlotOf([])).toEqual({ state: "empty" });
   });

   it("a Publisher view, and the table it reads", () => {
      expect(
         nameViewSlotOf([{ kind: "VIEW", comment: markOf("t_g1") }]),
      ).toEqual({ state: "ours", target: "t_g1", owner: OWNER });
      // Snowflake reports upper-case columns unless aliased; any case reads.
      expect(
         nameViewSlotOf([{ KIND: "view", COMMENT: markOf("t_g1") }]),
      ).toEqual({ state: "ours", target: "t_g1", owner: OWNER });
   });

   it("a table, or a view Publisher did not mark, is taken", () => {
      expect(nameViewSlotOf([{ kind: "BASE TABLE", comment: null }])).toEqual({
         state: "taken",
         kind: "BASE TABLE",
      });
      expect(nameViewSlotOf([{ kind: "VIEW", comment: "mine" }])).toEqual({
         state: "taken",
         kind: "VIEW",
      });
      // A marked view beside a table of the same name is still taken.
      expect(
         nameViewSlotOf([
            { kind: "VIEW", comment: markOf("t") },
            { kind: "TABLE", comment: markOf("t") },
         ]),
      ).toEqual({ state: "taken", kind: "TABLE" });
   });
});

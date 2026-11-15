// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   dropViewSql,
   packageOwner,
   parseNameViewComment,
   parseOwnedTableComment,
   relationProbeSql,
   relationProbeSupported,
   sqlString,
   viewDefinitionReadsOnly,
   viewReadsSql,
} from "./table_mark_sql";

/**
 * The SQL of the marks Publisher reads: the relation probe the v1 `name=`
 * ownership check runs, per dialect; string literals; the strict reading of
 * a leftover `name=` view's comment; a view's drop; and how each dialect
 * proves what a view reads.
 */

const MARK = "malloy-publisher:name-view";
const OWNER = packageOwner("env", "pkg");
/** The comment an earlier release wrote (each value URI-encoded, the quote too). */
const markOf = (target: string, owner = OWNER) => {
   const enc = (v: string) => encodeURIComponent(v).replace(/'/g, "%27");
   return `${MARK} owner=${enc(owner)} target=${enc(target)}`;
};

describe("a leftover name= view's comment mark", () => {
   it("names the package that made the view and the table it reads, and reads back", () => {
      const comment = markOf("analytics.flights_g7");
      expect(comment).toBe(
         `${MARK} owner=env%2Fpkg target=analytics.flights_g7`,
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
      const values = comment.slice(MARK.length + 1);
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
         MARK,
         `${MARK} target=`,
         `${MARK} target=t`,
         `${MARK} owner=o`,
         `${MARK} owner=o target=`,
         `${MARK} owner=o target=t extra=x`,
         `${MARK} owner=o owner=p`,
         `${MARK} owner=o target=%E0%A4%A`,
         `${MARK}x owner=o target=t`,
         `${MARK}  owner=o target=t`,
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

describe("which dialects the shared probe reads", () => {
   it("every dialect that can read a relation's comment back, and no other", () => {
      for (const dialect of [
         "duckdb",
         "postgres",
         "standardsql",
         "snowflake",
         "databricks",
         "trino",
         "sqlserver",
      ]) {
         expect(relationProbeSupported(dialect)).toBe(true);
      }
      for (const dialect of ["mysql", "presto", "unknown"]) {
         expect(relationProbeSupported(dialect)).toBe(false);
         expect(() => relationProbeSql(["v"], dialect)).toThrow(Error);
         expect(() => dropViewSql(["v"], dialect)).toThrow(Error);
      }
   });
});

describe("the ownership mark", () => {
   it("is read strictly, and never as a view's mark", () => {
      expect(
         parseOwnedTableComment("malloy-publisher:table owner=env%2Fpkg"),
      ).toBe("env/pkg");
      expect(parseOwnedTableComment(markOf("t"))).toBeUndefined();
      expect(
         parseNameViewComment("malloy-publisher:table owner=env%2Fpkg"),
      ).toBeUndefined();
   });
});

describe("dropping a view", () => {
   it("is DROP VIEW, never DROP TABLE, in every dialect", () => {
      for (const dialect of [
         "duckdb",
         "postgres",
         "standardsql",
         "snowflake",
         "databricks",
         "trino",
         "sqlserver",
      ]) {
         const sql = dropViewSql(["s", "v"], dialect);
         expect(sql).toStartWith("DROP VIEW IF EXISTS ");
         expect(sql).not.toContain("TABLE");
      }
      expect(dropViewSql(["s", "v"], "duckdb")).toBe(
         `DROP VIEW IF EXISTS "s"."v"`,
      );
      expect(dropViewSql(["p", "d", "v"], "standardsql")).toBe(
         "DROP VIEW IF EXISTS `p`.`d`.`v`",
      );
      expect(() => dropViewSql(["db", "s", "v"], "sqlserver")).toThrow(Error);
   });
});

describe("probing what holds a name, per dialect", () => {
   it("DuckDB: views and tables, case-insensitively, by catalog or schema", () => {
      const sql = relationProbeSql(["lake", "v"], "duckdb");
      expect(sql).toContain("FROM duckdb_views() WHERE NOT internal");
      expect(sql).toContain("FROM duckdb_tables()");
      expect(sql).toContain(
         "(lower(database_name) = lower('lake') AND schema_name = 'main')",
      );
      expect(sql).toContain(
         "(database_name = current_database() AND lower(schema_name) = lower('lake'))",
      );
      expect(relationProbeSql(["v"], "duckdb")).toContain(
         "schema_name = current_schema()",
      );
      expect(relationProbeSql(["c", "s", "v"], "duckdb")).toContain(
         "lower(database_name) = lower('c') AND lower(schema_name) = lower('s')",
      );
   });

   it("Postgres: to_regclass over the quoted path", () => {
      expect(relationProbeSql(["s", "v"], "postgres")).toContain(
         `WHERE c.oid = to_regclass('"s"."v"')`,
      );
   });

   it("BigQuery: the dataset's INFORMATION_SCHEMA, and only a dataset-qualified name", () => {
      const sql = relationProbeSql(["my-proj", "ds", "v"], "standardsql");
      expect(sql).toContain(
         "FROM `my-proj`.`ds`.INFORMATION_SCHEMA.TABLES t LEFT JOIN `my-proj`.`ds`.INFORMATION_SCHEMA.TABLE_OPTIONS o",
      );
      expect(sql).toContain("WHERE t.table_name = 'v'");
      expect(() => relationProbeSql(["v"], "standardsql")).toThrow(Error);
   });

   it("Snowflake: exact case, in the named database's INFORMATION_SCHEMA", () => {
      expect(relationProbeSql(["DB", "S", "v"], "snowflake")).toBe(
         `SELECT TABLE_TYPE AS "kind", COMMENT AS "comment" FROM "DB".INFORMATION_SCHEMA.TABLES ` +
            `WHERE TABLE_SCHEMA = 'S' AND TABLE_NAME = 'v'`,
      );
      expect(relationProbeSql(["v"], "snowflake")).toContain(
         "TABLE_SCHEMA = CURRENT_SCHEMA()",
      );
   });

   it("Databricks and Trino: lower-cased names", () => {
      expect(relationProbeSql(["c", "S", "V"], "databricks")).toBe(
         "SELECT table_type AS kind, comment FROM `c`.information_schema.tables " +
            "WHERE table_schema = lower('S') AND table_name = lower('V')",
      );
      expect(relationProbeSql(["V"], "databricks")).toContain(
         "table_schema = current_schema()",
      );
      const trino = relationProbeSql(["S", "V"], "trino");
      expect(trino).toContain("FROM information_schema.tables t");
      expect(trino).toContain("LEFT JOIN system.metadata.table_comments c");
      expect(trino).toContain("t.table_schema = lower('S')");
   });

   it("SQL Server: the object and its MS_Description", () => {
      const sql = relationProbeSql(["dbo", "v"], "sqlserver");
      expect(sql).toContain(`WHERE o.object_id = OBJECT_ID(N'"dbo"."v"')`);
      expect(sql).toContain("ep.name = N'MS_Description'");
   });
});

describe("proving what a view reads, per dialect", () => {
   it("Postgres: the view's rule depends on the target, and on no other relation", () => {
      const { sql, answer } = viewReadsSql(["s", "v"], ["s", "t"], "postgres");
      expect(answer).toBe("reads");
      expect(sql).toContain(`r.ev_class = to_regclass('"s"."v"')`);
      expect(sql).toContain(`d.refobjid = to_regclass('"s"."t"')`);
      expect(sql).toContain(`d.refobjid <> to_regclass('"s"."t"')`);
      expect(sql).toContain("rc.relkind IN ('r', 'v', 'm', 'p', 'f')");
   });

   it("SQL Server: depends on the target, and on nothing else or unresolved", () => {
      const { sql, answer } = viewReadsSql(
         ["dbo", "v"],
         ["dbo", "t"],
         "sqlserver",
      );
      expect(answer).toBe("reads");
      expect(sql).toContain(`d.referencing_id = OBJECT_ID(N'"dbo"."v"')`);
      expect(sql).toContain(`d.referenced_id = OBJECT_ID(N'"dbo"."t"')`);
      expect(sql).toContain("d.referenced_id IS NULL");
      expect(sql).toContain(`d.referenced_id <> OBJECT_ID(N'"dbo"."t"')`);
   });

   it("every other dialect: the view's definition and its schema", () => {
      const duck = viewReadsSql(["s", "v"], ["s", "t"], "duckdb");
      expect(duck.answer).toBe("definition");
      expect(duck.sql).toContain(
         "SELECT sql AS definition, schema_name AS view_schema FROM duckdb_views()",
      );
      expect(
         viewReadsSql(["p", "d", "v"], ["p", "d", "t"], "standardsql").sql,
      ).toContain(
         "FROM `p`.`d`.INFORMATION_SCHEMA.VIEWS WHERE table_name = 'v'",
      );
      expect(
         viewReadsSql(["DB", "S", "v"], ["DB", "S", "t"], "snowflake").sql,
      ).toContain(
         `FROM "DB".INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'S' AND TABLE_NAME = 'v'`,
      );
      expect(viewReadsSql(["S", "V"], ["S", "T"], "trino").sql).toContain(
         "FROM information_schema.views WHERE table_schema = lower('S') AND table_name = lower('V')",
      );
      expect(viewReadsSql(["V"], ["T"], "databricks").sql).toContain(
         "table_schema = current_schema()",
      );
   });

   it("reads a definition as reading exactly one relation, in the view's schema when unqualified", () => {
      const reads = (definition: string, target: string, schema?: string) =>
         viewDefinitionReadsOnly(definition, target, schema);
      expect(reads("CREATE VIEW v AS SELECT * FROM g1;", "g1", "main")).toBe(
         true,
      );
      expect(reads('SELECT x FROM "MAIN"."G1"', "g1", "main")).toBe(true);
      expect(reads("SELECT * FROM main.g1", "g1", "main")).toBe(true);
      // The same name in another schema is another table.
      expect(reads("SELECT * FROM s2.g1", "g1", "main")).toBe(false);
      expect(reads("SELECT * FROM g1", "s2.g1", "main")).toBe(false);
      // A join, a second relation, or a subquery over another is not one.
      expect(reads("SELECT * FROM g1 JOIN o ON o.x = g1.x", "g1", "main")).toBe(
         false,
      );
      expect(reads("SELECT * FROM g1, o", "g1", "main")).toBe(false);
      expect(
         reads("SELECT * FROM o WHERE x IN (SELECT x FROM g1)", "g1", "main"),
      ).toBe(false);
      expect(reads("SELECT * FROM g2", "g1", "main")).toBe(false);
      expect(viewDefinitionReadsOnly(undefined, "g1", "main")).toBe(false);
   });
});

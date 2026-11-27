// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   bigqueryCollatedSql,
   bigqueryColumnCollation,
   bigqueryNamesNoCollation,
   canonicalCollation,
   collationOf,
   duckdbTableCollations,
   postgresColumnCollation,
} from "./collation";

describe("duckdbTableCollations", () => {
   it("reads every column binary from a definition with no COLLATE", () => {
      const t = duckdbTableCollations(
         'CREATE TABLE t("name" VARCHAR, n INTEGER);',
      );
      expect(collationOf(t, "name")).toBe("binary");
      expect(collationOf(t, "n")).toBe("binary");
   });

   it("places a column's COLLATE, quoted or not, and reads the rest binary", () => {
      const t = duckdbTableCollations(
         'CREATE TABLE t("Name" VARCHAR COLLATE NOCASE, code VARCHAR COLLATE nfc, n INTEGER);',
      );
      expect(collationOf(t, "Name")).toBe("duckdb:nocase");
      expect(collationOf(t, "name")).toBe("duckdb:nocase");
      expect(collationOf(t, "code")).toBe("duckdb:nfc");
      expect(collationOf(t, "n")).toBe("binary");
   });

   it("reads every column unplaced when a COLLATE cannot be placed", () => {
      const t = duckdbTableCollations(
         "CREATE TABLE t(a VARCHAR, CHECK (a COLLATE nocase = 'x'));",
      );
      expect(collationOf(t, "a")).toBe("duckdb:unplaced");
   });
});

describe("the other engines' identities", () => {
   it("reads BigQuery's empty collation binary", () => {
      expect(bigqueryColumnCollation(null)).toBe("binary");
      expect(bigqueryColumnCollation("")).toBe("binary");
      expect(bigqueryColumnCollation("und:ci")).toBe("bigquery:und:ci");
   });

   it("reads a binary Postgres collation binary, else by identity", () => {
      expect(postgresColumnCollation("coll|c|C", "binary")).toBe("binary");
      expect(postgresColumnCollation("db|c|en_US", "deterministic")).toBe(
         "postgres:db|c|en_US",
      );
   });
});

describe("BigQuery's uncollated collation_name", () => {
   it("reads the catalog's literal 'NULL' binary, however it is spelled", () => {
      expect(bigqueryColumnCollation("NULL")).toBe("binary");
      for (const name of ["null", " Null ", "\tNULL\n", "", "  ", undefined]) {
         expect(bigqueryColumnCollation(name)).toBe("binary");
         expect(bigqueryNamesNoCollation(name)).toBe(true);
      }
      expect(bigqueryNamesNoCollation("und:ci")).toBe(false);
      expect(bigqueryColumnCollation(" und:ci ")).toBe("bigquery:und:ci");
   });

   it("reads a recorded bigquery:NULL binary, and every other identity as itself", () => {
      expect(canonicalCollation("bigquery:NULL")).toBe("binary");
      expect(canonicalCollation("bigquery:null")).toBe("binary");
      expect(canonicalCollation("bigquery:")).toBe("binary");
      expect(canonicalCollation("bigquery:und:ci")).toBe("bigquery:und:ci");
      expect(canonicalCollation("duckdb:nocase")).toBe("duckdb:nocase");
      expect(canonicalCollation("binary")).toBe("binary");
   });

   it("is the same rule as the catalog's SQL predicate", () => {
      expect(bigqueryCollatedSql("collation_name")).toBe(
         "UPPER(TRIM(COALESCE(collation_name, ''))) NOT IN ('', 'NULL')",
      );
   });
});

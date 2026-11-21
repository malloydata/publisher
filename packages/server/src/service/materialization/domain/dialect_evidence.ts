// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What backs each cell of the capability table (`dialect.ts`): a probe that
 * observes the real engine, or a reason the cell is taken on trust.
 *
 * Every capability has a safe value ({@link SAFE_VALUES}), the one a dialect
 * the table does not name gets: whatever asks a capability refuses, rebuilds
 * in full or answers live on it. A cell that claims more than that needs
 * evidence here, either `{ probe: "<spec path>#<test name>" }`, naming a test
 * that runs the engine in CI and asserts the cell, or `{ unverified:
 * "<reason>" }`. `dialect_evidence.spec.ts` holds the table to it: every cell
 * that differs from its safe value has an entry, every probe names a real
 * test, and each dialect's count of unverified cells is frozen, so a cell
 * moves from unverified to probed and never back without a reviewed change.
 * The docs table in `docs/materialization.md` is generated from this file.
 *
 * Data only.
 */

import type { Capabilities, DialectName } from "./dialect";

/** What backs one capability cell. */
export type Evidence =
   | { readonly probe: string }
   | { readonly unverified: string };

/**
 * Each capability's safe value: what an unknown dialect gets, and so what a
 * cell may claim with no evidence. The spec checks it is exactly the table's
 * answer for an unregistered dialect.
 */
export const SAFE_VALUES: Readonly<Capabilities> = Object.freeze({
   incrementalSourceRange: false,
   incrementalTargetDml: false,
   merge: false,
   mergeGatedByServerVersion: false,
   pairDelta: false,
   transactionalSwap: false,
   dependencyGuard: false,
   relationProbe: false,
   tableOwnership: false,
   needsQualifiedTable: false,
   nestedColumns: false,
   driverKeepsBigIntegers: false,
   liveRoundsBigIntegersMeasured: false,
   liveWritesFloat4Shortest: false,
   castsToIntegerType: false,
   onlyBigIntegerType: undefined,
   foldsTableNameCase: false,
   decodedTablePath: false,
   utcSessions: false,
   leafBudget: 50,
   passthroughNumericAsDouble: false,
   catalogHidesTables: false,
   stringCollation: undefined,
   ungroupableTypes: undefined,
   binary64Types: undefined,
});

const POSTGRES_SPEC = "tests/conformance/postgres.conformance.spec.ts";
const DUCKDB_SPEC = "tests/conformance/duckdb.conformance.spec.ts";
const MYSQL_SPEC = "tests/conformance/mysql.conformance.spec.ts";
const TRINO_SPEC = "tests/conformance/trino.conformance.spec.ts";

const probe = (spec: string, test: string): Evidence => ({
   probe: `${spec}#${test}`,
});
const pg = (test: string) => probe(POSTGRES_SPEC, test);
const duck = (test: string) => probe(DUCKDB_SPEC, test);
const my = (test: string) => probe(MYSQL_SPEC, test);
const trino = (test: string) => probe(TRINO_SPEC, test);

const NO_BIGQUERY = { unverified: "no BigQuery conformance spec" } as const;
const NO_SNOWFLAKE = { unverified: "no Snowflake conformance spec" } as const;
const NO_DATABRICKS = {
   unverified: "no Databricks engine in CI",
} as const;
const NO_PRESTO = { unverified: "no Presto engine in CI" } as const;
const NO_SQLSERVER = { unverified: "no SQL Server engine in CI" } as const;

/** Each dialect's evidence, by capability. */
export const EVIDENCE: Readonly<
   Record<DialectName, Readonly<Partial<Record<keyof Capabilities, Evidence>>>>
> = {
   duckdb: {
      incrementalTargetDml: duck(
         "incrementalTargetDml: a delete and insert apply in one transaction and roll back together",
      ),
      merge: duck("merge: MERGE INTO updates and inserts"),
      pairDelta: duck(
         "pairDelta: a parent's rows are replaced by key in one transaction",
      ),
      transactionalSwap: duck(
         "transactionalSwap: the swap's drop and rename roll back together",
      ),
      relationProbe: duck(
         "relationProbe: a table's comment reads back through the relation probe",
      ),
      tableOwnership: duck(
         "tableOwnership: the ownership mark's comment reads back with its location",
      ),
      nestedColumns: duck(
         "nestedColumns: a stored list and struct read back as built",
      ),
      driverKeepsBigIntegers: duck(
         "driverKeepsBigIntegers: a BIGINT past 2^53 keeps every digit",
      ),
      castsToIntegerType: duck("castsToIntegerType: CAST AS BIGINT"),
      foldsTableNameCase: duck(
         "foldsTableNameCase: a quoted name resolves case-insensitively",
      ),
      decodedTablePath: duck(
         "decodedTablePath: a bare and a quoted path find the table's columns through the target probe",
      ),
      utcSessions: duck("utcSessions: the session's TimeZone is UTC"),
      leafBudget: duck(
         "leafBudget: an identifier of the budget plus the build's suffix is kept whole",
      ),
      stringCollation: duck(
         "stringCollation: comparisons are byte-exact unless default_collation is set",
      ),
      binary64Types: duck(
         "binary64Types: DOUBLE and FLOAT8 are binary64 with NaN equal to itself and above all",
      ),
   },
   postgres: {
      stringCollation: pg(
         "stringCollation: pg_collation_for reads each column's collation",
      ),
      passthroughNumericAsDouble: pg(
         "passthroughNumericAsDouble: DuckDB's postgres_query reads an unconstrained numeric as DOUBLE",
      ),
      incrementalSourceRange: pg(
         "incrementalSourceRange: a bounded range over every watermark type's literal",
      ),
      incrementalTargetDml: pg(
         "incrementalTargetDml: a delete and insert apply in one transaction and roll back together",
      ),
      merge: pg("merge: MERGE INTO updates and inserts"),
      mergeGatedByServerVersion: pg(
         "mergeGatedByServerVersion: server_version_num is read, and MERGE exists from 15",
      ),
      decodedTablePath: pg(
         "decodedTablePath: a bare and a quoted path find the table's columns through the target probe",
      ),
      transactionalSwap: pg(
         "transactionalSwap: the swap's drop and rename roll back together",
      ),
      dependencyGuard: pg(
         "dependencyGuard: a view's dependency on its table is read from the catalog",
      ),
      relationProbe: pg(
         "relationProbe: a table's comment reads back through the relation probe",
      ),
      tableOwnership: pg(
         "tableOwnership: the ownership mark's comment reads back with its location",
      ),
      castsToIntegerType: pg("castsToIntegerType: CAST AS BIGINT"),
      leafBudget: pg(
         "leafBudget: an identifier past 63 bytes is truncated, so a leaf has 63 less the build's 13",
      ),
      liveRoundsBigIntegersMeasured: pg(
         "liveRoundsBigIntegersMeasured: the driver's JSON stage rounds a bigint past 2^53 to the nearest double",
      ),
      liveWritesFloat4Shortest: pg(
         "liveWritesFloat4Shortest: the driver's JSON stage writes a float4 as its shortest text",
      ),
      binary64Types: pg(
         "binary64Types: format_type spells float8 and float(25..53) as double precision",
      ),
      ungroupableTypes: pg(
         "ungroupableTypes: each listed native type fails GROUP BY, and jsonb groups",
      ),
   },
   standardsql: {
      decodedTablePath: NO_BIGQUERY,
      incrementalSourceRange: NO_BIGQUERY,
      incrementalTargetDml: NO_BIGQUERY,
      merge: NO_BIGQUERY,
      relationProbe: NO_BIGQUERY,
      tableOwnership: NO_BIGQUERY,
      needsQualifiedTable: NO_BIGQUERY,
      castsToIntegerType: NO_BIGQUERY,
      onlyBigIntegerType: NO_BIGQUERY,
      leafBudget: NO_BIGQUERY,
      stringCollation: NO_BIGQUERY,
      ungroupableTypes: NO_BIGQUERY,
   },
   snowflake: {
      catalogHidesTables: NO_SNOWFLAKE,
      decodedTablePath: NO_SNOWFLAKE,
      incrementalSourceRange: NO_SNOWFLAKE,
      incrementalTargetDml: NO_SNOWFLAKE,
      merge: NO_SNOWFLAKE,
      relationProbe: NO_SNOWFLAKE,
      tableOwnership: NO_SNOWFLAKE,
      castsToIntegerType: NO_SNOWFLAKE,
      leafBudget: NO_SNOWFLAKE,
      binary64Types: NO_SNOWFLAKE,
   },
   databricks: {
      relationProbe: NO_DATABRICKS,
      tableOwnership: NO_DATABRICKS,
      castsToIntegerType: NO_DATABRICKS,
      foldsTableNameCase: NO_DATABRICKS,
      leafBudget: NO_DATABRICKS,
   },
   trino: {
      relationProbe: trino(
         "relationProbe: a table's comment reads back through the relation probe",
      ),
      tableOwnership: trino(
         "tableOwnership: the ownership mark's comment reads back with its location",
      ),
      castsToIntegerType: trino("castsToIntegerType: CAST AS BIGINT"),
      foldsTableNameCase: trino(
         "foldsTableNameCase: a quoted name resolves case-insensitively",
      ),
   },
   presto: {
      tableOwnership: NO_PRESTO,
      castsToIntegerType: NO_PRESTO,
      foldsTableNameCase: NO_PRESTO,
   },
   mysql: {
      catalogHidesTables: my(
         "catalogHidesTables: a table the session's grants do not cover is missing from its catalog",
      ),
      tableOwnership: my(
         "tableOwnership: the ownership mark's comment reads back with its location",
      ),
      castsToIntegerType: my("castsToIntegerType: CAST names no integer type"),
   },
   sqlserver: {
      relationProbe: NO_SQLSERVER,
      tableOwnership: NO_SQLSERVER,
      castsToIntegerType: NO_SQLSERVER,
      foldsTableNameCase: NO_SQLSERVER,
      leafBudget: NO_SQLSERVER,
   },
};

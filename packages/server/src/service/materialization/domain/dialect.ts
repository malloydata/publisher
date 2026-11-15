// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What each warehouse dialect can do, as one table: the capability flags plan,
 * run and serve decide by, keyed by Malloy's `dialectName` (so BigQuery is
 * `standardsql`, as in `quoting.ts`; no source's `dialectName` is ever
 * "bigquery", so keying by the friendly name would reject every BigQuery
 * source).
 *
 * Flags only: whether a dialect can, not the SQL it is asked in. The text that
 * differs by dialect is the table's other half, `run/warehouse_sql.ts`.
 *
 * An allowlist, not a probe: a dialect this table does not name has no
 * capability, so whatever asks refuses it or rebuilds in full. Adding a dialect
 * is a row here, and the record is total, so a missing flag does not compile.
 *
 * Not here: whether a dialect can hold a table at all. That is a deny-list
 * (`READ_ONLY_DIALECTS` in `plan/materialization_eligibility.ts`, and a
 * connection type's `canPersist` in `connection_config.ts`), so an unknown
 * dialect is writable; as a capability here it would turn every unknown
 * dialect read-only. A new read-only dialect is added there too.
 *
 * Every row is frozen: a dialect's capabilities change only here.
 */

/** Every dialect a decision branches on: Malloy's registered dialects. */
export type DialectName =
   | "duckdb"
   | "postgres"
   | "standardsql"
   | "snowflake"
   | "databricks"
   | "trino"
   | "presto"
   | "mysql"
   | "sqlserver";

/** One dialect's capability flags. */
export interface Capabilities {
   /**
    * A bounded watermark range can be rendered and probed FOR it, the source
    * side of an incremental refresh: it has literal spellings for every
    * watermark type and a decodable table path.
    */
   incrementalSourceRange: boolean;
   /**
    * Its transactional delta apply is proven, the target side, where the DML
    * lands. DuckDB only as a `storage=` destination; a DuckDB source is never
    * materialized.
    */
   incrementalTargetDml: boolean;
   /** It has a `MERGE INTO` statement, which `merge_key=` needs. */
   merge: boolean;
   /**
    * Its `MERGE` exists only from a server version on (Postgres 15), so a
    * merge refresh probes the server's version first.
    */
   mergeGatedByServerVersion: boolean;
   /**
    * A pair table's delta, which replaces whole parents by their key, can be
    * applied in it: DuckDB, the target of every `storage=` table.
    */
   pairDelta: boolean;
   /**
    * DDL is transactional and a swap reaches one session, so a build's swap
    * (and a leftover `name=` view's drop with it) is one transaction. Not
    * Snowflake, MySQL, Databricks or Trino, which commit DDL as it runs, nor
    * BigQuery, which refuses DROP and RENAME in a transaction, nor SQL
    * Server, whose transactional DDL does not cover this rename.
    */
   transactionalSwap: boolean;
   /**
    * The in-transaction guard on a leftover `name=` view reads the view's
    * dependencies from the catalog, not its definition's text.
    */
   dependencyGuard: boolean;
   /**
    * Its relations' comments can be read back by the shared relation probe.
    * MySQL and Presto are probed by the ownership mark's own queries.
    */
   relationProbe: boolean;
   /** What holds a table's name can be read, and the table marked as owned. */
   tableOwnership: boolean;
   /**
    * A table needs a container its connection does not give it (BigQuery: a
    * dataset), which only a `name=` can supply.
    */
   needsQualifiedTable: boolean;
   /**
    * A stored table can hold nested (array and record) columns that Malloy
    * reads back as it built them.
    */
   nestedColumns: boolean;
   /**
    * Its driver returns a 64-bit integer with every digit, whatever Malloy's
    * dialect says (DuckDB inherits `supportsBigIntPrecision: false` from its
    * Postgres base, yet keeps every digit).
    */
   driverKeepsBigIntegers: boolean;
   /**
    * Its `CAST` takes an integer type by its catalog name. Not MySQL, whose
    * `CAST` names no integer type (`CAST(x AS bigint)` is a syntax error).
    */
   castsToIntegerType: boolean;
   /**
    * Its one type Malloy reads as a `bigint` number, when it has exactly one
    * (BigQuery's INT64): a column whose field is one is that type, captured
    * or not. Undefined: none is inferred.
    */
   onlyBigIntegerType: string | undefined;
   /**
    * Table names resolve case-insensitively, quoted or not (SQL Server under
    * its default collation). Not MySQL, where it depends on the server's
    * `lower_case_table_names`, which Publisher cannot see.
    */
   foldsTableNameCase: boolean;
}

const NONE: Capabilities = Object.freeze({
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
   castsToIntegerType: false,
   onlyBigIntegerType: undefined,
   foldsTableNameCase: false,
});

/** Each dialect's capabilities. */
export const CAPABILITIES: Record<DialectName, Capabilities> = {
   duckdb: Object.freeze({
      ...NONE,
      incrementalTargetDml: true,
      merge: true,
      pairDelta: true,
      transactionalSwap: true,
      relationProbe: true,
      tableOwnership: true,
      nestedColumns: true,
      driverKeepsBigIntegers: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
   }),
   postgres: Object.freeze({
      ...NONE,
      incrementalSourceRange: true,
      incrementalTargetDml: true,
      merge: true,
      mergeGatedByServerVersion: true,
      transactionalSwap: true,
      dependencyGuard: true,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
   }),
   standardsql: Object.freeze({
      ...NONE,
      incrementalSourceRange: true,
      incrementalTargetDml: true,
      merge: true,
      relationProbe: true,
      tableOwnership: true,
      needsQualifiedTable: true,
      castsToIntegerType: true,
      onlyBigIntegerType: "INT64",
   }),
   snowflake: Object.freeze({
      ...NONE,
      incrementalSourceRange: true,
      incrementalTargetDml: true,
      merge: true,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
   }),
   databricks: Object.freeze({
      ...NONE,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
   }),
   trino: Object.freeze({
      ...NONE,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
   }),
   presto: Object.freeze({
      ...NONE,
      tableOwnership: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
   }),
   mysql: Object.freeze({
      ...NONE,
      tableOwnership: true,
   }),
   sqlserver: Object.freeze({
      ...NONE,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
   }),
};

/** A dialect's capabilities; none for a dialect the table does not name. */
export function capabilitiesOf(dialect: unknown): Capabilities {
   return isDialectName(dialect) ? CAPABILITIES[dialect] : NONE;
}

function isDialectName(name: unknown): name is DialectName {
   return typeof name === "string" && Object.hasOwn(CAPABILITIES, name);
}

/** The dialects that have a capability. */
export function dialectsWith(
   capability: keyof Capabilities,
): ReadonlySet<DialectName> {
   return new Set(
      (Object.keys(CAPABILITIES) as DialectName[]).filter(
         (d) => CAPABILITIES[d][capability],
      ),
   );
}

/** Malloy `dialectName` -> the name an author would recognize, for a message. */
const DIALECT_DISPLAY_NAMES: Record<string, string> = {
   standardsql: "standardsql (BigQuery)",
};

/** A dialect set as message text, e.g. `postgres, standardsql (BigQuery)`. */
export function describeDialects(dialects: ReadonlySet<string>): string {
   return [...dialects]
      .sort()
      .map((d) => DIALECT_DISPLAY_NAMES[d] ?? d)
      .join(", ");
}

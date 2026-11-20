// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What each warehouse dialect can do, as one table: the capability flags plan,
 * run and serve decide by, keyed by Malloy's `dialectName` (so BigQuery is
 * `standardsql`, as in `quoting.ts`; no source's `dialectName` is ever
 * "bigquery", so keying by the friendly name would reject every BigQuery
 * source).
 *
 * Capabilities only: whether a dialect can, and how much (its identifier
 * budget), not the SQL it is asked in. The text that differs by dialect is
 * the table's other half, `run/warehouse_sql.ts`.
 *
 * An allowlist, not a probe: a dialect this table does not name has no
 * capability (and the least leaf budget), so whatever asks refuses it or
 * rebuilds in full. Adding a dialect
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

/**
 * The dialect every storage destination speaks (DuckDB and DuckLake are both
 * DuckDB-family): the dialect of a stored table's DDL, DML and identifier
 * quoting, which is NOT its source's.
 */
export const STORAGE_DIALECT: DialectName = "duckdb";

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
    * Its live answer rounds a 64-bit integer past 2^53 through JSON in the
    * way measured (to the nearest double, a BigInt below 1e21 and a number
    * from it), so a `storage=` answer is read exactly and rounded the same
    * way after it runs (`live_big_integers.ts`). Postgres alone: the other
    * dialects whose live answer rounds are unmeasured.
    */
   liveRoundsBigIntegersMeasured: boolean;
   /**
    * Its live answer writes a float4 as the shortest text that reads back as
    * it (Postgres's `float4out`, through its JSON stage), so a `storage=`
    * answer's float4 values, which a DuckDB read widens to a double, are
    * written the same way after it runs (`live_float4.ts`).
    */
   liveWritesFloat4Shortest: boolean;
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
   /**
    * Publisher's sessions on it run in UTC, so a stored timestamp reads alike
    * with or without its zone: a column altered from one to the other after
    * its build is no drift (DuckDB). Elsewhere the session's zone is the
    * warehouse's, which Publisher cannot see.
    */
   utcSessions: boolean;
   /**
    * The longest leaf a table Publisher names itself may have
    * (`domain/table_name.ts`): its identifier limit less the 13 characters a
    * build appends for its staging and aside copies, capped at 120 for
    * readability. 50 for Postgres (and Redshift, which connects as
    * `postgres`), MySQL, Trino and Presto (whose limit is the catalog's behind
    * it, 63 or 64 over Postgres or MySQL), and for a dialect this table does
    * not name; 128 less 13 for SQL Server; 120 for the rest.
    */
   leafBudget: number;
   /**
    * A `storage=` build's DuckDB passthrough reads some of its exact decimals
    * as DOUBLE (Postgres: an unconstrained `numeric`, a `numeric(p,s)` with
    * p > 38, and every numeric expression), so the build reads those columns
    * as the warehouse's text and stores the DECIMAL that holds them
    * (`run/postgres_numeric_precision.ts`). BigQuery's NUMERIC and
    * Snowflake's NUMBER already arrive as DECIMAL.
    */
   passthroughNumericAsDouble: boolean;
   /**
    * A table the session's grants do not show is missing from its catalog
    * views (Snowflake, MySQL), so a probe that finds none cannot tell a
    * dropped table from a hidden one.
    */
   catalogHidesTables: boolean;
   /**
    * How a string column's comparison is read, for a gate or a `where:` a
    * `storage=` copy re-applies byte-exact (`serve/gate_collation.ts`):
    * `binary` (always byte-exact), `instance` (byte-exact unless the DuckDB
    * instance sets a `default_collation` or a `COLLATE` is written), `column`
    * (the column's own collation, read with Postgres's `pg_collation_for`);
    * absent when it
    * cannot be read, which keeps no gate over a string column there.
    */
   stringCollation: "binary" | "instance" | "column" | undefined;
   /**
    * The column types its `GROUP BY` refuses. `malloy`: Malloy types
    * (`json`, `array`, `record`). `native`: a `sql native` field's raw type
    * in lower case (BigQuery's `geography`, Postgres's `json`, which Malloy
    * reads as native beside `jsonb`, which groups), and an array of one. A
    * pair table's build groups by every stored column, so a pair table
    * holding one is refused at plan. Undefined: none is known.
    */
   ungroupableTypes:
      | { malloy: readonly string[]; native: readonly string[] }
      | undefined;
   /**
    * Its native type names (as its catalog spells them, compared whole,
    * trimmed and case-blind) that are an IEEE 754 binary64 whose comparisons are
    * DuckDB's: NaN equal to itself and above every other value. A
    * `storage=` copy holds such a column as a DuckDB DOUBLE bit for bit, so a
    * gate or a scope compares the copy as live compares the column
    * (`domain/gate_policy.ts`). Not BigQuery's FLOAT64, whose comparisons
    * with NaN are false, nor a 4-byte float (Postgres's and DuckDB's
    * `real`), whose comparison with a decimal literal rounds the literal to
    * a 4-byte float live and not on the copy. Undefined: none.
    */
   binary64Types: readonly string[] | undefined;
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
   liveRoundsBigIntegersMeasured: false,
   liveWritesFloat4Shortest: false,
   castsToIntegerType: false,
   onlyBigIntegerType: undefined,
   foldsTableNameCase: false,
   utcSessions: false,
   leafBudget: 50,
   passthroughNumericAsDouble: false,
   catalogHidesTables: false,
   stringCollation: undefined,
   ungroupableTypes: undefined,
   binary64Types: undefined,
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
      utcSessions: true,
      leafBudget: 120,
      stringCollation: "instance",
      binary64Types: ["DOUBLE", "FLOAT8"],
   }),
   postgres: Object.freeze({
      ...NONE,
      stringCollation: "column",
      passthroughNumericAsDouble: true,
      incrementalSourceRange: true,
      incrementalTargetDml: true,
      merge: true,
      mergeGatedByServerVersion: true,
      transactionalSwap: true,
      dependencyGuard: true,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      liveRoundsBigIntegersMeasured: true,
      liveWritesFloat4Shortest: true,
      // `format_type` spells float8 and float(25..53) so.
      binary64Types: ["DOUBLE PRECISION"],
      // No equality operator: "could not identify an equality operator for
      // type json" (and its arrays). `jsonb` groups.
      ungroupableTypes: {
         malloy: [],
         native: [
            "json",
            "xml",
            "point",
            "line",
            "lseg",
            "box",
            "path",
            "polygon",
            "circle",
            "txid_snapshot",
            "pg_snapshot",
         ],
      },
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
      leafBudget: 120,
      stringCollation: "binary",
      ungroupableTypes: {
         malloy: ["json", "array", "record"],
         native: ["geography"],
      },
   }),
   snowflake: Object.freeze({
      ...NONE,
      catalogHidesTables: true,
      incrementalSourceRange: true,
      incrementalTargetDml: true,
      merge: true,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      leafBudget: 120,
      // Every Snowflake float type is a double; its catalog names each
      // `FLOAT`.
      binary64Types: [
         "FLOAT",
         "FLOAT4",
         "FLOAT8",
         "DOUBLE",
         "DOUBLE PRECISION",
         "REAL",
      ],
   }),
   databricks: Object.freeze({
      ...NONE,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
      leafBudget: 120,
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
      catalogHidesTables: true,
      tableOwnership: true,
   }),
   sqlserver: Object.freeze({
      ...NONE,
      relationProbe: true,
      tableOwnership: true,
      castsToIntegerType: true,
      foldsTableNameCase: true,
      leafBudget: 115,
   }),
};

/**
 * Whether `nativeType`, a column's type as `dialect`'s catalog names it, is
 * one of its {@link Capabilities.binary64Types}.
 */
export function isBinary64Type(dialect: unknown, nativeType: string): boolean {
   const named = nativeType.trim().replace(/\s+/g, " ").toUpperCase();
   return capabilitiesOf(dialect).binary64Types?.includes(named) === true;
}

/** A dialect's capabilities; none for a dialect the table does not name. */
export function capabilitiesOf(dialect: unknown): Capabilities {
   const name = registeredDialect(dialect);
   return name === undefined ? NONE : CAPABILITIES[name];
}

/**
 * `dialect` as a registered dialect, or undefined for one this table does not
 * name. A caller holding a connection's own `dialectName` (a string) narrows
 * it here once, so its branches compare a `DialectName` and a switch over it
 * is checked for a missing case.
 */
export function registeredDialect(dialect: unknown): DialectName | undefined {
   return typeof dialect === "string" && Object.hasOwn(CAPABILITIES, dialect)
      ? (dialect as DialectName)
      : undefined;
}

/** The dialects that have a capability (any but the leaf budget, a number). */
export function dialectsWith(
   capability: Exclude<keyof Capabilities, "leafBudget">,
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

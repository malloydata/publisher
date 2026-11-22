// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Loading a file into a DuckLake destination as a table
 * ({@link loadStorageTable}), copying a table between catalogs for a
 * migration ({@link copyStorageTable}), and applying Parquet objects to a table
 * by key under a schema contract ({@link mergeStorageTable}), with the reader
 * SQL and the refusals around them. Read by catalog_admin.ts.
 *
 * Effects: one DuckLake transaction per table on a transient read-write
 * session. A load reads only under the destination's data path, and renders
 * a caller's reader options as typed literals, never spliced text.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import path from "path";
import { components } from "../../../api";
import { duckDBByteSize } from "../../../config";
import {
   BadRequestError,
   ConflictError,
   logInternalFailure,
} from "../../../errors";
import {
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
} from "../../connection";
import {
   type PartitionKey,
   recordedCatalogSettings,
   stripTrailingSlashes,
   tablePartitionKeys,
} from "../../dialects/ducklake_catalog";
import { runInternalSQL } from "../../duckdb_lockdown";
import { escapeSQL, installAndLoadExtension } from "../../duckdb_session_setup";
import {
   applyLoadSessionSettings,
   createLocalDataPaths,
   inTransaction,
   isCatalogDestination,
   lockDownDestinationSession,
   pinSessionToUTC,
   withIsolatedSession,
   writePartitionedTable,
} from "./materialization_build_session";
import { nullSafeKeyMatch } from "./incremental_apply";
import { quoteIdentifier } from "../../quoting";
import { STORAGE_DIALECT } from "../domain/dialect";
import { losslessWidening } from "../domain/lossless_widening";

type ApiConnection = components["schemas"]["Connection"];

export type LoadFormat = "csv" | "parquet" | "xlsx" | "json";

/**
 * Reader options a caller may pass, each typed and rendered as a literal: an
 * allowlist, never a string spliced into SQL.
 */
export interface LoadOptions {
   header?: boolean;
   delimiter?: string;
   sheet?: string;
   skip?: number;
}

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** A `k=v` directory above an object would otherwise replace its column k with the path's text; read_xlsx has no such parameter. */
const NO_HIVE = "hive_partitioning = false";

/** The reader call for one object, with only allowlisted, typed options. */
export function readerSql(
   uri: string,
   format: LoadFormat,
   options: LoadOptions = {},
): string {
   const path = `'${escapeSQL(uri)}'`;
   const args: string[] = [];
   if (format === "csv") {
      if (options.header !== undefined) {
         args.push(`header = ${options.header ? "true" : "false"}`);
      }
      if (options.delimiter !== undefined) {
         if (
            typeof options.delimiter !== "string" ||
            options.delimiter.length !== 1
         ) {
            throw new BadRequestError(
               "'delimiter' must be a single character.",
            );
         }
         args.push(`delim = '${escapeSQL(options.delimiter)}'`);
      }
      if (options.skip !== undefined) {
         if (!Number.isInteger(options.skip) || options.skip < 0) {
            throw new BadRequestError("'skip' must be a non-negative integer.");
         }
         args.push(`skip = ${options.skip}`);
      }
      return `read_csv(${[path, ...args, NO_HIVE].join(", ")})`;
   }
   if (format === "xlsx") {
      if (options.header !== undefined) {
         args.push(`header = ${options.header ? "true" : "false"}`);
      }
      if (options.sheet !== undefined) {
         if (typeof options.sheet !== "string" || options.sheet.length === 0) {
            throw new BadRequestError("'sheet' must be a sheet name.");
         }
         args.push(`sheet = '${escapeSQL(options.sheet)}'`);
      }
      return `read_xlsx(${[path, ...args].join(", ")})`;
   }
   if (format === "json") {
      return `read_json_auto(${[path, NO_HIVE].join(", ")})`;
   }
   return `read_parquet(${[path, NO_HIVE].join(", ")})`;
}

/**
 * Refuses an object outside the catalog's data path: the session holds only
 * that location's credential, and a load may read nothing else. A sibling
 * prefix that merely shares the string (`…/ds_a` vs `…/ds_ab`) is outside.
 */
export function assertUnderDataPath(uri: string, dataPath: string): void {
   const root = stripTrailingSlashes(dataPath);
   const outside = () =>
      new BadRequestError(
         `The object must sit under the catalog's data path (${root}/).`,
      );
   if (!root || !uri.startsWith(`${root}/`)) throw outside();
   // A prefix match is only a containment check when nothing in the rest can
   // climb or fan out: DuckDB resolves `..` in a local path, and its readers
   // expand glob patterns, so either would read outside the prefix through a
   // string that starts inside it.
   const rest = uri.slice(root.length + 1);
   if (
      rest.length === 0 ||
      /[\\*?[\]{}]/.test(rest) ||
      rest
         .split("/")
         .some((part) => part === "" || part === "." || part === "..")
   ) {
      throw outside();
   }
   // A local path is also resolved, so no spelling of it lands elsewhere.
   if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(uri)) {
      const resolved = path.resolve(uri);
      if (!resolved.startsWith(`${path.resolve(root)}${path.sep}`)) {
         throw outside();
      }
   }
}

/**
 * Loads one object into a table of a DuckLake destination, for an orchestrator
 * that stages a file in the destination's own bucket.
 *
 * - The object must sit under the destination's `bucketUrl`: the session holds
 *   only that bucket's credential, and a load may read nothing else.
 * - The bytes are rewritten as the catalog's own Parquet, never registered in
 *   place, so an encrypted catalog stays encrypted throughout.
 * - One DuckLake transaction: a reader sees the old table or the new one.
 *   `create` refuses a table that exists (409); `replace` swaps it.
 * - A transient session with the build session's teardown, so it strands no
 *   catalog backend.
 */
export async function loadStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   tableName: string;
   uri: string;
   format: LoadFormat;
   options?: LoadOptions;
   mode: "create" | "replace";
}): Promise<{ rowCount: number; columns: { name: string; type: string }[] }> {
   const reader = readerSql(params.uri, params.format, params.options);
   return withLoadSession(
      {
         ...params,
         purpose: "load",
         uris: [params.uri],
         extensions: params.format === "xlsx" ? ["excel"] : [],
      },
      (session) =>
         writeTableAs(session, params, reader, (error) =>
            readerRefusal(error, params.uri),
         ),
   );
}

/**
 * The session an admin table write that reads objects runs on: an isolated
 * session with the load settings, the destination attached read-write, every
 * object checked against the destination's data path (configured, or recorded by
 * the attach), `extensions` loaded, and the session confined to that path
 * (duckdb_lockdown.ts) before `fn` runs. The reader takes caller options, so
 * nothing outside the data path may be read.
 */
async function withLoadSession<T>(
   params: {
      purpose: string;
      destinationName: string;
      destinationConnection: ApiConnection;
      schemaName: string;
      tableName: string;
      uris: readonly string[];
      extensions?: string[];
   },
   fn: (session: DuckDBConnection) => Promise<T>,
): Promise<T> {
   const { destinationName, destinationConnection, schemaName, tableName } =
      params;
   if (
      !isCatalogDestination(destinationConnection) ||
      !destinationConnection.ducklakeConnection
   ) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is not DuckLake; only a DuckLake catalog takes a load.`,
      );
   }
   if (!IDENT.test(schemaName) || !IDENT.test(tableName)) {
      throw new BadRequestError(
         "Schema and table names must be plain identifiers.",
      );
   }
   // A configured data path is checked before anything attaches; with none,
   // the catalog's recorded path is checked once it is attached.
   const ducklake = destinationConnection.ducklakeConnection;
   const configuredPath = ducklake.storage?.bucketUrl;
   if (configuredPath) {
      for (const uri of params.uris) assertUnderDataPath(uri, configuredPath);
   }

   return withIsolatedSession(
      `${params.purpose}_${destinationName}`,
      async (session) => {
         await applyLoadSessionSettings(session);
         await attachDuckLakeReadWrite(session, destinationName, ducklake);
         if (!configuredPath) {
            const recorded = await recordedCatalogSettings(
               session,
               destinationName,
               ducklake.catalog?.metadataSchema,
            );
            for (const uri of params.uris) {
               assertUnderDataPath(uri, recorded.data_path ?? "");
            }
         }
         for (const extension of params.extensions ?? []) {
            await installAndLoadExtension(session, extension);
         }
         const allow = configuredPath ? [configuredPath] : [];
         // The load writes the destination's table files.
         createLocalDataPaths(session, allow);
         await lockDownDestinationSession(session, allow);
         return fn(session);
      },
   );
}

/**
 * What a reader's failure says of the objects it read: `missing` (nothing at
 * the URI) or `content` (not Parquet, or a value its column's type cannot hold),
 * neither of which a retry cures. Null for anything else -- a storage error, a
 * refused credential, a full spill disk -- which a retry may cure, so it stays a 500.
 * A missing local file counts only when the message names one of `uris`, since a
 * spill file can go missing too.
 */
function readerFault(
   error: Error,
   uris: readonly string[],
): "missing" | "content" | null {
   const message = error.message;
   if (
      /^(Invalid Input|Conversion) Error:|^Invalid Error: TProtocolException/.test(
         message,
      )
   ) {
      return "content";
   }
   if (!/^(IO|HTTP) Error:/.test(message)) return null;
   if (
      /No files found that match the pattern|\bHTTP 404\b|\b404 \(Not Found\)/.test(
         message,
      ) ||
      (/No such file or directory/.test(message) &&
         uris.some((uri) => message.includes(uri)))
   ) {
      return "missing";
   }
   return null;
}

/** Where DuckDB starts echoing the statement it failed, which names every object it read. */
const ECHOED_SQL = /\n\s*LINE \d+:/;

/** An object address in a message, as httpfs may respell it (`https://storage.googleapis.com/…`, percent-encoded). */
const OBJECT_ADDRESS = /\b(?:gs|s3|gcs|r2|az|abfss|https?):\/\/[^\s'"]+/g;

/**
 * DuckDB's words for a reader's failure, each URI replaced by its label and the
 * SQL it echoes after `LINE` dropped. When no sent URI appears, the reader
 * respelled it, so any object address left is replaced by a generic label
 * rather than shown.
 */
function readerWords(
   error: Error,
   labels: readonly (readonly [uri: string, label: string])[],
): string {
   let words = error.message.split(ECHOED_SQL)[0];
   let named = false;
   // Longest first, so a URI that prefixes another never splits it.
   for (const [uri, label] of [...labels].sort(
      ([a], [b]) => b.length - a.length,
   )) {
      if (words.includes(uri)) named = true;
      words = words.split(uri).join(label);
   }
   if (!named) words = words.replace(OBJECT_ADDRESS, "the object");
   return words.trim();
}

/**
 * A load the file reader refused, as the person who sent the file should read it: a 400
 * carrying DuckDB's own words ("No magic bytes found", `Could not convert string "N/A" to
 * 'DOUBLE'`), which name the fix, rather than a 500 a caller can only retry. The object's URI,
 * an internal staging path, is replaced by its file name, and the SQL DuckDB echoes after
 * `LINE` is dropped. A missing object is refused the same way, so a 500 is always one a retry
 * may cure. Null for anything else (a storage or catalog failure), which stays a 500.
 *
 * Applied only to the `CREATE TABLE ... AS SELECT` that runs the reader (see
 * {@link writeTableAs}): the flush, count and describe after it run against the
 * catalog, not the file, so their failures stay 500 whatever their prefix.
 *
 * A 400 is not logged on its own, and the route logs only the message returned, so
 * the original -- the staging path and the SQL -- is logged here at warn for the
 * operator reading an odd 400.
 */
export function readerRefusal(
   error: unknown,
   uri: string,
): BadRequestError | null {
   if (!(error instanceof Error) || readerFault(error, [uri]) === null) {
      return null;
   }
   logInternalFailure("Load refused by the file reader", error, "warn");
   return new BadRequestError(
      readerWords(error, [[uri, path.posix.basename(uri)]]),
   );
}

/**
 * Copies a table from one DuckLake catalog into a table of another (or the
 * same) catalog, both in the same catalog database: a migration's copy, run on
 * the bytes the source catalog already holds.
 *
 * - A source in another catalog is attached read-only and the destination
 *   read-write, each as its own owner; a source within the destination's own
 *   catalog is read through the destination's attach. The rows are rewritten
 *   as the destination's own Parquet, so an encrypted destination stays
 *   encrypted whatever the source is.
 * - The same table guarantees as {@link loadStorageTable}: one DuckLake
 *   transaction, `create` refusing a table that exists (409), a transient
 *   session with the build session's teardown.
 * - A partitioned source (one an earlier release's `partition=` build wrote) is
 *   written with its partition spec and in partition order, and the spec is
 *   returned. A partitioned table copied onto itself is refused (400).
 *   Copied with `replace` onto a table an incremental build manages, the
 *   spec stays until that build's next full refresh, and its deltas into it
 *   run without the copy's memory bounds.
 * - The caller checks that the source exists and that the destination may
 *   hold its rows before calling: this attaches and writes.
 *
 * Exists for one migration (legacy per-organization catalogs into per-database
 * ones); remove it with the `table` source once those catalogs are dropped.
 */
export async function copyStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   tableName: string;
   source: {
      catalogName: string;
      connection: ApiConnection;
      schemaName: string;
      tableName: string;
   };
   mode: "create" | "replace";
}): Promise<{
   rowCount: number;
   columns: { name: string; type: string }[];
   partitionedBy: PartitionKey[];
}> {
   const { destinationName, destinationConnection, source } = params;
   for (const conn of [destinationConnection, source.connection]) {
      if (!isCatalogDestination(conn) || !conn.ducklakeConnection) {
         throw new BadRequestError(
            "Only a DuckLake catalog is a copy's source or destination.",
         );
      }
   }
   for (const part of [
      params.schemaName,
      params.tableName,
      source.schemaName,
      source.tableName,
   ]) {
      if (!IDENT.test(part)) {
         throw new BadRequestError(
            "Schema and table names must be plain identifiers.",
         );
      }
   }
   const sameCatalog = source.catalogName === destinationName;
   // Attached under its own alias, so it never shadows the destination: the
   // attach is ATTACH OR REPLACE, so a destination catalog named like the alias
   // would otherwise be replaced by the read-only source.
   const sourceAlias = sameCatalog
      ? destinationName
      : destinationName === COPY_SOURCE_ALIAS
        ? `${COPY_SOURCE_ALIAS}_2`
        : COPY_SOURCE_ALIAS;

   return withIsolatedSession(`copy_${destinationName}`, async (session) => {
      await applyLoadSessionSettings(session);
      await attachDuckLakeReadWrite(
         session,
         destinationName,
         destinationConnection.ducklakeConnection!,
      );
      // The destination's own data path, before the read-only source is
      // attached: only the catalog written into is created.
      const destinationPath =
         destinationConnection.ducklakeConnection?.storage?.bucketUrl;
      createLocalDataPaths(session, destinationPath ? [destinationPath] : []);
      if (!sameCatalog) {
         await attachDuckLakeReadOnly(
            session,
            sourceAlias,
            source.connection.ducklakeConnection!,
         );
      }
      // Confined to the two catalogs' data paths (configured, or recorded by
      // the attaches) before it reads or writes (duckdb_lockdown.ts).
      await lockDownDestinationSession(
         session,
         [destinationConnection, source.connection].flatMap((conn) => {
            const dataPath = conn.ducklakeConnection?.storage?.bucketUrl;
            return dataPath ? [dataPath] : [];
         }),
      );
      const from = [sourceAlias, source.schemaName, source.tableName]
         .map((part) => quoteIdentifier(part, STORAGE_DIALECT))
         .join(".");
      // The copy keeps the source's layout: DuckLake applies a partition spec
      // only to files written after it is set, so a copy written without it
      // stays unpartitioned until something rewrites it.
      const partitionedBy = await tablePartitionKeys(
         session,
         sourceAlias,
         source.connection.ducklakeConnection!.catalog?.metadataSchema,
         source.schemaName,
         source.tableName,
      );
      if (
         partitionedBy.length > 0 &&
         sameCatalog &&
         fold(source.schemaName) === fold(params.schemaName) &&
         fold(source.tableName) === fold(params.tableName)
      ) {
         // The partitioned write replaces the table empty before it reads the
         // source, so onto itself it would commit zero rows. DuckDB resolves
         // names case-insensitively, so `Events` onto `events` is itself too.
         throw new BadRequestError(
            `Table '${source.schemaName}.${source.tableName}' is partitioned and cannot ` +
               "be copied onto itself; copy it to another name.",
         );
      }
      return {
         ...(await writeTableAs(
            session,
            params,
            from,
            undefined,
            partitionedBy,
         )),
         partitionedBy,
      };
   });
}

/** The alias a copy attaches its source catalog under. */
const COPY_SOURCE_ALIAS = "__copy_source";

/**
 * `CREATE [OR REPLACE] TABLE <destination>.<schema>.<table> AS SELECT * FROM
 * <from>` on a session that has the destination attached read-write, with
 * inlining off, and the table's row count and columns after.
 *
 * `refused` classifies a failure of that statement alone: the error it returns
 * is thrown in place of the original (the load passes {@link readerRefusal}).
 * It never sees the flush, count or describe that follow, which fail against the
 * catalog after the table is committed, so a retry would meet the table.
 *
 * Exported as a test seam: its callers reach it only after a DuckLake attach,
 * which needs a Postgres catalog, so a spec drives it on a file catalog.
 */
export async function writeTableAs(
   session: DuckDBConnection,
   params: {
      destinationName: string;
      schemaName: string;
      tableName: string;
      mode: "create" | "replace";
   },
   from: string,
   refused?: (error: unknown) => Error | null,
   partitionedBy: readonly PartitionKey[] = [],
): Promise<{ rowCount: number; columns: { name: string; type: string }[] }> {
   const { destinationName, schemaName, tableName } = params;
   // Rows go to Parquet, never into the catalog database: inlining off for this
   // session, and the flush below for a catalog that records its own limit.
   await session.runSQL("SET ducklake_default_data_inlining_row_limit = 0");
   const target = [destinationName, schemaName, tableName]
      .map((part) => quoteIdentifier(part, STORAGE_DIALECT))
      .join(".");
   const create =
      params.mode === "replace" ? "CREATE OR REPLACE TABLE" : "CREATE TABLE";
   try {
      if (partitionedBy.length === 0) {
         await session.runSQL(`${create} ${target} AS SELECT * FROM ${from}`);
      } else {
         // Bounded: the writer's memory, not the source's, is what a wide
         // many-partition insert exhausts.
         await writePartitionedTable(session, {
            quotedTablePath: target,
            selectSQL: `SELECT * FROM ${from}`,
            keys: partitionedBy,
            createStatement: create,
         });
      }
   } catch (error) {
      if (
         params.mode === "create" &&
         error instanceof Error &&
         /already exists/i.test(error.message)
      ) {
         throw new ConflictError(
            `Table '${schemaName}.${tableName}' already exists in '${destinationName}'.`,
         );
      }
      throw refused?.(error) ?? error;
   }
   // The SET above is only DuckLake's default: a catalog that records its own
   // data_inlining_row_limit (dataInliningRowLimit on POST/PATCH) keeps it, so a
   // table under that limit would sit as rows in the catalog database,
   // unencrypted whatever the catalog's encryption. Flush them to its Parquet.
   await flushInlinedRows(session, destinationName, schemaName, tableName);
   const counted = rows(
      await session.runSQL(`SELECT count(*) AS n FROM ${target}`),
   );
   return {
      rowCount: Number(counted[0]?.n ?? 0),
      columns: await describeColumns(session, target),
   };
}

/** Moves the rows a table holds inlined in the catalog database into Parquet; a table with none is untouched. */
async function flushInlinedRows(
   session: DuckDBConnection,
   destinationName: string,
   schemaName: string,
   tableName: string,
): Promise<void> {
   await session.runSQL(
      `CALL ducklake_flush_inlined_data('${escapeSQL(destinationName)}', schema_name => '${escapeSQL(schemaName)}', table_name => '${escapeSQL(tableName)}')`,
   );
}

/** The columns of a table or temporary table, as DuckDB names and types them. */
async function describeColumns(
   session: DuckDBConnection,
   relation: string,
): Promise<{ name: string; type: string }[]> {
   return rows(await session.runSQL(`DESCRIBE ${relation}`)).map((r) => ({
      name: String(r.column_name),
      type: String(r.column_type),
   }));
}

function rows(result: unknown): Record<string, unknown>[] {
   const r = result as
      | { rows?: Record<string, unknown>[] }
      | Record<string, unknown>[];
   return Array.isArray(r) ? r : (r?.rows ?? []);
}

/** What a keyed load answers: counts of rows, with the cursor column's greatest value. */
export interface LoadResult {
   rowsInserted: number;
   rowsReplaced: number;
   rowsUnchanged: number;
   rowsDeleted: number;
   rowCount: number;
   cursorMax: string | null;
   cursorType?: string;
}

type LoadRefusalReason =
   | "LOAD_SCHEMA_CONTRACT"
   | "LOAD_ROW_DROP_GUARD"
   | "LOAD_OBJECT_UNREADABLE";

/** A keyed load the request's objects or the table's contract refuse: 422 with `reason`, and `detail` for the caller to act on. */
export class LoadRefusalError extends Error {
   constructor(
      readonly reason: LoadRefusalReason,
      message: string,
      readonly detail?: Record<string, unknown>,
   ) {
      super(message);
   }
}

interface Column {
   name: string;
   type: string;
}

/** A column of the table a load writes, as the load's statements read it. */
interface LoadColumn {
   /** The table's spelling when it has the column, else the objects'. */
   name: string;
   /** The table's type when it has the column, else the objects'. */
   type: string;
   inStage: boolean;
   inTable: boolean;
}

/** The columns a load settles on once the objects are checked against the table. */
interface LoadLayout {
   keys: string[];
   /** The type of each of `keys`, in order. */
   keyTypes: string[];
   /** Every non-key column of the table after the load. */
   columns: LoadColumn[];
   /** Columns of the objects the table lacks, added before the rows are written. */
   added: Column[];
   /** Columns of the table the objects type wider without loss, each with the objects' type, set before the rows are written. */
   retyped: Column[];
   cursor?: { name: string; type: string; maxSql: string };
}

const STAGE = quoteIdentifier("__publisher_stage", STORAGE_DIALECT);
const CHANGES = quoteIdentifier("__publisher_changes", STORAGE_DIALECT);
/** Reserved for the load's own columns; a column of the objects may not start with it. */
const HELPER_PREFIX = "__publisher_";

const q = (name: string) => quoteIdentifier(name, STORAGE_DIALECT);
export const quotedList = (values: readonly string[]) =>
   values.map((v) => `'${escapeSQL(v)}'`).join(", ");
/** DuckDB matches names by ASCII case, so a collision is judged the same way. */
const fold = (name: string) => name.replace(/[A-Z]/g, (c) => c.toLowerCase());
const readObjects = (uris: readonly string[], options: string[] = []) =>
   `read_parquet(${[`[${quotedList(uris)}]`, NO_HIVE, ...options].join(", ")})`;

const SIGNED_INTEGERS = ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"];
const UNSIGNED_INTEGERS = [
   "UTINYINT",
   "USMALLINT",
   "UINTEGER",
   "UBIGINT",
   "UHUGEINT",
];

type CursorKind =
   | "integer"
   | "date"
   | "timestamp"
   | "timestamp_ns"
   | "timestamptz";

function cursorKind(type: string): CursorKind | undefined {
   const t = type.toUpperCase();
   if (SIGNED_INTEGERS.includes(t) || UNSIGNED_INTEGERS.includes(t)) {
      return "integer";
   }
   if (t === "DATE") return "date";
   if (["TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS"].includes(t)) {
      return "timestamp";
   }
   if (t === "TIMESTAMP_NS") return "timestamp_ns";
   if (t === "TIMESTAMP WITH TIME ZONE") return "timestamptz";
   return undefined;
}

/** The cursor's greatest value as text: an integer exactly, a date or time as ISO 8601 (UTC for a zoned one). */
function cursorMaxSql(name: string, kind: CursorKind): string {
   const max = `max(${q(name)})`;
   switch (kind) {
      case "integer":
         return `${max}::VARCHAR`;
      case "date":
         return `strftime(${max}, '%Y-%m-%d')`;
      case "timestamp":
         return `strftime(${max}, '%Y-%m-%dT%H:%M:%S.%f')`;
      case "timestamp_ns":
         return `strftime(${max}, '%Y-%m-%dT%H:%M:%S.%n')`;
      case "timestamptz":
         return `strftime(${max} AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ')`;
   }
}

function contractRefusal(
   detail: string,
   column: string,
   message: string,
   types: { tableType?: string; loadType?: string } = {},
): LoadRefusalError {
   return new LoadRefusalError("LOAD_SCHEMA_CONTRACT", message, {
      column,
      tableType: types.tableType ?? null,
      loadType: types.loadType ?? null,
      detail,
   });
}

/**
 * Settles a load's columns against the table's, or refuses it: a column the
 * table lacks is added; one it has keeps its type when the objects' type widens
 * into it without loss ({@link losslessWidening}), takes the objects' type when
 * that widens the table's, and is refused otherwise, because a narrowing insert
 * casts without error. Every key must be a column of both, and the cursor a
 * column of the objects the cursor kinds cover. A table column named like the
 * load's own helper columns is refused, since it would shadow them in the
 * plan. `table` is null for a table that does not exist yet.
 */
export function loadLayout(
   params: { key: readonly string[]; cursor?: string },
   stage: readonly Column[],
   table: readonly Column[] | null,
): LoadLayout {
   const shadowing = table?.find((c) => fold(c.name).startsWith(HELPER_PREFIX));
   if (shadowing) {
      throw contractRefusal(
         "reserved_name",
         shadowing.name,
         "A column of the table is reserved by the load.",
      );
   }
   const stageByName = new Map(stage.map((c) => [fold(c.name), c]));
   const tableByName = new Map((table ?? []).map((c) => [fold(c.name), c]));
   const columns: LoadColumn[] = [];
   const added: Column[] = [];
   const retyped = new Map<string, Column>();
   for (const s of stage) {
      const t = tableByName.get(fold(s.name));
      if (t && !losslessWidening(s.type, t.type)) {
         if (!losslessWidening(t.type, s.type)) {
            throw contractRefusal(
               "type_change",
               t.name,
               `Column '${t.name}' is ${t.type} in the table and ${s.type} in the objects.`,
               { tableType: t.type, loadType: s.type },
            );
         }
         retyped.set(fold(s.name), { name: t.name, type: s.type });
      }
      if (!t && table) added.push(s);
      columns.push({
         name: t?.name ?? s.name,
         type: retyped.get(fold(s.name))?.type ?? t?.type ?? s.type,
         inStage: true,
         inTable: !!t,
      });
   }
   for (const t of table ?? []) {
      if (!stageByName.has(fold(t.name))) {
         columns.push({
            name: t.name,
            type: t.type,
            inStage: false,
            inTable: true,
         });
      }
   }
   const keyColumns = params.key.map((key) => {
      const s = stageByName.get(fold(key));
      const t = tableByName.get(fold(key));
      if (!s || (table && !t)) {
         throw contractRefusal(
            "missing_key",
            key,
            `Key column '${key}' is not a column of ${s ? "the table" : "the objects"}.`,
            { tableType: t?.type, loadType: s?.type },
         );
      }
      return {
         name: t?.name ?? s.name,
         type: retyped.get(fold(key))?.type ?? t?.type ?? s.type,
      };
   });
   const keys = keyColumns.map((k) => k.name);
   const keyNames = new Set(keys.map(fold));
   return {
      keys,
      keyTypes: keyColumns.map((k) => k.type),
      columns: columns.filter((c) => !keyNames.has(fold(c.name))),
      added,
      retyped: [...retyped.values()],
      cursor: cursorLayout(params.cursor, stageByName, tableByName),
   };
}

function cursorLayout(
   cursor: string | undefined,
   stageByName: Map<string, Column>,
   tableByName: Map<string, Column>,
): LoadLayout["cursor"] {
   if (cursor === undefined) return undefined;
   const s = stageByName.get(fold(cursor));
   if (!s) {
      throw contractRefusal(
         "missing_cursor",
         cursor,
         `Cursor column '${cursor}' is not a column of the objects.`,
      );
   }
   const t = tableByName.get(fold(cursor));
   const kind = cursorKind(s.type);
   if (!kind || (t && !cursorKind(t.type))) {
      throw contractRefusal(
         "cursor_type",
         cursor,
         `Cursor column '${cursor}' must be an integer, DATE or TIMESTAMP column.`,
         { tableType: t?.type, loadType: s.type },
      );
   }
   return { name: s.name, type: s.type, maxSql: cursorMaxSql(s.name, kind) };
}

/** One Parquet schema as `parquet_schema` lists it: flattened pre-order, the root first, each element with its child count, logical type and physical type. */
interface SchemaShape {
   names: readonly string[];
   kids: readonly number[];
   kinds: readonly (string | null)[];
   /** Each element's physical type, converted type, scale, precision and length, as one text. */
   types: readonly string[];
   /** One object that has this schema. */
   uri: string;
}

/** The names a load's own statements use for the stage's rows, besides `__publisher_…`. */
const RESERVED_NAMES = ["file_row_number", "rowid"];

/** What `parquet_schema` calls a column of no type: DuckDB's own spelling, and the one pyarrow's `null()` is written as. */
const NULL_TYPES = ["UNKNOWN", "NullType()"];
const isNullTyped = (kind: string | null | undefined) =>
   NULL_TYPES.includes(kind ?? "");

/** The columns at the top of a schema, each with the elements it spans: a nested column spans its children. */
function topLevelColumns(
   shape: SchemaShape,
): { name: string; from: number; to: number }[] {
   const { names, kids } = shape;
   const end = (i: number): number => {
      let next = i + 1;
      for (let child = 0; child < (kids[i] ?? 0); child++) next = end(next);
      return next;
   };
   const columns: { name: string; from: number; to: number }[] = [];
   let from = 1;
   for (let n = 0; n < (kids[0] ?? 0) && from < names.length; n++) {
      const to = end(from);
      columns.push({ name: names[from], from, to });
      from = to;
   }
   return columns;
}

/**
 * The columns the objects hold, from their schemas alone (see
 * {@link SchemaShape}; one per distinct schema, not one per object): the
 * columns at the top of each, shape by shape. A column typed NULL in every
 * object is absent, and objects that leave no column are refused. A name the
 * load reserves, one DuckDB cannot tell from another by case (it renames the
 * second `A_1`, a NULL-typed one included), or one holding a NUL is refused.
 */
export function sourceColumns(shapes: readonly SchemaShape[]): string[] {
   const nullTyped = new Map<string, boolean>();
   for (const shape of shapes) {
      const seen = new Set<string>();
      for (const { name, from } of topLevelColumns(shape)) {
         if (seen.has(name)) {
            throw contractRefusal(
               "case_collision",
               fold(name),
               `Column '${name}' appears twice in an object.`,
            );
         }
         seen.add(name);
         nullTyped.set(
            name,
            (nullTyped.get(name) ?? true) && isNullTyped(shape.kinds[from]),
         );
      }
   }
   const present = [...nullTyped].filter(([, absent]) => !absent);
   if (present.length === 0) {
      throw contractRefusal(
         "no_columns",
         "",
         "The objects have no column to load.",
      );
   }
   // Over every physical name: DuckDB renames the second spelling whether or not the first is typed NULL.
   const byFold = new Map<string, string[]>();
   for (const name of nullTyped.keys()) {
      byFold.set(fold(name), [...(byFold.get(fold(name)) ?? []), name]);
   }
   for (const [name] of present) {
      if (
         name.includes("\0") ||
         fold(name).startsWith(HELPER_PREFIX) ||
         RESERVED_NAMES.includes(fold(name))
      ) {
         throw contractRefusal(
            "reserved_name",
            name,
            "A column name of the objects is reserved by the load or cannot be written.",
         );
      }
   }
   for (const [folded, spellings] of byFold) {
      if (spellings.length > 1) {
         throw contractRefusal(
            "case_collision",
            folded,
            `Columns ${spellings.map((s) => `'${s}'`).join(" and ")} differ only by case.`,
         );
      }
   }
   return present.map(([name]) => name);
}

/**
 * The columns the objects type in more than one way, each with one object of
 * every distinct type, from their schemas alone (a column typed NULL has no
 * type). `union_by_name` reads such a column as the one type that holds every
 * object's, and says nothing of the cast.
 */
export function typeVariants(
   shapes: readonly SchemaShape[],
): Map<string, string[]> {
   const byColumn = new Map<string, Map<string, string>>();
   for (const shape of shapes) {
      for (const { name, from, to } of topLevelColumns(shape)) {
         if (isNullTyped(shape.kinds[from])) continue;
         const type = JSON.stringify(
            [shape.names, shape.kids, shape.kinds, shape.types].map((list) =>
               list.slice(from, to),
            ),
         );
         const variants = byColumn.get(name) ?? new Map<string, string>();
         if (!variants.has(type)) variants.set(type, shape.uri);
         byColumn.set(name, variants);
      }
   }
   return new Map(
      [...byColumn]
         .filter(([, variants]) => variants.size > 1)
         .map(([name, variants]): [string, string[]] => [
            name,
            [...variants.values()],
         ]),
   );
}

/** The schemas of the objects, one row per distinct schema rather than one per object, in a fixed order. */
async function readSchemaShapes(
   session: DuckDBConnection,
   uris: readonly string[],
): Promise<SchemaShape[]> {
   const shapes = rows(
      await session.runSQL(
         "SELECT names, kids, kinds, types, min(file_name) AS uri FROM (SELECT file_name, list(name) AS names, " +
            "list(coalesce(num_children, 0)) AS kids, list(logical_type) AS kinds, " +
            "list(concat_ws('|', type, converted_type, scale, precision, type_length)) AS types " +
            `FROM parquet_schema([${quotedList(uris)}]) GROUP BY file_name) ` +
            "GROUP BY names, kids, kinds, types ORDER BY names, kids, kinds, types",
      ),
   );
   return shapes.map((shape) => ({
      names: shape.names as string[],
      kids: (shape.kids as unknown[]).map(Number),
      kinds: shape.kinds as (string | null)[],
      types: shape.types as string[],
      uri: String(shape.uri),
   }));
}

/**
 * Refuses a column some object types in a way that does not convert without
 * loss ({@link losslessWidening}) to the type the objects are read together
 * as. Only the objects behind a differing type are described, in one
 * statement with the combined read.
 */
async function assertObjectTypesConvert(
   session: DuckDBConnection,
   uris: readonly string[],
   variants: ReadonlyMap<string, readonly string[]>,
): Promise<void> {
   if (variants.size === 0) return;
   const describe = (uri: string, reader: string) =>
      `SELECT '${escapeSQL(uri)}' AS uri, column_name, column_type FROM (DESCRIBE SELECT * FROM ${reader})`;
   const objects = [...new Set([...variants.values()].flat())];
   const described = rows(
      await session.runSQL(
         [
            describe("", readObjects(uris, ["union_by_name = true"])),
            ...objects.map((uri) => describe(uri, readObjects([uri]))),
         ].join(" UNION ALL "),
      ),
   );
   const typeOf = new Map(
      described.map((r) => [
         `${r.uri}\0${r.column_name}`,
         String(r.column_type),
      ]),
   );
   for (const [column, objects] of variants) {
      const together = typeOf.get(`\0${column}`);
      for (const uri of objects) {
         const own = typeOf.get(`${uri}\0${column}`);
         if (own && together && !losslessWidening(own, together)) {
            throw contractRefusal(
               "type_change",
               column,
               `Column '${column}' is ${own} in an object and ${together} once the objects are read together.`,
               { loadType: own },
            );
         }
      }
   }
}

/**
 * A keyed load's objects the reader refused ({@link readerFault}), as a 422 in
 * DuckDB's words with each URI named by its place in `objects`. Null for
 * anything else, which stays a 500. Exported for tests.
 */
export function objectUnreadable(
   error: unknown,
   uris: readonly string[],
): LoadRefusalError | null {
   if (!(error instanceof Error) || !readerFault(error, uris)) return null;
   logInternalFailure("Load could not read an object", error, "warn");
   const said = error.message.split(ECHOED_SQL)[0];
   const object = uris.findIndex((uri) => said.includes(uri));
   const words = readerWords(
      error,
      uris.map((uri, i) => [uri, `objects[${i}]`] as const),
   );
   return new LoadRefusalError(
      "LOAD_OBJECT_UNREADABLE",
      object < 0
         ? `An object of the load could not be read: ${words}`
         : `objects[${object}] could not be read: ${words}`,
      object < 0 ? undefined : { object },
   );
}

/**
 * Reads the objects into a temporary stage keyed by their position in the
 * request, which decides which of two rows of one key is later, and returns
 * its columns. Each row also carries its place in its own object. The join to
 * the positions is one hash join, where a per-row search of the list costs
 * seconds per million rows.
 */
async function createStage(
   session: DuckDBConnection,
   uris: readonly string[],
): Promise<Column[]> {
   try {
      const shapes = await readSchemaShapes(session, uris);
      const columns = sourceColumns(shapes);
      await assertObjectTypesConvert(session, uris, typeVariants(shapes));
      const positions = uris
         .map((uri, i) => `('${escapeSQL(uri)}', ${i})`)
         .join(", ");
      const threads = Number(
         rows(await session.runSQL("SELECT current_setting('threads') AS n"))[0]
            .n,
      );
      // Remote objects wait on storage, not the CPU: more threads for the read alone.
      await session.runSQL("SET threads = 4");
      try {
         await session.runSQL(
            `CREATE TEMP TABLE ${STAGE} AS SELECT ${columns.map((c) => `__r.${q(c)}`).join(", ")}, ` +
               "__u.pos AS __publisher_pos, __r.file_row_number AS __publisher_row " +
               `FROM ${readObjects(uris, ["union_by_name = true", "filename = '__publisher_file'", "file_row_number = true"])} AS __r ` +
               `LEFT JOIN (VALUES ${positions}) AS __u(uri, pos) ON __r.__publisher_file = __u.uri`,
         );
      } finally {
         try {
            await session.runSQL(`SET threads = ${threads}`);
         } catch {
            // The session is this load's own and is disposed with it.
         }
      }
   } catch (error) {
      throw objectUnreadable(error, uris) ?? error;
   }
   return (await describeColumns(session, STAGE)).filter(
      (c) => !c.name.startsWith(HELPER_PREFIX),
   );
}

/**
 * The stage's row count and, when it can hold two rows of one key, the later
 * row of each (by object position, then place in the object). Refuses an
 * object whose name is not the URI it was given and a NULL key. Returns the
 * rows left and the cursor's greatest value over them.
 */
async function settleStage(
   session: DuckDBConnection,
   layout: LoadLayout,
): Promise<{ rows: number; cursorMax: string | null }> {
   const keys = layout.keys.map(q);
   const measure = async () => {
      const [r] = rows(
         await session.runSQL(
            "SELECT count(*) AS n, count(*) FILTER (WHERE __publisher_pos IS NULL) AS strays, " +
               (keys.length > 0
                  ? `count(*) FILTER (WHERE ${keys.map((k) => `${k} IS NULL`).join(" OR ")}) AS null_keys, ` +
                    `count(DISTINCT ${keys.length === 1 ? keys[0] : `(${keys.join(", ")})`}) AS distinct_keys, `
                  : "0 AS null_keys, 0 AS distinct_keys, ") +
               `${layout.cursor?.maxSql ?? "NULL"} AS cursor_max FROM ${STAGE}`,
         ),
      );
      return {
         rows: Number(r.n),
         strays: Number(r.strays),
         nullKeys: Number(r.null_keys),
         distinctKeys: Number(r.distinct_keys),
         cursorMax: r.cursor_max == null ? null : String(r.cursor_max),
      };
   };
   const stats = await measure();
   if (stats.strays > 0) {
      throw new BadRequestError(
         "An object's name is not the URI it was given in 'objects'.",
      );
   }
   if (stats.nullKeys > 0) {
      throw contractRefusal(
         "null_key",
         layout.keys.join(", "),
         "The objects hold a row with a NULL key.",
      );
   }
   if (keys.length === 0 || stats.rows === stats.distinctKeys) return stats;
   await session.runSQL(
      `DELETE FROM ${STAGE} WHERE rowid IN (SELECT rowid FROM (SELECT rowid, row_number() OVER ` +
         `(PARTITION BY ${keys.join(", ")} ORDER BY __publisher_pos DESC, __publisher_row DESC) AS n ` +
         `FROM ${STAGE}) WHERE n > 1)`,
   );
   const { rows: kept, cursorMax } = await measure();
   return { rows: kept, cursorMax };
}

/**
 * The statements that plan and apply a keyed load of the stage onto `target`.
 * `changes`, run in order, builds one row per key the load touches (new,
 * changed, and for a replace deleted), joined from the stage on plain `=`: a
 * NULL-safe join condition plans as a nested loop. A key with several stored
 * rows is changed, which collapses the copies. The stage's keys are never
 * NULL; only the delete, which must reach stored rows of a NULL key, matches
 * NULLs.
 *
 * One join over every compared column hashes a whole row of each side, so the
 * keys are settled in a pass over the keys alone and the columns are compared
 * in `groups`: one inner join each, carrying only the keys and that group's
 * columns, adding the keys it finds changed to `changes` unless already there.
 */
export function mergeStatements(plan: {
   target: string;
   mode: "merge" | "replace";
   keys: readonly string[];
   columns: readonly LoadColumn[];
   /** The columns each compare pass reads, together `columns` (see {@link packColumnGroups}); one pass over all of them when omitted. */
   groups?: readonly (readonly LoadColumn[])[];
}): { changes: string[]; delete: string; insert: string } {
   const { target, keys } = plan;
   const on = keys.map((k) => `__s.${q(k)} = __t.${q(k)}`).join(" AND ");
   // A replace also sees the stored keys the stage lacks.
   const replace = plan.mode === "replace";
   const key = (k: string) =>
      replace ? `coalesce(__s.${q(k)}, __t.${q(k)})` : `__s.${q(k)}`;
   const status = replace
      ? "CASE WHEN count(__s.__publisher_pos) = 0 THEN 'deleted' WHEN count(__t.__publisher_present) = 0 THEN 'new' ELSE 'changed' END"
      : "CASE WHEN count(__t.__publisher_present) = 0 THEN 'new' ELSE 'changed' END";
   const touched = [
      replace ? "count(__s.__publisher_pos) = 0" : undefined,
      "count(__t.__publisher_present) <> 1",
   ]
      .filter(Boolean)
      .join(" OR ");
   const keysPass =
      `CREATE TEMP TABLE ${CHANGES} AS SELECT ${keys.map((k) => `${key(k)} AS ${q(k)}`).join(", ")}, ` +
      `${status} AS __publisher_status, count(__t.__publisher_present) AS __publisher_stored ` +
      `FROM ${STAGE} AS __s ${replace ? "FULL" : "LEFT"} JOIN ` +
      `(SELECT ${keys.map(q).join(", ")}, TRUE AS __publisher_present FROM ${target}) AS __t ON ${on} ` +
      `GROUP BY ${keys.map(key).join(", ")} HAVING ${touched}`;
   // Anti joined to CHANGES so a key several groups change lands once; the keys pass already has every key stored more than once.
   const groupPass = (group: readonly LoadColumn[]) =>
      `INSERT INTO ${CHANGES} SELECT ${keys.map((k) => `__s.${q(k)}`).join(", ")}, 'changed', 1 ` +
      `FROM ${STAGE} AS __s JOIN ${target} AS __t ON ${on} WHERE (` +
      group
         .map(
            (c) =>
               `${c.inTable ? `__t.${q(c.name)}` : "NULL"} IS DISTINCT FROM ${c.inStage ? `__s.${q(c.name)}` : "NULL"}`,
         )
         .join(" OR ") +
      `) AND NOT EXISTS (SELECT 1 FROM ${CHANGES} AS __c WHERE ` +
      `${keys.map((k) => `__c.${q(k)} = __s.${q(k)}`).join(" AND ")})`;
   const passes = (plan.groups ?? [plan.columns])
      .filter((group) => group.length > 0)
      .map(groupPass);
   const columns = [
      ...keys,
      ...plan.columns.filter((c) => c.inStage).map((c) => c.name),
   ];
   return {
      changes: [keysPass, ...passes],
      delete:
         `DELETE FROM ${target} AS __t WHERE EXISTS (SELECT 1 FROM ${CHANGES} AS __s WHERE ` +
         `__s.__publisher_status <> 'new' AND ${nullSafeKeyMatch(keys.map(q), "__t", "__s")})`,
      // A SEMI JOIN, not EXISTS: with the status filter inside it DuckDB decorrelates EXISTS into a delim join that holds every column of the stage.
      insert:
         `INSERT INTO ${target} (${columns.map(q).join(", ")}) SELECT ${columns.map((c) => `__s.${q(c)}`).join(", ")} ` +
         `FROM ${STAGE} AS __s SEMI JOIN (SELECT ${keys.map(q).join(", ")} FROM ${CHANGES} WHERE __publisher_status <> 'deleted') AS __c ` +
         `ON ${keys.map((k) => `__s.${q(k)} = __c.${q(k)}`).join(" AND ")}`,
   };
}

/** A column's bytes in a join's hash table when the objects do not say: a fixed-width type's, else a generous guess for text, binary and nested values. */
const FIXED_COLUMN_BYTES = 8;
const WIDE_FIXED_COLUMN_BYTES = 16;
const VARIABLE_COLUMN_BYTES = 32;
const WIDE_FIXED_TYPES = new Set(["HUGEINT", "UHUGEINT", "UUID", "INTERVAL"]);
const FIXED_WIDTH_TYPES = new Set([
   "BOOLEAN",
   ...SIGNED_INTEGERS,
   ...UNSIGNED_INTEGERS,
   "FLOAT",
   "DOUBLE",
   "DATE",
   "TIME",
   "TIMESTAMP",
   "TIMESTAMP_S",
   "TIMESTAMP_MS",
   "TIMESTAMP_NS",
   "TIMESTAMP WITH TIME ZONE",
]);

export const typeBytes = (type: string) => {
   const t = type.toUpperCase();
   if (WIDE_FIXED_TYPES.has(t)) return WIDE_FIXED_COLUMN_BYTES;
   const decimal = /^DECIMAL\((\d+)/.exec(t);
   if (decimal) {
      return Number(decimal[1]) > 18
         ? WIDE_FIXED_COLUMN_BYTES
         : FIXED_COLUMN_BYTES;
   }
   return FIXED_WIDTH_TYPES.has(t) ? FIXED_COLUMN_BYTES : VARIABLE_COLUMN_BYTES;
};

/**
 * Packs columns into the groups of {@link mergeStatements}: the keys
 * (`keyBytes` a row) ride in every group, and a group takes columns while both
 * sides of its rows (`rows × bytes × 2`), plus one more `rows` of keys for the
 * anti join, stay within `budget`. `rows` is the smaller side's, which a hash
 * join builds on; the anti join builds on the smaller of its inputs, so the
 * changes table's full size need not fit. A column over the budget on its own gets a
 * group of its own, and may spill. First fit, widest first, so the passes are
 * few; each group keeps the columns' order.
 */
export function packColumnGroups<T>(
   columns: readonly { column: T; bytes: number }[],
   keyBytes: number,
   rows: number,
   budget: number,
): T[][] {
   const fits = (bytes: number) =>
      (keyBytes + bytes) * rows * 2 + keyBytes * rows <= budget;
   const groups: { bytes: number; members: { column: T; at: number }[] }[] = [];
   const widestFirst = columns
      .map((c, at) => ({ ...c, at }))
      .sort((a, b) => b.bytes - a.bytes || a.at - b.at);
   for (const { column, bytes, at } of widestFirst) {
      const group = groups.find((g) => fits(g.bytes + bytes));
      if (group) {
         group.bytes += bytes;
         group.members.push({ column, at });
      } else {
         groups.push({ bytes, members: [{ column, at }] });
      }
   }
   return groups.map((g) =>
      g.members.sort((a, b) => a.at - b.at).map((m) => m.column),
   );
}

/**
 * Each column's bytes per stage row in the objects, by case-folded top-level
 * name, from the Parquet footers alone. A nested column sums its leaves, whose
 * `path_in_schema` begins with its name.
 */
export async function parquetRowBytes(
   session: DuckDBConnection,
   uris: readonly string[],
   names: readonly string[],
   stageRows: number,
): Promise<Map<string, number>> {
   const longestFirst = [...names].sort((a, b) => b.length - a.length);
   const perRow = new Map<string, number>();
   for (const { path, bytes } of rows(
      await session.runSQL(
         "SELECT path_in_schema AS path, sum(total_uncompressed_size) AS bytes " +
            `FROM parquet_metadata([${quotedList(uris)}]) GROUP BY path_in_schema`,
      ),
   )) {
      const leaf = fold(String(path));
      const name = longestFirst.find(
         (n) => leaf === n || leaf.startsWith(`${n}, `),
      );
      if (name !== undefined) {
         perRow.set(
            name,
            (perRow.get(name) ?? 0) + Number(bytes) / Math.max(stageRows, 1),
         );
      }
   }
   return perRow;
}

/**
 * The compared columns split into the groups of {@link mergeStatements}, each
 * sized to hold both sides of the smaller side's rows within half the
 * session's memory (or `columnGroupBudget`). No column is taken as narrower than its type's width,
 * since a Parquet encoding is smaller than the row a hash join holds, and one
 * the objects lack has only its type to go by.
 */
async function columnGroups(
   session: DuckDBConnection,
   params: KeyedLoadParams,
   layout: LoadLayout,
   stageRows: number,
   rowsBefore: number,
): Promise<LoadColumn[][]> {
   if (layout.columns.length < 2) return [layout.columns];
   const budget =
      params.columnGroupBudget ??
      duckDBByteSize(
         String(
            rows(
               await session.runSQL(
                  "SELECT current_setting('memory_limit') AS size",
               ),
            )[0].size,
         ),
      ) / 2;
   const measured = await parquetRowBytes(
      session,
      params.objects,
      [...layout.keys, ...layout.columns.map((c) => c.name)].map(fold),
      stageRows,
   );
   return packColumnGroups(
      layout.columns.map((column) => ({
         column,
         bytes: Math.max(
            measured.get(fold(column.name)) ?? 0,
            typeBytes(column.type),
         ),
      })),
      layout.keys.reduce(
         (sum, k, i) =>
            sum +
            Math.max(measured.get(fold(k)) ?? 0, typeBytes(layout.keyTypes[i])),
         0,
      ),
      Math.min(stageRows, rowsBefore),
      budget,
   );
}

/** What a load will do, counted in rows before anything is written. */
interface LoadPlan {
   inserted: number;
   replaced: number;
   deleted: number;
   unchanged: number;
}

async function planKeyedLoad(
   session: DuckDBConnection,
   params: KeyedLoadParams,
   layout: LoadLayout,
   target: string,
   stageRows: number,
   rowsBefore: number,
): Promise<LoadPlan> {
   const { changes } = mergeStatements({
      target,
      mode: params.mode,
      keys: layout.keys,
      columns: layout.columns,
      groups: await columnGroups(
         session,
         params,
         layout,
         stageRows,
         rowsBefore,
      ),
   });
   for (const statement of changes) await session.runSQL(statement);
   const [r] = rows(
      await session.runSQL(
         "SELECT count(*) FILTER (WHERE __publisher_status = 'new') AS inserted, " +
            "count(*) FILTER (WHERE __publisher_status = 'changed') AS replaced, " +
            "coalesce(sum(__publisher_stored) FILTER (WHERE __publisher_status = 'changed'), 0) AS changed_stored, " +
            "coalesce(sum(__publisher_stored) FILTER (WHERE __publisher_status = 'deleted'), 0) AS deleted_stored " +
            `FROM ${CHANGES}`,
      ),
   );
   const [inserted, replaced] = [Number(r.inserted), Number(r.replaced)];
   return {
      inserted,
      replaced,
      // Stored rows that leave: absent keys, and the copies beyond one of a key rewritten.
      deleted: Number(r.changed_stored) - replaced + Number(r.deleted_stored),
      unchanged: stageRows - inserted - replaced,
   };
}

/** A keyless replace: the table's rows and the stage's compared as bags, and every row replaced when they differ. */
async function planKeylessLoad(
   session: DuckDBConnection,
   layout: LoadLayout,
   target: string,
   stageRows: number,
   rowsBefore: number,
): Promise<LoadPlan> {
   // Bags of different sizes differ, so only equal counts (a retry) need the compare, which holds a row of every column of both sides.
   if (stageRows === rowsBefore) {
      const side = (
         from: string,
         has: (c: LoadColumn) => boolean,
         sign: number,
      ) =>
         `SELECT ${layout.columns.map((c) => (has(c) ? q(c.name) : `NULL AS ${q(c.name)}`)).join(", ")}, ${sign} AS __publisher_side FROM ${from}`;
      const [r] = rows(
         await session.runSQL(
            `SELECT count(*) AS n FROM (SELECT ${layout.columns.map((c) => q(c.name)).join(", ")}, sum(__publisher_side) AS net FROM (` +
               `${side(STAGE, (c) => c.inStage, 1)} UNION ALL ${side(target, (c) => c.inTable, -1)}` +
               ") GROUP BY ALL HAVING sum(__publisher_side) <> 0)",
         ),
      );
      if (Number(r.n) === 0) {
         return { inserted: 0, replaced: 0, deleted: 0, unchanged: stageRows };
      }
   }
   return {
      inserted: Math.max(0, stageRows - rowsBefore),
      replaced: Math.min(rowsBefore, stageRows),
      deleted: Math.max(0, rowsBefore - stageRows),
      unchanged: 0,
   };
}

interface KeyedLoadParams {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   tableName: string;
   loadId: string;
   mode: "merge" | "replace";
   /** Empty for a keyless replace. */
   key: string[];
   objects: string[];
   cursor?: string;
   maxDeletedRows?: number;
   /** Throws when the caller's exclusion on the table is gone; called right before the commit. */
   assertLockHeld: () => Promise<void>;
   /** Internal: the bytes both sides of a compare pass may hold, in place of half the session's memory limit; tests set it small to force several passes. */
   columnGroupBudget?: number;
}

/**
 * Applies Parquet objects to a table of a DuckLake destination by key,
 * leaving every row they do not change as it is: a retry after a commit finds
 * nothing to do and writes nothing.
 *
 * - `merge` upserts by `key`; `replace` makes the table equal to the objects,
 *   by `key`, or as a whole table when it has none, and is refused when it
 *   would delete more rows than `maxDeletedRows`.
 * - The objects are read once into a stage that keeps the latest row of a
 *   key; they are checked against the table's columns (see {@link loadLayout}).
 * - Everything that reads the table (its columns, the plan, the counts, the
 *   row-drop guard) runs inside the one transaction (one snapshot) that writes
 *   it, so DuckLake's commit-time check refuses a load whose table another
 *   replica replaced or dropped meanwhile. The caller's lock is confirmed
 *   before the commit; a load that changes nothing commits nothing.
 * - The caller holds the table's exclusion and the load slot: DuckLake checks
 *   a commit's conflicts per table, not per key, so it would let two loads of
 *   one new key both commit.
 */
export async function mergeStorageTable(
   params: KeyedLoadParams,
): Promise<LoadResult> {
   const { destinationName, schemaName, tableName } = params;
   return withLoadSession(
      { ...params, purpose: "load", uris: params.objects },
      async (session) => {
         await session.runSQL(
            "SET ducklake_default_data_inlining_row_limit = 0",
         );
         await pinSessionToUTC(session);
         const target = [destinationName, schemaName, tableName]
            .map(q)
            .join(".");
         const stageColumns = await createStage(session, params.objects);
         const { plan, layout, stats, rowsBefore } = await inTransaction(
            session,
            target,
            () => planAndApply(session, params, target, stageColumns),
            "Failed to roll back a table load; the load was not committed",
         );
         await flushInlinedRows(
            session,
            destinationName,
            schemaName,
            tableName,
         );
         return {
            rowsInserted: plan.inserted,
            rowsReplaced: plan.replaced,
            rowsUnchanged: plan.unchanged,
            rowsDeleted: plan.deleted,
            rowCount: rowsBefore + plan.inserted - plan.deleted,
            cursorMax: stats.cursorMax,
            ...(layout.cursor ? { cursorType: layout.cursor.type } : {}),
         };
      },
   );
}

/** Reads the table, plans the load against it, refuses a replace past its guard, and writes what changes: the body of the load's transaction. */
async function planAndApply(
   session: DuckDBConnection,
   params: KeyedLoadParams,
   target: string,
   stageColumns: readonly Column[],
): Promise<{
   plan: LoadPlan;
   layout: LoadLayout;
   stats: { rows: number; cursorMax: string | null };
   rowsBefore: number;
}> {
   const exists =
      rows(
         await session.runSQL(
            `SELECT 1 FROM duckdb_tables() WHERE database_name = '${escapeSQL(params.destinationName)}' ` +
               `AND lower(schema_name) = lower('${escapeSQL(params.schemaName)}') AND lower(table_name) = lower('${escapeSQL(params.tableName)}')`,
         ),
      ).length > 0;
   const layout = loadLayout(
      params,
      stageColumns,
      exists ? await describeColumns(session, target) : null,
   );
   const stats = await settleStage(session, layout);
   const rowsBefore = exists
      ? Number(
           rows(await session.runSQL(`SELECT count(*) AS n FROM ${target}`))[0]
              .n,
        )
      : 0;
   const plan: LoadPlan = !exists
      ? { inserted: stats.rows, replaced: 0, deleted: 0, unchanged: 0 }
      : params.key.length > 0
        ? await planKeyedLoad(
             session,
             params,
             layout,
             target,
             stats.rows,
             rowsBefore,
          )
        : await planKeylessLoad(
             session,
             layout,
             target,
             stats.rows,
             rowsBefore,
          );
   if (
      params.mode === "replace" &&
      plan.deleted > (params.maxDeletedRows ?? 0)
   ) {
      throw new LoadRefusalError(
         "LOAD_ROW_DROP_GUARD",
         `The load would delete ${plan.deleted} of the table's ${rowsBefore} rows, more than maxDeletedRows (${params.maxDeletedRows ?? 0}).`,
         {
            wouldDelete: plan.deleted,
            rowsBefore,
            maxDeletedRows: params.maxDeletedRows ?? 0,
         },
      );
   }
   const changesRows = plan.inserted + plan.replaced + plan.deleted > 0;
   if (
      !exists ||
      changesRows ||
      layout.added.length > 0 ||
      layout.retyped.length > 0
   ) {
      await applyLoad(session, params, layout, target, exists, changesRows);
   }
   return { plan, layout, stats, rowsBefore };
}

/**
 * Writes the stage to the table inside the load's transaction: the new and
 * widened columns, then, when rows change, the stale rows deleted and the new and
 * changed rows inserted, and the commit message set.
 */
async function applyLoad(
   session: DuckDBConnection,
   params: KeyedLoadParams,
   layout: LoadLayout,
   target: string,
   exists: boolean,
   changesRows: boolean,
): Promise<void> {
   const written = [
      ...layout.keys,
      ...layout.columns.filter((c) => c.inStage).map((c) => c.name),
   ];
   const keyed = params.key.length > 0;
   const statements = keyed
      ? mergeStatements({
           target,
           mode: params.mode,
           keys: layout.keys,
           columns: layout.columns,
        })
      : {
           delete: `DELETE FROM ${target}`,
           insert: `INSERT INTO ${target} (${written.map(q).join(", ")}) SELECT ${written.map(q).join(", ")} FROM ${STAGE}`,
        };
   const message = [
      "load",
      `${params.schemaName}.${params.tableName}`,
      params.loadId,
   ].join(":");
   if (!exists) {
      await session.runSQL(
         `CREATE TABLE ${target} AS SELECT ${written.map(q).join(", ")} FROM ${STAGE}`,
      );
   } else {
      for (const column of layout.added) {
         await session.runSQL(
            `ALTER TABLE ${target} ADD COLUMN ${q(column.name)} ${column.type}`,
         );
      }
      // DuckLake records a widening as metadata: files already written keep their type and read back cast.
      for (const column of layout.retyped) {
         await session
            .runSQL(
               `ALTER TABLE ${target} ALTER COLUMN ${q(column.name)} SET TYPE ${column.type}`,
            )
            .catch((error: unknown) => {
               if (
                  error instanceof Error &&
                  /Cannot change type of column/.test(error.message)
               ) {
                  throw contractRefusal(
                     "type_change",
                     column.name,
                     `Column '${column.name}' cannot be widened to ${column.type}.`,
                     { loadType: column.type },
                  );
               }
               throw error;
            });
      }
      if (changesRows) {
         await session.runSQL(statements.delete);
         await session.runSQL(statements.insert);
      }
   }
   await runInternalSQL(
      session,
      `CALL ${q(params.destinationName)}.set_commit_message('publisher-admin', ` +
         `'${escapeSQL(message)}')`,
   );
   await params.assertLockHeld();
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Connection, getDialect } from "@malloydata/malloy";
import { components } from "../api";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import { CONNECTION_DIALECTS } from "./dialects";
import type { Introspector } from "./dialects/types";
import {
   assertSafeSqlIdentifier,
   columnsOfSchema,
   extractErrorDataFromError,
   sqlInFilter,
   type SchemaField,
} from "./introspection_helpers";
import { sqlLiteral } from "./introspection_sql";
import { ApiConnection } from "./model";
import { RECORD_COLUMN_TYPE } from "./record_column_type";

export {
   assertSafeSqlIdentifier,
   columnsOfSchema,
   extractErrorDataFromError,
   sqlInFilter,
   sqlLiteral,
};

type ApiSchema = components["schemas"]["Schema"];
type ApiTable = components["schemas"]["Table"];

type ApiColumn = NonNullable<ApiTable["columns"]>[number];

/** Schema reads in flight at once: each is a round trip to the warehouse. */
const SCHEMA_READ_CONCURRENCY = 8;
/**
 * How long one listing waits for schema reads. A table not read by then is
 * listed flat and marked `membersUnread`. Reads already in flight still
 * finish and fill the cache, and the next listing starts with the tables
 * tried longest ago, so a schema too large for one listing is covered over
 * the next few.
 */
const SCHEMA_READ_DEADLINE_MS = 10_000;
/**
 * How long a listing waits on a read another listing started. A read older
 * than this is taken to be hung (a driver with no timeout of its own), and
 * the listing starts its own, so one read that never returns cannot leave
 * its table unread until a restart. Longer than the listing deadline, so a
 * merely slow read is not started again by every listing.
 */
const SHARED_READ_MAX_AGE_MS = 60_000;
/** The limits in force; a spec shortens them to exercise the time limits. */
export const schemaReadLimits = {
   deadlineMs: SCHEMA_READ_DEADLINE_MS,
   sharedReadMaxAgeMs: SHARED_READ_MAX_AGE_MS,
};
/** Tables whose members are cached per connection. Past it, the oldest go. */
const MAX_CACHED_TABLES = 2_000;

/** One table's record members, as last read, and the record types they were read at. */
interface CachedMembers {
   /** Every record column's name and INFORMATION_SCHEMA type, in order. */
   recordTypes: string;
   /** Each record column's members (columnsOfSchema), by the record's name. */
   members: Map<string, ApiColumn[]>;
}

/**
 * Record members by table, per Malloy connection. A listing reuses an entry
 * without reading the schema when the table's record columns still have the
 * same INFORMATION_SCHEMA types, since on DuckDB and Trino that type string
 * spells out every member: `STRUCT(city VARCHAR, ...)`, and so does a
 * Databricks `struct<city:string>` (its listing reads full_data_type). A
 * type that names no members (a bare `struct`) is read every time and never
 * cached: an unchanged `struct` says nothing about whether its members
 * changed.
 */
const memberCache = new WeakMap<Connection, Map<string, CachedMembers>>();

/**
 * When each table's members were last asked for, per Malloy connection, as a
 * count of reads started (not the clock). A listing starts its reads with the
 * tables tried longest ago, never-tried ones first, so a schema too large for
 * one listing's time limit is covered over the next few, and a table whose
 * read always fails, or whose type always needs a read (a bare `struct`),
 * cannot keep the tables after it from being read.
 */
const lastTried = new WeakMap<Connection, Map<string, number>>();

/**
 * Schema reads in flight, by table and record types, per Malloy connection: a listing that
 * needs a table another listing is already reading waits on that read rather
 * than starting a second one, unless the read is older than
 * `sharedReadMaxAgeMs`. `read` resolves to the members, or undefined when the
 * read failed.
 */
interface SharedRead {
   read: Promise<Map<string, ApiColumn[]> | undefined>;
   startedAt: number;
}
const inFlight = new WeakMap<Connection, Map<string, SharedRead>>();
let readsStarted = 0;

/**
 * The refresh timestamp of the last fresh read. Malloy re-reads a cached
 * schema only when the timestamp passed is strictly newer than the cached
 * one, so two reads in the same millisecond would get the first one's schema.
 */
let lastRefreshTimestamp = 0;
function nextRefreshTimestamp(): number {
   lastRefreshTimestamp = Math.max(Date.now() + 1, lastRefreshTimestamp + 1);
   return lastRefreshTimestamp;
}

/**
 * Whether Malloy will read a table's schema by this path. It refuses a path
 * that is not canonical SQL for the dialect (`memory.main.my-tbl`, which needs
 * quoting), on every read, so such a table's members are never listed, like a
 * plain array's, rather than marked unread on every listing.
 */
function isReadablePath(dialectName: string, resource: string): boolean {
   try {
      const result = getDialect(dialectName).sqlValidateTableName(resource);
      return result.ok && result.canonical === resource;
   } catch {
      return false;
   }
}

function recordColumnsOf(table: ApiTable): ApiColumn[] {
   return (table.columns ?? []).filter((c) =>
      RECORD_COLUMN_TYPE.test(c.type ?? ""),
   );
}

/**
 * Add each record column's members after it (columnsOfSchema), for every
 * table with a record column. The database's own list stays the authority:
 * its columns, their order and their types are kept as reported, and members
 * are spliced in after the record they belong to. A table whose members can
 * be neither read nor taken from the cache keeps its flat list, marked
 * `membersUnread`.
 *
 * The members come from the Malloy schema, read fresh (Malloy caches a schema
 * for the life of the connection, and a cached one would list members of a
 * struct that has since changed), unless memberCache already holds them for
 * the same record types.
 */
async function withRecordMembers(
   tables: ApiTable[],
   malloyConnection: Connection,
): Promise<ApiTable[]> {
   const nested = tables.filter(
      (t) =>
         t.resource &&
         recordColumnsOf(t).length > 0 &&
         isReadablePath(malloyConnection.dialectName, t.resource),
   );
   if (nested.length === 0) return tables;

   let cache = memberCache.get(malloyConnection);
   if (!cache) {
      cache = new Map();
      memberCache.set(malloyConnection, cache);
   }
   let reading = inFlight.get(malloyConnection);
   if (!reading) {
      reading = new Map();
      inFlight.set(malloyConnection, reading);
   }
   let tried = lastTried.get(malloyConnection);
   if (!tried) {
      tried = new Map();
      lastTried.set(malloyConnection, tried);
   }
   const recordTypesOf = (t: ApiTable) =>
      recordColumnsOf(t)
         .map((c) => `${c.name}:${c.type}`)
         .join("\n");
   // The resolved members of each table in this listing.
   const resolved = new Map<string, Map<string, ApiColumn[]>>();
   const toRead: ApiTable[] = [];
   // Whether the record types spell out their members (`STRUCT(city ...)`),
   // so an unchanged type means unchanged members. A bare `struct` names
   // none: its cached members may be stale, so they never stand in.
   const typesNameMembers = (t: ApiTable) =>
      recordColumnsOf(t).every((c) => /[(<]/.test(c.type ?? ""));
   for (const t of nested) {
      const hit = cache.get(t.resource!);
      if (hit && typesNameMembers(t) && hit.recordTypes === recordTypesOf(t)) {
         resolved.set(t.resource!, hit.members);
         // Used: move it to the back of the eviction order.
         cache.delete(t.resource!);
         cache.set(t.resource!, hit);
      } else {
         toRead.push(t);
      }
   }

   const failed: string[] = [];
   if (toRead.length > 0) {
      // Longest since last tried first; a stable sort keeps listing order
      // among the never-tried.
      toRead.sort(
         (a, b) =>
            (tried!.get(a.resource!) ?? -1) - (tried!.get(b.resource!) ?? -1),
      );
      const refreshTimestamp = nextRefreshTimestamp();
      let next = 0;
      let expired = false;
      // Reads another listing started, which this one waits on without
      // holding a worker: a slow one cannot keep this listing's own reads
      // from starting.
      const joined: Promise<void>[] = [];
      const readOne = async (t: ApiTable) => {
         const resource = t.resource!;
         // Keyed by the record types too: a read started for another version
         // of the table would list that version's members.
         const key = `${resource}\n${recordTypesOf(t)}`;
         const pending = reading!.get(key);
         if (
            pending &&
            Date.now() - pending.startedAt < schemaReadLimits.sharedReadMaxAgeMs
         ) {
            joined.push(
               pending.read.then((members) => {
                  if (members && !expired) resolved.set(resource, members);
                  else if (!members) failed.push(resource);
               }),
            );
            return;
         }
         // No read to share, or one too old to trust: start this one. A
         // listing still waiting on the old read keeps waiting on it.
         const entry: SharedRead = {
            read: readFresh(t),
            startedAt: Date.now(),
         };
         reading!.set(key, entry);
         try {
            const members = await entry.read;
            if (members && !expired) resolved.set(resource, members);
         } finally {
            // A hung read replaced by a newer one must not remove the newer.
            if (reading!.get(key) === entry) reading!.delete(key);
         }
      };
      const readFresh = async (
         t: ApiTable,
      ): Promise<Map<string, ApiColumn[]> | undefined> => {
         const resource = t.resource!;
         tried!.delete(resource);
         tried!.set(resource, readsStarted++);
         if (tried!.size > MAX_CACHED_TABLES) {
            tried!.delete(tried!.keys().next().value!);
         }
         let error: unknown;
         try {
            const read = await malloyConnection.fetchSchemaForTables(
               { [resource]: resource },
               { refreshTimestamp },
            );
            const schema = read.schemas[resource];
            if (schema) {
               const fields = new Map(
                  (schema.fields as SchemaField[]).map((f) => [f.name, f]),
               );
               const members = new Map<string, ApiColumn[]>();
               for (const column of recordColumnsOf(t)) {
                  const field = fields.get(column.name ?? "");
                  // The members alone: the record itself is the reported column.
                  if (field) {
                     members.set(
                        column.name!,
                        columnsOfSchema([field]).slice(1),
                     );
                  }
               }
               cache!.delete(resource);
               if (typesNameMembers(t))
                  cache!.set(resource, {
                     recordTypes: recordTypesOf(t),
                     members,
                  });
               if (cache!.size > MAX_CACHED_TABLES) {
                  cache!.delete(cache!.keys().next().value!);
               }
               return members;
            }
            error = read.errors?.[resource];
         } catch (e) {
            error = extractErrorDataFromError(e);
         }
         failed.push(resource);
         logger.warn("Record members not read: the table schema read failed", {
            table: resource,
            error,
         });
         return undefined;
      };
      const worker = async () => {
         while (!expired && next < toRead.length) await readOne(toRead[next++]);
      };
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([
         Promise.all(
            Array.from(
               { length: Math.min(SCHEMA_READ_CONCURRENCY, toRead.length) },
               worker,
            ),
         ).then(() => Promise.all(joined)),
         new Promise<void>((resolve) => {
            timer = setTimeout(resolve, schemaReadLimits.deadlineMs);
         }),
      ]);
      clearTimeout(timer);
      expired = true;
      // A table read neither in time nor successfully is listed flat and
      // marked. No cached read stands in: a table is only read when its
      // cached members are missing or may be stale.
      const failedSet = new Set(failed);
      const missed = toRead.filter(
         (t) => !resolved.has(t.resource!) && !failedSet.has(t.resource!),
      ).length;
      if (missed > 0) {
         logger.warn(
            "Record members not read for some tables: the schema reads ran past the deadline",
            { tables: missed, deadlineMs: schemaReadLimits.deadlineMs },
         );
      }
   }

   const unlisted = new Set(nested);
   return tables.map((t) => {
      const members = t.resource ? resolved.get(t.resource) : undefined;
      // A record whose members are not listed is flagged, so a reader can
      // tell a member it was not given from one the table no longer has.
      if (!members) return unlisted.has(t) ? { ...t, membersUnread: true } : t;
      const columns: ApiColumn[] = [];
      for (const column of t.columns ?? []) {
         columns.push(column);
         columns.push(...(members.get(column.name ?? "") ?? []));
      }
      return { ...t, columns };
   });
}

// Every registered type's introspector. A Map, so a caller-supplied type
// such as "constructor" finds nothing.
const INTROSPECTOR_BY_TYPE: ReadonlyMap<string, Introspector> = new Map(
   Object.entries(CONNECTION_DIALECTS).map(([type, entry]) => [
      type,
      entry.introspector,
   ]),
);

function introspectorFor(connection: ApiConnection): Introspector {
   const introspector = INTROSPECTOR_BY_TYPE.get(connection.type ?? "");
   if (!introspector) {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
   return introspector;
}

export async function getSchemasForConnection(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   return introspectorFor(connection).schemas(connection, malloyConnection);
}

export async function listTablesForSchema(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const tables = await introspectorFor(connection).tables(
      connection,
      schemaName,
      malloyConnection,
      tableNames,
   );
   // BigQuery's introspector reads the Malloy schema already, members
   // included; a `publisher` connection lists what the remote Publisher
   // listed, which has expanded them already.
   return connection.type === "bigquery" || connection.type === "publisher"
      ? tables
      : withRecordMembers(tables, malloyConnection);
}

const AZURE_SUPPORTED_SCHEMES = ["https://", "http://", "abfss://", "az://"];
const AZURE_DATA_EXTENSIONS = [
   ".parquet",
   ".csv",
   ".json",
   ".jsonl",
   ".ndjson",
];

/**
 * Validates an Azure URL against the three supported patterns:
 *   1. Single file:         path/file.parquet
 *   2. Directory glob:      path/*.ext   (direct children only, no sub-dirs)
 *   3. Recursive:           path/**      (all data files recursively)
 */
export function validateAzureUrl(url: string, fieldName: string): void {
   if (!AZURE_SUPPORTED_SCHEMES.some((s) => url.startsWith(s))) {
      throw new BadRequestError(
         `Azure ${fieldName} must use one of: ${AZURE_SUPPORTED_SCHEMES.join(", ")}`,
      );
   }

   const pathWithoutQuery = url.split("?")[0];
   const stars = (pathWithoutQuery.match(/\*/g) || []).length;

   if (stars === 0) {
      // Single file — must end with a data extension
      const lower = pathWithoutQuery.toLowerCase();
      if (!AZURE_DATA_EXTENSIONS.some((ext) => lower.endsWith(ext))) {
         throw new BadRequestError(
            `Azure ${fieldName}: a single-file URL must end with a data file extension (${AZURE_DATA_EXTENSIONS.join(", ")})`,
         );
      }
   } else if (pathWithoutQuery.endsWith("**")) {
      // Recursive — valid, no further checks needed
   } else {
      // Must be exactly path/*.ext — one star, in the last path segment only
      const lastSegment = pathWithoutQuery.split("/").pop() || "";
      if (stars !== 1 || !lastSegment.startsWith("*")) {
         throw new BadRequestError(
            `Azure ${fieldName}: only three URL patterns are supported:\n` +
               `  • Single file:      path/file.parquet\n` +
               `  • Directory glob:   path/*.ext  (direct children only)\n` +
               `  • Recursive:        path/**     (all data files in subtree)\n` +
               `Multi-level globs such as "sub_dir/*/*.parquet" are not supported.`,
         );
      }
   }
}

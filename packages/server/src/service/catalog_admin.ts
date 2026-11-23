// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admin API's domain (admin-api-doc.yaml): the DuckLake catalogs in one
 * configured Postgres database, their schemas and tables, and their snapshots.
 *
 * Nothing here is registered or stored. A catalog is a Postgres schema holding
 * DuckLake's `ducklake_metadata` table, named by that schema; its data path,
 * encryption and settings are what it recorded at its creation. The listener is
 * configured once (`PUBLISHER_ADMIN_CATALOG_DATABASE`, the storage credential),
 * so no request carries a credential and no response returns one.
 *
 * Every attach acts as the catalog's owner (`options=-crole=<owner>`) when the
 * configured role is not it, so a table an orchestrator's admin role creates is
 * owned by the role that owns the catalog and its grants keep applying.
 */

import { settings } from "../settings";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { Client } from "pg";
import type { components } from "../api";
import { BadRequestError, ConflictError } from "../errors";
import { logger } from "../logger";
import { countLoadRejection, loadSlots } from "../query_concurrency";
import {
   attachDuckLakeForCreate,
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
} from "./connection";
import {
   isDuckLakeInitRace,
   READABLE_SCHEMA_NAME,
   RESERVED_SCHEMA_NAMES,
   SYSTEM_SCHEMAS,
} from "./dialects/ducklake";
import {
   METADATA_CATALOG_PREFIX,
   metadataCatalogRef,
   type PartitionKey,
   tablePartitionKeys,
} from "./dialects/ducklake_catalog";
import { applySessionResourceLimits, escapeSQL } from "./duckdb_session_setup";
import {
   connectPgClient,
   createPgAttachSecret,
   pgClientConfig,
} from "./postgres_conninfo";
import { createIsolatedBuildSession } from "./materialization/run/materialization_build_session";
import {
   cleanupStorageFiles,
   dropStorageTable,
} from "./materialization/run/storage_admin";
import { parseS3Endpoint } from "./gcs_s3_utils";
import { redactPgSecrets } from "../pg_helpers";
import { errMessage } from "../utils";
import { quoteIdentifier } from "./quoting";
import { containsStatementSeparator } from "./single_select";
import { withStatementUsage } from "./statement_usage";
import {
   type LoadFormat,
   type LoadOptions,
   type LoadResult,
   LoadRefusalError,
   copyStorageTable,
   loadStorageTable,
   mergeStorageTable,
   quotedList,
} from "./materialization/run/storage_table_load";

type ApiConnection = components["schemas"]["Connection"];
type DucklakeStorage = NonNullable<
   components["schemas"]["DucklakeConnection"]["storage"]
>;

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** A role name safe in a libpq `options=-crole=` value, hyphens included (`<prefix>-ducklake`). */
const ROLE = /^[A-Za-z_][A-Za-z0-9_-]*$/;
/** Reserved on a schema create or drop; `main` is DuckLake's default and cannot be dropped. */
const RESERVED_SCHEMAS = ["main", ...RESERVED_SCHEMA_NAMES];
/** DuckLake's interval spelling for `delete_older_than`, e.g. `1 day`, `12 hours`. */
const INTERVAL = /^(\d+) (second|minute|hour|day|week)s?$/;
/** Used by a snapshot delete when the catalog records no file-deletion grace. */
const DEFAULT_FILE_DELETION_GRACE = "1 day";

/** An admin refusal: its HTTP status, the machine-readable `reason`, and the `detail` a caller can act on. */
export class CatalogAdminError extends Error {
   constructor(
      readonly status: number,
      readonly reason: string,
      message: string,
      readonly detail?: Record<string, unknown>,
   ) {
      super(message);
   }
}

const notFound = (name: string) =>
   new CatalogAdminError(
      404,
      "CATALOG_NOT_FOUND",
      `No DuckLake catalog named '${name}' in the catalog database.`,
   );
const badRequest = (message: string) =>
   new CatalogAdminError(400, "INVALID_REQUEST", message);
/** A refusal to start or finish a load (503 `LOAD_BUSY`), counted by what was busy. */
const loadBusy = (cause: "slot" | "lock" | "lost", message: string) => {
   countLoadRejection(cause);
   return new CatalogAdminError(503, "LOAD_BUSY", message);
};

/**
 * Runs one heavy load or copy: this process holds one slot for them, and a busy
 * slot is refused (503 `LOAD_BUSY`), not queued, so the caller retries, possibly
 * on another replica.
 */
export async function withLoadSlot<T>(fn: () => Promise<T>): Promise<T> {
   const slot = loadSlots.tryAcquire();
   if (!slot) {
      throw loadBusy(
         "slot",
         "This server is already running a load; retry shortly.",
      );
   }
   try {
      return await fn();
   } finally {
      slot.release();
   }
}

/** A read of the catalog database, rows out. */
type CatalogQuery = (sql: string) => Promise<Record<string, unknown>[]>;

/** What {@link CatalogAdmin.withTableLock} hands its callback. */
export interface TableLock {
   /** Reads the catalog database on the lock's own connection. */
   pg: CatalogQuery;
   /**
    * Throws 503 `LOAD_BUSY` when the lock's connection is gone. A session lock
    * dies with its connection, so call it right before the commit it guards.
    */
   assertLockHeld: () => Promise<void>;
}

/** Postgres's advisory-lock key for a table's name, as {@link tableLockName} spells it. */
const TABLE_LOCK_KEY = "hashtextextended($1, 0)";

/** A table's lock name: the schema and table folded as the engine folds them, the catalog as given. */
const tableLockName = (catalog: string, schema: string, table: string) =>
   `${catalog}.${schema.toLowerCase()}.${table.toLowerCase()}`;

export interface CatalogAdminConfig {
   /** A libpq connection string (keyword or URI form) for the catalog database. */
   catalogDatabase: string;
   /** The storage credential every catalog's data path is read and written with. */
   storage?: Omit<DucklakeStorage, "bucketUrl">;
   /** Internal: bounds a query on a table lock's connection (default 30 s); specs shorten it. */
   lockQueryTimeoutMillis?: number;
}

/**
 * Reads the listener's configuration (the `admin.*` settings). Null when no
 * catalog database is set: the routes then answer 503, so a listener started
 * for nothing else still serves its health endpoints. A storage setting that
 * cannot be used throws, so a typo fails startup.
 */
export function catalogAdminConfig(): CatalogAdminConfig | null {
   const catalogDatabase = settings.get("admin.catalogDatabase");
   if (!catalogDatabase) return null;
   // A session acts as each catalog's owner through libpq `options`, which would
   // silently replace options given here; refuse them rather than drop them.
   if (/(^|[\s?&])options=/i.test(catalogDatabase)) {
      throw new Error(
         "PUBLISHER_ADMIN_CATALOG_DATABASE must not set `options`: the admin API sets the session role per catalog. Set server parameters on the role or database instead (ALTER ROLE … SET).",
      );
   }
   // The load lock's client reads this string too; a form it cannot use fails startup, not every load.
   pgClientConfig("PUBLISHER_ADMIN_CATALOG_DATABASE", catalogDatabase);
   const provider = settings.get("admin.storage.provider");
   const keyId = settings.get("admin.storage.keyId");
   const secret = settings.get("admin.storage.secret");
   const region = settings.get("admin.storage.region");
   const endpoint = settings.get("admin.storage.endpoint");
   let storage: CatalogAdminConfig["storage"];
   switch (provider) {
      case undefined:
         storage = undefined;
         break;
      case "s3":
         if (!keyId || !secret) {
            throw new Error(
               "PUBLISHER_ADMIN_STORAGE_PROVIDER=s3 needs PUBLISHER_ADMIN_STORAGE_KEY_ID and PUBLISHER_ADMIN_STORAGE_SECRET.",
            );
         }
         if (endpoint) {
            try {
               parseS3Endpoint(endpoint);
            } catch (error) {
               throw new Error(
                  `PUBLISHER_ADMIN_STORAGE_ENDPOINT: ${(error as Error).message}`,
               );
            }
         }
         storage = {
            s3Connection: {
               provider: "config",
               accessKeyId: keyId,
               secretAccessKey: secret,
               region,
               endpoint,
            },
         };
         break;
      case "s3_credential_chain":
         storage = {
            s3Connection: {
               provider: "credential_chain",
               region,
            },
         };
         break;
      case "gcs":
         if (!keyId || !secret) {
            throw new Error(
               "PUBLISHER_ADMIN_STORAGE_PROVIDER=gcs needs PUBLISHER_ADMIN_STORAGE_KEY_ID and PUBLISHER_ADMIN_STORAGE_SECRET (an HMAC key pair).",
            );
         }
         storage = { gcsConnection: { keyId, secret } };
         break;
   }
   return { catalogDatabase, storage };
}

/** What a catalog records, and the bytes it holds. */
export interface Catalog {
   name: string;
   dataPath: string;
   encrypted: boolean;
   formatVersion: string;
   dataInliningRowLimit?: number;
   fileDeletionGrace?: string;
   sizeBytes: number;
   /** Present on a single catalog's read, not on the list. */
   schemas?: string[];
}

export interface CatalogTableSummary {
   name: string;
   sizeBytes: number;
}

/** A table of a catalog-wide listing, which names each table's schema. */
export interface CatalogTableListing extends CatalogTableSummary {
   schema: string;
}

export interface CatalogTable {
   schema: string;
   name: string;
   columns: { name: string; type: string }[];
   rowCount?: number;
   /** The partition spec in key order; empty when the table is not partitioned. */
   partitionedBy: PartitionKey[];
}

export interface CatalogSnapshot {
   id: number;
   time: string;
   changes: Record<string, unknown>;
   /** Who committed it, as DuckLake records it; null when none was set. */
   author: string | null;
   commitMessage: string | null;
}

/** Appends a libpq `options` setting that makes the session act as `role`. */
export function connectionStringAsRole(
   connectionString: string,
   role: string | undefined,
): string {
   if (!role) return connectionString;
   // The owner comes from pg_namespace, and a Postgres role name may hold a
   // space or `=`: spliced unquoted, `x host=elsewhere` would re-point the
   // connection, credential and all. Only a role name that needs no quoting in
   // either connection-string form is acted as.
   if (!ROLE.test(role)) {
      throw new CatalogAdminError(
         409,
         "CATALOG_OWNER_UNSUPPORTED",
         `The catalog's owner role '${role}' is not a plain name (letters, digits, '_' or '-'), so no session can act as it.`,
      );
   }
   if (/^postgres(ql)?:\/\//i.test(connectionString)) {
      const sep = connectionString.includes("?") ? "&" : "?";
      return `${connectionString}${sep}options=${encodeURIComponent(`-crole=${role}`)}`;
   }
   return `${connectionString} options=-crole=${role}`;
}

/** A Postgres string literal, for SQL sent through `postgres_query`. */
const pgLiteral = (value: string) => `'${value.replace(/'/g, "''")}'`;

/** Postgres's own identifier quoting, for SQL sent through `postgres_query`. */
const pgIdent = (name: string) => `"${name.replace(/"/g, '""')}"`;

export class CatalogAdmin {
   constructor(private readonly config: CatalogAdminConfig) {}

   async listCatalogs(): Promise<Catalog[]> {
      return this.withCatalogDatabase(async (pg) => {
         const names = (await pg(
            "SELECT table_schema AS name FROM information_schema.tables " +
               "WHERE table_name = 'ducklake_metadata' ORDER BY 1",
         )) as { name: string }[];
         const catalogs: Catalog[] = [];
         // One catalog at a time, on the one connection: N catalogs never become
         // N concurrent queries on the catalog database. A schema whose name no
         // route could address is not listed, and one that holds a table named
         // ducklake_metadata without being a catalog is skipped, not fatal.
         for (const { name } of names) {
            if (!IDENT.test(name)) continue;
            try {
               catalogs.push(await this.describe(pg, name));
            } catch (error) {
               logger.warn(
                  "Skipped a schema that does not read as a DuckLake catalog",
                  {
                     catalog: name,
                     error: redactPgSecrets(
                        error instanceof Error ? error.message : String(error),
                     ),
                  },
               );
            }
         }
         return catalogs;
      });
   }

   async getCatalog(name: string): Promise<Catalog> {
      const catalog = await this.withCatalogDatabase(async (pg) => {
         await this.requireCatalog(pg, name);
         return this.describe(pg, name);
      });
      catalog.schemas = await this.withAttached(name, "read", async (s) =>
         (
            (await rows(
               s,
               `SELECT schema_name FROM duckdb_schemas() WHERE database_name = '${escapeSQL(name)}' ORDER BY 1`,
            )) as { schema_name: string }[]
         ).map((r) => r.schema_name),
      );
      return catalog;
   }

   async createCatalog(request: {
      name?: unknown;
      dataPath?: unknown;
      encrypted?: unknown;
      dataInliningRowLimit?: unknown;
      fileDeletionGrace?: unknown;
      schemas?: unknown;
   }): Promise<Catalog> {
      const name = identifier(request.name, "name");
      if (typeof request.dataPath !== "string" || !request.dataPath.trim()) {
         throw badRequest(
            "'dataPath' is required: where the catalog's files live.",
         );
      }
      const dataPath = request.dataPath.trim();
      const encrypted = request.encrypted ?? true;
      if (typeof encrypted !== "boolean") {
         throw badRequest("'encrypted' must be a boolean.");
      }
      const limit = optionalLimit(request.dataInliningRowLimit);
      const grace = optionalGrace(request.fileDeletionGrace);
      const schemas = request.schemas ?? [];
      if (!Array.isArray(schemas)) {
         throw badRequest("'schemas' must be a list of names.");
      }
      const schemaNames = schemas.map((s) => schemaName(s, "schemas[]"));

      const owner = await this.withCatalogDatabase(async (pg) => {
         if (await this.catalogExists(pg, name)) {
            throw new CatalogAdminError(
               409,
               "CATALOG_EXISTS",
               `A DuckLake catalog named '${name}' already exists.`,
            );
         }
         return this.schemaOwner(pg, name);
      });
      const { session, dispose, workDir } = createIsolatedBuildSession(
         `admin_create_${name}`,
      );
      try {
         await applySessionResourceLimits(session, { tempDirectory: workDir });
         await attachDuckLakeForCreate(
            session,
            name,
            this.ducklake(name, owner, dataPath),
            { encrypted, dataInliningRowLimit: limit },
         );
         await setOptions(session, name, { limit, grace });
         for (const schema of schemaNames) {
            await session.runSQL(
               `CREATE SCHEMA IF NOT EXISTS ${q(name)}.${q(schema)}`,
            );
         }
      } catch (error) {
         // Another create of this catalog, begun after the check above, made it
         // first.
         if (isDuckLakeInitRace(error)) {
            throw new CatalogAdminError(
               409,
               "CATALOG_EXISTS",
               `A DuckLake catalog named '${name}' already exists.`,
            );
         }
         throw this.redacted(error);
      } finally {
         await dispose();
      }
      logger.info("Created a DuckLake catalog", { catalog: name, encrypted });
      return this.getCatalog(name);
   }

   async updateCatalog(
      name: string,
      request: { dataInliningRowLimit?: unknown; fileDeletionGrace?: unknown },
   ): Promise<Catalog> {
      const limit = optionalLimit(request.dataInliningRowLimit);
      const grace = optionalGrace(request.fileDeletionGrace);
      await this.withAttached(name, "write", (s) =>
         setOptions(s, name, { limit, grace }),
      );
      return this.getCatalog(name);
   }

   /**
    * Creates a schema, and sets its inlining off so a sync table written into it
    * never stays rows in the catalog database. Returns whether it is new: a
    * schema that exists, or one another request created first, is not an error.
    */
   async createSchema(
      name: string,
      schema: string,
      request: unknown,
      ifAbsent: boolean,
   ): Promise<{ schema: { name: string }; created: boolean }> {
      if (request && Object.keys(request).length > 0) {
         throw badRequest("The request takes no body.");
      }
      schemaName(schema, "schema");
      const created = await this.withAttached(name, "write", async (s) => {
         if (await schemaExists(s, name, schema)) return false;
         try {
            await s.runSQL(`CREATE SCHEMA ${q(name)}.${q(schema)}`);
         } catch (error) {
            // Another create of this schema won: in one session it is "already
            // exists", across sessions DuckLake's "Transaction conflict ... created".
            const message = errMessage(error);
            if (
               /already exists/i.test(message) ||
               (/Transaction conflict/i.test(message) &&
                  /created/i.test(message))
            ) {
               return false;
            }
            throw error;
         }
         try {
            await s.runSQL(
               `CALL ${q(name)}.set_option('data_inlining_row_limit', 0, schema => '${escapeSQL(schema)}')`,
            );
         } catch (error) {
            // The schema exists by now; the post-load flush still keeps rows out.
            logger.warn("Could not turn inlining off for a new schema", {
               catalog: name,
               schema,
               error: redactPgSecrets(errMessage(error)),
            });
         }
         return true;
      });
      if (!created && ifAbsent) {
         throw new CatalogAdminError(
            412,
            "SCHEMA_EXISTS",
            `Schema '${schema}' already exists in catalog '${name}'.`,
         );
      }
      logger.info("Created a schema in a DuckLake catalog", {
         catalog: name,
         schema,
         created,
      });
      return { schema: { name: schema }, created };
   }

   /** Drops a schema and every table in it. A schema that does not exist is not an error. */
   async deleteSchema(name: string, schema: string): Promise<void> {
      identifier(schema, "schema");
      refuseReserved(schema);
      await this.withAttached(name, "write", (s) =>
         s.runSQL(`DROP SCHEMA IF EXISTS ${q(name)}.${q(schema)} CASCADE`),
      );
      logger.info("Dropped a schema from a DuckLake catalog", {
         catalog: name,
         schema,
      });
   }

   /** A schema's tables, or with no schema every table of the catalog's schemas. */
   async listTables(
      name: string,
      schema: string,
   ): Promise<CatalogTableSummary[]>;
   async listTables(name: string): Promise<CatalogTableListing[]>;
   async listTables(
      name: string,
      schema?: string,
   ): Promise<(CatalogTableSummary | CatalogTableListing)[]> {
      if (schema !== undefined) identifier(schema, "schema");
      return this.withAttached(name, "read", async (s) => {
         if (schema !== undefined) await requireSchema(s, name, schema);
         const tables = (await rows(
            s,
            `SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = '${escapeSQL(name)}' AND ` +
               (schema === undefined
                  ? `schema_name NOT IN (${quotedList(SYSTEM_SCHEMAS)}) ORDER BY 1, 2`
                  : `schema_name = '${escapeSQL(schema)}' ORDER BY 2`),
         )) as { schema_name: string; table_name: string }[];
         // ducklake_table_info names a table's schema by id; the catalog's own
         // schema table maps the name to it (the live row, end_snapshot null).
         const meta = metadataCatalogRef(name, name);
         const sizes = new Map<string, number>();
         for (const r of (await rows(
            s,
            `SELECT m.schema_name, i.table_name, i.file_size_bytes + i.delete_file_size_bytes AS bytes ` +
               `FROM ducklake_table_info('${escapeSQL(name)}') i ` +
               `JOIN ${meta}.ducklake_schema m ON m.schema_id = CAST(i.schema_id AS BIGINT) ` +
               `AND m.end_snapshot IS NULL` +
               (schema === undefined
                  ? ""
                  : ` WHERE m.schema_name = '${escapeSQL(schema)}'`),
         )) as { schema_name: string; table_name: string; bytes: unknown }[]) {
            sizes.set(
               `${r.schema_name}\u0000${r.table_name}`,
               Number(r.bytes ?? 0),
            );
         }
         return tables.map((t) => ({
            ...(schema === undefined ? { schema: t.schema_name } : {}),
            name: t.table_name,
            sizeBytes: sizes.get(`${t.schema_name}\u0000${t.table_name}`) ?? 0,
         }));
      });
   }

   async getTable(
      name: string,
      schema: string,
      table: string,
   ): Promise<CatalogTable> {
      identifier(schema, "schema");
      identifier(table, "table");
      return this.withAttached(name, "read", async (s) => {
         const found = (await rows(
            s,
            `SELECT estimated_size FROM duckdb_tables() WHERE database_name = '${escapeSQL(name)}' ` +
               `AND schema_name = '${escapeSQL(schema)}' AND table_name = '${escapeSQL(table)}'`,
         )) as { estimated_size: unknown }[];
         if (found.length === 0) {
            throw new CatalogAdminError(
               404,
               "TABLE_NOT_FOUND",
               `No table '${schema}.${table}' in catalog '${name}'.`,
            );
         }
         const columns = (
            (await rows(s, `DESCRIBE ${q(name)}.${q(schema)}.${q(table)}`)) as {
               column_name: string;
               column_type: string;
            }[]
         ).map((c) => ({ name: c.column_name, type: c.column_type }));
         const estimate = found[0].estimated_size;
         // Every catalog this API manages keeps its metadata in the schema
         // named after it (see `ducklake`).
         const partitionedBy = await tablePartitionKeys(
            s,
            name,
            name,
            schema,
            table,
         );
         return {
            schema,
            name: table,
            columns,
            ...(estimate == null ? {} : { rowCount: Number(estimate) }),
            partitionedBy,
         };
      });
   }

   /**
    * Creates or replaces a table from a source: one object under the catalog's
    * data path, or a table of a catalog in the same catalog database (a
    * migration's copy). `ifAbsent` refuses a table that exists (the
    * `If-None-Match: *` precondition). Returns the table and whether it is new.
    */
   async putTable(
      name: string,
      schema: string,
      table: string,
      request: { source?: unknown },
      ifAbsent: boolean,
   ): Promise<{ table: CatalogTable; created: boolean }> {
      identifier(schema, "schema");
      identifier(table, "table");
      const source = tableSource(request.source);
      // Taken first, as a load takes it, so a busy server refuses before reading anything.
      const created = await withLoadSlot(async () => {
         const { owner, sourceOwner } = await this.withCatalogDatabase(
            async (pg) => {
               await this.requireCatalog(pg, name);
               const owner = await this.schemaOwner(pg, name);
               if (source.kind !== "table")
                  return { owner, sourceOwner: undefined };
               return {
                  owner,
                  sourceOwner: await this.copySource(pg, name, owner, source),
               };
            },
         );
         // A schema that does not exist answers 404 (SCHEMA_NOT_FOUND) here,
         // before anything is read.
         const existing = await this.listTables(name, schema);
         const exists = existing.some((t) => t.name === table);
         if (exists && ifAbsent) {
            throw new CatalogAdminError(
               412,
               "TABLE_EXISTS",
               `Table '${schema}.${table}' already exists in catalog '${name}'.`,
            );
         }
         const mode = ifAbsent ? "create" : "replace";
         try {
            await withStatementUsage(
               {
                  environment: "",
                  connection: name,
                  // The admin API addresses a catalog by its own name.
                  catalog: name,
                  surface: source.kind === "table" ? "copy" : "load",
               },
               () =>
                  source.kind === "table"
                     ? copyStorageTable({
                          destinationName: name,
                          destinationConnection: this.connection(name, owner),
                          schemaName: schema,
                          tableName: table,
                          source: {
                             catalogName: source.catalog,
                             connection: this.connection(
                                source.catalog,
                                sourceOwner,
                             ),
                             schemaName: source.schema,
                             tableName: source.table,
                          },
                          mode,
                       })
                     : loadStorageTable({
                          destinationName: name,
                          destinationConnection: this.connection(name, owner),
                          schemaName: schema,
                          tableName: table,
                          uri: source.uri,
                          format: source.format,
                          options: source.options,
                          mode,
                       }),
               (r) => r.rowCount,
            );
         } catch (error) {
            // Another create-only PUT won the race between the existence check
            // and the load: the same answer as if it had come first.
            if (ifAbsent && error instanceof ConflictError) {
               throw new CatalogAdminError(
                  412,
                  "TABLE_EXISTS",
                  `Table '${schema}.${table}' already exists in catalog '${name}'.`,
               );
            }
            throw this.redacted(error);
         }
         logger.info(
            source.kind === "table"
               ? "Copied a table into a DuckLake catalog"
               : "Loaded a table into a DuckLake catalog",
            {
               catalog: name,
               schema,
               table,
               ...(source.kind === "table"
                  ? {
                       source: `${source.catalog}.${source.schema}.${source.table}`,
                    }
                  : { format: source.format }),
               created: !exists,
            },
         );
         return !exists;
      });
      return { table: await this.getTable(name, schema, table), created };
   }

   /** Drops a table. A table that does not exist is not an error. */
   async deleteTable(
      name: string,
      schema: string,
      table: string,
   ): Promise<void> {
      identifier(schema, "schema");
      identifier(table, "table");
      const owner = await this.withCatalogDatabase(async (pg) => {
         await this.requireCatalog(pg, name);
         return this.schemaOwner(pg, name);
      });
      try {
         await dropStorageTable({
            destinationName: name,
            destinationConnection: this.connection(name, owner),
            physicalTableName: `${schema}.${table}`,
            environmentPath: "",
         });
      } catch (error) {
         throw this.redacted(error);
      }
   }

   async listSnapshots(
      name: string,
      limit: number,
   ): Promise<CatalogSnapshot[]> {
      return this.withAttached(name, "read", async (s) =>
         (
            (await rows(
               s,
               `SELECT snapshot_id, snapshot_time, changes, author, commit_message FROM ${q(name)}.snapshots() ` +
                  `ORDER BY snapshot_id DESC LIMIT ${Math.max(1, Math.min(limit, 1000))}`,
            )) as {
               snapshot_id: unknown;
               snapshot_time: unknown;
               changes: unknown;
               author: unknown;
               commit_message: unknown;
            }[]
         ).map((r) => ({
            id: Number(r.snapshot_id),
            time: new Date(String(r.snapshot_time)).toISOString(),
            changes: (r.changes ?? {}) as Record<string, unknown>,
            author: r.author == null ? null : String(r.author),
            commitMessage:
               r.commit_message == null ? null : String(r.commit_message),
         })),
      );
   }

   /**
    * Expires the snapshots older than `olderThan` (never the latest), then deletes
    * the data files that have been retired for longer than the catalog's
    * file-deletion grace. Files this call retires are deleted by a later call,
    * once their grace has passed, so a reader still scanning one is not cut off.
    */
   async deleteSnapshots(
      name: string,
      olderThan: Date,
   ): Promise<{ snapshotsExpired: number; filesDeleted: number }> {
      const { owner, filesOlderThan } = await this.withCatalogDatabase(
         async (pg) => {
            await this.requireCatalog(pg, name);
            const settings = await this.metadata(pg, name);
            const grace =
               settings.delete_older_than ?? DEFAULT_FILE_DELETION_GRACE;
            // Postgres computes the cutoff, so any interval DuckLake records is
            // understood (`1 week 2 days`, `00:30:00`), not only the spellings
            // the API accepts on input.
            const cutoff = (await pg(
               `SELECT extract(epoch FROM now() - ${pgLiteral(grace)}::interval)::float8 AS t`,
            ).catch(() => {
               throw new CatalogAdminError(
                  409,
                  "CATALOG_SETTING_UNREADABLE",
                  `The catalog's file-deletion grace '${grace}' is not an interval; set it with PATCH.`,
               );
            })) as { t: unknown }[];
            return {
               owner: await this.schemaOwner(pg, name),
               filesOlderThan: new Date(Number(cutoff[0].t) * 1000),
            };
         },
      );
      try {
         const result = await cleanupStorageFiles({
            destinationName: name,
            destinationConnection: this.connection(name, owner),
            snapshotsOlderThan: olderThan,
            filesOlderThan,
         });
         return {
            snapshotsExpired: result.expiredSnapshots,
            filesDeleted: result.deletedFiles,
         };
      } catch (error) {
         throw this.redacted(error);
      }
   }

   /**
    * Runs `fn` holding a Postgres advisory lock on the table, taken in the
    * catalog database so every replica contends for the same one. DuckLake
    * tracks commit conflicts per table and operation, not per key, so it cannot
    * stop two loads inserting the same new keys (see
    * ducklake_commit_conflict.integration.spec.ts). A held lock is refused (503
    * `LOAD_BUSY`), never waited on.
    */
   async withTableLock<T>(
      catalog: string,
      schema: string,
      table: string,
      fn: (lock: TableLock) => Promise<T>,
   ): Promise<T> {
      const key = tableLockName(catalog, schema, table);
      let lost = false;
      let client: Client;
      try {
         client = await connectPgClient(
            "The catalog database",
            this.config.catalogDatabase,
            () => {
               lost = true;
            },
            this.config.lockQueryTimeoutMillis,
         );
      } catch (error) {
         throw this.redacted(error);
      }
      const query = async (sql: string, values?: string[]) => {
         try {
            return (await client.query(sql, values)).rows;
         } catch (error) {
            // A query that gets no answer leaves the connection in doubt: it is dropped, not used again.
            if (/Query read timeout/i.test(errMessage(error))) lost = true;
            throw error;
         }
      };
      try {
         const locked = await query(
            `SELECT pg_try_advisory_lock(${TABLE_LOCK_KEY}) AS locked`,
            [key],
         ).catch((error: unknown) => {
            lost = true;
            logger.warn("Could not take a table load lock", {
               table: key,
               error: errMessage(error),
            });
            throw loadBusy(
               "lost",
               `The lock on '${key}' could not be taken; retry the load.`,
            );
         });
         if (locked[0]?.locked !== true) {
            throw loadBusy("lock", `A load of '${key}' is already running.`);
         }
         try {
            return await fn({
               pg: async (sql) => {
                  try {
                     return await query(sql);
                  } catch (error) {
                     // A read cut off by the lock's connection dying is a lost lock.
                     if (!lost) throw error;
                     throw loadBusy(
                        "lost",
                        `The lock on '${key}' was lost; retry the load.`,
                     );
                  }
               },
               assertLockHeld: async () => {
                  try {
                     if (lost) throw new Error("connection lost");
                     await query("SELECT 1");
                  } catch {
                     lost = true;
                     throw loadBusy(
                        "lost",
                        `The lock on '${key}' was lost; retry the load.`,
                     );
                  }
               },
            });
         } finally {
            if (!lost) {
               await query(`SELECT pg_advisory_unlock(${TABLE_LOCK_KEY})`, [
                  key,
               ]).catch(() => {
                  lost = true;
               });
            }
         }
      } finally {
         // end() on a connection that has stopped answering never settles: its socket is destroyed instead.
         if (lost) destroyConnection(client);
         else await client.end().catch(() => undefined);
      }
   }

   /**
    * Applies Parquet objects under the catalog's data path to a table by key
    * (see {@link mergeStorageTable}), creating the table when it does not
    * exist. Takes this process's load slot, then holds the table's lock for the
    * whole load: either busy is refused (503 `LOAD_BUSY`), not waited on, so
    * neither order can deadlock, and a refusal by a busy slot opens no
    * connection to the catalog database.
    */
   async loadTable(
      name: string,
      schema: string,
      table: string,
      body: unknown,
   ): Promise<LoadResult> {
      identifier(name, "catalog");
      identifier(schema, "schema");
      identifier(table, "table");
      const request = loadRequest(body);
      return withLoadSlot(() =>
         this.withTableLock(
            name,
            schema,
            table,
            async ({ pg, assertLockHeld }) => {
               await this.requireCatalog(pg, name);
               await requireRecordedSchema(pg, name, schema);
               const owner = await this.schemaOwner(pg, name);
               let result: LoadResult;
               try {
                  result = await withStatementUsage(
                     {
                        environment: "",
                        connection: name,
                        catalog: name,
                        surface: "load",
                     },
                     () =>
                        mergeStorageTable({
                           ...request,
                           destinationName: name,
                           destinationConnection: this.connection(name, owner),
                           schemaName: schema,
                           tableName: table,
                           assertLockHeld,
                        }),
                     (r) => r.rowsInserted + r.rowsReplaced,
                  );
               } catch (error) {
                  throw this.loadFailure(error);
               }
               logger.info("Loaded rows into a DuckLake table", {
                  catalog: name,
                  schema,
                  table,
                  mode: request.mode,
                  objects: request.objects.length,
                  rowsInserted: result.rowsInserted,
                  rowsReplaced: result.rowsReplaced,
                  rowsDeleted: result.rowsDeleted,
               });
               return result;
            },
         ),
      );
   }

   // ---- internals ----

   /** A load's failure as an admin refusal: DuckLake's commit conflict, or a table another request created first, as a 409; the load's own 422s come from {@link redacted}. */
   private loadFailure(error: unknown): unknown {
      const mapped = this.redacted(error);
      if (
         mapped instanceof Error &&
         !(mapped instanceof CatalogAdminError) &&
         /Transaction conflict|Table with name .* already exists/i.test(
            mapped.message,
         )
      ) {
         return new CatalogAdminError(409, "LOAD_CONFLICT", mapped.message);
      }
      // Unmapped, so a 500 whose log line is this message rather than DuckDB's bare "Out of Memory Error".
      if (
         mapped instanceof Error &&
         !(mapped instanceof CatalogAdminError) &&
         /Out of Memory Error/.test(mapped.message)
      ) {
         return new Error(
            `The load exceeded the load session's memory and spill limits: ${mapped.message}`,
            { cause: mapped },
         );
      }
      return mapped;
   }

   /** The catalog as a DuckLake config with no data path: its recorded one is used. */
   private ducklake(
      name: string,
      owner: string | undefined,
      dataPath?: string,
   ): components["schemas"]["DucklakeConnection"] {
      return {
         catalog: {
            postgresConnection: {
               connectionString: connectionStringAsRole(
                  this.config.catalogDatabase,
                  owner,
               ),
            },
            metadataSchema: name,
         },
         ...(this.config.storage || dataPath
            ? {
                 storage: {
                    ...(this.config.storage ?? {}),
                    ...(dataPath ? { bucketUrl: dataPath } : {}),
                 },
              }
            : {}),
      } as components["schemas"]["DucklakeConnection"];
   }

   private connection(name: string, owner: string | undefined): ApiConnection {
      return {
         name,
         type: "ducklake",
         ducklakeConnection: this.ducklake(name, owner),
      } as ApiConnection;
   }

   /** Attaches a catalog on a transient session, as its owner, for one operation. */
   private async withAttached<T>(
      name: string,
      mode: "read" | "write",
      fn: (session: DuckDBConnection) => Promise<T>,
   ): Promise<T> {
      identifier(name, "catalog");
      const owner = await this.withCatalogDatabase(async (pg) => {
         await this.requireCatalog(pg, name);
         return this.schemaOwner(pg, name);
      });
      const { session, dispose, workDir } = createIsolatedBuildSession(
         `admin_${mode}_${name}`,
      );
      try {
         await applySessionResourceLimits(session, { tempDirectory: workDir });
         const cfg = this.ducklake(name, owner);
         if (mode === "read") {
            await attachDuckLakeReadOnly(session, name, cfg);
         } else {
            await attachDuckLakeReadWrite(session, name, cfg);
         }
         return await fn(session);
      } catch (error) {
         throw this.redacted(error);
      } finally {
         await dispose();
      }
   }

   /** Runs `fn` against the catalog database itself, through DuckDB's postgres scanner. */
   private async withCatalogDatabase<T>(
      fn: (
         pg: (sql: string) => Promise<Record<string, unknown>[]>,
      ) => Promise<T>,
   ): Promise<T> {
      const { session, dispose, workDir } =
         createIsolatedBuildSession("admin_catalogs");
      try {
         await applySessionResourceLimits(session, { tempDirectory: workDir });
         await session.runSQL("INSTALL postgres; LOAD postgres;");
         const { path, secret } = await createPgAttachSecret(
            session,
            "admin_pg",
            this.config.catalogDatabase,
         );
         await session.runSQL(
            `ATTACH '${escapeSQL(path)}' AS admin_pg (TYPE postgres, READ_ONLY${secret ? `, SECRET ${secret}` : ""})`,
         );
         return await fn((sql) =>
            rows(
               session,
               `SELECT * FROM postgres_query('admin_pg', '${escapeSQL(sql)}')`,
            ),
         );
      } catch (error) {
         throw this.redacted(error);
      } finally {
         await dispose();
      }
   }

   /**
    * Checks a copy's source before anything attaches or is metered: the source
    * catalog and its live table exist, the configured role can act as the
    * source's owner, and the copy would not write an encrypted catalog's rows
    * into an unencrypted one. Returns the role the source attaches as.
    */
   private async copySource(
      pg: (sql: string) => Promise<Record<string, unknown>[]>,
      name: string,
      owner: string | undefined,
      source: { catalog: string; schema: string; table: string },
   ): Promise<string | undefined> {
      const same = source.catalog === name;
      let sourceOwner = owner;
      if (!same) {
         // Ownership first, from pg_namespace: a catalog owned by a role this
         // one cannot act as would otherwise read as missing, since
         // information_schema lists only tables the role has a privilege on.
         // USAGE, not MEMBER: the reads below run as this role without SET
         // ROLE, which a member granted INHERIT FALSE cannot make.
         const found = (await pg(
            "SELECT pg_get_userbyid(nspowner) AS owner, current_user AS me, " +
               "pg_has_role(current_user, nspowner, 'USAGE') AS member " +
               `FROM pg_namespace WHERE nspname = ${pgLiteral(source.catalog)}`,
         )) as { owner: string; me: string; member: unknown }[];
         const row = found[0];
         if (!row) {
            throw sourceNotFound(
               `Source catalog '${source.catalog}' does not exist.`,
            );
         }
         if (row.member !== true) {
            throw new CatalogAdminError(
               409,
               "CATALOG_OWNER_UNSUPPORTED",
               `Source catalog '${source.catalog}' is owned by a role this server's catalog role cannot act as.`,
            );
         }
         if (!(await this.catalogExists(pg, source.catalog))) {
            throw sourceNotFound(
               `Source catalog '${source.catalog}' does not exist.`,
            );
         }
         sourceOwner = row.owner === row.me ? undefined : row.owner;
      }
      const catalog = pgIdent(source.catalog);
      const live = await pg(
         `SELECT 1 FROM ${catalog}.ducklake_table t JOIN ${catalog}.ducklake_schema s ` +
            "ON t.schema_id = s.schema_id " +
            `WHERE s.schema_name = ${pgLiteral(source.schema)} ` +
            `AND t.table_name = ${pgLiteral(source.table)} ` +
            "AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL",
      );
      if (live.length === 0) {
         throw sourceNotFound(
            `Source table '${source.schema}.${source.table}' does not exist in catalog '${source.catalog}'.`,
         );
      }
      if (!same) {
         const from = await this.metadata(pg, source.catalog);
         const to = await this.metadata(pg, name);
         if (from.encrypted === "true" && to.encrypted !== "true") {
            throw badRequest(
               `Source catalog '${source.catalog}' is encrypted and '${name}' is not; a copy would write its rows unencrypted.`,
            );
         }
      }
      return sourceOwner;
   }

   private async catalogExists(
      pg: (sql: string) => Promise<Record<string, unknown>[]>,
      name: string,
   ): Promise<boolean> {
      const found = await pg(
         "SELECT 1 FROM information_schema.tables WHERE table_name = 'ducklake_metadata' " +
            `AND table_schema = '${escapeSQL(name)}'`,
      );
      return found.length > 0;
   }

   private async requireCatalog(
      pg: (sql: string) => Promise<Record<string, unknown>[]>,
      name: string,
   ): Promise<void> {
      identifier(name, "catalog");
      if (!(await this.catalogExists(pg, name))) throw notFound(name);
   }

   /**
    * The role a session should act as: the schema's owner when that is not the
    * configured role itself. Undefined when the schema does not exist yet (a
    * create makes it, owned by the configured role) or is already ours.
    */
   private async schemaOwner(
      pg: (sql: string) => Promise<Record<string, unknown>[]>,
      name: string,
   ): Promise<string | undefined> {
      const found = (await pg(
         "SELECT pg_get_userbyid(nspowner) AS owner, current_user AS me " +
            `FROM pg_namespace WHERE nspname = '${escapeSQL(name)}'`,
      )) as { owner: string; me: string }[];
      const row = found[0];
      if (!row || row.owner === row.me) return undefined;
      return row.owner;
   }

   private async metadata(
      pg: (sql: string) => Promise<Record<string, unknown>[]>,
      name: string,
   ): Promise<Record<string, string>> {
      const found = (await pg(
         `SELECT key, value FROM ${pgIdent(name)}.ducklake_metadata WHERE scope IS NULL`,
      )) as { key: string; value: string }[];
      return Object.fromEntries(found.map((r) => [r.key, String(r.value)]));
   }

   /**
    * What a catalog records, and the bytes it holds: every data and delete file
    * it records, live or retired but not yet deleted, plus rows inlined in the
    * catalog database.
    */
   private async describe(
      pg: (sql: string) => Promise<Record<string, unknown>[]>,
      name: string,
   ): Promise<Catalog> {
      const settings = await this.metadata(pg, name);
      const schema = pgIdent(name);
      const sized = (await pg(
         `SELECT (SELECT COALESCE(SUM(file_size_bytes), 0) FROM ${schema}.ducklake_data_file) + ` +
            `(SELECT COALESCE(SUM(file_size_bytes), 0) FROM ${schema}.ducklake_delete_file) AS files`,
      )) as { files: unknown }[];
      let inlined = 0;
      try {
         const tables = (await pg(
            `SELECT table_name FROM ${schema}.ducklake_inlined_data_tables`,
         )) as { table_name: string }[];
         for (const { table_name } of tables) {
            const size = (await pg(
               `SELECT pg_total_relation_size(${pgLiteral(`${pgIdent(name)}.${pgIdent(table_name)}`)}) AS n`,
            )) as { n: unknown }[];
            inlined += Number(size[0]?.n ?? 0);
         }
      } catch {
         // A catalog format without inlined-data tables holds no inlined bytes.
      }
      const limit = settings.data_inlining_row_limit;
      return {
         name,
         dataPath: settings.data_path ?? "",
         encrypted: settings.encrypted === "true",
         formatVersion: settings.version ?? "",
         ...(limit !== undefined
            ? { dataInliningRowLimit: Number(limit) }
            : {}),
         ...(settings.delete_older_than !== undefined
            ? { fileDeletionGrace: settings.delete_older_than }
            : {}),
         sizeBytes: Number(sized[0]?.files ?? 0) + inlined,
      };
   }

   /** An error whose message may echo the catalog database's connection string. */
   private redacted(error: unknown): unknown {
      if (error instanceof CatalogAdminError) return error;
      const message = error instanceof Error ? error.message : String(error);
      const safe = redactPgSecrets(message);
      if (error instanceof LoadRefusalError) {
         return new CatalogAdminError(422, error.reason, safe, error.detail);
      }
      if (
         /Existing DuckLake at metadata catalog .* does not exist/i.test(safe)
      ) {
         return new CatalogAdminError(404, "CATALOG_NOT_FOUND", safe);
      }
      if (/must sit under the catalog's data path/.test(safe)) {
         return new CatalogAdminError(400, "OBJECT_OUTSIDE_DATA_PATH", safe);
      }
      // A request the load path refused (a reader option it cannot use): a 400 with a reason, like every other admin refusal.
      if (error instanceof BadRequestError) {
         return new CatalogAdminError(400, "INVALID_REQUEST", safe);
      }
      if (error instanceof Error && error.message !== safe) {
         const copy = new Error(safe);
         copy.name = error.name;
         return copy;
      }
      return error;
   }
}

async function rows(
   session: DuckDBConnection,
   sql: string,
): Promise<Record<string, unknown>[]> {
   const result = (await session.runSQL(sql)) as unknown;
   if (Array.isArray(result)) return result as Record<string, unknown>[];
   return ((result as { rows?: unknown[] })?.rows ?? []) as Record<
      string,
      unknown
   >[];
}

const q = (name: string) => quoteIdentifier(name, "duckdb");

/**
 * A table write's source, validated: an object to read, or a catalog's table.
 * The `table` kind exists for one migration, moving an orchestrator's tables out
 * of its legacy per-organization catalogs into per-database ones without
 * rebuilding them, and can be removed (with copyStorageTable) once those legacy
 * catalogs are dropped.
 */
type TableSource =
   | { kind: "object"; uri: string; format: LoadFormat; options: LoadOptions }
   | { kind: "table"; catalog: string; schema: string; table: string };

function tableSource(value: unknown): TableSource {
   const source = value as Record<string, unknown> | undefined;
   if (!source || typeof source !== "object") {
      throw badRequest("'source' is required.");
   }
   if ("catalog" in source || "table" in source || "schema" in source) {
      if ("uri" in source || "format" in source || "options" in source) {
         throw badRequest(
            "'source' is either an object ({uri, format}) or a table ({catalog, schema, table}), not both.",
         );
      }
      onlyKeys(source, ["catalog", "schema", "table"]);
      return {
         kind: "table",
         catalog: identifier(source.catalog, "source.catalog"),
         schema: identifier(source.schema, "source.schema"),
         table: identifier(source.table, "source.table"),
      };
   }
   onlyKeys(source, ["uri", "format", "options"]);
   if (typeof source.uri !== "string") {
      throw badRequest("'source.uri' is required.");
   }
   if (!["csv", "parquet", "xlsx", "json"].includes(String(source.format))) {
      throw badRequest("'source.format' must be csv, parquet, xlsx or json.");
   }
   const options = (source.options ?? {}) as LoadOptions;
   for (const key of Object.keys(options)) {
      if (!["header", "delimiter", "sheet", "skip"].includes(key)) {
         throw badRequest(`Unknown reader option '${key}'.`);
      }
   }
   return {
      kind: "object",
      uri: source.uri,
      format: source.format as LoadFormat,
      options,
   };
}

/** An object may carry only its own shape's keys, as the spec's `additionalProperties: false` says; `prefix` is the path the refusal names them under. */
function onlyKeys(
   source: Record<string, unknown>,
   allowed: string[],
   prefix = "source.",
): void {
   for (const key of Object.keys(source)) {
      if (!allowed.includes(key)) {
         throw badRequest(`Unknown key '${prefix}${key}'.`);
      }
   }
}

const MAX_COMMIT_MESSAGE = 1024;
/** Key and cursor columns are lowercase identifiers; the load matches them to the objects' columns by name, ignoring case. */
const LOAD_COLUMN = /^[a-z_][a-z0-9_]*$/;
const MAX_LOAD_OBJECTS = 1000;
const MAX_LOAD_KEYS = 16;

interface LoadRequest {
   commitMessage?: string;
   mode: "merge" | "replace";
   /** Empty for a keyless replace. */
   key: string[];
   objects: string[];
   cursor?: string;
   maxDeletedRows?: number;
}

/** A load's request body, validated: every key allowed, a `merge` keyed and never guarded, a `replace` guarded. */
function loadRequest(value: unknown): LoadRequest {
   const body = value as Record<string, unknown> | undefined;
   if (!body || typeof body !== "object" || Array.isArray(body)) {
      throw badRequest("The request body must be a JSON object.");
   }
   onlyKeys(
      body,
      ["commitMessage", "mode", "key", "objects", "cursor", "maxDeletedRows"],
      "",
   );
   const { commitMessage, mode, cursor, maxDeletedRows } = body;
   if (commitMessage !== undefined) {
      if (
         typeof commitMessage !== "string" ||
         commitMessage === "" ||
         [...commitMessage].length > MAX_COMMIT_MESSAGE ||
         commitMessage.includes("\0")
      ) {
         throw badRequest(
            `'commitMessage' must be 1 to ${MAX_COMMIT_MESSAGE} characters, with no NUL.`,
         );
      }
      // runSQL splits on the separator even inside a literal, so such a message would fail every retry with a 500.
      if (containsStatementSeparator(commitMessage)) {
         throw badRequest(
            "'commitMessage' may not contain a statement separator.",
         );
      }
   }
   if (mode !== "merge" && mode !== "replace") {
      throw badRequest("'mode' must be 'merge' or 'replace'.");
   }
   const key = stringList(body.key, "key", MAX_LOAD_KEYS, LOAD_COLUMN) ?? [];
   if (body.key !== undefined && key.length === 0) {
      throw badRequest("'key' must list at least one column.");
   }
   if (mode === "merge" && key.length === 0) {
      throw badRequest("A merge needs a 'key'.");
   }
   if (mode === "merge" && maxDeletedRows !== undefined) {
      throw badRequest("'maxDeletedRows' applies only to a replace.");
   }
   if (mode === "replace" && maxDeletedRows === undefined) {
      throw badRequest("A replace needs 'maxDeletedRows'.");
   }
   if (
      maxDeletedRows !== undefined &&
      (typeof maxDeletedRows !== "number" ||
         !Number.isSafeInteger(maxDeletedRows) ||
         maxDeletedRows < 0)
   ) {
      throw badRequest("'maxDeletedRows' must be a non-negative integer.");
   }
   if (
      cursor !== undefined &&
      (typeof cursor !== "string" || !LOAD_COLUMN.test(cursor))
   ) {
      throw badRequest(
         "'cursor' must be a lowercase column name ([a-z_][a-z0-9_]*).",
      );
   }
   const objects = stringList(body.objects, "objects", MAX_LOAD_OBJECTS);
   if (!objects || objects.length === 0) {
      throw badRequest("'objects' must list at least one object.");
   }
   return {
      ...(commitMessage !== undefined
         ? { commitMessage: commitMessage as string }
         : {}),
      mode,
      key,
      objects,
      ...(cursor !== undefined ? { cursor: cursor as string } : {}),
      ...(maxDeletedRows !== undefined
         ? { maxDeletedRows: maxDeletedRows as number }
         : {}),
   };
}

/** An optional list of distinct strings, at most `max` long, each matching `pattern` when given. */
function stringList(
   value: unknown,
   what: string,
   max: number,
   pattern?: RegExp,
): string[] | undefined {
   if (value === undefined) return undefined;
   if (
      !Array.isArray(value) ||
      value.length > max ||
      value.some(
         (v) =>
            typeof v !== "string" || v === "" || (pattern && !pattern.test(v)),
      ) ||
      new Set(value).size !== value.length
   ) {
      throw badRequest(
         `'${what}' must be a list of at most ${max} distinct ${pattern ? "lowercase column names ([a-z_][a-z0-9_]*)" : "object URIs"}.`,
      );
   }
   return value as string[];
}

/** Refuses a schema the catalog does not record, read on the catalog database itself. */
async function requireRecordedSchema(
   pg: CatalogQuery,
   catalog: string,
   schema: string,
): Promise<void> {
   const found = await pg(
      `SELECT 1 FROM ${pgIdent(catalog)}.ducklake_schema WHERE schema_name = ${pgLiteral(schema)} AND end_snapshot IS NULL`,
   );
   if (found.length === 0) {
      throw new CatalogAdminError(
         404,
         "SCHEMA_NOT_FOUND",
         `No schema '${schema}' in catalog '${catalog}'.`,
      );
   }
}

/** Drops a connection that has stopped answering by destroying its socket: node-pg has no public destroy, and `end()` waits for an `end` event a dead socket may never emit. */
function destroyConnection(client: Client): void {
   (
      client as unknown as { connection?: { stream?: { destroy(): void } } }
   ).connection?.stream?.destroy();
}

function sourceNotFound(message: string): CatalogAdminError {
   return new CatalogAdminError(404, "SOURCE_NOT_FOUND", message);
}

function identifier(value: unknown, what: string): string {
   if (typeof value !== "string" || !IDENT.test(value)) {
      throw badRequest(
         `'${what}' must be a plain identifier ([A-Za-z_][A-Za-z0-9_]*).`,
      );
   }
   return value;
}

/** A schema name the admin API may create: lowercase and not reserved. */
function schemaName(value: unknown, what: string): string {
   if (typeof value !== "string" || !READABLE_SCHEMA_NAME.test(value)) {
      throw badRequest(
         `'${what}' must be a lowercase name ([a-z_][a-z0-9_]*).`,
      );
   }
   refuseReserved(value);
   return value;
}

/** Refuses a name the engine owns or DuckLake needs, whatever its case, and the metadata catalogs' prefix, as `readableSchemas` does. */
function refuseReserved(schema: string): void {
   const folded = schema.toLowerCase();
   if (
      RESERVED_SCHEMAS.includes(folded) ||
      folded.startsWith(METADATA_CATALOG_PREFIX)
   ) {
      throw new CatalogAdminError(
         400,
         "SCHEMA_RESERVED",
         `'${schema}' is a reserved schema name.`,
      );
   }
}

async function schemaExists(
   session: DuckDBConnection,
   catalog: string,
   schema: string,
): Promise<boolean> {
   const found = await rows(
      session,
      `SELECT 1 FROM duckdb_schemas() WHERE database_name = '${escapeSQL(catalog)}' ` +
         `AND schema_name = '${escapeSQL(schema)}'`,
   );
   return found.length > 0;
}

async function requireSchema(
   session: DuckDBConnection,
   catalog: string,
   schema: string,
): Promise<void> {
   if (!(await schemaExists(session, catalog, schema))) {
      throw new CatalogAdminError(
         404,
         "SCHEMA_NOT_FOUND",
         `No schema '${schema}' in catalog '${catalog}'.`,
      );
   }
}

function optionalLimit(value: unknown): number | undefined {
   if (value === undefined) return undefined;
   if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
      throw badRequest(
         "'dataInliningRowLimit' must be a non-negative integer.",
      );
   }
   return value;
}

function optionalGrace(value: unknown): string | undefined {
   if (value === undefined) return undefined;
   if (typeof value !== "string" || !INTERVAL.test(value.trim())) {
      throw badRequest(
         "'fileDeletionGrace' must be an interval like '1 day' or '12 hours'.",
      );
   }
   return value.trim();
}

async function setOptions(
   session: DuckDBConnection,
   name: string,
   options: { limit?: number; grace?: string },
): Promise<void> {
   // Recorded in the catalog with set_option, so every later attach reads them
   // (an ATTACH option lasts one session).
   if (options.limit !== undefined) {
      await session.runSQL(
         `CALL ${q(name)}.set_option('data_inlining_row_limit', ${options.limit})`,
      );
   }
   if (options.grace !== undefined) {
      await session.runSQL(
         `CALL ${q(name)}.set_option('delete_older_than', '${escapeSQL(options.grace)}')`,
      );
   }
}

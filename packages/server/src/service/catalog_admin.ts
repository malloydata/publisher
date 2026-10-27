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

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../api";
import { BadRequestError, ConflictError, TableNotFoundError } from "../errors";
import { logger } from "../logger";
import {
   applySessionResourceLimits,
   attachDuckLakeForCreate,
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
   escapeSQL,
} from "./connection";
import {
   cleanupStorageFiles,
   createIsolatedBuildSession,
   dropStorageTable,
} from "./materialization_build_session";
import { redactPgSecrets } from "../pg_helpers";
import { quoteIdentifier } from "./quoting";
import { withStatementUsage } from "./statement_usage";
import {
   type LoadFormat,
   type LoadOptions,
   copyStorageTable,
   loadStorageTable,
} from "./storage_table_load";

type ApiConnection = components["schemas"]["Connection"];
type DucklakeStorage = NonNullable<
   components["schemas"]["DucklakeConnection"]["storage"]
>;

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** A role name safe in a libpq `options=-crole=` value, hyphens included (`<prefix>-ducklake`). */
const ROLE = /^[A-Za-z_][A-Za-z0-9_-]*$/;
/** DuckLake's interval spelling for `delete_older_than`, e.g. `1 day`, `12 hours`. */
const INTERVAL = /^(\d+) (second|minute|hour|day|week)s?$/;
/** Used by a snapshot delete when the catalog records no file-deletion grace. */
const DEFAULT_FILE_DELETION_GRACE = "1 day";

/** An admin refusal: its HTTP status and the machine-readable `reason`. */
export class CatalogAdminError extends Error {
   constructor(
      readonly status: number,
      readonly reason: string,
      message: string,
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

export interface CatalogAdminConfig {
   /** A libpq connection string (keyword or URI form) for the catalog database. */
   catalogDatabase: string;
   /** The storage credential every catalog's data path is read and written with. */
   storage?: Omit<DucklakeStorage, "bucketUrl">;
}

/**
 * Reads the listener's configuration. Null when no catalog database is set: the
 * routes then answer 503, so a listener started for nothing else still serves
 * its health endpoints. A storage setting that cannot be used throws, so a typo
 * fails startup.
 */
export function catalogAdminConfigFromEnv(
   env: NodeJS.ProcessEnv,
): CatalogAdminConfig | null {
   const catalogDatabase = env.PUBLISHER_ADMIN_CATALOG_DATABASE?.trim();
   if (!catalogDatabase) return null;
   // A session acts as each catalog's owner through libpq `options`, which would
   // silently replace options given here; refuse them rather than drop them.
   if (/(^|[\s?&])options=/i.test(catalogDatabase)) {
      throw new Error(
         "PUBLISHER_ADMIN_CATALOG_DATABASE must not set `options`: the admin API sets the session role per catalog. Set server parameters on the role or database instead (ALTER ROLE … SET).",
      );
   }
   const provider = env.PUBLISHER_ADMIN_STORAGE_PROVIDER?.trim() ?? "";
   const keyId = env.PUBLISHER_ADMIN_STORAGE_KEY_ID;
   const secret = env.PUBLISHER_ADMIN_STORAGE_SECRET;
   let storage: CatalogAdminConfig["storage"];
   switch (provider) {
      case "":
         storage = undefined;
         break;
      case "s3":
         if (!keyId || !secret) {
            throw new Error(
               "PUBLISHER_ADMIN_STORAGE_PROVIDER=s3 needs PUBLISHER_ADMIN_STORAGE_KEY_ID and PUBLISHER_ADMIN_STORAGE_SECRET.",
            );
         }
         storage = {
            s3Connection: {
               provider: "config",
               accessKeyId: keyId,
               secretAccessKey: secret,
               region: env.PUBLISHER_ADMIN_STORAGE_REGION,
               endpoint: env.PUBLISHER_ADMIN_STORAGE_ENDPOINT,
            },
         };
         break;
      case "s3_credential_chain":
         storage = {
            s3Connection: {
               provider: "credential_chain",
               region: env.PUBLISHER_ADMIN_STORAGE_REGION,
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
      default:
         throw new Error(
            `PUBLISHER_ADMIN_STORAGE_PROVIDER must be s3, s3_credential_chain or gcs, got '${provider}'.`,
         );
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

export interface CatalogTable {
   schema: string;
   name: string;
   columns: { name: string; type: string }[];
   rowCount?: number;
}

export interface CatalogSnapshot {
   id: number;
   time: string;
   changes: Record<string, unknown>;
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
      const schemaNames = schemas.map((s) => identifier(s, "schemas[]"));

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

   async listTables(
      name: string,
      schema: string,
   ): Promise<CatalogTableSummary[]> {
      identifier(schema, "schema");
      return this.withAttached(name, "read", async (s) => {
         await requireSchema(s, name, schema);
         const tables = (await rows(
            s,
            `SELECT table_name FROM duckdb_tables() WHERE database_name = '${escapeSQL(name)}' ` +
               `AND schema_name = '${escapeSQL(schema)}' ORDER BY 1`,
         )) as { table_name: string }[];
         // ducklake_table_info names a table's schema by id; the catalog's own
         // schema table maps the name to it (the live row, end_snapshot null).
         const meta = `${q(`__ducklake_metadata_${name}`)}.${q(name)}`;
         const sizes = new Map<string, number>();
         for (const r of (await rows(
            s,
            `SELECT i.table_name, i.file_size_bytes + i.delete_file_size_bytes AS bytes ` +
               `FROM ducklake_table_info('${escapeSQL(name)}') i ` +
               `JOIN ${meta}.ducklake_schema m ON m.schema_id = CAST(i.schema_id AS BIGINT) ` +
               `AND m.end_snapshot IS NULL WHERE m.schema_name = '${escapeSQL(schema)}'`,
         )) as { table_name: string; bytes: unknown }[]) {
            sizes.set(r.table_name, Number(r.bytes ?? 0));
         }
         return tables.map((t) => ({
            name: t.table_name,
            sizeBytes: sizes.get(t.table_name) ?? 0,
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
         return {
            schema,
            name: table,
            columns,
            ...(estimate == null ? {} : { rowCount: Number(estimate) }),
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
      const { owner, sourceOwner } = await this.withCatalogDatabase(
         async (pg) => {
            await this.requireCatalog(pg, name);
            if (source.kind === "table" && source.catalog !== name) {
               if (!(await this.catalogExists(pg, source.catalog))) {
                  throw sourceNotFound(
                     `Source catalog '${source.catalog}' does not exist.`,
                  );
               }
            }
            return {
               owner: await this.schemaOwner(pg, name),
               sourceOwner:
                  source.kind === "table"
                     ? await this.schemaOwner(pg, source.catalog)
                     : undefined,
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
         if (error instanceof TableNotFoundError) {
            throw sourceNotFound(redactPgSecrets(error.message));
         }
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
      return {
         table: await this.getTable(name, schema, table),
         created: !exists,
      };
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
               `SELECT snapshot_id, snapshot_time, changes FROM ${q(name)}.snapshots() ` +
                  `ORDER BY snapshot_id DESC LIMIT ${Math.max(1, Math.min(limit, 1000))}`,
            )) as {
               snapshot_id: unknown;
               snapshot_time: unknown;
               changes: unknown;
            }[]
         ).map((r) => ({
            id: Number(r.snapshot_id),
            time: new Date(String(r.snapshot_time)).toISOString(),
            changes: (r.changes ?? {}) as Record<string, unknown>,
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

   // ---- internals ----

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
         await session.runSQL(
            `ATTACH '${escapeSQL(this.config.catalogDatabase)}' AS admin_pg (TYPE postgres, READ_ONLY)`,
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
      if (
         /Existing DuckLake at metadata catalog .* does not exist/i.test(safe)
      ) {
         return new CatalogAdminError(404, "CATALOG_NOT_FOUND", safe);
      }
      if (/must sit under the catalog's data path/.test(safe)) {
         return new CatalogAdminError(400, "OBJECT_OUTSIDE_DATA_PATH", safe);
      }
      // A request the load path refused (a reader option it cannot use):
      // a 400 with a reason, like every other admin refusal.
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

/** A table write's source, validated: an object to read, or a catalog's table. */
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
      return {
         kind: "table",
         catalog: identifier(source.catalog, "source.catalog"),
         schema: identifier(source.schema, "source.schema"),
         table: identifier(source.table, "source.table"),
      };
   }
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

async function requireSchema(
   session: DuckDBConnection,
   catalog: string,
   schema: string,
): Promise<void> {
   const found = await rows(
      session,
      `SELECT 1 FROM duckdb_schemas() WHERE database_name = '${escapeSQL(catalog)}' ` +
         `AND schema_name = '${escapeSQL(schema)}'`,
   );
   if (found.length === 0) {
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

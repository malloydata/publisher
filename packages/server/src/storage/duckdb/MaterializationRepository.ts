// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BuildManifestResult,
   Materialization,
   MaterializationListOptions,
   MaterializationStatus,
   MaterializationUpdate,
} from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { newId, Row, rowTimestamps, updateRow } from "./rows";

const TERMINAL_STATUSES: ReadonlySet<MaterializationStatus> = new Set([
   "MANIFEST_FILE_READY",
   "FAILED",
   "CANCELLED",
]);

/** Non-terminal statuses count as "active" for the single-active guard. */
const ACTIVE_STATUSES: readonly MaterializationStatus[] = [
   "PENDING",
   "MANIFEST_ROWS_READY",
];

/**
 * The slot a run holds while it is active. One run at a time per package by
 * default; a run that writes only one version's own tables holds that
 * version's slot instead (`lockVersion`), so versions build side by side.
 * Package names cannot contain `@`, so the two shapes never collide.
 */
function activeKeyFor(
   environmentId: string,
   packageName: string,
   lockVersion?: string,
): string {
   return lockVersion === undefined
      ? `${environmentId}|${packageName}`
      : `${environmentId}|${packageName}@${lockVersion}`;
}

/**
 * Thrown when an atomic insert loses a race on (environment, package) active
 * materialization. Surfaced separately from a generic DB error so the service
 * layer can translate to `MaterializationConflictError`.
 */
export class DuplicateActiveMaterializationError extends Error {
   constructor(environmentId: string, packageName: string) {
      super(
         `Active materialization already exists for (${environmentId}, ${packageName})`,
      );
   }
}

/**
 * DuckDB-backed repository for package materializations.
 *
 * A Materialization tracks a single build run for an (environment, package)
 * pair through its lifecycle: PENDING -> MANIFEST_ROWS_READY ->
 * MANIFEST_FILE_READY, or -> FAILED | CANCELLED.
 */
export class MaterializationRepository {
   constructor(private db: DuckDBConnection) {}

   // Unbounded without a limit: callers may rely on one package's full history,
   // which is bounded by how often that package is built.
   async list(
      environmentId: string,
      packageName: string,
      options?: MaterializationListOptions,
   ): Promise<Materialization[]> {
      let sql =
         "SELECT * FROM materializations WHERE environment_id = ? AND package_name = ?";
      const params: unknown[] = [environmentId, packageName];
      if (options?.version !== undefined) {
         sql += options.includeUnversioned
            ? " AND (version = ? OR version IS NULL)"
            : " AND version = ?";
         params.push(options.version);
      }
      sql += " ORDER BY created_at DESC";
      if (options?.limit !== undefined) {
         sql += " LIMIT ?";
         params.push(options.limit);
      }
      if (options?.offset !== undefined) {
         sql += " OFFSET ?";
         params.push(options.offset);
      }
      const rows = await this.db.all<Row>(sql, params);
      return rows.map(mapRow);
   }

   /** Every run of an environment, across its packages, newest first. */
   async listByEnvironment(environmentId: string): Promise<Materialization[]> {
      const rows = await this.db.all<Row>(
         "SELECT * FROM materializations WHERE environment_id = ? ORDER BY created_at DESC",
         [environmentId],
      );
      return rows.map(mapRow);
   }

   /**
    * `created_at` of the newest scheduler-fired (metadata.trigger=SCHEDULER)
    * materialization for (env, package), or null if none. The standalone
    * scheduler re-anchors `nextFireAt` to `nextAfter(lastScheduledFire)` so a
    * restart fires exactly one catch-up instead of skipping the occurrence.
    */
   async getLatestScheduledFireAt(
      environmentId: string,
      packageName: string,
      // One published version's fires; undefined, the package's.
      version?: string,
   ): Promise<Date | null> {
      const row = await this.db.get<{ created_at: unknown }>(
         `SELECT created_at FROM materializations
           WHERE environment_id = ? AND package_name = ?
             AND json_extract_string(metadata, '$.trigger') = 'SCHEDULER'
             ${version !== undefined ? "AND version = ?" : ""}
           ORDER BY created_at DESC
           LIMIT 1`,
         version !== undefined
            ? [environmentId, packageName, version]
            : [environmentId, packageName],
      );
      return row?.created_at ? new Date(row.created_at as string) : null;
   }

   async getById(id: string): Promise<Materialization | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM materializations WHERE id = ?",
         [id],
      );
      return row ? mapRow(row) : null;
   }

   /**
    * The active run holding the package's slot, or with `lockVersion` that
    * version's slot (see activeKeyFor).
    */
   async getActive(
      environmentId: string,
      packageName: string,
      lockVersion?: string,
   ): Promise<Materialization | null> {
      const placeholders = ACTIVE_STATUSES.map(() => "?").join(", ");
      const row = await this.db.get<Row>(
         `SELECT * FROM materializations WHERE active_key = ? AND status IN (${placeholders})`,
         [
            activeKeyFor(environmentId, packageName, lockVersion),
            ...ACTIVE_STATUSES,
         ],
      );
      return row ? mapRow(row) : null;
   }

   async create(
      environmentId: string,
      packageName: string,
      status: MaterializationStatus = "PENDING",
      metadata: Record<string, unknown> | null = null,
      options: {
         /** The package version the run builds; null when it has none. */
         version?: string | null;
         /** Hold this version's slot rather than the package's. */
         lockVersion?: string;
      } = {},
   ): Promise<Materialization> {
      const id = newId();
      const iso = new Date().toISOString();
      // active_key is set iff the row is non-terminal. Its unique index makes a
      // concurrent second create on the same (environment, package) fail here
      // rather than slip through a check-then-write window.
      const activeKey = TERMINAL_STATUSES.has(status)
         ? null
         : activeKeyFor(environmentId, packageName, options.lockVersion);
      const metadataJson = metadata ? JSON.stringify(metadata) : null;

      try {
         const rows = await this.db.all<Row>(
            `INSERT INTO materializations (id, environment_id, package_name, status, active_key, metadata, manifest, created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            RETURNING *`,
            [
               id,
               environmentId,
               packageName,
               status,
               activeKey,
               metadataJson,
               iso,
               iso,
               options.version ?? null,
            ],
         );
         return mapRow(rows[0]);
      } catch (err) {
         if (isUniqueViolation(err, "idx_materializations_active_key")) {
            throw new DuplicateActiveMaterializationError(
               environmentId,
               packageName,
            );
         }
         throw err;
      }
   }

   async update(
      id: string,
      updates: MaterializationUpdate,
   ): Promise<Materialization> {
      // Clear active_key on any transition to a terminal state; keep it on a
      // transition between non-terminal states, so a run keeps the slot it
      // was created holding (its package's or its version's). A terminal
      // state has no way out, so a key is never set again; the fallback only
      // covers a row written without one. The unique index guarantees no two
      // active rows ever hold one slot.
      const activeKey =
         updates.status === undefined
            ? []
            : TERMINAL_STATUSES.has(updates.status)
              ? ["active_key = NULL"]
              : [
                   "active_key = COALESCE(active_key, environment_id || '|' || package_name)",
                ];

      await updateRow(
         this.db,
         "materializations",
         id,
         {
            status: updates.status,
            started_at: updates.startedAt?.toISOString(),
            completed_at: updates.completedAt?.toISOString(),
            error: updates.error,
            metadata: jsonOrNull(updates.metadata),
            manifest: jsonOrNull(updates.manifest),
         },
         activeKey,
      );

      const updated = await this.getById(id);
      if (!updated) {
         throw new Error(`Materialization ${id} not found after update`);
      }
      return updated;
   }

   async deleteById(id: string): Promise<void> {
      await this.db.run("DELETE FROM materializations WHERE id = ?", [id]);
   }

   // Cascade deletes remove records only and issue no DDL: dropping physical
   // tables is opt-in (deleteMaterialization({ dropTables })), and a DROP here
   // could fire against a connection the delete path already tore down.
   //
   // Limitation: standalone, with no control plane owning physical GC, nothing
   // drops those tables, so they accumulate in the warehouse. A best-effort
   // drop-before-teardown belongs above these methods, not in them.
   async deleteByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM materializations WHERE environment_id = ?",
         [environmentId],
      );
   }

   async deleteByPackage(
      environmentId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM materializations WHERE environment_id = ? AND package_name = ?",
         [environmentId, packageName],
      );
   }
}

function mapRow(row: Row): Materialization {
   const metadata = parseJsonColumn<Row>(row.metadata);
   return {
      id: row.id as string,
      environmentId: row.environment_id as string,
      packageName: row.package_name as string,
      status: row.status as MaterializationStatus,
      manifest: parseJsonColumn<BuildManifestResult>(row.manifest),
      metadata,
      startedAt: row.started_at ? new Date(row.started_at as string) : null,
      completedAt: row.completed_at
         ? new Date(row.completed_at as string)
         : null,
      error: row.error != null ? (row.error as string) : null,
      version: row.version != null ? (row.version as string) : null,
      ...rowTimestamps(row),
   };
}

/** `undefined` stays unset; null and other falsy values are written as NULL. */
function jsonOrNull(
   value: object | null | undefined,
): string | null | undefined {
   if (value === undefined) return undefined;
   return value ? JSON.stringify(value) : null;
}

/** Parse a nullable JSON text column, returning null on absence or parse error. */
function parseJsonColumn<T>(value: unknown): T | null {
   if (value == null) return null;
   try {
      return JSON.parse(value as string) as T;
   } catch {
      return null;
   }
}

/**
 * DuckDB surfaces unique-constraint violations as plain Errors. Matches the
 * index name, or DuckDB's generic duplicate-key / unique-constraint wording.
 */
function isUniqueViolation(err: unknown, indexName: string): boolean {
   if (!(err instanceof Error)) return false;
   const msg = err.message;
   return (
      msg.includes(indexName) || /duplicate key|unique constraint/i.test(msg)
   );
}

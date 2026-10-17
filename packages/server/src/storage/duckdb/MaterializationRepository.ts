// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BuildManifestResult,
   Materialization,
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

function activeKeyFor(environmentId: string, packageName: string): string {
   return `${environmentId}|${packageName}`;
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

   // Intentionally UNBOUNDED when no limit is passed: callers may rely on the
   // full history for a single package, which is bounded by how often that one
   // package is built.
   async list(
      environmentId: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      let sql =
         "SELECT * FROM materializations WHERE environment_id = ? AND package_name = ? ORDER BY created_at DESC";
      const params: unknown[] = [environmentId, packageName];
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

   /**
    * `created_at` of the newest scheduler-fired (metadata.trigger=SCHEDULER)
    * materialization for (env, package), or null if none. The standalone
    * scheduler uses this to recover a missed occurrence across a restart:
    * re-anchoring `nextFireAt` to `nextAfter(lastScheduledFire)` fires exactly
    * one catch-up after downtime (then jumps forward), rather than skipping the
    * occurrence entirely. `metadata` is a JSON column, so the trigger is read
    * with `json_extract_string`.
    */
   async getLatestScheduledFireAt(
      environmentId: string,
      packageName: string,
   ): Promise<Date | null> {
      const row = await this.db.get<{ created_at: unknown }>(
         `SELECT created_at FROM materializations
           WHERE environment_id = ? AND package_name = ?
             AND json_extract_string(metadata, '$.trigger') = 'SCHEDULER'
           ORDER BY created_at DESC
           LIMIT 1`,
         [environmentId, packageName],
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

   async getActive(
      environmentId: string,
      packageName: string,
   ): Promise<Materialization | null> {
      const placeholders = ACTIVE_STATUSES.map(() => "?").join(", ");
      const row = await this.db.get<Row>(
         `SELECT * FROM materializations WHERE environment_id = ? AND package_name = ? AND status IN (${placeholders})`,
         [environmentId, packageName, ...ACTIVE_STATUSES],
      );
      return row ? mapRow(row) : null;
   }

   async create(
      environmentId: string,
      packageName: string,
      status: MaterializationStatus = "PENDING",
      metadata: Record<string, unknown> | null = null,
   ): Promise<Materialization> {
      const id = newId();
      const iso = new Date().toISOString();
      // Set active_key iff the row is in a non-terminal state. The unique
      // index on active_key makes the race-free conditional insert: a second
      // concurrent create on the same (environment, package) fails here rather
      // than in a check-then-write window.
      const activeKey = TERMINAL_STATUSES.has(status)
         ? null
         : activeKeyFor(environmentId, packageName);
      const metadataJson = metadata ? JSON.stringify(metadata) : null;

      try {
         const rows = await this.db.all<Row>(
            `INSERT INTO materializations (id, environment_id, package_name, status, active_key, metadata, manifest, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
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
      // Clear active_key on any transition to a terminal state; set it on
      // any transition to a non-terminal state. The unique index
      // guarantees we can never end up with two active rows for the same
      // (environment, package).
      const activeKey =
         updates.status === undefined
            ? []
            : TERMINAL_STATUSES.has(updates.status)
              ? ["active_key = NULL"]
              : ["active_key = environment_id || '|' || package_name"];

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

   // Cascade deletes remove the publisher's records only; they intentionally
   // issue no DDL. Dropping the physical tables is opt-in
   // (deleteMaterialization({ dropTables })) and otherwise the caller's
   // responsibility, so a materialized table outlives its record when an
   // environment or package is deleted. Adding a DROP here would also risk
   // firing against a connection already torn down by the delete path.
   //
   // Standalone caveat (#901): with no control plane owning physical GC,
   // nothing drops those tables today, so they accumulate in the warehouse.
   // That is an open follow-up, not a settled contract — its best-effort
   // drop-before-teardown belongs ABOVE these methods, so it completes this
   // design rather than moving DDL into the low-level record deletes.
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
 * DuckDB surfaces unique-constraint violations as plain Errors whose message
 * mentions the violated index. We match on the index name rather than a
 * generic substring so we don't misclassify unrelated constraint errors.
 */
function isUniqueViolation(err: unknown, indexName: string): boolean {
   if (!(err instanceof Error)) return false;
   const msg = err.message;
   return (
      msg.includes(indexName) || /duplicate key|unique constraint/i.test(msg)
   );
}

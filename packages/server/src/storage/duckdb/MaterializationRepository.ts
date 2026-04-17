import { Materialization, MaterializationStatus } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

const TERMINAL_STATUSES: ReadonlySet<MaterializationStatus> = new Set([
   "SUCCESS",
   "FAILED",
   "CANCELLED",
]);

function activeKeyFor(projectId: string, packageName: string): string {
   return `${projectId}|${packageName}`;
}

/**
 * Thrown when an atomic insert loses a race on (project, package) active
 * materialization. Surfaced separately from a generic DB error so the service
 * layer can translate to `MaterializationConflictError`.
 */
export class DuplicateActiveMaterializationError extends Error {
   constructor(projectId: string, packageName: string) {
      super(
         `Active materialization already exists for (${projectId}, ${packageName})`,
      );
   }
}

/**
 * DuckDB-backed repository for package materializations.
 *
 * A Materialization tracks a single build run for a (project, package) pair
 * through its lifecycle: PENDING -> RUNNING -> SUCCESS | FAILED | CANCELLED.
 */
export class MaterializationRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
   }

   private now(): Date {
      return new Date();
   }

   async list(
      projectId: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      let sql =
         "SELECT * FROM materializations WHERE project_id = ? AND package_name = ? ORDER BY created_at DESC";
      const params: unknown[] = [projectId, packageName];
      if (options?.limit !== undefined) {
         sql += " LIMIT ?";
         params.push(options.limit);
      }
      if (options?.offset !== undefined) {
         sql += " OFFSET ?";
         params.push(options.offset);
      }
      const rows = await this.db.all<Record<string, unknown>>(sql, params);
      return rows.map(this.mapRow);
   }

   async getById(id: string): Promise<Materialization | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM materializations WHERE id = ?",
         [id],
      );
      return row ? this.mapRow(row) : null;
   }

   async getActive(
      projectId: string,
      packageName: string,
   ): Promise<Materialization | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM materializations WHERE project_id = ? AND package_name = ? AND status IN ('PENDING', 'RUNNING')",
         [projectId, packageName],
      );
      return row ? this.mapRow(row) : null;
   }

   async create(
      projectId: string,
      packageName: string,
      status: MaterializationStatus = "PENDING",
      metadata: Record<string, unknown> | null = null,
   ): Promise<Materialization> {
      const id = this.generateId();
      const now = this.now();
      const iso = now.toISOString();
      // Set active_key iff the row is in a non-terminal state. The unique
      // index on active_key makes the race-free conditional insert: a second
      // concurrent create on the same (project, package) fails here rather
      // than in a check-then-write window.
      const activeKey = TERMINAL_STATUSES.has(status)
         ? null
         : activeKeyFor(projectId, packageName);
      const metadataJson = metadata ? JSON.stringify(metadata) : null;

      try {
         const rows = await this.db.all<Record<string, unknown>>(
            `INSERT INTO materializations (id, project_id, package_name, status, active_key, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING *`,
            [
               id,
               projectId,
               packageName,
               status,
               activeKey,
               metadataJson,
               iso,
               iso,
            ],
         );
         return this.mapRow(rows[0]);
      } catch (err) {
         if (isUniqueViolation(err, "idx_materializations_active_key")) {
            throw new DuplicateActiveMaterializationError(
               projectId,
               packageName,
            );
         }
         throw err;
      }
   }

   async update(
      id: string,
      updates: {
         status?: MaterializationStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<Materialization> {
      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];

      if (updates.status !== undefined) {
         setClauses.push(`status = ?`);
         params.push(updates.status);
         // Clear active_key on any transition to a terminal state; set it on
         // any transition to a non-terminal state. The unique index
         // guarantees we can never end up with two active rows for the same
         // (project, package).
         if (TERMINAL_STATUSES.has(updates.status)) {
            setClauses.push(`active_key = NULL`);
         } else {
            setClauses.push(`active_key = project_id || '|' || package_name`);
         }
      }
      if (updates.startedAt !== undefined) {
         setClauses.push(`started_at = ?`);
         params.push(updates.startedAt.toISOString());
      }
      if (updates.completedAt !== undefined) {
         setClauses.push(`completed_at = ?`);
         params.push(updates.completedAt.toISOString());
      }
      if (updates.error !== undefined) {
         setClauses.push(`error = ?`);
         params.push(updates.error);
      }
      if (updates.metadata !== undefined) {
         setClauses.push(`metadata = ?`);
         params.push(
            updates.metadata ? JSON.stringify(updates.metadata) : null,
         );
      }

      setClauses.push(`updated_at = ?`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE materializations SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      const updated = await this.getById(id);
      if (!updated) {
         throw new Error(`Materialization ${id} not found after update`);
      }
      return updated;
   }

   async deleteByProjectId(projectId: string): Promise<void> {
      await this.db.run("DELETE FROM materializations WHERE project_id = ?", [
         projectId,
      ]);
   }

   async deleteById(id: string): Promise<void> {
      await this.db.run("DELETE FROM materializations WHERE id = ?", [id]);
   }

   async deleteByPackage(
      projectId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM materializations WHERE project_id = ? AND package_name = ?",
         [projectId, packageName],
      );
   }

   private mapRow(row: Record<string, unknown>): Materialization {
      let metadata: Record<string, unknown> | null = null;
      if (row.metadata) {
         try {
            metadata = JSON.parse(row.metadata as string);
         } catch {
            metadata = null;
         }
      }

      return {
         id: row.id as string,
         projectId: row.project_id as string,
         packageName: row.package_name as string,
         status: row.status as MaterializationStatus,
         startedAt: row.started_at ? new Date(row.started_at as string) : null,
         completedAt: row.completed_at
            ? new Date(row.completed_at as string)
            : null,
         error: row.error != null ? (row.error as string) : null,
         metadata,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
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

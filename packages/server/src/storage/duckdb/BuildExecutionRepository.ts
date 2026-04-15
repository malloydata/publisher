import { BuildExecution, BuildExecutionStatus } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * DuckDB-backed repository for package build executions.
 *
 * A BuildExecution tracks a single build run for a (project, package) pair
 * through its lifecycle: PENDING -> RUNNING -> SUCCESS | FAILED | CANCELLED.
 */
export class BuildExecutionRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
   }

   private now(): Date {
      return new Date();
   }

   async listExecutions(
      projectId: string,
      packageName: string,
   ): Promise<BuildExecution[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM build_executions WHERE project_id = ? AND package_name = ? ORDER BY created_at DESC",
         [projectId, packageName],
      );
      return rows.map(this.mapToExecution);
   }

   async getExecutionById(id: string): Promise<BuildExecution | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM build_executions WHERE id = ?",
         [id],
      );
      return row ? this.mapToExecution(row) : null;
   }

   async getRunningExecution(
      projectId: string,
      packageName: string,
   ): Promise<BuildExecution | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM build_executions WHERE project_id = ? AND package_name = ? AND status IN ('PENDING', 'RUNNING')",
         [projectId, packageName],
      );
      return row ? this.mapToExecution(row) : null;
   }

   /**
    * Atomically creates an execution only if no PENDING/RUNNING execution
    * exists for this (project, package). Returns null when an active
    * execution already exists.
    */
   async createExecution(
      projectId: string,
      packageName: string,
      status: BuildExecutionStatus = "PENDING",
   ): Promise<BuildExecution | null> {
      const id = this.generateId();
      const now = this.now();
      const iso = now.toISOString();

      const rows = await this.db.all<Record<string, unknown>>(
         `INSERT INTO build_executions (id, project_id, package_name, status, created_at, updated_at)
         SELECT ?, ?, ?, ?, ?, ?
         WHERE NOT EXISTS (
            SELECT 1 FROM build_executions
            WHERE project_id = ? AND package_name = ? AND status IN ('PENDING', 'RUNNING')
         )
         RETURNING *`,
         [id, projectId, packageName, status, iso, iso, projectId, packageName],
      );

      if (rows.length === 0) {
         return null;
      }

      return this.mapToExecution(rows[0]);
   }

   async updateExecution(
      id: string,
      updates: {
         status?: BuildExecutionStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<BuildExecution> {
      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];

      if (updates.status !== undefined) {
         setClauses.push(`status = ?`);
         params.push(updates.status);
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
         `UPDATE build_executions SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      const updated = await this.getExecutionById(id);
      if (!updated) {
         throw new Error(`Build execution ${id} not found after update`);
      }
      return updated;
   }

   async deleteByProjectId(projectId: string): Promise<void> {
      await this.db.run("DELETE FROM build_executions WHERE project_id = ?", [
         projectId,
      ]);
   }

   async deleteByPackage(
      projectId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM build_executions WHERE project_id = ? AND package_name = ?",
         [projectId, packageName],
      );
   }

   private mapToExecution(row: Record<string, unknown>): BuildExecution {
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
         status: row.status as BuildExecutionStatus,
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

import { Task, TaskExecution, TaskExecutionStatus } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * DuckDB-backed repository for materialization tasks and their executions.
 *
 * A Task defines what to materialize and is scoped to a project. 
 * A TaskExecution tracks a single run of a task through its lifecycle: 
 * PENDING -> RUNNING -> SUCCESS | FAILED | CANCELLED.
 */
export class TaskRepository {
   constructor(private db: DuckDBConnection) {}

   /** Timestamp-prefixed ID for rough chronological ordering without DB sequences. */
   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
   }

   private now(): Date {
      return new Date();
   }

   // ==================== TASKS ====================

   async listTasks(projectId: string): Promise<Task[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM tasks WHERE project_id = ? ORDER BY name",
         [projectId],
      );
      return rows.map(this.mapToTask);
   }

   async getTaskById(id: string): Promise<Task | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM tasks WHERE id = ?",
         [id],
      );
      return row ? this.mapToTask(row) : null;
   }

   async getTaskByName(projectId: string, name: string): Promise<Task | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM tasks WHERE project_id = ? AND name = ?",
         [projectId, name],
      );
      return row ? this.mapToTask(row) : null;
   }

   async createTask(
      task: Omit<Task, "id" | "createdAt" | "updatedAt">,
   ): Promise<Task> {
      const id = this.generateId();
      const now = this.now();

      await this.db.run(
         `INSERT INTO tasks (id, project_id, name, type, config, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
         [
            id,
            task.projectId,
            task.name,
            task.type,
            JSON.stringify(task.config),
            now.toISOString(),
            now.toISOString(),
         ],
      );

      return {
         id,
         ...task,
         createdAt: now,
         updatedAt: now,
      };
   }

   /**
    * Applies a partial update to a task. Only provided fields are set;
    * `updated_at` is always bumped. Re-fetches the full row after writing
    * to return the canonical state.
    */
   async updateTask(id: string, updates: Partial<Task>): Promise<Task> {
      const existing = await this.getTaskById(id);
      if (!existing) {
         throw new Error(`Task with id ${id} not found`);
      }

      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];

      if (updates.name !== undefined) {
         setClauses.push(`name = ?`);
         params.push(updates.name);
      }
      if (updates.type !== undefined) {
         setClauses.push(`type = ?`);
         params.push(updates.type);
      }
      if (updates.config !== undefined) {
         setClauses.push(`config = ?`);
         params.push(JSON.stringify(updates.config));
      }

      setClauses.push(`updated_at = ?`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE tasks SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      return this.getTaskById(id) as Promise<Task>;
   }

   /** Cascade-deletes child executions before removing the task itself. */
   async deleteTask(id: string): Promise<void> {
      await this.db.run(
         "DELETE FROM task_executions WHERE task_id = ?",
         [id],
      );
      await this.db.run("DELETE FROM tasks WHERE id = ?", [id]);
   }

   /**
    * Removes all tasks (and their executions) belonging to a project.
    * Executions are deleted per-task to respect the FK from task_executions -> tasks.
    */
   async deleteTasksByProjectId(projectId: string): Promise<void> {
      const tasks = await this.listTasks(projectId);
      for (const task of tasks) {
         await this.db.run(
            "DELETE FROM task_executions WHERE task_id = ?",
            [task.id],
         );
      }
      await this.db.run("DELETE FROM tasks WHERE project_id = ?", [projectId]);
   }

   // ==================== TASK EXECUTIONS ====================

   async listExecutions(taskId: string): Promise<TaskExecution[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM task_executions WHERE task_id = ? ORDER BY created_at DESC",
         [taskId],
      );
      return rows.map(this.mapToExecution);
   }

   async getExecutionById(id: string): Promise<TaskExecution | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM task_executions WHERE id = ?",
         [id],
      );
      return row ? this.mapToExecution(row) : null;
   }

   /** Returns an in-flight execution (PENDING or RUNNING) for the task, if any.
    *  Callers use this to prevent overlapping runs of the same task. */
   async getRunningExecution(taskId: string): Promise<TaskExecution | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM task_executions WHERE task_id = ? AND status IN ('PENDING', 'RUNNING')",
         [taskId],
      );
      return row ? this.mapToExecution(row) : null;
   }

   /**
    * Atomically creates an execution only if no PENDING/RUNNING execution
    * exists for this task. Returns null when an active execution already
    * exists, avoiding the TOCTOU race of a separate check-then-insert.
    */
   async createExecution(
      taskId: string,
      status: TaskExecutionStatus = "PENDING",
   ): Promise<TaskExecution | null> {
      const id = this.generateId();
      const now = this.now();
      const iso = now.toISOString();

      const rows = await this.db.all<Record<string, unknown>>(
         `INSERT INTO task_executions (id, task_id, status, created_at, updated_at)
         SELECT ?, ?, ?, ?, ?
         WHERE NOT EXISTS (
            SELECT 1 FROM task_executions
            WHERE task_id = ? AND status IN ('PENDING', 'RUNNING')
         )
         RETURNING *`,
         [id, taskId, status, iso, iso, taskId],
      );

      if (rows.length === 0) {
         return null;
      }

      return this.mapToExecution(rows[0]);
   }

   /**
    * Partial update for an execution's lifecycle fields. Typically called to
    * transition status and set startedAt/completedAt/error as the run progresses.
    */
   async updateExecution(
      id: string,
      updates: {
         status?: TaskExecutionStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<TaskExecution> {
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
         `UPDATE task_executions SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      return this.getExecutionById(id) as Promise<TaskExecution>;
   }

   // ==================== ROW MAPPERS ====================
   // DuckDB returns snake_case columns and JSON as strings;
   // these mappers convert to the camelCase domain types.

   private mapToTask(row: Record<string, unknown>): Task {
      return {
         id: row.id as string,
         projectId: row.project_id as string,
         name: row.name as string,
         type: row.type as string,
         config: JSON.parse(row.config as string),
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }

   private mapToExecution(row: Record<string, unknown>): TaskExecution {
      return {
         id: row.id as string,
         taskId: row.task_id as string,
         status: row.status as TaskExecutionStatus,
         startedAt: row.started_at ? new Date(row.started_at as string) : null,
         completedAt: row.completed_at
            ? new Date(row.completed_at as string)
            : null,
         error: (row.error as string) || null,
         metadata: row.metadata ? JSON.parse(row.metadata as string) : null,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}

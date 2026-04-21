import { logger } from "../../logger";
import { Environment } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

export class EnvironmentRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   }

   private now(): Date {
      return new Date();
   }

   async listEnvironments(): Promise<Environment[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM environments ORDER BY name",
      );
      return rows.map(this.mapToEnvironment);
   }

   async getEnvironmentById(id: string): Promise<Environment | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM environments WHERE id = ?",
         [id],
      );
      return row ? this.mapToEnvironment(row) : null;
   }

   async getEnvironmentByName(name: string): Promise<Environment | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM environments WHERE name = ?",
         [name],
      );
      if (!row) {
         // Diagnostic: when a lookup misses, dump the full name list so we
         // can tell whether the table is empty (INSERT never landed) vs.
         // populated with unexpected names (row was deleted / name encoding
         // drift). Safe to leave in — miss path is rare.
         const all = await this.db.all<{ name: string }>(
            "SELECT name FROM environments",
         );
         logger.warn(
            `getEnvironmentByName("${name}") miss; table has ${all.length} rows: ${JSON.stringify(all.map((r) => r.name))}`,
         );
      }
      return row ? this.mapToEnvironment(row) : null;
   }

   async createEnvironment(
      environment: Omit<Environment, "id" | "createdAt" | "updatedAt">,
   ): Promise<Environment> {
      const id = this.generateId();
      const now = this.now();

      const params = [
         id,
         environment.name,
         environment.path,
         environment.description || null,
         environment.metadata ? JSON.stringify(environment.metadata) : null,
         now.toISOString(),
         now.toISOString(),
      ];

      try {
         await this.db.run(
            `INSERT INTO environments (id, name, path, description, metadata, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
            params,
         );

         return {
            id,
            ...environment,
            createdAt: now,
            updatedAt: now,
         };
      } catch (err: unknown) {
         const error = err as Error;
         // If unique constraint violation, return existing environment
         if (
            error.message?.includes("UNIQUE") ||
            error.message?.includes("Constraint")
         ) {
            const existing = await this.db.get<Record<string, unknown>>(
               "SELECT * FROM environments WHERE name = ?",
               [environment.name],
            );
            if (existing) {
               console.log("Returning existing environment");
               return this.mapToEnvironment(existing);
            }
         }
         throw error;
      }
   }

   async updateEnvironment(
      id: string,
      updates: Partial<Environment>,
   ): Promise<Environment> {
      const existing = await this.getEnvironmentById(id);
      if (!existing) {
         throw new Error(`Environment with id ${id} not found`);
      }

      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];

      if (updates.name !== undefined && updates.name !== existing.name) {
         setClauses.push(`name = ?`);
         params.push(updates.name);
      }

      if (updates.path !== undefined && updates.path !== existing.path) {
         setClauses.push(`path = ?`);
         params.push(updates.path);
      }

      if (updates.description !== undefined) {
         setClauses.push(`description = ?`);
         params.push(updates.description);
      }
      if (updates.metadata !== undefined) {
         setClauses.push(`metadata = ?`);
         params.push(JSON.stringify(updates.metadata));
      }

      setClauses.push(`updated_at = ?`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE environments SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      return this.getEnvironmentById(id) as Promise<Environment>;
   }

   async deleteEnvironment(id: string): Promise<void> {
      await this.db.run("DELETE FROM environments WHERE id = ?", [id]);
   }

   private mapToEnvironment(row: Record<string, unknown>): Environment {
      return {
         id: row.id as string,
         name: row.name as string,
         path: row.path as string,
         description: row.description as string | undefined,
         metadata: row.metadata
            ? JSON.parse(row.metadata as string)
            : undefined,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}

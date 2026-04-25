import { Project } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

export class ProjectRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   }

   private now(): Date {
      return new Date();
   }

   async listProjects(): Promise<Project[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM projects ORDER BY name",
      );
      return rows.map(this.mapToProject);
   }

   async getProjectById(id: string): Promise<Project | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM projects WHERE id = ?",
         [id],
      );
      return row ? this.mapToProject(row) : null;
   }

   async getProjectByName(name: string): Promise<Project | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM projects WHERE name = ?",
         [name],
      );
      return row ? this.mapToProject(row) : null;
   }

   async createProject(
      project: Omit<Project, "id" | "createdAt" | "updatedAt">,
   ): Promise<Project> {
      const id = this.generateId();
      const now = this.now();

      const params = [
         id,
         project.name,
         project.path,
         project.description || null,
         project.metadata ? JSON.stringify(project.metadata) : null,
         now.toISOString(),
         now.toISOString(),
      ];

      try {
         await this.db.run(
            `INSERT INTO projects (id, name, path, description, metadata, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
            params,
         );

         return {
            id,
            ...project,
            createdAt: now,
            updatedAt: now,
         };
      } catch (err: unknown) {
         const error = err as Error;
         // If unique constraint violation, return existing project
         if (
            error.message?.includes("UNIQUE") ||
            error.message?.includes("Constraint")
         ) {
            const existing = await this.db.get<Record<string, unknown>>(
               "SELECT * FROM projects WHERE name = ?",
               [project.name],
            );
            if (existing) {
               console.log("Returning existing project");
               return this.mapToProject(existing);
            }
         }
         throw error;
      }
   }

   async updateProject(
      id: string,
      updates: Partial<Project>,
   ): Promise<Project> {
      const existing = await this.getProjectById(id);
      if (!existing) {
         throw new Error(`Project with id ${id} not found`);
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
         `UPDATE projects SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      return this.getProjectById(id) as Promise<Project>;
   }

   async deleteProject(id: string): Promise<void> {
      await this.db.run("DELETE FROM projects WHERE id = ?", [id]);
   }

   private mapToProject(row: Record<string, unknown>): Project {
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

import {
   ResourceRepository,
   Project,
   Package,
   Connection,
} from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

export class DuckDBRepository implements ResourceRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   }

   private now(): Date {
      return new Date();
   }

   // ==================== PROJECTS ====================

   async getProjects(): Promise<Project[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM projects ORDER BY name",
      );
      return rows.map(this.mapToProject);
   }

   async getProject(id: string): Promise<Project | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM projects WHERE id = ?",
         [id],
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
      const existing = await this.getProject(id);
      if (!existing) {
         throw new Error(`Project with id ${id} not found`);
      }

      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];
      let paramIndex = 1;

      if (updates.name !== undefined) {
         setClauses.push(`name = $${paramIndex++}`);
         params.push(updates.name);
      }
      if (updates.path !== undefined) {
         setClauses.push(`path = $${paramIndex++}`);
         params.push(updates.path);
      }
      if (updates.description !== undefined) {
         setClauses.push(`description = $${paramIndex++}`);
         params.push(updates.description);
      }
      if (updates.metadata !== undefined) {
         setClauses.push(`metadata = $${paramIndex++}`);
         params.push(JSON.stringify(updates.metadata));
      }

      setClauses.push(`updated_at = $${paramIndex++}`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE projects SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      return this.getProject(id) as Promise<Project>;
   }

   async deleteProject(id: string): Promise<void> {
      // First delete all related records
      await this.db.run("DELETE FROM connections WHERE project_id = ?", [id]);

      // Delete packages
      await this.db.run("DELETE FROM packages WHERE project_id = ?", [id]);

      // Finally delete the project
      await this.db.run("DELETE FROM projects WHERE id = ?", [id]);
   }

   // ==================== PACKAGES ====================

   async getPackages(projectId: string): Promise<Package[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM packages WHERE project_id = ? ORDER BY name",
         [projectId],
      );
      return rows.map(this.mapToPackage);
   }

   async getPackage(id: string): Promise<Package | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM packages WHERE id = ?",
         [id],
      );
      return row ? this.mapToPackage(row) : null;
   }

   async createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package> {
      const id = this.generateId();
      const now = this.now();

      await this.db.run(
         `INSERT INTO packages (id, project_id, name, version, description, manifest_path, metadata, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            id,
            pkg.projectId,
            pkg.name,
            pkg.version,
            pkg.description || null,
            pkg.manifestPath,
            pkg.metadata ? JSON.stringify(pkg.metadata) : null,
            now.toISOString(),
            now.toISOString(),
         ],
      );

      return {
         id,
         ...pkg,
         createdAt: now,
         updatedAt: now,
      };
   }

   async updatePackage(
      id: string,
      updates: Partial<Package>,
   ): Promise<Package> {
      const existing = await this.getPackage(id);
      if (!existing) {
         throw new Error(`Package with id ${id} not found`);
      }

      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];
      let paramIndex = 1;

      if (updates.name !== undefined) {
         setClauses.push(`name = $${paramIndex++}`);
         params.push(updates.name);
      }
      if (updates.version !== undefined) {
         setClauses.push(`version = $${paramIndex++}`);
         params.push(updates.version);
      }
      if (updates.description !== undefined) {
         setClauses.push(`description = $${paramIndex++}`);
         params.push(updates.description);
      }
      if (updates.manifestPath !== undefined) {
         setClauses.push(`manifest_path = $${paramIndex++}`);
         params.push(updates.manifestPath);
      }
      if (updates.metadata !== undefined) {
         setClauses.push(`metadata = $${paramIndex++}`);
         params.push(JSON.stringify(updates.metadata));
      }

      setClauses.push(`updated_at = $${paramIndex++}`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE packages SET ${setClauses.join(", ")} WHERE id = $${paramIndex}`,
         params,
      );

      return this.getPackage(id) as Promise<Package>;
   }

   async deletePackage(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE id = ?", [id]);
   }

   // ==================== CONNECTIONS ====================

   async getConnections(projectId: string): Promise<Connection[]> {
      try {
         const rows = await this.db.all<Record<string, unknown>>(
            "SELECT * FROM connections WHERE project_id = ? ORDER BY name",
            [projectId],
         );
         return rows.map(this.mapToConnection);
      } catch (err: unknown) {
         const error = err as Error;
         console.error("Failed to get connections:", error.message);
         throw error;
      }
   }

   async getConnection(id: string): Promise<Connection | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM connections WHERE id = ?",
         [id],
      );
      return row ? this.mapToConnection(row) : null;
   }

   async createConnection(
      connection: Omit<Connection, "id" | "createdAt" | "updatedAt">,
   ): Promise<Connection> {
      const id = this.generateId();
      const now = this.now();

      try {
         const configJson = JSON.stringify(connection.config);

         await this.db.run(
            `INSERT INTO connections (id, project_id, name, type, config, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [
               id,
               connection.projectId,
               connection.name,
               connection.type,
               configJson,
               now.toISOString(),
               now.toISOString(),
            ],
         );

         return {
            id,
            ...connection,
            createdAt: now,
            updatedAt: now,
         };
      } catch (err: unknown) {
         const error = err as Error;
         console.error("Failed to create connection:", error.message);
         throw error;
      }
   }

   async updateConnection(
      id: string,
      updates: Partial<Connection>,
   ): Promise<Connection> {
      const existing = await this.getConnection(id);
      if (!existing) {
         throw new Error(`Connection with id ${id} not found`);
      }

      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];
      let paramIndex = 1;

      if (updates.name !== undefined) {
         setClauses.push(`name = $${paramIndex++}`);
         params.push(updates.name);
      }
      if (updates.type !== undefined) {
         setClauses.push(`type = $${paramIndex++}`);
         params.push(updates.type);
      }
      if (updates.config !== undefined) {
         setClauses.push(`config = $${paramIndex++}`);
         params.push(JSON.stringify(updates.config));
      }

      setClauses.push(`updated_at = $${paramIndex++}`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE connections SET ${setClauses.join(", ")} WHERE id = $${paramIndex}`,
         params,
      );

      return this.getConnection(id) as Promise<Connection>;
   }

   async deleteConnection(id: string): Promise<void> {
      await this.db.run("DELETE FROM connections WHERE id = ?", [id]);
   }

   // ==================== MAPPERS ====================

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

   private mapToPackage(row: any): Package {
      return {
         id: row.id as string,
         projectId: row.project_id as string,
         name: row.name as string,
         version: row.version as string,
         description: row.description as string | undefined,
         manifestPath: row.manifest_path as string,
         metadata: row.metadata
            ? JSON.parse(row.metadata as string)
            : undefined,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }

   private mapToConnection(row: Record<string, unknown>): Connection {
      return {
         id: row.id as string,
         projectId: row.project_id as string,
         name: row.name as string,
         type: row.type as Connection["type"],
         config: JSON.parse(row.config as string),
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}

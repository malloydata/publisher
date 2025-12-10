import { Connection } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

export class ConnectionRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   }

   private now(): Date {
      return new Date();
   }

   async listConnections(projectId: string): Promise<Connection[]> {
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

   async getConnectionById(id: string): Promise<Connection | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM connections WHERE id = ?",
         [id],
      );
      return row ? this.mapToConnection(row) : null;
   }

   async getConnectionByName(
      projectId: string,
      name: string,
   ): Promise<Connection | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM connections WHERE project_id = ? AND name = ?",
         [projectId, name],
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
      const existing = await this.getConnectionById(id);
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

      return this.getConnectionById(id) as Promise<Connection>;
   }

   async deleteConnection(id: string): Promise<void> {
      await this.db.run("DELETE FROM connections WHERE id = ?", [id]);
   }

   async deleteConnectionsByProjectId(id: string): Promise<void> {
      await this.db.run("DELETE FROM connections WHERE project_id = ?", [id]);
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

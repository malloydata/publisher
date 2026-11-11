// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../../logger";
import { ConnectionRecord } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { newId, Row, rowTimestamps, updateRow } from "./rows";

export class ConnectionRepository {
   constructor(private db: DuckDBConnection) {}

   async listConnections(environmentId: string): Promise<ConnectionRecord[]> {
      try {
         const rows = await this.db.all<Row>(
            "SELECT * FROM connections WHERE environment_id = ? ORDER BY name",
            [environmentId],
         );
         return rows.map(mapToConnection);
      } catch (err: unknown) {
         const error = err as Error;
         logger.error("Failed to get connections:", error.message);
         throw error;
      }
   }

   async getConnectionById(id: string): Promise<ConnectionRecord | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM connections WHERE id = ?",
         [id],
      );
      return row ? mapToConnection(row) : null;
   }

   async getConnectionByName(
      environmentId: string,
      name: string,
   ): Promise<ConnectionRecord | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM connections WHERE environment_id = ? AND name = ?",
         [environmentId, name],
      );
      return row ? mapToConnection(row) : null;
   }

   async createConnection(
      connection: Omit<ConnectionRecord, "id" | "createdAt" | "updatedAt">,
   ): Promise<ConnectionRecord> {
      const id = newId();
      const now = new Date();

      try {
         await this.db.run(
            `INSERT INTO connections (id, environment_id, name, type, config, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [
               id,
               connection.environmentId,
               connection.name,
               connection.type,
               JSON.stringify(connection.config),
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
         logger.error("Failed to create connection:", error.message);
         throw error;
      }
   }

   async updateConnection(
      id: string,
      updates: Partial<ConnectionRecord>,
   ): Promise<ConnectionRecord> {
      const existing = await this.getConnectionById(id);
      if (!existing) {
         throw new Error(`Connection with id ${id} not found`);
      }

      await updateRow(this.db, "connections", id, {
         name: updates.name,
         type: updates.type,
         config:
            updates.config !== undefined
               ? JSON.stringify(updates.config)
               : undefined,
      });

      return this.getConnectionById(id) as Promise<ConnectionRecord>;
   }

   async deleteConnection(id: string): Promise<void> {
      await this.db.run("DELETE FROM connections WHERE id = ?", [id]);
   }

   async deleteConnectionsByEnvironmentId(id: string): Promise<void> {
      await this.db.run("DELETE FROM connections WHERE environment_id = ?", [
         id,
      ]);
   }
}

function mapToConnection(row: Row): ConnectionRecord {
   return {
      id: row.id as string,
      environmentId: row.environment_id as string,
      name: row.name as string,
      type: row.type as ConnectionRecord["type"],
      config: JSON.parse(row.config as string),
      ...rowTimestamps(row),
   };
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { StorageDestination } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { newId, Row, rowTimestamps } from "./rows";

/**
 * Persists an environment's storage destinations so they survive a restart, as
 * connections do.
 *
 * Smaller than {@link ConnectionRepository} on purpose: an environment replaces
 * its whole destination list, so an upsert keyed on `(environment_id, name)` and
 * a delete suffice. No `getById`, because no caller holds a destination id.
 */
export class StorageDestinationRepository {
   constructor(private db: DuckDBConnection) {}

   async list(environmentId: string): Promise<StorageDestination[]> {
      const rows = await this.db.all<Row>(
         "SELECT * FROM storage_destinations WHERE environment_id = ? ORDER BY name",
         [environmentId],
      );
      return rows.map(mapToDestination);
   }

   async getByName(
      environmentId: string,
      name: string,
   ): Promise<StorageDestination | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM storage_destinations WHERE environment_id = ? AND name = ?",
         [environmentId, name],
      );
      return row ? mapToDestination(row) : null;
   }

   /**
    * Writes a destination, replacing any row with that name in the environment.
    * Keyed on name, not id, because config and the API address destinations by
    * name; the id is an internal handle.
    */
   async upsert(
      destination: Omit<StorageDestination, "id" | "createdAt" | "updatedAt">,
   ): Promise<StorageDestination> {
      const existing = await this.getByName(
         destination.environmentId,
         destination.name,
      );
      const now = new Date();
      const configJson = JSON.stringify(destination.config);

      if (existing) {
         await this.db.run(
            `UPDATE storage_destinations
                SET type = ?, config = ?, updated_at = ?
              WHERE id = ?`,
            [destination.type, configJson, now.toISOString(), existing.id],
         );
         return { ...existing, ...destination, updatedAt: now };
      }

      const id = newId();
      await this.db.run(
         `INSERT INTO storage_destinations
             (id, environment_id, name, type, config, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)`,
         [
            id,
            destination.environmentId,
            destination.name,
            destination.type,
            configJson,
            now.toISOString(),
            now.toISOString(),
         ],
      );
      return { id, ...destination, createdAt: now, updatedAt: now };
   }

   async deleteById(id: string): Promise<void> {
      await this.db.run("DELETE FROM storage_destinations WHERE id = ?", [id]);
   }

   async deleteByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM storage_destinations WHERE environment_id = ?",
         [environmentId],
      );
   }
}

function mapToDestination(row: Row): StorageDestination {
   return {
      id: row.id as string,
      environmentId: row.environment_id as string,
      name: row.name as string,
      type: row.type as string,
      config: JSON.parse(row.config as string),
      ...rowTimestamps(row),
   };
}

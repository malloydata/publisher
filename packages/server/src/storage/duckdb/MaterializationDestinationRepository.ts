import { MaterializationDestination } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * Persistence for an environment's materialization destinations, so the
 * warehouses a worker builds into and serves from survive a restart the same way
 * its connections do.
 *
 * Deliberately a smaller surface than {@link ConnectionRepository}: destinations
 * are written as a whole list (the environment replaces its list, it does not
 * patch one entry), so an upsert keyed on `(environment_id, name)` and a delete
 * are all the store needs. There is no `getById` because no caller holds a
 * destination id.
 */
export class MaterializationDestinationRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
   }

   async list(environmentId: string): Promise<MaterializationDestination[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM materialization_destinations WHERE environment_id = ? ORDER BY name",
         [environmentId],
      );
      return rows.map(this.mapToDestination);
   }

   async getByName(
      environmentId: string,
      name: string,
   ): Promise<MaterializationDestination | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM materialization_destinations WHERE environment_id = ? AND name = ?",
         [environmentId, name],
      );
      return row ? this.mapToDestination(row) : null;
   }

   /**
    * Writes a destination, replacing any row already holding that name in the
    * environment. Keyed on `(environment_id, name)` rather than on the row id
    * because the name is what the config and the API address a destination by;
    * the id is an internal handle.
    */
   async upsert(
      destination: Omit<
         MaterializationDestination,
         "id" | "createdAt" | "updatedAt"
      >,
   ): Promise<MaterializationDestination> {
      const existing = await this.getByName(
         destination.environmentId,
         destination.name,
      );
      const now = new Date();
      const configJson = JSON.stringify(destination.config);

      if (existing) {
         await this.db.run(
            `UPDATE materialization_destinations
                SET type = ?, config = ?, updated_at = ?
              WHERE id = ?`,
            [destination.type, configJson, now.toISOString(), existing.id],
         );
         return { ...existing, ...destination, updatedAt: now };
      }

      const id = this.generateId();
      await this.db.run(
         `INSERT INTO materialization_destinations
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
      await this.db.run(
         "DELETE FROM materialization_destinations WHERE id = ?",
         [id],
      );
   }

   async deleteByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM materialization_destinations WHERE environment_id = ?",
         [environmentId],
      );
   }

   private mapToDestination(
      row: Record<string, unknown>,
   ): MaterializationDestination {
      return {
         id: row.id as string,
         environmentId: row.environment_id as string,
         name: row.name as string,
         type: row.type as string,
         config: JSON.parse(row.config as string),
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}

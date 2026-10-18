// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../../logger";
import { Environment } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { newId, Row, rowTimestamps, updateRow } from "./rows";

export class EnvironmentRepository {
   constructor(private db: DuckDBConnection) {}

   async listEnvironments(): Promise<Environment[]> {
      const rows = await this.db.all<Row>(
         "SELECT * FROM environments ORDER BY name",
      );
      return rows.map(mapToEnvironment);
   }

   async getEnvironmentById(id: string): Promise<Environment | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM environments WHERE id = ?",
         [id],
      );
      return row ? mapToEnvironment(row) : null;
   }

   async getEnvironmentByName(name: string): Promise<Environment | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM environments WHERE name = ?",
         [name],
      );
      return row ? mapToEnvironment(row) : null;
   }

   async createEnvironment(
      environment: Omit<Environment, "id" | "createdAt" | "updatedAt">,
   ): Promise<Environment> {
      const id = newId();
      const now = new Date();

      try {
         await this.db.run(
            `INSERT INTO environments (id, name, path, description, metadata, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [
               id,
               environment.name,
               environment.path,
               environment.description || null,
               environment.metadata
                  ? JSON.stringify(environment.metadata)
                  : null,
               now.toISOString(),
               now.toISOString(),
            ],
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
            const existing = await this.getEnvironmentByName(environment.name);
            if (existing) {
               logger.info("Returning existing environment");
               return existing;
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

      await updateRow(this.db, "environments", id, {
         name: updates.name !== existing.name ? updates.name : undefined,
         path: updates.path !== existing.path ? updates.path : undefined,
         description: updates.description,
         metadata:
            updates.metadata !== undefined
               ? JSON.stringify(updates.metadata)
               : undefined,
      });

      return this.getEnvironmentById(id) as Promise<Environment>;
   }

   async deleteEnvironment(id: string): Promise<void> {
      await this.db.run("DELETE FROM environments WHERE id = ?", [id]);
   }
}

function mapToEnvironment(row: Row): Environment {
   return {
      id: row.id as string,
      name: row.name as string,
      path: row.path as string,
      description: row.description as string | undefined,
      metadata: row.metadata ? JSON.parse(row.metadata as string) : undefined,
      ...rowTimestamps(row),
   };
}

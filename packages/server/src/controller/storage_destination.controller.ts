// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { components } from "../api";
import {
   BadRequestError,
   DestinationNotFoundError,
   StorageDestinationNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { recordDropTables } from "../materialization_metrics";
import type { Environment } from "../service/environment";
import { EnvironmentStore } from "../service/environment_store";
import {
   cleanupStorageFiles,
   dropStorageTable,
   listStorageTables,
} from "../service/materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];
type ApiStorageTable = components["schemas"]["StorageTable"];
type ApiStorageFileCleanup = components["schemas"]["StorageFileCleanup"];

/**
 * The same shape the spec declares for every identifier in these paths. Checked
 * here rather than trusted to the spec, because a table name reaches DDL: the
 * pattern admits no quote, dot or whitespace, so a name that passes is one
 * unqualified identifier and cannot address anything but a table in the named
 * schema of the named destination.
 */
const IDENTIFIER = /^[a-zA-Z0-9_-]+$/;

/**
 * Table lifecycle inside an environment's storage destinations, for an
 * orchestrator that owns it.
 *
 * A destination is deliberately unreachable through the connection routes — a
 * model can never name one, and its credentials are never returned — so without
 * these an orchestrator that assigns its own physical table names (and so owns
 * their garbage collection, per `deleteMaterialization`) has no way to drop a
 * superseded table, find one it lost track of, or reclaim the storage behind
 * either.
 */
export class StorageDestinationController {
   constructor(private environmentStore: EnvironmentStore) {}

   async listTables(
      environmentName: string,
      destinationName: string,
      schemaName: string,
   ): Promise<ApiStorageTable[]> {
      assertIdentifier("schema", schemaName);
      const { environment, destination } = await this.resolve(
         environmentName,
         destinationName,
      );
      const names = await listStorageTables({
         destinationName,
         destinationConnection: destination,
         schemaName,
         environmentPath: environment.getEnvironmentPath(),
      });
      return names.map((name) => ({ name }));
   }

   async dropTable(
      environmentName: string,
      destinationName: string,
      schemaName: string,
      tableName: string,
   ): Promise<void> {
      assertIdentifier("schema", schemaName);
      assertIdentifier("table", tableName);
      const { environment, destination } = await this.resolve(
         environmentName,
         destinationName,
      );
      try {
         await dropStorageTable({
            destinationName,
            destinationConnection: destination,
            physicalTableName:
               schemaName === "main" ? tableName : `${schemaName}.${tableName}`,
            environmentPath: environment.getEnvironmentPath(),
         });
      } catch (error) {
         recordDropTables("failure", "storage");
         throw error;
      }
      recordDropTables("success", "storage");
      logger.info("Dropped a storage destination table", {
         environmentName,
         destinationName,
         schemaName,
         tableName,
      });
   }

   async createFileCleanup(
      environmentName: string,
      destinationName: string,
      body: Record<string, unknown>,
   ): Promise<ApiStorageFileCleanup> {
      const snapshotsOlderThan = requireTimestamp(body, "snapshotsOlderThan");
      const filesOlderThan = requireTimestamp(body, "filesOlderThan");
      const { destination } = await this.resolve(
         environmentName,
         destinationName,
      );
      const result = await cleanupStorageFiles({
         destinationName,
         destinationConnection: destination,
         snapshotsOlderThan,
         filesOlderThan,
      });
      logger.info("Cleaned up a storage destination's retired files", {
         environmentName,
         destinationName,
         snapshotsOlderThan: snapshotsOlderThan.toISOString(),
         filesOlderThan: filesOlderThan.toISOString(),
         ...result,
      });
      return {
         snapshotsOlderThan: snapshotsOlderThan.toISOString(),
         filesOlderThan: filesOlderThan.toISOString(),
         ...result,
      };
   }

   private async resolve(
      environmentName: string,
      destinationName: string,
   ): Promise<{ environment: Environment; destination: ApiConnection }> {
      assertIdentifier("storage destination", destinationName);
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      try {
         return {
            environment,
            destination: environment.getStorageDestination(destinationName),
         };
      } catch (error) {
         if (error instanceof DestinationNotFoundError) {
            throw new StorageDestinationNotFoundError(
               `Storage destination '${destinationName}' is not configured on ` +
                  `environment '${environmentName}'.`,
            );
         }
         throw error;
      }
   }
}

function assertIdentifier(what: string, value: string): void {
   if (!IDENTIFIER.test(value)) {
      throw new BadRequestError(
         `Invalid ${what} name '${value}': expected letters, digits, '_' or '-'.`,
      );
   }
}

function requireTimestamp(body: Record<string, unknown>, field: string): Date {
   const raw = body?.[field];
   const parsed = typeof raw === "string" ? new Date(raw) : undefined;
   if (!parsed || Number.isNaN(parsed.getTime())) {
      throw new BadRequestError(
         `'${field}' is required and must be an RFC 3339 date-time.`,
      );
   }
   return parsed;
}

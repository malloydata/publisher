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
import { validateStorageDestinations } from "../service/connection_config";
import {
   loadStorageTable,
   type LoadFormat,
   type LoadOptions,
} from "../service/storage_table_load";
import { EnvironmentStore } from "../service/environment_store";
import {
   bootstrapStorageCatalog,
   cleanupStorageFiles,
   dropStorageTable,
   listStorageTables,
} from "../service/materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];
type ApiStorageTable = components["schemas"]["StorageTable"];
type ApiStorageFileCleanup = components["schemas"]["StorageFileCleanup"];
type ApiStorageBootstrap = components["schemas"]["StorageBootstrap"];
type ApiStorageTableLoad = components["schemas"]["StorageTableLoad"];

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

   /**
    * The first attach of a catalog the caller provisioned. The destination is
    * the body's, validated as `storageDestinations` validates an entry, and never
    * resolved from the environment's list: the caller may not have distributed it
    * yet, and a same-named configured destination is a different catalog.
    */
   async createBootstrap(
      environmentName: string,
      destinationName: string,
      body: Record<string, unknown>,
   ): Promise<ApiStorageBootstrap> {
      const destination = await this.bodyDestination(
         environmentName,
         destinationName,
         body,
      );
      const accepted = [destination];
      const encrypted = body.encrypted === undefined ? true : body.encrypted;
      if (typeof encrypted !== "boolean") {
         throw new BadRequestError("'encrypted' must be a boolean.");
      }
      const limit = body.dataInliningRowLimit;
      if (
         limit !== undefined &&
         !(typeof limit === "number" && Number.isInteger(limit) && limit >= 0)
      ) {
         throw new BadRequestError(
            "'dataInliningRowLimit' must be a non-negative integer.",
         );
      }
      const schemas = body.schemas === undefined ? [] : body.schemas;
      if (!Array.isArray(schemas) || !schemas.every((x) => typeof x === "string")) {
         throw new BadRequestError("'schemas' must be a list of names.");
      }
      for (const schema of schemas as string[]) {
         if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema)) {
            throw new BadRequestError(
               `Invalid schema name '${schema}': expected a plain identifier.`,
            );
         }
      }
      const result = await bootstrapStorageCatalog({
         destinationName,
         destinationConnection: accepted[0],
         encrypted,
         dataInliningRowLimit: limit as number | undefined,
         schemas: schemas as string[],
      });
      logger.info("Bootstrapped a storage destination catalog", {
         environmentName,
         destinationName,
         encrypted: result.encrypted,
         schemas: result.schemas,
      });
      return result;
   }

   /** Loads an object from the destination's own bucket into a table. */
   async createTable(
      environmentName: string,
      destinationName: string,
      schemaName: string,
      body: Record<string, unknown>,
   ): Promise<ApiStorageTableLoad> {
      assertIdentifier("schema", schemaName);
      const destination = await this.bodyDestination(
         environmentName,
         destinationName,
         body,
      );
      const tableName = body.tableName;
      if (typeof tableName !== "string") {
         throw new BadRequestError("'tableName' is required.");
      }
      const mode = body.mode === undefined ? "create" : body.mode;
      if (mode !== "create" && mode !== "replace") {
         throw new BadRequestError("'mode' must be 'create' or 'replace'.");
      }
      const source = body.source as
         | { uri?: unknown; format?: unknown; options?: unknown }
         | undefined;
      if (!source || typeof source.uri !== "string") {
         throw new BadRequestError("'source.uri' is required.");
      }
      if (!["csv", "parquet", "xlsx", "json"].includes(String(source.format))) {
         throw new BadRequestError(
            "'source.format' must be csv, parquet, xlsx or json.",
         );
      }
      const options = (source.options ?? {}) as LoadOptions;
      const allowed = new Set(["header", "delimiter", "sheet", "skip"]);
      for (const key of Object.keys(options)) {
         if (!allowed.has(key)) {
            throw new BadRequestError(`Unknown reader option '${key}'.`);
         }
      }
      const result = await loadStorageTable({
         destinationName,
         destinationConnection: destination,
         schemaName,
         tableName,
         uri: source.uri,
         format: source.format as LoadFormat,
         options,
         mode,
      });
      logger.info("Loaded a table into a storage destination", {
         environmentName,
         destinationName,
         schemaName,
         tableName,
         format: source.format,
         mode,
         rowCount: result.rowCount,
      });
      return result;
   }

   /**
    * A destination carried in a request body, for a catalog the caller may not
    * have distributed: validated as a `storageDestinations` entry, never
    * resolved from the environment's list.
    */
   private async bodyDestination(
      environmentName: string,
      destinationName: string,
      body: Record<string, unknown>,
   ): Promise<ApiConnection> {
      assertIdentifier("storage destination", destinationName);
      // The environment must exist, as for every route under it.
      await this.environmentStore.getEnvironment(environmentName, false);
      const raw = body?.destination as ApiConnection | undefined;
      if (!raw || typeof raw !== "object") {
         throw new BadRequestError("'destination' is required.");
      }
      if (raw.name !== destinationName) {
         throw new BadRequestError(
            `The body's destination is named '${raw.name}', not '${destinationName}'.`,
         );
      }
      if (raw.type !== "ducklake") {
         throw new BadRequestError(
            `Only a DuckLake destination is taken here; '${destinationName}' is '${raw.type}'.`,
         );
      }
      const { accepted, rejected } = validateStorageDestinations([raw]);
      if (accepted.length !== 1) {
         throw new BadRequestError(
            `Storage destination '${destinationName}' is not valid: ${rejected
               .map((r) => r.reason)
               .join("; ")}`,
         );
      }
      return accepted[0];
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The narrow roles the EnvironmentStore plays for its consumers. A controller,
 * MCP tool, use case or retrieval function takes the smallest of these it
 * uses, so it (and its spec's fake) names exactly what it reads from the
 * store. EnvironmentStore implements them all; nothing else does in
 * production.
 */

import type { components } from "../api";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import type { Environment, PackageListingOptions } from "./environment";
import type { EnvironmentName } from "./identity";

type ApiEnvironment = components["schemas"]["Environment"];
type ApiConnection = components["schemas"]["Connection"];
type ApiServerStatus = components["schemas"]["ServerStatus"];

/**
 * The body of `GET /status` and `get_status`: the API schema, with the fields
 * the store always sets required.
 */
export type ServerStatusBody = ApiServerStatus &
   Required<
      Pick<
         ApiServerStatus,
         | "timestamp"
         | "environments"
         | "initialized"
         | "frozenConfig"
         | "operationalState"
         | "version"
      >
   >;

/** What `getStatus` may include beyond the default. */
export interface StatusOptions {
   /** Report packages still loading, as well as those loaded. */
   includeLoading?: boolean;
}

/** Write one connection row of an environment through `repository`. */
export type ConnectionRowWriter = (
   conn: ApiConnection,
   environmentId: string,
   repository: ResourceRepository,
) => Promise<void>;

/**
 * Resolve an environment by name, and through it its packages and models.
 * Waits for the store's initialization; throws EnvironmentNotFoundError for
 * an environment that does not exist.
 */
export interface PackageLookup {
   getEnvironment(
      environmentName: EnvironmentName,
      reload?: boolean,
   ): Promise<Environment>;
}

/** The environment held in memory under a name, without loading it. */
export interface EnvironmentPeek {
   peekEnvironment(name: string): Environment | undefined;
}

/** The environments loaded in memory now; never loads one. */
export interface LoadedEnvironmentsReader {
   getLoadedEnvironments(): Environment[];
}

/** Every environment, serialized for an API response, plus lookup by name. */
export interface EnvironmentCatalog extends PackageLookup {
   listEnvironments(
      skipInitializationCheck?: boolean,
      options?: PackageListingOptions,
   ): Promise<ApiEnvironment[]>;
}

/** Whether `frozenConfig` forbids runtime changes to environments and packages. */
export interface ConfigFreezeReader {
   readonly publisherConfigIsFrozen: boolean;
}

/** Whether MCP offers hidden files and sources (`mcp.includeHiddenFilesAndSources`). */
export interface McpExposureReader {
   readonly mcpIncludeHiddenFilesAndSources: boolean;
}

/** The body of `GET /status` and `get_status`. */
export interface StatusReader {
   getStatus(options?: StatusOptions): Promise<ServerStatusBody>;
}

/** Environment CRUD, for the environment routes. */
export interface EnvironmentAdmin extends EnvironmentCatalog {
   addEnvironment(environment: ApiEnvironment): Promise<Environment>;
   updateEnvironment(environment: ApiEnvironment): Promise<Environment>;
   deleteEnvironment(environmentName: string): Promise<Environment | undefined>;
}

/** Package publish and delete, for the package routes. */
export interface PackageAdmin extends PackageLookup, ConfigFreezeReader {
   addPackageToDatabase(
      environmentName: string,
      packageName: string,
   ): Promise<void>;
   deletePackageFromDatabase(
      environmentName: string,
      packageName: string,
   ): Promise<void>;
   downloadPackageInto(
      environmentName: string,
      packageName: string,
      packageLocation: string,
      targetPath: string,
   ): Promise<void>;
}

/**
 * Connection create, update and delete, for ConnectionService: the
 * environment's connection rows are written through the metadata repository
 * and the environment's live Malloy config is rebuilt to match.
 */
export interface ConnectionAdmin extends PackageLookup, ConfigFreezeReader {
   readonly finishedInitialization: Promise<void>;
   readonly storageManager: { getRepository(): ResourceRepository };
   addConnection: ConnectionRowWriter;
   updateConnection: ConnectionRowWriter;
}

/**
 * The server's own DuckDB catalog (search index rows, embeddings, the theme),
 * handed to the code that reads and writes it instead of that code reaching
 * through the store. Throws until storage has initialized.
 */
export interface CatalogDatabase {
   getDuckDbConnection(): DuckDBConnection;
}

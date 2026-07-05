import { components } from "../api";

/**
 * Generic database interface for storage abstraction
 * This allows switching between DuckDB, PostgreSQL, MySQL, etc.
 */
export interface DatabaseConnection {
   initialize(): Promise<void>;
   close(): Promise<void>;
   isInitialized(): Promise<boolean>;
}

export interface ResourceRepository {
   // Environments
   listEnvironments(): Promise<Environment[]>;
   getEnvironmentById(id: string): Promise<Environment | null>;
   getEnvironmentByName(name: string): Promise<Environment | null>;
   createEnvironment(
      environment: Omit<Environment, "id" | "createdAt" | "updatedAt">,
   ): Promise<Environment>;
   updateEnvironment(
      id: string,
      updates: Partial<Environment>,
   ): Promise<Environment>;
   deleteEnvironment(id: string): Promise<void>;

   // Packages
   listPackages(environmentId: string): Promise<Package[]>;
   getPackageById(id: string): Promise<Package | null>;
   getPackageByName(
      environmentId: string,
      name: string,
   ): Promise<Package | null>;
   createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package>;
   updatePackage(id: string, updates: Partial<Package>): Promise<Package>;
   deletePackage(id: string): Promise<void>;

   // Connections
   listConnections(environmentId: string): Promise<Connection[]>;
   getConnectionById(id: string): Promise<Connection | null>;
   getConnectionByName(
      environmentId: string,
      name: string,
   ): Promise<Connection | null>;
   createConnection(
      connection: Omit<Connection, "id" | "createdAt" | "updatedAt">,
   ): Promise<Connection>;
   updateConnection(
      id: string,
      updates: Partial<Connection>,
   ): Promise<Connection>;
   deleteConnection(id: string): Promise<void>;

   // Materializations
   listMaterializations(
      environmentId: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]>;
   getMaterializationById(id: string): Promise<Materialization | null>;
   getActiveMaterialization(
      environmentId: string,
      packageName: string,
   ): Promise<Materialization | null>;
   createMaterialization(
      environmentId: string,
      packageName: string,
      status?: MaterializationStatus,
      metadata?: Record<string, unknown> | null,
   ): Promise<Materialization>;
   updateMaterialization(
      id: string,
      updates: MaterializationUpdate,
   ): Promise<Materialization>;
   deleteMaterialization(id: string): Promise<void>;
}

export interface Environment {
   id: string;
   name: string;
   path: string;
   description?: string;
   createdAt: Date;
   updatedAt: Date;
   metadata?: Record<string, unknown>;
}

export interface Package {
   id: string;
   environmentId: string;
   name: string;
   description?: string;
   manifestPath: string;
   createdAt: Date;
   updatedAt: Date;
   metadata?: Record<string, unknown>;
}

export interface Connection {
   id: string;
   environmentId: string;
   name: string;
   type: "bigquery" | "postgres" | "duckdb" | "mysql" | "snowflake" | "trino";
   config: Record<string, unknown>;
   createdAt: Date;
   updatedAt: Date;
}

// Wire types for the build protocol, kept in sync with the OpenAPI spec via
// the generated `api.ts`.
export type MaterializationStatus =
   components["schemas"]["MaterializationStatus"];
export type BuildPlan = components["schemas"]["BuildPlan"];
export type BuildManifestResult = components["schemas"]["BuildManifest"];
export type ManifestEntry = components["schemas"]["ManifestEntry"];
export type BuildInstruction = components["schemas"]["BuildInstruction"];
export type Realization = components["schemas"]["Realization"];

export interface Materialization {
   id: string;
   environmentId: string;
   packageName: string;
   status: MaterializationStatus;
   /** Build output. Null until status = MANIFEST_FILE_READY. */
   manifest: BuildManifestResult | null;
   startedAt: Date | null;
   completedAt: Date | null;
   error: string | null;
   metadata: Record<string, unknown> | null;
   createdAt: Date;
   updatedAt: Date;
}

export interface MaterializationUpdate {
   status?: MaterializationStatus;
   manifest?: BuildManifestResult | null;
   startedAt?: Date;
   completedAt?: Date;
   error?: string | null;
   metadata?: Record<string, unknown> | null;
}

/**
 * Malloy-facing build manifest: maps a sourceEntityId to the physical table backing
 * that persist source. This is the shape the Malloy runtime consumes when
 * (re)loading models so persist references resolve to materialized tables.
 * Distinct from {@link BuildManifestResult} (the wire build output, which also
 * carries caller bookkeeping like materializedTableId and rowCount).
 */
export interface BuildManifestEntry {
   tableName: string;
}

export interface BuildManifest {
   entries: Record<string, BuildManifestEntry>;
   strict?: boolean;
}

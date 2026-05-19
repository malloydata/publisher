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
   /**
    * Atomically point a package at a new on-disk directory.
    *
    * Inserts the package row when it does not exist, otherwise updates the
    * existing row. Returns the directory path the package was previously
    * pointed at (`null` for a brand-new row) so the caller can schedule the
    * obsolete directory for sweeping. This is the ordering point that
    * replaces filesystem mutexes for package write operations.
    */
   swapPackageDirectory(args: {
      environmentId: string;
      name: string;
      newDirectoryPath: string;
      description?: string;
      metadata?: Record<string, unknown>;
   }): Promise<{ id: string; oldDirectoryPath: string | null }>;
   /** Set of every directory path currently referenced by some package row in the env. */
   listPackageDirectoryPaths(environmentId: string): Promise<Set<string>>;

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
      updates: {
         status?: MaterializationStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<Materialization>;
   deleteMaterialization(id: string): Promise<void>;
   // Build Manifests
   listManifestEntries(
      environmentId: string,
      packageName: string,
   ): Promise<ManifestEntry[]>;
   upsertManifestEntry(
      entry: Omit<ManifestEntry, "id" | "createdAt" | "updatedAt">,
   ): Promise<ManifestEntry>;
   deleteManifestEntry(id: string): Promise<void>;
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
   /**
    * Absolute path to the directory that currently holds the package's
    * contents on disk. Updated atomically via `swapPackageDirectory` whenever
    * a write operation produces a new version of the package. Empty string
    * for legacy rows that predate versioned directories — callers should
    * fall back to `<environmentPath>/<packageName>` in that case.
    */
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

export type MaterializationStatus =
   | "PENDING"
   | "RUNNING"
   | "SUCCESS"
   | "FAILED"
   | "CANCELLED";

export interface Materialization {
   id: string;
   environmentId: string;
   packageName: string;
   status: MaterializationStatus;
   startedAt: Date | null;
   completedAt: Date | null;
   error: string | null;
   metadata: Record<string, unknown> | null;
   createdAt: Date;
   updatedAt: Date;
}

export interface ManifestEntry {
   id: string;
   environmentId: string;
   packageName: string;
   buildId: string;
   tableName: string;
   sourceName: string;
   connectionName: string;
   createdAt: Date;
   updatedAt: Date;
}

// ==================== MANIFEST STORE ====================

export interface BuildManifestEntry {
   tableName: string;
}

export interface BuildManifest {
   entries: Record<string, BuildManifestEntry>;
   strict?: boolean;
}

/**
 * Abstraction for manifest storage. Standalone mode uses DuckDB;
 * orchestrated mode swaps in a DuckLakeManifestStore.
 */
export interface ManifestStore {
   getManifest(
      environmentId: string,
      packageName: string,
   ): Promise<BuildManifest>;
   writeEntry(
      environmentId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void>;
   deleteEntry(id: string): Promise<void>;
   listEntries(
      environmentId: string,
      packageName: string,
   ): Promise<ManifestEntry[]>;
}

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
   // Projects
   listProjects(): Promise<Project[]>;
   getProjectById(id: string): Promise<Project | null>;
   getProjectByName(name: string): Promise<Project | null>;
   createProject(
      project: Omit<Project, "id" | "createdAt" | "updatedAt">,
   ): Promise<Project>;
   updateProject(id: string, updates: Partial<Project>): Promise<Project>;
   deleteProject(id: string): Promise<void>;

   // Packages
   listPackages(projectId: string): Promise<Package[]>;
   getPackageById(id: string): Promise<Package | null>;
   getPackageByName(projectId: string, name: string): Promise<Package | null>;
   createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package>;
   updatePackage(id: string, updates: Partial<Package>): Promise<Package>;
   deletePackage(id: string): Promise<void>;

   // Connections
   listConnections(projectId: string): Promise<Connection[]>;
   getConnectionById(id: string): Promise<Connection | null>;
   getConnectionByName(
      projectId: string,
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

   // Build Executions
   listBuildExecutions(
      projectId: string,
      packageName: string,
   ): Promise<BuildExecution[]>;
   getBuildExecutionById(id: string): Promise<BuildExecution | null>;
   getRunningBuildExecution(
      projectId: string,
      packageName: string,
   ): Promise<BuildExecution | null>;
   createBuildExecution(
      projectId: string,
      packageName: string,
      status?: BuildExecutionStatus,
   ): Promise<BuildExecution | null>;
   updateBuildExecution(
      id: string,
      updates: {
         status?: BuildExecutionStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<BuildExecution>;
   // Build Manifests
   listManifestEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]>;
   upsertManifestEntry(
      entry: Omit<ManifestEntry, "id" | "createdAt" | "updatedAt">,
   ): Promise<ManifestEntry>;
   deleteManifestEntry(id: string): Promise<void>;
}

export interface Project {
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
   projectId: string;
   name: string;
   description?: string;
   manifestPath: string;
   createdAt: Date;
   updatedAt: Date;
   metadata?: Record<string, unknown>;
}

export interface Connection {
   id: string;
   projectId: string;
   name: string;
   type: "bigquery" | "postgres" | "duckdb" | "mysql" | "snowflake" | "trino";
   config: Record<string, unknown>;
   createdAt: Date;
   updatedAt: Date;
}

export type BuildExecutionStatus =
   | "PENDING"
   | "RUNNING"
   | "SUCCESS"
   | "FAILED"
   | "CANCELLED";

export interface BuildExecution {
   id: string;
   projectId: string;
   packageName: string;
   status: BuildExecutionStatus;
   startedAt: Date | null;
   completedAt: Date | null;
   error: string | null;
   metadata: Record<string, unknown> | null;
   createdAt: Date;
   updatedAt: Date;
}

export interface ManifestEntry {
   id: string;
   projectId: string;
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
   getManifest(projectId: string, packageName: string): Promise<BuildManifest>;
   writeEntry(
      projectId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void>;
   deleteEntry(id: string): Promise<void>;
   listEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]>;
}

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
      id: string,
   ): Promise<Connection | null>;
   createConnection(
      connection: Omit<Connection, "id" | "createdAt" | "updatedAt">,
   ): Promise<Connection>;
   updateConnection(
      id: string,
      updates: Partial<Connection>,
   ): Promise<Connection>;
   deleteConnection(id: string): Promise<void>;

   // Tasks
   listTasks(projectId: string): Promise<Task[]>;
   getTaskById(id: string): Promise<Task | null>;
   getTaskByName(projectId: string, name: string): Promise<Task | null>;
   createTask(
      task: Omit<Task, "id" | "createdAt" | "updatedAt">,
   ): Promise<Task>;
   updateTask(id: string, updates: Partial<Task>): Promise<Task>;
   deleteTask(id: string): Promise<void>;

   // Task Executions
   listExecutions(taskId: string): Promise<TaskExecution[]>;
   getExecutionById(id: string): Promise<TaskExecution | null>;
   getRunningExecution(taskId: string): Promise<TaskExecution | null>;
   createExecution(
      taskId: string,
      status?: TaskExecutionStatus,
   ): Promise<TaskExecution | null>;
   updateExecution(
      id: string,
      updates: {
         status?: TaskExecutionStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<TaskExecution>;

   // Build Manifests
   listManifestEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]>;
   getManifestEntryByBuildId(
      projectId: string,
      packageName: string,
      buildId: string,
   ): Promise<ManifestEntry | null>;
   getManifestEntryBySourceName(
      projectId: string,
      packageName: string,
      sourceName: string,
   ): Promise<ManifestEntry | null>;
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

export type TaskExecutionStatus =
   | "PENDING"
   | "RUNNING"
   | "SUCCESS"
   | "FAILED"
   | "CANCELLED";

export interface TaskConfig {
   package: string;
   modelPath: string;
   select?: string[];
   includeDependencies?: boolean;
}

export interface Task {
   id: string;
   projectId: string;
   name: string;
   type: string;
   config: TaskConfig;
   createdAt: Date;
   updatedAt: Date;
}

export interface TaskExecution {
   id: string;
   taskId: string;
   status: TaskExecutionStatus;
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
   sourceName: string | null;
   connectionName: string | null;
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
 * orchestrated mode will swap in a DuckLakeManifestStore.
 */
export interface ManifestStore {
   getManifest(projectId: string, packageName: string): Promise<BuildManifest>;
   writeEntry(
      projectId: string,
      packageName: string,
      buildId: string,
      entry: {
         tableName: string;
         sourceName?: string;
         connectionName?: string;
      },
   ): Promise<void>;
}

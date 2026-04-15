import { logger } from "../logger";
import {
   DatabaseConnection,
   ManifestStore,
   ResourceRepository,
} from "./DatabaseInterface";
import { DuckDBConnection } from "./duckdb/DuckDBConnection";
import { DuckDBManifestStore } from "./duckdb/DuckDBManifestStore";
import { DuckDBRepository } from "./duckdb/DuckDBRepository";
import { initializeSchema } from "./duckdb/schema";
import { DuckLakeManifestStore } from "./ducklake/DuckLakeManifestStore";

export type StorageType = "duckdb" | "postgres" | "mysql";

export interface DuckLakeManifestConfig {
   catalogUrl: string;
   dataPath: string;
}

export interface StorageConfig {
   type: StorageType;
   duckdb?: {
      path?: string;
   };
   postgres?: {
      host: string;
      port: number;
      database: string;
      user: string;
      password: string;
   };
   mysql?: {
      host: string;
      port: number;
      database: string;
      user: string;
      password: string;
   };
}

function escapeSQL(value: string): string {
   return value.replace(/'/g, "''");
}

/**
 * Manages the storage backend (DuckDB, Postgres, etc.) and per-project
 * manifest stores. Projects without `materializationStorage` config use
 * the default DuckDB manifest store. Projects with the config get a
 * DuckLake-backed store attached lazily on first access.
 */
export class StorageManager {
   private connection: DatabaseConnection | null = null;
   private duckDbConnection: DuckDBConnection | null = null;
   private repository: ResourceRepository | null = null;
   private defaultManifestStore: ManifestStore | null = null;

   /** Per-project DuckLake manifest stores, keyed by projectId. */
   private projectManifestStores = new Map<string, ManifestStore>();

   /** Tracks which catalogs have been attached to avoid duplicate ATTACHes. */
   private attachedCatalogs = new Set<string>();

   private config: StorageConfig;

   constructor(config: StorageConfig) {
      this.config = config;
   }

   async initialize(reinit: boolean = false): Promise<void> {
      if (reinit) {
         logger.info(
            "Reinitialization mode: Database will be dropped and recreated",
         );
      } else {
         logger.info("Normal mode: Loading from existing database");
      }

      switch (this.config.type) {
         case "duckdb":
            await this.initializeDuckDB(reinit);
            break;
         case "postgres":
            throw new Error("PostgreSQL storage not yet implemented");
         case "mysql":
            throw new Error("MySQL storage not yet implemented");
         default:
            throw new Error(`Unsupported storage type: ${this.config.type}`);
      }
   }

   private async initializeDuckDB(reinit: boolean): Promise<void> {
      const dbPath = this.config.duckdb?.path;

      const connection = new DuckDBConnection(dbPath);

      await connection.initialize();

      await initializeSchema(connection, reinit);

      this.connection = connection;
      this.duckDbConnection = connection;
      this.repository = new DuckDBRepository(connection);
      this.defaultManifestStore = new DuckDBManifestStore(this.repository);
   }

   /**
    * Lazily initializes a DuckLake manifest store for a project.
    * The DuckLake catalog is attached to the shared DuckDB connection
    * and persists for the lifetime of the process.
    */
   async initializeDuckLakeForProject(
      projectId: string,
      config: DuckLakeManifestConfig,
   ): Promise<void> {
      if (!this.duckDbConnection) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }

      const catalogName = `manifest_lake_${projectId.replace(/[^a-zA-Z0-9_]/g, "_")}`;

      if (!this.attachedCatalogs.has(catalogName)) {
         await this.attachDuckLakeCatalog(config, catalogName);
      }

      const store = new DuckLakeManifestStore(
         this.duckDbConnection,
         catalogName,
      );
      await store.bootstrapSchema();

      this.projectManifestStores.set(projectId, store);
      logger.info("DuckLake manifest store initialized for project", {
         projectId,
         catalogName,
      });
   }

   private async attachDuckLakeCatalog(
      config: DuckLakeManifestConfig,
      catalogName: string,
   ): Promise<void> {
      const connection = this.duckDbConnection!;

      await connection.run("INSTALL ducklake; LOAD ducklake;");

      const isPostgres = config.catalogUrl.startsWith("postgres:");
      if (isPostgres) {
         await connection.run("INSTALL postgres; LOAD postgres;");
      }

      const escapedCatalogUrl = escapeSQL(config.catalogUrl);
      const escapedDataPath = escapeSQL(config.dataPath);
      const isCloudStorage =
         config.dataPath.startsWith("gs://") ||
         config.dataPath.startsWith("s3://");

      let attachCmd = `ATTACH 'ducklake:${escapedCatalogUrl}' AS ${catalogName}`;
      const attachOpts: string[] = [`DATA_PATH '${escapedDataPath}'`];
      if (isCloudStorage) {
         attachOpts.push("OVERRIDE_DATA_PATH true");
      }
      attachCmd += ` (${attachOpts.join(", ")});`;

      logger.info(`Attaching DuckLake manifest catalog: ${attachCmd}`);
      await connection.run(attachCmd);

      this.attachedCatalogs.add(catalogName);
   }

   getRepository(): ResourceRepository {
      if (!this.repository) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }
      return this.repository;
   }

   /**
    * Returns the manifest store for a project. If the project has a
    * DuckLake store configured, returns that; otherwise returns the
    * default DuckDB-backed store.
    */
   getManifestStore(projectId?: string): ManifestStore {
      if (projectId) {
         const projectStore = this.projectManifestStores.get(projectId);
         if (projectStore) {
            return projectStore;
         }
      }
      if (!this.defaultManifestStore) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }
      return this.defaultManifestStore;
   }

   async close(): Promise<void> {
      if (this.connection) {
         await this.connection.close();
         this.connection = null;
         this.duckDbConnection = null;
         this.repository = null;
         this.defaultManifestStore = null;
         this.projectManifestStores.clear();
         this.attachedCatalogs.clear();
      }
   }

   isInitialized(): boolean {
      return this.connection !== null && this.repository !== null;
   }
}

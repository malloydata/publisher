import * as crypto from "crypto";
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

function configKey(c: DuckLakeManifestConfig): string {
   return `${c.catalogUrl}|${c.dataPath}`;
}

function catalogNameForConfig(c: DuckLakeManifestConfig): string {
   const hash = crypto
      .createHash("sha256")
      .update(configKey(c))
      .digest("hex")
      .slice(0, 8);
   return `manifest_lake_${hash}`;
}

/**
 * Manages the storage backend (DuckDB, Postgres, etc.) and per-environment
 * manifest stores. Environments without `materializationStorage` config use
 * the default DuckDB manifest store. Environments with the config get a
 * DuckLake-backed store attached lazily on first access.
 */
export class StorageManager {
   private connection: DatabaseConnection | null = null;
   private duckDbConnection: DuckDBConnection | null = null;
   private repository: ResourceRepository | null = null;
   private defaultManifestStore: ManifestStore | null = null;

   /** Per-environment DuckLake manifest stores, keyed by environmentId. */
   private environmentManifestStores = new Map<string, ManifestStore>();

   /**
    * Tracks attached DuckLake catalogs as `configKey -> catalogName`. Each
    * unique materializationStorage config gets its own ATTACHment under a
    * deterministic catalog name, so multiple configs can coexist on one worker.
    */
   private attachedCatalogs = new Map<string, string>();

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
    * Lazily initializes a DuckLake manifest store for an environment.
    *
    * One shared catalog per materializationStorage config: every environment
    * pointing at the same (catalogUrl, dataPath) shares one `build_manifests`
    * table inside it, partitioned by `environment_id` (set to the environment's name
    * so it's stable across worker replicas — required for cross-pod manifest
    * visibility in orchestrated mode). Different configs (e.g. different
    * orgs) attach as separate catalogs under distinct deterministic aliases.
    */
   async initializeDuckLakeForEnvironment(
      environmentId: string,
      environmentName: string,
      config: DuckLakeManifestConfig,
   ): Promise<void> {
      if (!this.duckDbConnection) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }

      const key = configKey(config);
      let catalogName = this.attachedCatalogs.get(key);
      if (!catalogName) {
         // Catalog name derived from the config so multiple configs can coexist as
         // separate ATTACHments without colliding on the name.
         catalogName = catalogNameForConfig(config);
         await this.attachDuckLakeCatalog(config, catalogName);
         this.attachedCatalogs.set(key, catalogName);
      }

      const store = new DuckLakeManifestStore(
         this.duckDbConnection,
         catalogName,
         environmentName,
      );
      await store.bootstrapSchema();

      this.environmentManifestStores.set(environmentId, store);
      logger.info("DuckLake manifest store initialized for environment", {
         environmentId,
         environmentName,
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
      const attachOpts: string[] = [
         `DATA_PATH '${escapedDataPath}'`,
         // The manifest table is small relational metadata (one row per build).
         // Set a high inlining limit so writes always land transactionally in
         // the postgres catalog rather than as parquet files in object storage,
         // sidestepping object-storage auth issues entirely for this path.
         "DATA_INLINING_ROW_LIMIT 100000",
      ];
      if (isCloudStorage) {
         attachOpts.push("OVERRIDE_DATA_PATH true");
      }
      attachCmd += ` (${attachOpts.join(", ")});`;

      logger.info(`Attaching DuckLake manifest catalog: ${attachCmd}`);
      await connection.run(attachCmd);
   }

   getRepository(): ResourceRepository {
      if (!this.repository) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }
      return this.repository;
   }

   /**
    * Returns the manifest store for an environment. If the environment has a
    * DuckLake store configured, returns that; otherwise returns the
    * default DuckDB-backed store.
    */
   getManifestStore(environmentId?: string): ManifestStore {
      if (environmentId) {
         const environmentStore =
            this.environmentManifestStores.get(environmentId);
         if (environmentStore) {
            return environmentStore;
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
         this.environmentManifestStores.clear();
         this.attachedCatalogs.clear();
      }
   }

   isInitialized(): boolean {
      return this.connection !== null && this.repository !== null;
   }
}

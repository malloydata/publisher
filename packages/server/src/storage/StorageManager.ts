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
   ducklakeManifest?: DuckLakeManifestConfig;
}

const DUCKLAKE_CATALOG_NAME = "manifest_lake";

function escapeSQL(value: string): string {
   return value.replace(/'/g, "''");
}

export class StorageManager {
   private connection: DatabaseConnection | null = null;
   private repository: ResourceRepository | null = null;
   private manifestStore: ManifestStore | null = null;
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
      this.repository = new DuckDBRepository(connection);

      if (this.config.ducklakeManifest) {
         this.manifestStore = await this.initializeDuckLakeManifest(connection);
      } else {
         this.manifestStore = new DuckDBManifestStore(this.repository);
      }
   }

   private async initializeDuckLakeManifest(
      connection: DuckDBConnection,
   ): Promise<ManifestStore> {
      const config = this.config.ducklakeManifest!;

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

      let attachCmd = `ATTACH 'ducklake:${escapedCatalogUrl}' AS ${DUCKLAKE_CATALOG_NAME}`;
      const attachOpts: string[] = [`DATA_PATH '${escapedDataPath}'`];
      if (isCloudStorage) {
         attachOpts.push("OVERRIDE_DATA_PATH true");
      }
      attachCmd += ` (${attachOpts.join(", ")});`;

      logger.info(`Attaching DuckLake manifest catalog: ${attachCmd}`);
      await connection.run(attachCmd);

      const store = new DuckLakeManifestStore(
         connection,
         DUCKLAKE_CATALOG_NAME,
      );
      await store.bootstrapSchema();
      logger.info("DuckLake manifest store initialized");
      return store;
   }

   getRepository(): ResourceRepository {
      if (!this.repository) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }
      return this.repository;
   }

   getManifestStore(): ManifestStore {
      if (!this.manifestStore) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }
      return this.manifestStore;
   }

   async close(): Promise<void> {
      if (this.connection) {
         await this.connection.close();
         this.connection = null;
         this.repository = null;
         this.manifestStore = null;
      }
   }

   isInitialized(): boolean {
      return this.connection !== null && this.repository !== null;
   }
}

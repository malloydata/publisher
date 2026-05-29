import { Mutex } from "async-mutex";
import * as crypto from "crypto";
import { ConnectionAuthError } from "../errors";
import { logger } from "../logger";
import {
   handlePgAttachError,
   pgConnectTimeoutSeconds,
   redactPgSecrets,
   withPgConnectTimeout,
} from "../pg_helpers";
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

   // Serializes DuckLake catalog attaches. Concurrent POST /environments calls
   // hitting the same DuckDB connection would otherwise race on extension
   // autoload (httpfs/azure/etc.), where multiple connections download the
   // extension to `.tmp-<uuid>` files in parallel; only one wins the rename
   // and the rest crash with "Could not remove file ... No such file or directory".
   private duckLakeAttachMutex: Mutex = new Mutex();

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
      const catalogName = await this.duckLakeAttachMutex.runExclusive(
         async () => {
            const existing = this.attachedCatalogs.get(key);
            if (existing) return existing;
            // Catalog name derived from the config so multiple configs can coexist as
            // separate ATTACHments without colliding on the name.
            const name = catalogNameForConfig(config);
            await this.attachDuckLakeCatalog(config, name);
            this.attachedCatalogs.set(key, name);
            return name;
         },
      );

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

      // For PG-backed catalogs, inject connect_timeout so a wedged libpq
      // handshake fails the caller in seconds rather than hanging the
      // worker until the K8s liveness probe trips (the 2026-05 incident).
      // Non-PG catalogs (e.g. SQLite, MySQL) pass through unchanged.
      const catalogUrl = isPostgres
         ? withPgConnectTimeout(config.catalogUrl, pgConnectTimeoutSeconds())
         : config.catalogUrl;

      const escapedCatalogUrl = escapeSQL(catalogUrl);
      const escapedDataPath = escapeSQL(config.dataPath);
      const isCloudStorage =
         config.dataPath.startsWith("gs://") ||
         config.dataPath.startsWith("s3://");

      // Pre-install httpfs explicitly so the ATTACH below doesn't trigger
      // DuckDB's autoloader. The autoloader downloads extensions to
      // `<ext>.tmp-<uuid>` and races when multiple connections within the
      // same process hit it concurrently — losers fail with
      // "Could not remove file ... No such file or directory" on cleanup
      // of their .tmp file. INSTALL/LOAD here is idempotent and serialized
      // by the caller's mutex.
      if (isCloudStorage) {
         await connection.run("INSTALL httpfs; LOAD httpfs;");
      }

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

      logger.info(
         `Attaching DuckLake manifest catalog: ${redactPgSecrets(attachCmd)}`,
      );
      try {
         await connection.run(attachCmd);
      } catch (error) {
         const outcome = handlePgAttachError(
            error,
            `DuckLake catalog credentials rejected for ${catalogName}`,
         );
         if (outcome.action === "swallow") {
            logger.info(
               `DuckLake catalog ${catalogName} is already attached, skipping`,
            );
            return;
         }
         if (outcome.error instanceof ConnectionAuthError) {
            logger.warn("DuckLake catalog credentials rejected", {
               catalogName,
            });
         }
         throw outcome.error;
      }
   }

   /**
    * Access the underlying DuckDB connection for callers that need raw
    * SQL access (e.g. the ThemeStore singleton) and don't fit the
    * structured `ResourceRepository` interface. Use sparingly; prefer
    * `getRepository()` for anything that maps cleanly onto the existing
    * repository methods.
    */
   getDuckDbConnection(): DuckDBConnection {
      if (!this.duckDbConnection) {
         throw new Error("Storage not initialized");
      }
      return this.duckDbConnection;
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

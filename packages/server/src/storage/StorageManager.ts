import { Mutex } from "async-mutex";
import * as crypto from "crypto";
import {
   isCatalogVersionSupported,
   SUPPORTED_CATALOG_VERSIONS,
} from "../ducklake_version";
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

// Read the catalog's recorded DuckLake format version from its
// `ducklake_metadata` table via a plain postgres ATTACH (does NOT invoke
// the DuckLake extension on the catalog). Returns the version string on
// success, or `undefined` on any failure (missing table, query timeout,
// connect failure) so the main ATTACH path stays the source of truth for
// unrelated errors. Only meaningful for postgres-backed catalogs; the
// caller must guard with `isPostgres`.
async function readDuckLakeCatalogVersion(
   connection: DuckDBConnection,
   catalogUrl: string,
   catalogName: string,
): Promise<string | undefined> {
   if (!catalogUrl.startsWith("postgres:")) {
      return undefined;
   }
   const pgConnString = catalogUrl.slice("postgres:".length);
   const tempDb = `${catalogName}_preflight`;
   const escaped = escapeSQL(pgConnString);
   try {
      await connection.run(
         `ATTACH '${escaped}' AS ${tempDb} (TYPE postgres, READ_ONLY);`,
      );
      const rows = await connection.all<{ value: string }>(
         `SELECT value FROM ${tempDb}.ducklake_metadata WHERE key = 'version' LIMIT 1;`,
      );
      const value = rows[0]?.value;
      return typeof value === "string" ? value : undefined;
   } catch (error) {
      logger.warn(
         "DuckLake catalog version preflight failed; falling back to ATTACH",
         {
            catalogName,
            error: redactPgSecrets(
               error instanceof Error ? error.message : String(error),
            ),
         },
      );
      return undefined;
   } finally {
      try {
         await connection.run(`DETACH ${tempDb};`);
      } catch {
         // ATTACH may have failed, so DETACH may have nothing to do.
      }
   }
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

      // Preflight: read the catalog's recorded format version via the
      // postgres extension (not DuckLake) and fail fast with a non-retryable
      // 422 if the baked DuckLake extension can't read it. Without this,
      // an unsupported catalog would surface as a generic DuckDB error
      // from the ATTACH below, which retry loops misclassify as transient.
      if (isPostgres) {
         const catalogVersion = await readDuckLakeCatalogVersion(
            connection,
            catalogUrl,
            catalogName,
         );
         if (catalogVersion && !isCatalogVersionSupported(catalogVersion)) {
            const supportedMax =
               SUPPORTED_CATALOG_VERSIONS[
                  SUPPORTED_CATALOG_VERSIONS.length - 1
               ];
            throw new ConnectionAuthError(
               `DuckLake catalog version ${catalogVersion} is newer than this Publisher's extension supports (max ${supportedMax}). Upgrade the Publisher image or downgrade the catalog.`,
            );
         }
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

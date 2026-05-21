import * as fs from "fs/promises";
import * as path from "path";

import "@malloydata/db-duckdb/native";
import {
   Connection,
   ConnectionRuntime,
   contextOverlay,
   EmptyURLReader,
   FixedConnectionMap,
   MalloyConfig,
   MalloyError,
   SourceDef,
} from "@malloydata/malloy";
import { metrics } from "@opentelemetry/api";
import recursive from "recursive-readdir";
import { components } from "../api";
import { getPackageLoadPool } from "../package_load/package_load_pool";
import {
   API_PREFIX,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import {
   ModelCompilationError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../errors";
import { formatDuration, logger } from "../logger";
import { BuildManifest } from "../storage/DatabaseInterface";
import { ignoreDotfiles } from "../utils";
import { Model } from "./model";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiNotebook = components["schemas"]["Notebook"];
export type ApiPackage = components["schemas"]["Package"];
type ApiColumn = components["schemas"]["Column"];
type ApiTableDescription = components["schemas"]["TableDescription"];
// A thunk lets callers pass a live reference to the *current* environment
// MalloyConfig so the package wrapper resolves environment connections against the
// generation that's active at lookup time, not the one that was current when
// the package was first loaded.
type PackageConnectionInput =
   | MalloyConfig
   | Map<string, Connection>
   | (() => MalloyConfig);

const ENABLE_LIST_MODEL_COMPILATION = true;

export class Package {
   private environmentName: string;
   private packageName: string;
   private packageMetadata: ApiPackage;
   private databases: ApiDatabase[];
   private models: Map<string, Model> = new Map();
   private packagePath: string;
   private malloyConfig: MalloyConfig;
   private static meter = metrics.getMeter("publisher");
   private static packageLoadHistogram = this.meter.createHistogram(
      "malloy_package_load_duration",
      {
         description: "Time taken to load a Malloy package",
         unit: "ms",
      },
   );

   constructor(
      environmentName: string,
      packageName: string,
      packagePath: string,
      packageMetadata: ApiPackage,
      databases: ApiDatabase[],
      models: Map<string, Model>,
      malloyConfig: MalloyConfig = new MalloyConfig({ connections: {} }),
   ) {
      this.environmentName = environmentName;
      this.packageName = packageName;
      this.packagePath = packagePath;
      this.packageMetadata = packageMetadata;
      this.databases = databases;
      this.models = models;
      this.malloyConfig = malloyConfig;
   }

   static async create(
      environmentName: string,
      packageName: string,
      packagePath: string,
      environmentMalloyConfig: PackageConnectionInput,
   ): Promise<Package> {
      const startTime = performance.now();
      await Package.validatePackageManifestExistsOrThrowError(packagePath);
      const manifestValidationTime = performance.now();
      logger.info("Package manifest validation completed", {
         packageName,
         duration: formatDuration(manifestValidationTime - startTime),
      });

      try {
         // The MalloyConfig is always built on the main thread — it
         // owns the live native connection handles the package needs
         // to *serve queries* after load (workers can't share native
         // handles across the V8 isolate boundary). The worker proxies
         // non-duckdb connection lookups back through this MalloyConfig
         // during compile.
         const malloyConfig = Package.buildPackageMalloyConfig(
            packagePath,
            typeof environmentMalloyConfig === "function"
               ? environmentMalloyConfig
               : () => Package.toMalloyConfig(environmentMalloyConfig),
         );

         return await Package.loadViaWorker(
            environmentName,
            packageName,
            packagePath,
            malloyConfig,
            startTime,
            manifestValidationTime,
         );
      } catch (error) {
         logger.error(`Error loading package ${packageName}`, { error });
         console.error(error);
         const endTime = performance.now();
         const executionTime = endTime - startTime;
         const status =
            error instanceof ModelCompilationError ||
            error instanceof MalloyError
               ? "compilation_error"
               : error instanceof ServiceUnavailableError
                 ? "pool_unavailable"
                 : "error";
         this.packageLoadHistogram.record(executionTime, {
            malloy_package_name: packageName,
            status,
         });
         // Clean up package directory on failure
         try {
            await fs.rm(packagePath, {
               recursive: true,
               force: true,
            });
            logger.info(`Cleaned up failed package directory: ${packagePath}`);
         } catch (cleanupError) {
            logger.warn(`Failed to clean up package directory ${packagePath}`, {
               error: cleanupError,
            });
         }
         throw error;
      }
   }

   /**
    * Load the package via the package-load worker pool. The worker
    * performs the CPU-bound bulk of the load off-thread (manifest
    * read, every `.malloy` / `.malloynb` compile) and ships back a
    * structured-clonable `LoadPackageOutcome`. Database probes
    * (`.parquet` / `.csv`) run on the main thread, in parallel with
    * the worker compile, against the package's existing DuckDB
    * connection — they're async-IO-bound and don't compete with the
    * worker for CPU.
    *
    * Pool-infrastructure failures (worker crash, RPC timeout, pool
    * shutting down) are rewrapped as `ServiceUnavailableError` so
    * the HTTP layer responds 503 (transient, retryable). Real compile
    * errors (`MalloyError` / `ModelCompilationError`) propagate
    * unchanged so they keep their 4xx mapping.
    */
   private static async loadViaWorker(
      environmentName: string,
      packageName: string,
      packagePath: string,
      malloyConfig: MalloyConfig,
      startTime: number,
      manifestValidationTime: number,
   ): Promise<Package> {
      const pool = getPackageLoadPool();
      const dispatchTime = performance.now();
      // Submit the worker job and run database probing on the main
      // thread in parallel. We isolate the worker-job promise inside
      // a wrapper so we can map pool-infrastructure failures (worker
      // crash, RPC timeout, pool shutting down) to a 503 without
      // accidentally re-mapping `readDatabases`'s own errors.
      const workerOutcome = pool
         .loadPackage({
            packagePath,
            packageName,
            malloyConfig,
            defaultConnectionName: "duckdb",
         })
         .catch((err: unknown) => {
            // Compile errors surface in-band via
            // `LoadPackageOutcome.models[i].compilationError`; if the
            // pool itself rejects, it's an infra-side failure
            // (shutting down, worker spawn failed, worker crashed,
            // RPC timeout) and the client should retry. Real Malloy
            // compile errors deserialised by the pool still carry
            // their MalloyError / ModelCompilationError identity —
            // let those bubble untouched so they keep their 4xx
            // mapping in `errors.ts`.
            const realError =
               err instanceof Error
                  ? err
                  : new Error(
                       `Package-load worker pool failure: ${String(err)}`,
                    );
            if (
               realError instanceof MalloyError ||
               realError instanceof ModelCompilationError
            ) {
               throw realError;
            }
            throw new ServiceUnavailableError(
               `Package-load worker pool unavailable: ${realError.message}`,
            );
         });
      const [outcome, databases] = await Promise.all([
         workerOutcome,
         Package.readDatabases(packagePath, malloyConfig),
      ]);
      const workerDoneTime = performance.now();
      logger.info("Package load via worker pool completed", {
         packageName,
         manifestValidationMs: dispatchTime - manifestValidationTime,
         workerDurationMs: outcome.loadDurationMs,
         dispatchOverheadMs:
            workerDoneTime - dispatchTime - outcome.loadDurationMs,
         modelCount: outcome.models.length,
         databaseCount: databases.length,
      });

      // Override the manifest-derived resource URI — the worker only
      // returns name/description from publisher.json, but the rest of
      // the API surface expects a `resource` field too.
      const packageConfig: ApiPackage = {
         name: outcome.packageMetadata.name,
         description: outcome.packageMetadata.description,
         resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}`,
      };

      // Build live `Model`s from worker output. Any per-model compile
      // failure aborts the load — matches the historical behaviour of
      // `Package.create` failing the whole package on the first model
      // error. (`Package.reloadAllModels` keeps the failed-model
      // placeholders instead; that branch goes through a different
      // hydration path.)
      const models = new Map<string, Model>();
      for (const sm of outcome.models) {
         if (sm.compilationError) {
            const err = Model.deserializeCompilationError(sm.compilationError);
            logger.error("Model compilation failed", {
               packageName,
               modelPath: sm.modelPath,
               error: err.message,
            });
            // The outer catch in Package.create records the metric +
            // cleans the package directory.
            throw err;
         }
         models.set(
            sm.modelPath,
            Model.fromSerialized(packageName, packagePath, malloyConfig, sm),
         );
      }

      const endTime = performance.now();
      const executionTime = endTime - startTime;
      this.packageLoadHistogram.record(executionTime, {
         malloy_package_name: packageName,
         status: "success",
      });
      logger.info(`Successfully loaded package ${packageName}`, {
         packageName,
         duration: formatDuration(executionTime),
      });

      return new Package(
         environmentName,
         packageName,
         packagePath,
         packageConfig,
         databases,
         models,
         malloyConfig,
      );
   }

   public getPackageName(): string {
      return this.packageName;
   }

   public getPackageMetadata(): ApiPackage {
      return this.packageMetadata;
   }

   public listDatabases(): ApiDatabase[] {
      return this.databases;
   }

   public getModel(modelPath: string): Model | undefined {
      return this.models.get(modelPath);
   }

   public async getMalloyConnection(
      connectionName: string,
   ): Promise<Connection> {
      return this.malloyConfig.connections.lookupConnection(connectionName);
   }

   public getMalloyConfig(): MalloyConfig {
      return this.malloyConfig;
   }

   public getPackagePath(): string {
      return this.packagePath;
   }

   public getModelPaths(): string[] {
      return Array.from(this.models.keys());
   }

   /**
    * Re-compile every model in the package against a new build
    * manifest (called after a materialization build commits new
    * physicalised tables). Runs through the package-load worker pool
    * — same off-main-thread compile path as initial `Package.create`
    * — so a reload of a large package can't block the K8s liveness
    * probe.
    *
    * Unlike `Package.create`, a per-model compile failure here does
    * NOT abort the reload: we keep the failed model as a placeholder
    * (`Model.fromCompilationError`) in `this.models`, matching the
    * historical reload semantics. Whole-pool failures (worker crash,
    * timeout, pool shutting down) propagate as `ServiceUnavailableError`
    * — the caller (manifest service) decides how to retry.
    */
   public async reloadAllModels(
      buildManifest: BuildManifest["entries"],
   ): Promise<void> {
      const modelPaths = Array.from(this.models.keys());
      logger.info("Reloading all models with build manifest", {
         packageName: this.packageName,
         modelCount: modelPaths.length,
         manifestEntryCount: Object.keys(buildManifest).length,
      });

      const pool = getPackageLoadPool();
      let outcome;
      try {
         outcome = await pool.loadPackage({
            packagePath: this.packagePath,
            packageName: this.packageName,
            malloyConfig: this.malloyConfig,
            defaultConnectionName: "duckdb",
            buildManifest,
         });
      } catch (err) {
         const realError =
            err instanceof Error
               ? err
               : new Error(`Package-load worker pool failure: ${String(err)}`);
         if (
            realError instanceof MalloyError ||
            realError instanceof ModelCompilationError
         ) {
            throw realError;
         }
         throw new ServiceUnavailableError(
            `Package-load worker pool unavailable: ${realError.message}`,
         );
      }

      const nextModels = new Map<string, Model>();
      for (const sm of outcome.models) {
         if (sm.compilationError) {
            const err = Model.deserializeCompilationError(sm.compilationError);
            logger.warn("Model compilation failed during reload", {
               packageName: this.packageName,
               modelPath: sm.modelPath,
               error: err.message,
            });
            nextModels.set(
               sm.modelPath,
               Model.fromCompilationError(
                  this.packageName,
                  sm.modelPath,
                  sm.modelType,
                  err,
               ),
            );
         } else {
            nextModels.set(
               sm.modelPath,
               Model.fromSerialized(
                  this.packageName,
                  this.packagePath,
                  this.malloyConfig,
                  sm,
               ),
            );
         }
      }
      this.models = nextModels;
   }

   public async getModelFileText(modelPath: string): Promise<string> {
      const model = this.getModel(modelPath);
      if (!model) {
         throw new Error(`Model not found: ${modelPath}`);
      }
      return await model.getFileText(this.packagePath);
   }

   public async listModels(): Promise<ApiModel[]> {
      const values = await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               return modelPath.endsWith(MODEL_FILE_SUFFIX);
            })
            .map(async (modelPath) => {
               let error: string | undefined;
               if (ENABLE_LIST_MODEL_COMPILATION) {
                  try {
                     await this.models.get(modelPath)?.getModel();
                  } catch (modelError) {
                     error =
                        modelError instanceof Error
                           ? modelError.message
                           : undefined;
                  }
               }
               return {
                  environmentName: this.environmentName,
                  path: modelPath,
                  packageName: this.packageName,
                  error,
               };
            }),
      );
      return values;
   }

   public async listNotebooks(): Promise<ApiNotebook[]> {
      return await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               return modelPath.endsWith(NOTEBOOK_FILE_SUFFIX);
            })
            .map(async (modelPath) => {
               let error: Error | undefined;
               if (ENABLE_LIST_MODEL_COMPILATION) {
                  error = this.models.get(modelPath)?.getNotebookError();
               }
               return {
                  environmentName: this.environmentName,
                  packageName: this.packageName,
                  path: modelPath,
                  error: error?.message,
               };
            }),
      );
   }

   private static buildPackageMalloyConfig(
      packagePath: string,
      getEnvironmentMalloyConfig: () => MalloyConfig,
   ): MalloyConfig {
      const malloyConfig = new MalloyConfig(
         {
            connections: {
               duckdb: {
                  is: "duckdb",
                  databasePath: ":memory:",
               },
            },
         },
         {
            config: contextOverlay({ rootDirectory: packagePath }),
         },
      );

      malloyConfig.wrapConnections((base) => ({
         lookupConnection: async (name?: string) => {
            if (!name || name === "duckdb") {
               return base.lookupConnection(name);
            }
            // Resolve against the *current* environment MalloyConfig so a
            // connection-generation swap on Environment propagates without a
            // package reload.
            return getEnvironmentMalloyConfig().connections.lookupConnection(
               name,
            );
         },
      }));

      return malloyConfig;
   }

   private static toMalloyConfig(
      input: MalloyConfig | Map<string, Connection>,
   ): MalloyConfig {
      if (input instanceof MalloyConfig) {
         return input;
      }

      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(input, "duckdb"),
      );
      return malloyConfig;
   }

   private static async validatePackageManifestExistsOrThrowError(
      packagePath: string,
   ) {
      const packageConfigPath = path.join(packagePath, PACKAGE_MANIFEST_NAME);
      try {
         await fs.stat(packageConfigPath);
      } catch {
         logger.error(`Can't find ${packageConfigPath}`);
         throw new PackageNotFoundError(
            `Package manifest for ${packagePath} does not exist.`,
         );
      }
   }

   private static async readDatabases(
      packagePath: string,
      malloyConfig: MalloyConfig,
   ): Promise<ApiDatabase[]> {
      const databasePaths = await Package.getDatabasePaths(packagePath);
      if (databasePaths.length === 0) {
         return [];
      }
      // Resolve the package's duckdb connection ONCE and reuse it for
      // every schema/row-count probe in this package. Malloy caches the
      // materialized connection on the MalloyConfig so the same instance
      // will be returned to model compiles later in `Package.create`.
      // This is the substantive optimization over the previous code:
      // we go from `databasePaths.length` separate DuckDBConnections
      // (each doing its own native init + extension load) to one.
      const conn = await malloyConfig.connections.lookupConnection("duckdb");
      return await Promise.all(
         databasePaths.map(async (databasePath) => ({
            path: databasePath,
            info: await Package.getDatabaseInfo(
               packagePath,
               databasePath,
               conn,
            ),
            type: "embedded" as const,
         })),
      );
   }

   private static async getDatabasePaths(
      packagePath: string,
   ): Promise<string[]> {
      const files = await recursive(packagePath, [ignoreDotfiles]);
      return files
         .map((fullPath: string) => {
            return path.relative(packagePath, fullPath).replace(/\\/g, "/");
         })
         .filter(
            (modelPath: string) =>
               modelPath.endsWith(".parquet") || modelPath.endsWith(".csv"),
         );
   }

   private static async getDatabaseInfo(
      packagePath: string,
      databasePath: string,
      conn: Connection,
   ): Promise<ApiTableDescription> {
      const fullPath = path.join(packagePath, databasePath);

      // Create a DuckDB source then:
      // 1. Load the model and get the table schema from model
      // 2. Run a query to get the row count from the table
      // ConnectionRuntime is cheap (just a wrapper), and creating one
      // per call keeps each probe's compile state isolated. The
      // expensive piece — the underlying DuckDBConnection — is shared
      // across all probes via `conn` (resolved once in readDatabases).
      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [conn],
      });
      // Normalize path to use forward slashes for cross-platform compatibility
      // DuckDB on Windows supports forward slashes, and this avoids escaping issues
      const normalizedPath = fullPath.replace(/\\/g, "/");
      const model = runtime.loadModel(
         `source: temp is duckdb.table('${normalizedPath}')`,
      );
      const modelDef = await model.getModel();
      const fields = (modelDef._modelDef.contents["temp"] as SourceDef).fields;
      const schema = fields.map((field): ApiColumn => {
         return { type: field.type, name: field.name };
      });
      const runner = model.loadQuery(
         "run: temp->{aggregate: row_count is count()}",
      );
      const result = await runner.run();
      const rowCount = result.data.value[0].row_count?.valueOf() as number;
      return { name: databasePath, rowCount, columns: schema };
   }

   public setName(name: string) {
      this.packageName = name;
   }

   public setEnvironmentName(environmentName: string) {
      this.environmentName = environmentName;
   }

   public setPackageMetadata(packageMetadata: ApiPackage) {
      this.packageMetadata = packageMetadata;
   }
}

import * as fs from "fs/promises";
import * as path from "path";

import { DuckDBConnection } from "@malloydata/db-duckdb";
import "@malloydata/db-duckdb/native";
import {
   Connection,
   ConnectionRuntime,
   contextOverlay,
   EmptyURLReader,
   FixedConnectionMap,
   MalloyConfig,
   SourceDef,
} from "@malloydata/malloy";
import { metrics } from "@opentelemetry/api";
import recursive from "recursive-readdir";
import { components } from "../api";
import { getCompilePool } from "../compile/compile_pool";
import {
   API_PREFIX,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import { PackageNotFoundError } from "../errors";
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
         const packageConfig = await Package.readPackageConfig(packagePath);
         const packageConfigTime = performance.now();
         logger.info("Package config read completed", {
            packageName,
            duration: formatDuration(
               packageConfigTime - manifestValidationTime,
            ),
         });
         packageConfig.resource = `${API_PREFIX}/environments/${environmentName}/packages/${packageName}`;

         const databases = await Package.readDatabases(packagePath);
         const databasesTime = performance.now();
         logger.info("Databases read completed", {
            packageName,
            databaseCount: databases.length,
            duration: formatDuration(databasesTime - packageConfigTime),
         });
         const malloyConfig = Package.buildPackageMalloyConfig(
            packagePath,
            typeof environmentMalloyConfig === "function"
               ? environmentMalloyConfig
               : () => Package.toMalloyConfig(environmentMalloyConfig),
         );

         const models = await Package.loadModels(
            packageName,
            packagePath,
            malloyConfig,
         );
         const modelsTime = performance.now();
         logger.info("Models loaded", {
            packageName,
            modelCount: models.size,
            duration: formatDuration(modelsTime - databasesTime),
         });
         for (const [modelPath, model] of models.entries()) {
            const maybeModel = model as unknown as {
               compilationError?: unknown;
            };
            if (maybeModel.compilationError) {
               const err = maybeModel.compilationError;
               const message =
                  err instanceof Error
                     ? err.message
                     : `Unknown compilation error in ${modelPath}`;

               logger.error("Model compilation failed", {
                  packageName,
                  modelPath,
                  error: message,
               });

               this.packageLoadHistogram.record(performance.now() - startTime, {
                  malloy_package_name: packageName,
                  status: "compilation_error",
               });
               throw err;
            }
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
      } catch (error) {
         logger.error(`Error loading package ${packageName}`, { error });
         console.error(error);
         const endTime = performance.now();
         const executionTime = endTime - startTime;
         this.packageLoadHistogram.record(executionTime, {
            malloy_package_name: packageName,
            status: "error",
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

   public async reloadAllModels(
      buildManifest: BuildManifest["entries"],
   ): Promise<void> {
      const modelPaths = Array.from(this.models.keys());
      logger.info("Reloading all models with build manifest", {
         packageName: this.packageName,
         modelCount: modelPaths.length,
         manifestEntryCount: Object.keys(buildManifest).length,
      });
      const reloaded = await Promise.all(
         modelPaths.map((modelPath) =>
            Model.create(
               this.packageName,
               this.packagePath,
               modelPath,
               this.malloyConfig,
               { buildManifest },
            ),
         ),
      );
      const nextModels = new Map<string, Model>();
      for (const model of reloaded) {
         nextModels.set(model.getPath(), model);
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

   private static async loadModels(
      packageName: string,
      packagePath: string,
      malloyConfig: MalloyConfig,
   ): Promise<Map<string, Model>> {
      const modelPaths = await Package.getModelPaths(packagePath);
      const models = await Promise.all(
         modelPaths.map((modelPath) =>
            Model.create(packageName, packagePath, modelPath, malloyConfig),
         ),
      );
      return new Map(models.map((model) => [model.getPath(), model]));
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

   private static async getModelPaths(packagePath: string): Promise<string[]> {
      let files = undefined;
      try {
         files = await recursive(packagePath, [ignoreDotfiles]);
      } catch (error) {
         logger.error(error);
         throw new PackageNotFoundError(
            `Package config for ${packagePath} does not exist.`,
         );
      }
      return files
         .map((fullPath: string) => {
            return path.relative(packagePath, fullPath).replace(/\\/g, "/");
         })
         .filter(
            (modelPath: string) =>
               modelPath.endsWith(MODEL_FILE_SUFFIX) ||
               modelPath.endsWith(NOTEBOOK_FILE_SUFFIX),
         );
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

   private static async readPackageConfig(
      packagePath: string,
   ): Promise<ApiPackage> {
      const packageConfigPath = path.join(packagePath, PACKAGE_MANIFEST_NAME);
      const packageConfigContents = await fs.readFile(packageConfigPath);
      // TODO: Validate package manifest.  Define manifest type in public API.
      const packageManifest = JSON.parse(packageConfigContents.toString());
      return {
         name: packageManifest.name,
         description: packageManifest.description,
      };
   }

   private static async readDatabases(
      packagePath: string,
   ): Promise<ApiDatabase[]> {
      return await Promise.all(
         (await Package.getDatabasePaths(packagePath)).map(
            async (databasePath) => {
               const databaseInfo = await Package.getDatabaseInfo(
                  packagePath,
                  databasePath,
               );

               return {
                  path: databasePath,
                  info: databaseInfo,
                  type: "embedded",
               };
            },
         ),
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
   ): Promise<ApiTableDescription> {
      const fullPath = path.join(packagePath, databasePath);
      // Normalize path to use forward slashes for cross-platform compatibility.
      // DuckDB on Windows supports forward slashes, which avoids escaping issues.
      const normalizedPath = fullPath.replace(/\\/g, "/");

      // One DuckDB connection per file (matches the historical
      // ConnectionRuntime shape). Reused for both the schema probe and
      // the row-count SQL so we only pay native init once per call.
      const conn = new DuckDBConnection("duckdb");

      // Schema probe. We need Malloy's view of the column types
      // (consumers of the API rely on Malloy type strings, not DuckDB
      // native types), so this stays a Malloy compile. The compile is
      // CPU-heavy on the main thread relative to the work it produces,
      // so when the worker pool is enabled we ship the synthetic
      // `source: temp is duckdb.table(…)` snippet to a worker and read
      // the resulting modelDef back here. Schema-fetch RPCs from the
      // worker proxy through the pool against `conn` below.
      const pool = getCompilePool();
      let schema: ApiColumn[];
      if (pool.enabled) {
         schema = await Package.getSchemaViaPool(
            pool,
            packagePath,
            normalizedPath,
            conn,
         );
      } else {
         schema = await Package.getSchemaInProcess(normalizedPath, conn);
      }

      // Row count. We previously compiled
      // `run: temp->{aggregate: row_count is count()}` via Malloy, which
      // is another full parse + type-check + SQL gen on the main thread
      // just to produce a literal `SELECT count(*)`. Skip Malloy entirely
      // and call DuckDB directly — same wire result, no main-thread CPU.
      // Single quotes in the path are doubled to be safe against
      // SQL-injection from filenames (DuckDB uses ANSI quoting rules).
      const escapedPath = normalizedPath.replace(/'/g, "''");
      const sqlResult = await conn.runSQL(
         `SELECT count(*)::BIGINT AS row_count FROM '${escapedPath}'`,
      );
      const firstRow = sqlResult.rows[0] as { row_count?: bigint | number };
      const rowCount = Number(firstRow.row_count ?? 0);

      return { name: databasePath, rowCount, columns: schema };
   }

   /**
    * In-process schema probe (legacy / kill-switch path). Builds a
    * minimal ConnectionRuntime + compiles the synthetic snippet on the
    * main thread. Same behaviour as the pre-worker-pool implementation.
    */
   private static async getSchemaInProcess(
      normalizedPath: string,
      conn: DuckDBConnection,
   ): Promise<ApiColumn[]> {
      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [conn],
      });
      const model = runtime.loadModel(
         `source: temp is duckdb.table('${normalizedPath}')`,
      );
      const modelDef = await model.getModel();
      const fields = (modelDef._modelDef.contents["temp"] as SourceDef).fields;
      return fields.map((field): ApiColumn => {
         return { type: field.type, name: field.name };
      });
   }

   /**
    * Worker-pool schema probe. The synthetic Malloy snippet is compiled
    * in a worker_threads worker; the worker's schema-fetch RPC bounces
    * back to the main thread, which services it against the
    * `MalloyConfig` we hold on `conn` below. The returned modelDef has
    * the resolved field list ready to read.
    */
   private static async getSchemaViaPool(
      pool: ReturnType<typeof getCompilePool>,
      packagePath: string,
      normalizedPath: string,
      conn: DuckDBConnection,
   ): Promise<ApiColumn[]> {
      // Keep schema-fetch RPCs from the worker routed to *this* conn
      // (same instance used by the row-count SQL) by wrapping it in a
      // MalloyConfig. We don't ship this config across the worker
      // boundary; the pool holds it on the main side.
      const malloyConfig = new MalloyConfig(
         { connections: {} },
         { config: contextOverlay({ rootDirectory: packagePath }) },
      );
      malloyConfig.wrapConnections(() => ({
         lookupConnection: async (_name?: string) =>
            conn as unknown as Connection,
      }));

      try {
         const outcome = await pool.compileInline({
            packagePath,
            source: `source: temp is duckdb.table('${normalizedPath}')`,
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         const modelDef = outcome.modelDef as unknown as {
            contents: Record<string, SourceDef>;
         };
         const fields = modelDef.contents["temp"].fields;
         return fields.map((field): ApiColumn => {
            return { type: field.type, name: field.name };
         });
      } catch (error) {
         // Transient pool issues (worker exit, RPC timeout) shouldn't
         // break package loading. Compile errors here would mean the
         // file isn't a readable parquet/csv, which in-process would
         // also throw — so let those propagate.
         if (
            error instanceof Error &&
            !/timed out|exited unexpectedly|shutting down/i.test(error.message)
         ) {
            throw error;
         }
         logger.warn(
            "Compile worker failed for database probe; falling back to in-process",
            { normalizedPath, error: (error as Error).message },
         );
         return Package.getSchemaInProcess(normalizedPath, conn);
      }
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

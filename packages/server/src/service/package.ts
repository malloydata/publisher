import * as fs from "fs/promises";
import * as path from "path";

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   Connection,
   ConnectionRuntime,
   EmptyURLReader,
   SourceDef,
} from "@malloydata/malloy";
import { metrics } from "@opentelemetry/api";
import recursive from "recursive-readdir";
import { components } from "../api";
import {
   API_PREFIX,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import { PackageNotFoundError } from "../errors";
import { logger } from "../logger";
import { createPackageDuckDBConnections } from "./connection";
import { ApiConnection, Model } from "./model";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiNotebook = components["schemas"]["Notebook"];
export type ApiPackage = components["schemas"]["Package"];
type ApiColumn = components["schemas"]["Column"];
type ApiTableDescription = components["schemas"]["TableDescription"];

const ENABLE_LIST_MODEL_COMPILATION = true;
export class Package {
   private projectName: string;
   private packageName: string;
   private packageMetadata: ApiPackage;
   private databases: ApiDatabase[];
   private models: Map<string, Model> = new Map();
   private packagePath: string;
   private connections: Map<string, Connection> = new Map();
   private static meter = metrics.getMeter("publisher");
   private static packageLoadHistogram = this.meter.createHistogram(
      "malloy_package_load_duration",
      {
         description: "Time taken to load a Malloy package",
         unit: "ms",
      },
   );

   constructor(
      projectName: string,
      packageName: string,
      packagePath: string,
      packageMetadata: ApiPackage,
      databases: ApiDatabase[],
      models: Map<string, Model>,
      connections: Map<string, Connection> = new Map(),
   ) {
      this.projectName = projectName;
      this.packageName = packageName;
      this.packagePath = packagePath;
      this.packageMetadata = packageMetadata;
      this.databases = databases;
      this.models = models;
      this.connections = connections;
   }

   static async create(
      projectName: string,
      packageName: string,
      packagePath: string,
      projectConnections: Map<string, Connection>,
      packageConnections: ApiConnection[],
   ): Promise<Package> {
      const startTime = performance.now();
      await Package.validatePackageManifestExistsOrThrowError(packagePath);
      const manifestValidationTime = performance.now();
      logger.info("Package manifest validation completed", {
         packageName,
         duration: manifestValidationTime - startTime,
         unit: "ms",
      });

      try {
         const packageConfig = await Package.readPackageConfig(packagePath);
         const packageConfigTime = performance.now();
         logger.info("Package config read completed", {
            packageName,
            duration: packageConfigTime - manifestValidationTime,
            unit: "ms",
         });
         packageConfig.resource = `${API_PREFIX}/projects/${projectName}/packages/${packageName}`;

         const databases = await Package.readDatabases(packagePath);
         const databasesTime = performance.now();
         logger.info("Databases read completed", {
            packageName,
            databaseCount: databases.length,
            duration: databasesTime - packageConfigTime,
            unit: "ms",
         });
         const connections = new Map<string, Connection>(projectConnections);

         // Add a duckdb connection for the package.
         const duckdbConnections = await createPackageDuckDBConnections(
            packageConnections,
            packagePath,
         );
         duckdbConnections.malloyConnections.forEach((connection, name) => {
            connections.set(name, connection);
         });

         const models = await Package.loadModels(
            packageName,
            packagePath,
            connections,
         );
         const modelsTime = performance.now();
         logger.info("Models loaded", {
            packageName,
            modelCount: models.size,
            duration: modelsTime - databasesTime,
            unit: "ms",
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
            duration: executionTime,
            unit: "ms",
         });
         return new Package(
            projectName,
            packageName,
            packagePath,
            packageConfig,
            databases,
            models,
            connections,
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

   public getMalloyConnection(connectionName: string): Connection {
      const connection = this.connections.get(connectionName);
      if (!connection) {
         throw new Error(
            `Connection ${connectionName} not found in package ${this.packageName}`,
         );
      }
      return connection;
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
                  projectName: this.projectName,
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
                  projectName: this.projectName,
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
      connections: Map<string, Connection>,
   ): Promise<Map<string, Model>> {
      const modelPaths = await Package.getModelPaths(packagePath);
      const models = await Promise.all(
         modelPaths.map((modelPath) =>
            Model.create(packageName, packagePath, modelPath, connections),
         ),
      );
      return new Map(models.map((model) => [model.getPath(), model]));
   }

   private static async getModelPaths(packagePath: string): Promise<string[]> {
      let files = undefined;
      try {
         files = await recursive(packagePath);
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
      logger.info("Starting to read databases", { packagePath });
      const databasePaths = await Package.getDatabasePaths(packagePath);
      logger.info("Found database paths", {
         packagePath,
         databasePaths,
         count: databasePaths.length,
      });
      return await Promise.all(
         databasePaths.map(async (databasePath) => {
            logger.info("Processing database", { packagePath, databasePath });
            const startTime = Date.now();
            try {
               const databaseInfo = await Package.getDatabaseInfo(
                  packagePath,
                  databasePath,
               );
               const duration = Date.now() - startTime;
               logger.info("Database info retrieved", {
                  packagePath,
                  databasePath,
                  duration,
                  rowCount: databaseInfo.rowCount,
                  columnCount: databaseInfo.columns?.length ?? 0,
               });
               return {
                  path: databasePath,
                  info: databaseInfo,
                  type: "embedded",
               };
            } catch (error) {
               const duration = Date.now() - startTime;
               logger.error("Failed to get database info", {
                  packagePath,
                  databasePath,
                  duration,
                  error: error instanceof Error ? error.message : String(error),
               });
               throw error;
            }
         }),
      );
   }

   private static async getDatabasePaths(
      packagePath: string,
   ): Promise<string[]> {
      logger.info("Getting database paths", { packagePath });
      let files = undefined;
      try {
         files = await recursive(packagePath);
         logger.info("Recursive file search completed", {
            packagePath,
            fileCount: files.length,
         });
      } catch (error) {
         logger.error("Failed to recursively search for files", {
            packagePath,
            error: error instanceof Error ? error.message : String(error),
         });
         throw error;
      }
      const databasePaths = files
         .map((fullPath: string) => {
            const relativePath = path.relative(packagePath, fullPath);
            const normalizedPath = relativePath.replace(/\\/g, "/");
            return normalizedPath;
         })
         .filter(
            (modelPath: string) =>
               modelPath.endsWith(".parquet") || modelPath.endsWith(".csv"),
         );
      logger.info("Filtered database paths", {
         packagePath,
         databasePaths,
         count: databasePaths.length,
      });
      return databasePaths;
   }

   private static async getDatabaseInfo(
      packagePath: string,
      databasePath: string,
   ): Promise<ApiTableDescription> {
      logger.info("Getting database info", { packagePath, databasePath });

      // Ensure databasePath is treated as a relative path
      // getDatabasePaths returns normalized paths with forward slashes,
      // but path.join needs to work correctly, so we normalize after joining
      // Use path.resolve to ensure we have an absolute path
      const fullPath = path.resolve(packagePath, databasePath);
      logger.info("Resolved full path", {
         packagePath,
         databasePath,
         fullPath,
         platform: process.platform,
      });

      // Create a DuckDB source then:
      // 1. Load the model and get the table schema from model
      // 2. Run a query to get the row count from the table
      logger.info("Creating DuckDB runtime", { fullPath });
      const duckdbConnection = new DuckDBConnection("duckdb");
      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [duckdbConnection],
      });

      // Normalize Windows paths to use forward slashes for DuckDB
      // DuckDB accepts forward slashes on all platforms, including Windows
      // Also escape single quotes in the path for SQL string safety
      const normalizedPath = fullPath.replace(/\\/g, "/").replace(/'/g, "''");
      logger.info("Normalized path for DuckDB", {
         originalPath: fullPath,
         normalizedPath,
      });

      const modelString = `source: temp is duckdb.table('${normalizedPath}')`;
      logger.info("Loading Malloy model", { modelString });

      try {
         const model = runtime.loadModel(modelString);
         logger.info("Malloy model loaded successfully", { normalizedPath });

         logger.info("Getting model definition", { normalizedPath });
         const modelDef = await model.getModel();
         const fields = (modelDef._modelDef.contents["temp"] as SourceDef)
            .fields;
         logger.info("Extracted schema fields", {
            normalizedPath,
            fieldCount: fields.length,
            fields: fields.map((f) => ({ name: f.name, type: f.type })),
         });

         const schema = fields.map((field): ApiColumn => {
            return { type: field.type, name: field.name };
         });

         logger.info("Running row count query", { normalizedPath });
         const runner = model.loadQuery(
            "run: temp->{aggregate: row_count is count()}",
         );
         const result = await runner.run();
         const rowCount = result.data.value[0].row_count?.valueOf() as number;
         logger.info("Row count query completed", {
            normalizedPath,
            rowCount,
         });

         return { name: databasePath, rowCount, columns: schema };
      } catch (error) {
         logger.error("Failed to get database info", {
            packagePath,
            databasePath,
            fullPath,
            normalizedPath,
            error: error instanceof Error ? error.message : String(error),
            errorStack: error instanceof Error ? error.stack : undefined,
         });
         throw error;
      } finally {
         // Close DuckDB connection to release file handles on Windows
         // This prevents EBUSY errors when cleaning up test directories
         try {
            logger.info("Closing DuckDB connection", { normalizedPath });
            if (
               duckdbConnection &&
               typeof duckdbConnection.close === "function"
            ) {
               await duckdbConnection.close();
               logger.info("DuckDB connection closed successfully", {
                  normalizedPath,
               });
               // On Windows, file handles may not be released immediately after closing
               // Add a small delay to allow the OS to release file locks
               if (process.platform === "win32") {
                  await new Promise((resolve) => setTimeout(resolve, 50));
               }
            }
         } catch (closeError) {
            logger.warn("Failed to close DuckDB connection", {
               normalizedPath,
               error:
                  closeError instanceof Error
                     ? closeError.message
                     : String(closeError),
            });
         }
      }
   }

   public setName(name: string) {
      this.packageName = name;
   }

   public setProjectName(projectName: string) {
      this.projectName = projectName;
   }

   public setPackageMetadata(packageMetadata: ApiPackage) {
      this.packageMetadata = packageMetadata;
   }
}

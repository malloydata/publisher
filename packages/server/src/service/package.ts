import * as fs from "fs/promises";
import * as path from "path";

import { DuckDBConnection } from "@malloydata/db-duckdb";
import "@malloydata/db-duckdb/native";
import {
   Connection,
   ConnectionRuntime,
   EmptyURLReader,
   FixedConnectionMap,
   contextOverlay,
   MalloyConfig,
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
import { formatDuration, logger } from "../logger";
import { Model } from "./model";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiNotebook = components["schemas"]["Notebook"];
export type ApiPackage = components["schemas"]["Package"];
type ApiColumn = components["schemas"]["Column"];
type ApiTableDescription = components["schemas"]["TableDescription"];
type PackageConnectionInput = MalloyConfig | Map<string, Connection>;

const ENABLE_LIST_MODEL_COMPILATION = true;
export class Package {
   private projectName: string;
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
      projectName: string,
      packageName: string,
      packagePath: string,
      packageMetadata: ApiPackage,
      databases: ApiDatabase[],
      models: Map<string, Model>,
      malloyConfig: MalloyConfig = new MalloyConfig({ connections: {} }),
   ) {
      this.projectName = projectName;
      this.packageName = packageName;
      this.packagePath = packagePath;
      this.packageMetadata = packageMetadata;
      this.databases = databases;
      this.models = models;
      this.malloyConfig = malloyConfig;
   }

   static async create(
      projectName: string,
      packageName: string,
      packagePath: string,
      projectMalloyConfig: PackageConnectionInput,
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
         packageConfig.resource = `${API_PREFIX}/projects/${projectName}/packages/${packageName}`;

         const databases = await Package.readDatabases(packagePath);
         const databasesTime = performance.now();
         logger.info("Databases read completed", {
            packageName,
            databaseCount: databases.length,
            duration: formatDuration(databasesTime - packageConfigTime),
         });
         const malloyConfig = Package.buildPackageMalloyConfig(
            packagePath,
            Package.toMalloyConfig(projectMalloyConfig),
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
            projectName,
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

   public async getMalloyConnection(connectionName: string): Promise<Connection> {
      return this.malloyConfig.connections.lookupConnection(connectionName);
   }

   public getMalloyConfig(): MalloyConfig {
      return this.malloyConfig;
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
      projectMalloyConfig: MalloyConfig,
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
            return projectMalloyConfig.connections.lookupConnection(name);
         },
      }));

      return malloyConfig;
   }

   private static toMalloyConfig(input: PackageConnectionInput): MalloyConfig {
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
      let files = undefined;
      files = await recursive(packagePath);
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

      // Create a DuckDB source then:
      // 1. Load the model and get the table schema from model
      // 2. Run a query to get the row count from the table
      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [new DuckDBConnection("duckdb")],
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

   public setProjectName(projectName: string) {
      this.projectName = projectName;
   }

   public setPackageMetadata(packageMetadata: ApiPackage) {
      this.packageMetadata = packageMetadata;
   }
}

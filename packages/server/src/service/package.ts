import * as fs from "fs/promises";
import * as path from "path";
import { components } from "../api";
import recursive from "recursive-readdir";
import { PackageNotFoundError } from "../errors";
import { Model } from "./model";
import {
   PACKAGE_MANIFEST_NAME,
   CONNECTIONS_MANIFEST_NAME,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   getWorkingDirectory,
} from "../utils";
import { Scheduler } from "./scheduler";

type ApiConnection = components["schemas"]["Connection"];
type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
export type ApiPackage = components["schemas"]["Package"];
type ApiSchedule = components["schemas"]["Schedule"];

export class Package {
   private packageName: string;
   private packageMetadata: ApiPackage;
   private databases: ApiDatabase[];
   private models: Map<string, Model> = new Map();
   private scheduler: Scheduler | undefined;

   constructor(
      packageName: string,
      packageMetadata: ApiPackage,
      databases: ApiDatabase[],
      models: Map<string, Model>,
      scheduler: Scheduler | undefined,
   ) {
      this.packageName = packageName;
      this.packageMetadata = packageMetadata;
      this.databases = databases;
      this.models = models;
      this.scheduler = scheduler;
   }

   static async create(packageName: string): Promise<Package> {
      // If package manifest does not exist, we throw a not found error.  If the package
      // manifest exists, we create a Package object and record errors in the object's fields.
      await Package.validatePackageManifestExistsOrThrowError(packageName);

      try {
         const packageConfig = await Package.readPackageConfig(packageName);
         const connectionConfig =
            await Package.readConnectionConfig(packageName);
         const databases = await Package.readDatabases(packageName);
         const models = await Package.loadModels(packageName, connectionConfig);
         const scheduler = Scheduler.create(models);
         return new Package(
            packageName,
            packageConfig,
            databases,
            models,
            scheduler,
         );
      } catch (error) {
         console.error(error);
         return new Package(
            packageName,
            {
               name: packageName,
               description:
                  "Unable to load package: " + (error as Error).message,
            },
            new Array<ApiDatabase>(),
            new Map<string, Model>(),
            undefined,
         );
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

   public listSchedules(): ApiSchedule[] {
      return this.scheduler ? this.scheduler.list() : [];
   }

   public getModel(modelPath: string): Model | undefined {
      return this.models.get(modelPath);
   }

   public listModels(): ApiModel[] {
      return Array.from(this.models.keys()).map((modelPath) => {
         return {
            path: modelPath,
            type: modelPath.endsWith(MODEL_FILE_SUFFIX) ? "source" : "notebook",
         } as ApiModel;
      });
   }

   private static async loadModels(
      packageName: string,
      connectionConfig: ApiConnection[] | undefined,
   ): Promise<Map<string, Model>> {
      const modelPaths = await Package.getModelPaths(packageName);
      const models = await Promise.all(
         modelPaths.map((modelPath) =>
            Model.create(packageName, modelPath, connectionConfig),
         ),
      );
      return new Map(models.map((model) => [model.getPath(), model]));
   }

   private static async getModelPaths(packageName: string): Promise<string[]> {
      const workingDirectory = getWorkingDirectory();
      const packagePath = path.join(workingDirectory, packageName);
      let files = undefined;
      try {
         files = await recursive(packagePath);
      } catch (error) {
         console.log(error);
         throw new PackageNotFoundError(
            `Package config for ${packageName} does not exist.`,
         );
      }
      return files
         .map((fullPath: string) => {
            return fullPath.replace(packagePath + "/", "");
         })
         .filter(
            (modelPath: string) =>
               modelPath.endsWith(MODEL_FILE_SUFFIX) ||
               modelPath.endsWith(NOTEBOOK_FILE_SUFFIX),
         );
   }

   private static async validatePackageManifestExistsOrThrowError(
      packageName: string,
   ) {
      const workingDirectory = getWorkingDirectory();
      const packageConfigPath = path.join(
         workingDirectory,
         packageName,
         PACKAGE_MANIFEST_NAME,
      );
      try {
         await fs.stat(packageConfigPath);
      } catch {
         throw new PackageNotFoundError(
            `Package manifest for ${packageName} does not exist.`,
         );
      }
   }

   private static async readPackageConfig(
      packageName: string,
   ): Promise<ApiPackage> {
      const workingDirectory = getWorkingDirectory();
      const packageConfigPath = path.join(
         workingDirectory,
         packageName,
         PACKAGE_MANIFEST_NAME,
      );
      const packageConfigContents = await fs.readFile(packageConfigPath);
      // TODO: Validate package manifest.  Define manifest type in public API.
      const packageManifest = JSON.parse(packageConfigContents.toString());
      return { name: packageName, description: packageManifest.description };
   }

   public static async readConnectionConfig(
      packageName: string,
   ): Promise<ApiConnection[]> {
      const workingDirectory = getWorkingDirectory();
      const fullPath = path.join(
         workingDirectory,
         packageName,
         CONNECTIONS_MANIFEST_NAME,
      );

      try {
         await fs.stat(fullPath);
      } catch {
         // If there's no connection manifest, it's no problem.  Just return an
         // empty array.
         return new Array<ApiConnection>();
      }

      const connectionFileContents = await fs.readFile(fullPath);
      // TODO: Validate connection manifest.  Define manifest type in public API.
      return JSON.parse(connectionFileContents.toString()) as ApiConnection[];
   }

   private static async readDatabases(
      packageName: string,
   ): Promise<ApiDatabase[]> {
      return await Promise.all(
         (await Package.getDatabasePaths(packageName)).map(
            async (databasePath) => {
               const databaseSize: number = await Package.getDatabaseSize(
                  packageName,
                  databasePath,
               );
               return {
                  path: databasePath,
                  size: databaseSize,
                  type: "embedded",
               } as ApiDatabase;
            },
         ),
      );
   }

   private static async getDatabasePaths(
      packageName: string,
   ): Promise<string[]> {
      const workingDirectory = getWorkingDirectory();
      const packagePath = path.join(workingDirectory, packageName);
      let files = undefined;
      files = await recursive(packagePath);
      return files
         .map((fullPath: string) => {
            return fullPath.replace(packagePath + "/", "");
         })
         .filter((modelPath: string) => modelPath.endsWith(".parquet"));
   }

   private static async getDatabaseSize(
      packageName: string,
      databasePath: string,
   ): Promise<number> {
      const workingDirectory = getWorkingDirectory();
      const fullPath = path.join(workingDirectory, packageName, databasePath);
      return (await fs.stat(fullPath)).size;
   }
}

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { Stats } from "fs";
import fs from "fs/promises";
import { join } from "path";
import sinon from "sinon";
import { PackageNotFoundError } from "../errors";
import { readConnectionConfig } from "./connection";
import { Model } from "./model";
import { Package } from "./package";

// Minimal partial types for mocking
type PartialModel = Pick<Model, "getPath">;

describe("service/package", () => {
   const testPackageDirectory = "testPackage";

   beforeEach(async () => {
      await fs.mkdir(testPackageDirectory, { recursive: true });
      await fs.writeFile(join(testPackageDirectory, "model1.model"), "");
      // Create a simple parquet file with schema "name: string, value: int"
      const parquetBuffer = Buffer.from("Name,Value\nJohn,10\nJane,20\nJim,30");

      await fs.writeFile(
         join(testPackageDirectory, "database.csv"),
         parquetBuffer,
      );
      const publisherContent = JSON.stringify({ description: "Test package" });
      await fs.writeFile(
         join(testPackageDirectory, "publisher.json"),
         publisherContent,
      );
   });

   afterEach(async () => {
      sinon.restore();
      await fs.rm(testPackageDirectory, { recursive: true });
   });

   it("should create a package instance", async () => {
      // Using 'as any' for simplified mock Map value in test
      const pkg = new Package(
         "testProject",
         "testPackage",
         testPackageDirectory,
         { name: "testPackage", description: "Test package" },
         [],
         new Map([
            [
               "model1.malloy",
               { getPath: () => "model1.malloy" } as unknown as Model,
            ],
            [
               "model2.malloynb",
               { getPath: () => "model2.malloynb" } as unknown as Model,
            ],
         ]),
      );

      expect(pkg).toBeInstanceOf(Package);
   });

   describe("instance methods", () => {
      describe("create", () => {
         it("should throw PackageNotFoundError if the package manifest does not exist", async () => {
            await fs.rm(join(testPackageDirectory, "publisher.json"));
            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            await expect(
               Package.create(
                  "testProject",
                  "testPackage",
                  testPackageDirectory,
                  new Map(),
                  "/server/root",
               ),
            ).rejects.toThrowError(
               new PackageNotFoundError(
                  "Package manifest for testPackage does not exist.",
               ),
            );
         });
         it(
            "should return a Package object if the package exists",
            async () => {
               sinon.stub(fs, "stat").resolves();
               const readFileStub = sinon
                  .stub(fs, "readFile")
                  .resolves(
                     Buffer.from(
                        JSON.stringify({ description: "Test package" }),
                     ),
                  );

               // Still use Partial<Model> for the stub resolution type
               sinon
                  .stub(Model, "create")
                  // @ts-expect-error PartialModel is a partial type
                  .resolves({ getPath: () => "model1.model" } as PartialModel);

               readFileStub.restore();
               readFileStub.resolves(Buffer.from(JSON.stringify([])));

               const packageInstance = await Package.create(
                  "testProject",
                  "testPackage",
                  testPackageDirectory,
                  new Map(),
                  "/server/root",
               );

               expect(packageInstance).toBeInstanceOf(Package);
               expect(packageInstance.getPackageName()).toBe("testPackage");
               expect(packageInstance.getPackageMetadata().description).toBe(
                  "Test package",
               );
               expect(packageInstance.listDatabases()).toEqual([
                  {
                     path: "database.csv",
                     type: "embedded",
                     info: {
                        name: "database.csv",
                        columns: [
                           { name: "Name", type: "string" },
                           { name: "Value", type: "number" },
                        ],
                        rowCount: 3,
                     },
                  },
               ]);
               expect(packageInstance.listModels()).toBeEmpty();
            },
            { timeout: 15000 },
         );
      });

      describe("listModels", () => {
         it("should return a list of models with their paths and types", async () => {
            // Using 'as any' for simplified mock Map value in test
            const packageInstance = new Package(
               "testProject",
               "testPackage",
               testPackageDirectory,
               { name: "testPackage", description: "Test package" },
               [],
               new Map([
                  [
                     "model1.malloy",
                     {
                        getPath: () => "model1.malloy",
                        getModel: () => "foo",
                     } as unknown as Model,
                  ],
                  [
                     "model2.malloynb",
                     {
                        getPath: () => "model2.malloynb",
                        getNotebookError: () => {
                           return {
                              message: "This is the error",
                           };
                        },
                     } as unknown as Model,
                  ],
               ]),
            );

            const models = await packageInstance.listModels();
            expect(models).toEqual([
               {
                  // @ts-expect-error TODO: Fix missing projectName type in API
                  projectName: "testProject",
                  packageName: "testPackage",
                  path: "model1.malloy",
                  error: undefined,
               },
            ]);

            const notebooks = await packageInstance.listNotebooks();
            expect(notebooks).toEqual([
               {
                  // @ts-expect-error TODO: Fix missing projectName type in API
                  projectName: "testProject",
                  packageName: "testPackage",
                  path: "model2.malloynb",
                  error: "This is the error",
               },
            ]);
         });
      });

      describe("getDatabaseInfo", () => {
         it("should return the size of the database file", async () => {
            sinon.stub(fs, "stat").resolves({ size: 13 } as Stats);

            // @ts-expect-error Accessing private static method for testing
            const info = await Package.getDatabaseInfo(
               testPackageDirectory,
               "database.csv",
            );

            expect(info).toEqual({
               name: "database.csv",
               columns: [
                  { name: "Name", type: "string" },
                  { name: "Value", type: "number" },
               ],
               rowCount: 3,
            });
         });
      });

      describe("readConnectionConfig", () => {
         it("should return an empty array if no project name or server root path is provided", async () => {
            const config = await readConnectionConfig(testPackageDirectory);
            expect(Array.isArray(config)).toBe(true);
            expect(config).toHaveLength(0);
         });

         it("should return an empty array if project name or server root path is missing", async () => {
            const config1 = await readConnectionConfig(
               testPackageDirectory,
               "testProject",
            );
            expect(Array.isArray(config1)).toBe(true);
            expect(config1).toHaveLength(0);

            const config2 = await readConnectionConfig(
               testPackageDirectory,
               undefined,
               "/server/root",
            );
            expect(Array.isArray(config2)).toBe(true);
            expect(config2).toHaveLength(0);
         });

         it("should return connections from publisher config when project name and server root are provided", async () => {
            const tempServerRoot = "/tmp/test-server-root";
            const tempConfigPath = join(
               tempServerRoot,
               "publisher.config.json",
            );

            // Create temp directory and config file
            await fs.mkdir(tempServerRoot, { recursive: true });
            await fs.writeFile(
               tempConfigPath,
               JSON.stringify({
                  frozenConfig: false,
                  projects: [
                     {
                        name: "testProject",
                        packages: [],
                        connections: [
                           { name: "test-conn", type: "postgres" },
                           { name: "test-conn2", type: "bigquery" },
                        ],
                     },
                  ],
               }),
            );

            const config = await readConnectionConfig(
               testPackageDirectory,
               "testProject",
               tempServerRoot,
            );

            expect(Array.isArray(config)).toBe(true);
            expect(config).toHaveLength(2);
            expect(config[0]).toMatchObject({
               name: "test-conn",
               type: "postgres",
               resource: "/api/v0/connections/test-conn",
            });
            expect(config[1]).toMatchObject({
               name: "test-conn2",
               type: "bigquery",
               resource: "/api/v0/connections/test-conn2",
            });

            // Clean up
            await fs.rm(tempServerRoot, { recursive: true, force: true });
         });
      });
   });
});

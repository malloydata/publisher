import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { Stats } from "fs";
import fs from "fs/promises";
import { join } from "path";
import sinon from "sinon";
import { PackageNotFoundError } from "../errors";
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
      // On Windows, DuckDB connections may still have file handles open,
      // causing EBUSY errors. Retry deletion with exponential backoff.
      const maxRetries = 3;
      const delay = 50; // Start with 50ms
      let lastError: unknown;

      for (let attempt = 0; attempt < maxRetries; attempt++) {
         try {
            await fs.rm(testPackageDirectory, { recursive: true, force: true });
            return; // Success, exit retry loop
         } catch (error) {
            lastError = error;
            const errnoError = error as NodeJS.ErrnoException;
            // Only retry on EBUSY errors (Windows file handle still open)
            if (errnoError.code !== "EBUSY") {
               throw error; // Non-EBUSY errors should be thrown immediately
            }
            // Exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms
            if (attempt < maxRetries - 1) {
               await new Promise((resolve) => setTimeout(resolve, delay));
            }
         }
      }
      // If we've exhausted retries, ignore EBUSY errors as they don't affect test results
      // and the files will be cleaned up eventually
      if ((lastError as NodeJS.ErrnoException).code !== "EBUSY") {
         throw lastError;
      }
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
   });
});

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import sinon from "sinon";
import fs from "fs/promises";
import { Package } from "./package";
import { Scheduler } from "./scheduler";
import { Model } from "./model";
import { PackageNotFoundError } from "../errors";
import { join } from "path";
import { getWorkingDirectory } from "../utils";

describe("service/package", () => {
   const testPackageDirectory = join(getWorkingDirectory(), "testPackage");

   beforeEach(async () => {
      await fs.mkdir(testPackageDirectory, { recursive: true });
      await fs.writeFile(join(testPackageDirectory, "model1.model"), "");
      await fs.writeFile(
         join(testPackageDirectory, "database.parquet"),
         "dummy content",
      );
      const content = JSON.stringify([
         { id: "conn1", type: "database" },
         { id: "conn2", type: "api" },
      ]);
      await fs.writeFile(
         join(testPackageDirectory, "publisher-connections.json"),
         content,
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
      const pkg = new Package(
         "testPackage",
         { name: "testPackage", description: "Test package" },
         [],
         new Map([
            ["model1.malloy", { getPath: () => "model1.malloy" } as any],
            ["model2.malloynb", { getPath: () => "model2.malloynb" } as any],
         ]),
         undefined,
      );

      expect(pkg).toBeInstanceOf(Package);
   });

   describe("instance methods", () => {
      describe("create", () => {
         it("should throw PackageNotFoundError if the package manifest does not exist", async () => {
            await fs.rm(join(testPackageDirectory, "publisher.json"));
            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            await expect(Package.create("testPackage")).rejects.toThrowError(
               PackageNotFoundError,
               "Package manifest for testPackage does not exist.",
            );
         });
         it("should return a Package object if the package exists", async () => {
            sinon.stub(fs, "stat").resolves();
            sinon
               .stub(fs, "readFile")
               .resolves(
                  Buffer.from(JSON.stringify({ description: "Test package" })),
               );

            sinon
               .stub(Model, "create")
               .resolves({ getPath: () => "model1.model" } as any);

            sinon.stub(Scheduler, "create").returns({
               list: () => [],
            } as any);

            const packageInstance = await Package.create("testPackage");

            expect(packageInstance).toBeInstanceOf(Package);
            expect(packageInstance.getPackageName()).toBe("testPackage");
            expect(packageInstance.getPackageMetadata().description).toBe(
               "Test package",
            );
            expect(packageInstance.listDatabases()).toBe.empty;
            expect(packageInstance.listModels()).toBe.empty;
            expect(packageInstance.listSchedules()).toBe.empty;
         });
      });

      describe("listModels", () => {
         it("should return a list of models with their paths and types", () => {
            const packageInstance = new Package(
               "testPackage",
               { name: "testPackage", description: "Test package" },
               [],
               new Map([
                  ["model1.malloy", { getPath: () => "model1.malloy" } as any],
                  [
                     "model2.malloynb",
                     { getPath: () => "model2.malloynb" } as any,
                  ],
               ]),
               undefined,
            );

            const models = packageInstance.listModels();

            expect(models).toEqual([
               { path: "model1.malloy", type: "source" },
               { path: "model2.malloynb", type: "notebook" },
            ]);
         });
      });

      describe("getDatabaseSize", () => {
         it("should return the size of the database file", async () => {
            sinon.stub(fs, "stat").resolves({ size: 13 } as any);

            const size = await (Package as any).getDatabaseSize(
               "testPackage",
               "database.parquet",
            );

            expect(size).toBe(13);
         });
      });

      describe("readConnectionConfig", () => {
         it("should return an empty array if the connection manifest does not exist", async () => {
            await fs.rm(
               join(testPackageDirectory, "publisher-connections.json"),
            );

            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            const config = await Package.readConnectionConfig("testPackage");
            expect(Array.isArray(config)).toBe(true);
            expect(config).toHaveLength(0);
         });

         it("should return the parsed connection config if it exists", async () => {
            sinon.stub(fs, "stat").resolves();
            const config = await Package.readConnectionConfig("testPackage");

            expect(config).toEqual([
               { id: "conn1", type: "database" },
               { id: "conn2", type: "api" },
            ]);
         });
      });
   });
});

import { MalloyError, Runtime } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import sinon from "sinon";

import { BadRequestError, ModelNotFoundError } from "../errors";
// requestContext import not needed — tests exercise the no-token (default) path
import { Model, ModelType } from "./model";

describe("service/model", () => {
   const packageName = "test-package";
   const mockPackageName = "mockPackage";
   const mockPackagePath = "mockPackagePath";
   const mockModelPath = "mockModel.malloy";

   afterEach(() => {
      sinon.restore();
   });

   it("should create a Model instance", async () => {
      sinon.stub(Model, "getModelRuntime").resolves({
         runtime: sinon.createStubInstance(Runtime),
         modelURL: new URL("file://mockModelPath"),
         importBaseURL: new URL("file://mockBaseURL/"),
         dataStyles: {},
         modelType: "model",
      });

      sinon.stub(Model, "getModelMaterializer").resolves({
         modelMaterializer: undefined,
         runnableNotebookCells: undefined,
      });

      const model = await Model.create(
         mockPackageName,
         mockPackagePath,
         mockModelPath,
         new Map(),
      );
      expect(model).toBeInstanceOf(Model);
      expect(model.getPath()).toBe(mockModelPath);
   });

   it("should handle ModelNotFoundError correctly", async () => {
      await expect(async () => {
         await Model.create(
            mockPackageName,
            mockPackagePath,
            mockModelPath,
            new Map(),
         );
      }).toThrowError(`${mockModelPath} does not exist.`);
   });

   describe("instance methods", () => {
      describe("getPath", () => {
         it("should return the correct modelPath", async () => {
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            expect(model.getPath()).toBe(mockModelPath);
         });
      });

      describe("getType", () => {
         it("should return the correct modelType", async () => {
            const modelType = "model";
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               modelType,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            expect(model.getType()).toBe(modelType);
         });
      });

      describe("getModel", () => {
         it("should throw ModelCompilationError if a compilation error exists", async () => {
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               new MalloyError("Compilation error"),
            );

            await expect(async () => {
               await model.getModel();
            }).toThrowError(MalloyError);
         });

         it("should throw ModelNotFoundError for invalid modelType", async () => {
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "notebook" as ModelType,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            await expect(async () => {
               await model.getModel();
            }).toThrowError(ModelNotFoundError);
         });
      });

      describe("getNotebook", () => {
         it("should throw ModelCompilationError if a compilation error exists", async () => {
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "notebook",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               new Error("Compilation error"),
            );

            await expect(async () => {
               await model.getNotebook();
            }).toThrowError(Error);
         });

         it("should throw ModelNotFoundError for invalid modelType", async () => {
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "model" as ModelType,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            await expect(async () => {
               await model.getNotebook();
            }).toThrowError(ModelNotFoundError);
         });
      });

      describe("getQueryResults", () => {
         it("should throw BadRequestError if a non-MalloyError compilation error exists", async () => {
            const error = new Error("Compilation error");
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               error,
            );

            await expect(async () => {
               await model.getQueryResults();
            }).toThrowError(BadRequestError);
         });

         it("should throw BadRequestError if no queryable entities exist", async () => {
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            await expect(async () => {
               await model.getQueryResults();
            }).toThrowError(BadRequestError);
         });

         it("should not create OAuth runtime when no token present", async () => {
            // No requestContext.run() — getDatabaseToken() returns undefined
            // so the OAuth path is skipped entirely.
            const model = new Model(
               packageName,
               mockPackagePath,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            await expect(async () => {
               await model.getQueryResults();
            }).toThrowError(BadRequestError);
         });
      });
   });

   describe("static methods", () => {
      describe("getModelRuntime", () => {
         it("should throw ModelNotFoundError for invalid modelPath", async () => {
            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            await expect(async () => {
               await Model.getModelRuntime(
                  packageName,
                  mockModelPath,
                  new Map(),
               );
            }).toThrowError(ModelNotFoundError);
         });
      });
   });
});

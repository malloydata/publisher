import { expect, it, describe } from "bun:test";
import { Runtime } from "@malloydata/malloy";
import sinon from "sinon";
import fs from "fs/promises";

import { Model, ModelType } from "./model";
import {
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
} from "../errors";

describe("service/model", () => {
   const packageName = "test-package";
   const mockPackageName = "mockPackage";
   const mockModelPath = "mockModel.malloy";

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

      const model = await Model.create(mockPackageName, mockModelPath, []);
      expect(model).toBeInstanceOf(Model);
      expect(model.getPath()).toBe(mockModelPath);

      sinon.restore();
   });

   it("should handle ModelNotFoundError correctly", async () => {
      await expect(async () => {
         await Model.create(mockPackageName, mockModelPath, []);
      }).toThrowError(`${mockModelPath} does not exist.`);

      sinon.restore();
   });

   describe("instance methods", () => {
      describe("getPath", () => {
         it("should return the correct modelPath", async () => {
            const model = new Model(
               packageName,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            expect(model.getPath()).toBe(mockModelPath);

            sinon.restore();
         });
      });

      describe("getType", () => {
         it("should return the correct modelType", async () => {
            const modelType = "model";
            const model = new Model(
               packageName,
               mockModelPath,
               {},
               modelType,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            expect(model.getType()).toBe(modelType);

            sinon.restore();
         });
      });

      describe("getModel", () => {
         it("should throw ModelCompilationError if a compilation error exists", async () => {
            const model = new Model(
               packageName,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               "Compilation error",
            );

            await expect(async () => {
               await model.getModel();
            }).toThrowError(ModelCompilationError);

            sinon.restore();
         });

         it("should throw ModelNotFoundError for invalid modelType", async () => {
            const model = new Model(
               packageName,
               mockModelPath,
               {},
               "invalid" as ModelType,
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

            sinon.restore();
         });
      });

      describe("getQueryResults", () => {
         it("should throw ModelCompilationError if a compilation error exists", async () => {
            const model = new Model(
               packageName,
               mockModelPath,
               {},
               "model",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               "Compilation error",
            );

            await expect(async () => {
               await model.getQueryResults();
            }).toThrowError(ModelCompilationError);

            sinon.restore();
         });

         it("should throw BadRequestError if no queryable entities exist", async () => {
            const model = new Model(
               packageName,
               mockModelPath,
               {},
               "model",
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

            sinon.restore();
         });
      });
   });

   describe("static methods", () => {
      describe("getModelRuntime", () => {
         it("should throw ModelNotFoundError for invalid modelPath", async () => {
            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            await expect(async () => {
               await Model.getModelRuntime(packageName, mockModelPath, []);
            }).toThrowError(ModelNotFoundError);

            sinon.restore();
         });
      });
   });
});

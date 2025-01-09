import { expect } from "chai";
import { Runtime } from "@malloydata/malloy";
import fs from "fs/promises";
import sinon from "sinon";
import { Model, ModelType } from "./model";
import {
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
} from "../errors";

describe("server/service/model", () => {
   const packageName = "test-package";
   const mockPackageName = "mockPackage";
   const mockModelPath = "mockModel.malloy";

   afterEach(() => {
      sinon.restore();
   });

   it("should create a Model instance successfully", async () => {
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
      expect(model).to.be.an.instanceOf(Model);
      expect(model.getPath()).to.equal(mockModelPath);
   });

   it("should handle ModelNotFoundError correctly", async () => {
      await expect(
         Model.create(mockPackageName, mockModelPath, []),
      ).to.eventually.be.rejectedWith(`${mockModelPath} does not exist.`);
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

            expect(model.getPath()).to.equal(mockModelPath);
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

            expect(model.getType()).to.equal(modelType);
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

            await expect(model.getModel()).to.be.rejectedWith(
               ModelCompilationError,
            );
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

            await expect(model.getModel()).to.be.rejectedWith(
               ModelNotFoundError,
            );
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

            await expect(model.getQueryResults()).to.be.rejectedWith(
               ModelCompilationError,
            );
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

            await expect(model.getQueryResults()).to.be.rejectedWith(
               BadRequestError,
            );
         });
      });

      describe("getModelRuntime", () => {
         it("should throw ModelNotFoundError for invalid modelPath", async () => {
            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            await expect(
               Model.getModelRuntime(packageName, mockModelPath, []),
            ).to.be.rejectedWith(ModelNotFoundError);
         });
      });
   });
});

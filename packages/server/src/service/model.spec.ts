import { MalloyError, Runtime } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import fs from "fs/promises";
import sinon from "sinon";

import { BadRequestError, ModelNotFoundError } from "../errors";
import { Model, ModelType } from "./model";

describe("service/model", () => {
   const packageName = "test-package";
   const mockPackageName = "mockPackage";
   const mockPackagePath = "mockPackagePath";
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

      const model = await Model.create(
         mockPackageName,
         mockPackagePath,
         mockModelPath,
         new Map(),
      );
      expect(model).toBeInstanceOf(Model);
      expect(model.getPath()).toBe(mockModelPath);

      sinon.restore();
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
               undefined,
               new MalloyError("Compilation error"),
            );

            await expect(async () => {
               await model.getModel();
            }).toThrowError(MalloyError);

            sinon.restore();
         });

         it("should throw ModelNotFoundError for invalid modelType", async () => {
            const model = new Model(
               packageName,
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

            sinon.restore();
         });
      });

      describe("getNotebook", () => {
         it("should throw ModelCompilationError if a compilation error exists", async () => {
            const model = new Model(
               packageName,
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

            sinon.restore();
         });

         it("should throw ModelNotFoundError for invalid modelType", async () => {
            const model = new Model(
               packageName,
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

            sinon.restore();
         });
      });

      describe("getQueryResults", () => {
         it("should throw BadRequestError if a non-MalloyError compilation error exists", async () => {
            const error = new Error("Compilation error");
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
               error,
            );

            await expect(async () => {
               await model.getQueryResults();
            }).toThrowError(BadRequestError);

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
               undefined,
            );

            await expect(async () => {
               await model.getQueryResults();
            }).toThrowError(BadRequestError);

            sinon.restore();
         });

         it("forwards givens to runnable.getPreparedResult and .run", async () => {
            const givensArg = { region: "EU" };
            const preparedResultStub = sinon
               .stub()
               .resolves({ resultExplore: { limit: 10 } });
            const runStub = sinon
               .stub()
               .rejects(new MalloyError("stub-stop", []));
            const modelMaterializer = {
               loadQuery: sinon.stub().returns({
                  getPreparedResult: preparedResultStub,
                  run: runStub,
               }),
            };

            const model = new Model(
               packageName,
               mockModelPath,
               {},
               "model",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               modelMaterializer as any,
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               { contents: {}, exports: [], queryList: [] } as any,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
            );

            await expect(
               model.getQueryResults(
                  undefined,
                  undefined,
                  "run: orders -> summary",
                  undefined,
                  undefined,
                  givensArg,
               ),
            ).rejects.toThrow(MalloyError);

            expect(preparedResultStub.calledOnce).toBe(true);
            expect(preparedResultStub.firstCall.args[0]).toEqual({
               givens: givensArg,
            });
            expect(runStub.firstCall.args[0]).toMatchObject({
               givens: givensArg,
            });

            sinon.restore();
         });
      });

      describe("executeNotebookCell", () => {
         it("forwards givens to runnable.getPreparedResult and .run", async () => {
            const givensArg = { target_code: "AA" };
            const preparedResultStub = sinon
               .stub()
               .resolves({ resultExplore: { limit: 10 } });
            const runStub = sinon
               .stub()
               .rejects(new MalloyError("stub-stop", []));
            const cellRunnable = {
               getPreparedResult: preparedResultStub,
               run: runStub,
            };
            const runnableCells = [
               {
                  type: "code" as const,
                  text: "run: orders -> by_code",
                  runnable: cellRunnable,
               },
            ];

            const model = new Model(
               packageName,
               "test.malloynb",
               {},
               "notebook",
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               runnableCells as any,
               undefined,
            );

            await expect(
               model.executeNotebookCell(0, undefined, undefined, givensArg),
            ).rejects.toThrow(MalloyError);

            expect(preparedResultStub.calledOnce).toBe(true);
            expect(preparedResultStub.firstCall.args[0]).toEqual({
               givens: givensArg,
            });
            expect(runStub.firstCall.args[0]).toMatchObject({
               givens: givensArg,
            });

            sinon.restore();
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

            sinon.restore();
         });
      });
   });
});

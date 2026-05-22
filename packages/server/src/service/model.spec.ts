import { API, MalloyError, Runtime } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import sinon from "sinon";

import {
   BadRequestError,
   ModelNotFoundError,
   PayloadTooLargeError,
} from "../errors";
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

         /**
          * The row/byte caps live in `model_limits.ts` (unit-tested in
          * `model_limits.spec.ts`); these tests just confirm the wiring —
          * that `Model.getQueryResults` calls the helpers with the right
          * values and that an overflow propagates as `PayloadTooLargeError`
          * (HTTP 413), not the generic `BadRequestError` (HTTP 400).
          */
         describe("response caps", () => {
            const originalRowsEnv = process.env.PUBLISHER_MAX_QUERY_ROWS;
            const originalBytesEnv = process.env.PUBLISHER_MAX_RESPONSE_BYTES;
            const originalDefaultEnv =
               process.env.PUBLISHER_DEFAULT_QUERY_ROW_LIMIT;

            afterEach(() => {
               sinon.restore();
               for (const [name, original] of [
                  ["PUBLISHER_MAX_QUERY_ROWS", originalRowsEnv],
                  ["PUBLISHER_MAX_RESPONSE_BYTES", originalBytesEnv],
                  ["PUBLISHER_DEFAULT_QUERY_ROW_LIMIT", originalDefaultEnv],
               ] as const) {
                  if (original === undefined) {
                     delete process.env[name];
                  } else {
                     process.env[name] = original;
                  }
               }
            });

            /**
             * Build a Model whose `runnable.run` resolves to a fake Result
             * with the given totalRows; stub `API.util.wrapResult` so we
             * don't need to construct a real Malloy schema/queryResult.
             */
            function buildModelWithFakeRun(opts: {
               userLimit?: number;
               totalRows: number;
               wrappedJson: object;
            }): { model: Model; runStub: sinon.SinonStub } {
               const preparedResultStub = sinon
                  .stub()
                  .resolves({ resultExplore: { limit: opts.userLimit ?? 0 } });
               const fakeResult = {
                  _queryResult: { data: { rawData: [] } },
                  totalRows: opts.totalRows,
                  data: { value: [] },
                  connectionName: "fake",
               };
               const runStub = sinon.stub().resolves(fakeResult);
               sinon
                  .stub(API.util, "wrapResult")
                  .returns(
                     opts.wrappedJson as unknown as ReturnType<
                        typeof API.util.wrapResult
                     >,
                  );
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
               return { model, runStub };
            }

            it("clamps user LIMIT to maxRows + 1 when the user requested more than the cap", async () => {
               process.env.PUBLISHER_MAX_QUERY_ROWS = "100";
               const { model, runStub } = buildModelWithFakeRun({
                  userLimit: 1_000_000,
                  totalRows: 10,
                  wrappedJson: { rows: [] },
               });

               await model.getQueryResults(
                  undefined,
                  undefined,
                  "run: orders -> summary",
               );

               expect(runStub.firstCall.args[0].rowLimit).toBe(101);
            });

            it("passes user LIMIT through when below maxRows", async () => {
               process.env.PUBLISHER_MAX_QUERY_ROWS = "100";
               const { model, runStub } = buildModelWithFakeRun({
                  userLimit: 50,
                  totalRows: 10,
                  wrappedJson: { rows: [] },
               });

               await model.getQueryResults(
                  undefined,
                  undefined,
                  "run: orders -> summary",
               );

               expect(runStub.firstCall.args[0].rowLimit).toBe(50);
            });

            it("falls back to PUBLISHER_DEFAULT_QUERY_ROW_LIMIT when the user query has no LIMIT", async () => {
               process.env.PUBLISHER_DEFAULT_QUERY_ROW_LIMIT = "42";
               delete process.env.PUBLISHER_MAX_QUERY_ROWS;
               const { model, runStub } = buildModelWithFakeRun({
                  userLimit: 0,
                  totalRows: 10,
                  wrappedJson: { rows: [] },
               });

               await model.getQueryResults(
                  undefined,
                  undefined,
                  "run: orders -> summary",
               );

               expect(runStub.firstCall.args[0].rowLimit).toBe(42);
            });

            it("throws PayloadTooLargeError (not BadRequestError) when totalRows exceeds the cap", async () => {
               process.env.PUBLISHER_MAX_QUERY_ROWS = "100";
               const { model } = buildModelWithFakeRun({
                  userLimit: 1000,
                  totalRows: 101,
                  wrappedJson: { rows: [] },
               });

               await expect(
                  model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                  ),
               ).rejects.toBeInstanceOf(PayloadTooLargeError);
            });

            it("throws PayloadTooLargeError when the wrapped response exceeds the byte cap", async () => {
               process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "100";
               const huge = "x".repeat(500);
               const { model } = buildModelWithFakeRun({
                  userLimit: 10,
                  totalRows: 10,
                  wrappedJson: { rows: [{ s: huge }] },
               });

               await expect(
                  model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                  ),
               ).rejects.toBeInstanceOf(PayloadTooLargeError);
            });

            it("does not throw when both counts are within their caps", async () => {
               process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "10000";
               const { model } = buildModelWithFakeRun({
                  userLimit: 10,
                  totalRows: 10,
                  wrappedJson: { rows: [{ a: 1 }] },
               });

               await expect(
                  model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                  ),
               ).resolves.toBeDefined();
            });
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

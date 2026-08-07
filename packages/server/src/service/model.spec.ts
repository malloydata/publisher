import { API, MalloyError, Runtime } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import sinon from "sinon";

import {
   BadRequestError,
   ModelNotFoundError,
   PayloadTooLargeError,
   ResponseUnserializableError,
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

         it("embeds model-level givens in each newSources SourceInfo", async () => {
            const sourceInfo = {
               name: "carriers",
               schema: { fields: [] },
            };
            const givens = [
               {
                  name: "region",
                  type: "string",
                  annotations: ["#(doc) Region"],
               },
            ];
            const model = new Model(
               packageName,
               "test.malloynb",
               {},
               "notebook",
               undefined, // modelMaterializer
               undefined, // modelDef
               undefined, // sources
               undefined, // queries
               undefined, // sourceInfos
               [
                  {
                     type: "code",
                     text: "import 'carriers.malloy'",
                     newSources: [sourceInfo],
                  },
               ], // runnableNotebookCells
               undefined, // compilationError
               undefined, // filterMap
               givens, // givens
            );

            const notebook = await model.getNotebook();
            expect(notebook.notebookCells).toHaveLength(1);
            const parsed = JSON.parse(
               notebook.notebookCells![0].newSources![0],
            );
            expect(parsed.name).toBe("carriers");
            // SourceInfo fields are preserved untouched.
            expect(parsed.schema).toEqual({ fields: [] });
            // Givens ride along verbatim — no second getModel round-trip needed.
            expect(parsed.givens).toEqual(givens);
         });

         it("omits givens from newSources when the model declares none", async () => {
            const sourceInfo = { name: "carriers", schema: { fields: [] } };
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
               [
                  {
                     type: "code",
                     text: "import 'carriers.malloy'",
                     newSources: [sourceInfo],
                  },
               ],
               undefined,
               undefined,
               undefined, // no givens
            );

            const notebook = await model.getNotebook();
            const parsed = JSON.parse(
               notebook.notebookCells![0].newSources![0],
            );
            expect(parsed.name).toBe("carriers");
            expect(parsed).not.toHaveProperty("givens");
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

         // Both caller-driven compile paths — the free-form `query` text and the
         // `run: source->view` string built from `sourceName`/`queryName` — must
         // go through restricted mode. The trusted `loadQuery` is reserved for
         // author-curated content (notebook cells) and must never be reached from
         // `getQueryResults`. These tests pin the dispatch so a regression that
         // re-routes either path back to `loadQuery` is caught.
         describe("compile dispatch", () => {
            function buildDispatchModel(): {
               model: Model;
               loadQuery: sinon.SinonStub;
               loadRestrictedQuery: sinon.SinonStub;
            } {
               // getPreparedResult rejects so execution stops right after the
               // loader call; we only assert which loader was invoked.
               const runnableStub = {
                  getPreparedResult: sinon
                     .stub()
                     .rejects(new MalloyError("stub-stop", [])),
                  run: sinon.stub().rejects(new MalloyError("stub-stop", [])),
               };
               const loadQuery = sinon.stub().returns(runnableStub);
               const loadRestrictedQuery = sinon.stub().returns(runnableStub);
               const modelMaterializer = { loadQuery, loadRestrictedQuery };
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
               return { model, loadQuery, loadRestrictedQuery };
            }

            afterEach(() => sinon.restore());

            it("compiles ad-hoc query text in restricted mode, never trusted loadQuery", async () => {
               const { model, loadQuery, loadRestrictedQuery } =
                  buildDispatchModel();

               await expect(
                  model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> { aggregate: c is count() }",
                  ),
               ).rejects.toThrow(MalloyError);

               expect(loadRestrictedQuery.calledOnce).toBe(true);
               expect(loadQuery.called).toBe(false);
            });

            it("compiles the named source/view path in restricted mode, never trusted loadQuery", async () => {
               const { model, loadQuery, loadRestrictedQuery } =
                  buildDispatchModel();

               await expect(
                  model.getQueryResults("orders", "summary"),
               ).rejects.toThrow(MalloyError);

               expect(loadRestrictedQuery.calledOnce).toBe(true);
               expect(loadQuery.called).toBe(false);
            });
         });

         it("forwards givens to runnable.getPreparedResult and .run", async () => {
            const givensArg = { region: "EU" };
            const preparedResultStub = sinon
               .stub()
               .resolves({ resultExplore: { limit: 10 } });
            const runStub = sinon
               .stub()
               .rejects(new MalloyError("stub-stop", []));
            const runnableStub = {
               getPreparedResult: preparedResultStub,
               run: runStub,
            };
            const modelMaterializer = {
               loadQuery: sinon.stub().returns(runnableStub),
               loadRestrictedQuery: sinon.stub().returns(runnableStub),
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
               undefined,
               // Model surfaces `region` so filterGivensToModelSurface (see
               // model.ts) forwards it rather than dropping it as unknown.
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               [{ name: "region", type: "string" }] as any,
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

         it("maps a finalized-given rejection (code) to BadRequestError, not 500", async () => {
            // Malloy throws this (extends Error, not MalloyError, not root-exported)
            // when a client supplies a given an operator finalized. model.ts
            // duck-types on `.code`; guard against that mapping regressing.
            const finalizedErr = Object.assign(
               new Error(
                  "Given 'region' is finalized and cannot be overridden",
               ),
               { code: "runtime-given-finalized" },
            );
            const runnableStub = {
               getPreparedResult: sinon.stub().rejects(finalizedErr),
               run: sinon.stub(),
            };
            const modelMaterializer = {
               loadQuery: sinon.stub().returns(runnableStub),
               loadRestrictedQuery: sinon.stub().returns(runnableStub),
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
                  { region: "EU" },
               ),
            ).rejects.toThrow(BadRequestError);

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
               /**
                * The compact rows, i.e. what a `compactJson` request sends. Left
                * empty by default; set it to make the two shapes differ in size,
                * which is the whole point of `responseShape`.
                */
               compactRows?: unknown[];
            }): { model: Model; runStub: sinon.SinonStub } {
               const preparedResultStub = sinon
                  .stub()
                  .resolves({ resultExplore: { limit: opts.userLimit ?? 0 } });
               const fakeResult = {
                  _queryResult: { data: { rawData: [] } },
                  totalRows: opts.totalRows,
                  data: { value: opts.compactRows ?? [] },
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
               const runnableStub = {
                  getPreparedResult: preparedResultStub,
                  run: runStub,
               };
               const modelMaterializer = {
                  loadQuery: sinon.stub().returns(runnableStub),
                  loadRestrictedQuery: sinon.stub().returns(runnableStub),
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

            it("caps the shape the caller declared, not always the wrapped one", async () => {
               // The regression this plumbing exists to remove: a `compactJson`
               // request was measured against the WRAPPED result, so it could be
               // refused on bytes it would never receive. Wrapped JSON is far over
               // the cap here; the compact rows are far under it.
               process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "100";
               const huge = "x".repeat(500);
               const build = () =>
                  buildModelWithFakeRun({
                     userLimit: 10,
                     totalRows: 10,
                     wrappedJson: { rows: [{ s: huge }] },
                     compactRows: [{ a: 1 }],
                  });

               const compact = await build().model.getQueryResults(
                  undefined,
                  undefined,
                  "run: orders -> summary",
                  undefined,
                  undefined,
                  undefined,
                  undefined,
                  undefined,
                  "compact",
               );
               expect(compact.serializedResult).toBe(
                  JSON.stringify([{ a: 1 }]),
               );

               sinon.restore();
               await expect(
                  build().model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                     undefined,
                     undefined,
                     undefined,
                     undefined,
                     undefined,
                     "full",
                  ),
               ).rejects.toBeInstanceOf(PayloadTooLargeError);
            });

            it("guards the compact payload too, not just the wrapped one", async () => {
               // The second half: a compact shape that cannot be serialized must
               // report the same 413, rather than escaping as a bare 500 from
               // whoever stringifies it next.
               process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "0";
               const unserializable = [
                  {
                     toJSON() {
                        throw new RangeError("Invalid string length");
                     },
                  },
               ];
               const { model } = buildModelWithFakeRun({
                  userLimit: 10,
                  totalRows: 10,
                  wrappedJson: { rows: [{ a: 1 }] },
                  compactRows: unserializable,
               });

               await expect(
                  model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                     undefined,
                     undefined,
                     undefined,
                     undefined,
                     undefined,
                     "compact",
                  ),
               ).rejects.toBeInstanceOf(ResponseUnserializableError);
            });

            it("reports a row overflow as rows, even when serializing would fail", async () => {
               // Ordering: rows are checked before the payload is built, so the
               // caller is told to raise PUBLISHER_MAX_QUERY_ROWS rather than that
               // the response could not be serialized.
               process.env.PUBLISHER_MAX_QUERY_ROWS = "10";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "0";
               const { model } = buildModelWithFakeRun({
                  userLimit: 100,
                  totalRows: 11,
                  wrappedJson: {
                     toJSON() {
                        throw new RangeError("Invalid string length");
                     },
                  },
               });

               const error = await model
                  .getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                  )
                  .catch((e: unknown) => e);
               expect(error).toBeInstanceOf(PayloadTooLargeError);
               expect(error).not.toBeInstanceOf(ResponseUnserializableError);
               expect((error as Error).message).toContain("more than 10 rows");
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
               undefined,
               // Model surfaces `target_code` so filterGivensToModelSurface
               // (see model.ts) forwards it rather than dropping it as unknown.
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               [{ name: "target_code", type: "string" }] as any,
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

         it("maps a finalized-given rejection (code) to BadRequestError, not 500", async () => {
            const finalizedErr = Object.assign(
               new Error(
                  "Given 'target_code' is finalized and cannot be overridden",
               ),
               { code: "runtime-given-finalized" },
            );
            const cellRunnable = {
               getPreparedResult: sinon.stub().rejects(finalizedErr),
               run: sinon.stub(),
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
               model.executeNotebookCell(0, undefined, undefined, {
                  target_code: "AA",
               }),
            ).rejects.toThrow(BadRequestError);

            sinon.restore();
         });

         it("embeds model-level givens in executed cell newSources", async () => {
            const sourceInfo = { name: "carriers", schema: { fields: [] } };
            const givens = [
               {
                  name: "region",
                  type: "string",
                  annotations: ["#(doc) Region"],
               },
            ];
            // A source-only code cell (no runnable) still emits newSources.
            const runnableCells = [
               {
                  type: "code" as const,
                  text: "import 'carriers.malloy'",
                  newSources: [sourceInfo],
               },
            ];

            const model = new Model(
               packageName,
               "test.malloynb",
               {},
               "notebook",
               undefined, // modelMaterializer
               undefined, // modelDef
               undefined, // sources
               undefined, // queries
               undefined, // sourceInfos
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               runnableCells as any, // runnableNotebookCells
               undefined, // compilationError
               undefined, // filterMap
               givens, // givens
            );

            const result = await model.executeNotebookCell(0);
            const parsed = JSON.parse(result.newSources![0]);
            expect(parsed.name).toBe("carriers");
            expect(parsed.givens).toEqual(givens);
         });
      });
   });

   // A binding that declares `freshnessFallback=live` says the tier is an
   // optimisation, not a dependency: a store that fails under a routed query
   // degrades to serving live. The failure modes here are all silent-by-nature —
   // a wrong row cap returns an EMPTY success, a mislabelled metric inflates the
   // tier's headline KPI while it is broken — so they need assertions rather than
   // an end-to-end observation.
   describe("runtime storage failure → freshnessFallback=live", () => {
      const originalMode = process.env.PERSIST_STORAGE_MODE;
      const originalDefaultRows = process.env.PUBLISHER_DEFAULT_QUERY_ROW_LIMIT;
      const originalMetadata = process.env.PUBLISHER_QUERY_METADATA;

      afterEach(() => {
         sinon.restore();
         for (const [name, original] of [
            ["PERSIST_STORAGE_MODE", originalMode],
            ["PUBLISHER_DEFAULT_QUERY_ROW_LIMIT", originalDefaultRows],
            ["PUBLISHER_QUERY_METADATA", originalMetadata],
         ] as const) {
            if (original === undefined) delete process.env[name];
            else process.env[name] = original;
         }
      });

      const binding = (sourceName: string, freshnessFallback?: string) =>
         ({
            sourceName,
            destinationName: "lake",
            virtualHandle: `eid-${sourceName}`,
            tablePath: `lake.t_${sourceName}`,
            schema: [{ name: "region", type: "string" }],
            freshnessFallback,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
         }) as any;

      /**
       * A Model routed to storage, where the storage runnable fails and the live
       * one succeeds. `loadServeShapeQuery` is stubbed because the real one
       * compiles a transient model — the logic under test is the catch, and the
       * SHAPE'S bindings (not the package's) are what it must consult.
       */
      function routedModel(opts: {
         shapeBindings: unknown[];
         packageBindings?: unknown[];
         storageFailsAt: "prepare" | "run";
         liveRunFails?: boolean;
         livePreparedLimit?: number;
      }) {
         const storageErr = new Error("store table missing");
         const storageRunnable = {
            getPreparedResult:
               opts.storageFailsAt === "prepare"
                  ? sinon.stub().rejects(storageErr)
                  : sinon.stub().resolves({
                       resultExplore: { limit: 0 },
                       connectionName: "lake",
                    }),
            run:
               opts.storageFailsAt === "run"
                  ? sinon.stub().rejects(storageErr)
                  : sinon.stub().resolves({}),
         };
         const fakeResult = {
            _queryResult: { data: { rawData: [] } },
            totalRows: 1,
            data: { value: [] },
            connectionName: "live_pg",
         };
         const liveRun = opts.liveRunFails
            ? sinon.stub().rejects(new Error("warehouse down"))
            : sinon.stub().resolves(fakeResult);
         const liveRunnable = {
            getPreparedResult: sinon.stub().resolves({
               resultExplore: { limit: opts.livePreparedLimit ?? 0 },
               connectionName: "live_pg",
            }),
            run: liveRun,
         };
         sinon
            .stub(API.util, "wrapResult")
            .returns({ rows: [] } as unknown as ReturnType<
               typeof API.util.wrapResult
            >);
         const modelMaterializer = {
            loadQuery: sinon.stub().returns(liveRunnable),
            loadRestrictedQuery: sinon.stub().returns(liveRunnable),
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
         process.env.PERSIST_STORAGE_MODE = "on";
         model.setServeBindings(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (opts.packageBindings ?? opts.shapeBindings) as any,
         );
         // Only presence matters here: loadServeShapeQuery is stubbed below, so
         // the provider is never called.
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         model.setServeDestinationConfig((() => ({})) as any);
         sinon
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .stub(model as any, "loadServeShapeQuery")
            .resolves({
               runnable: storageRunnable,
               virtualMap: { v: "lake.t_x" },
               bindings: opts.shapeBindings,
            });
         const recordStub = sinon.stub(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (model as any).queryExecutionHistogram,
            "record",
         );
         return { model, liveRun, recordStub };
      }

      it("caps the live retry by the LIVE row limit when the store fails at prepare", async () => {
         // `rowLimit` is assigned from the storage prepare, so a prepare-time
         // failure leaves it 0 — which the connector reads as "stop before the
         // first row", returning a successful EMPTY answer instead of the data.
         process.env.PUBLISHER_DEFAULT_QUERY_ROW_LIMIT = "250";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "prepare",
         });

         await model.getQueryResults(undefined, undefined, "run: daily -> x");

         expect(liveRun.calledOnce).toBe(true);
         expect(liveRun.firstCall.args[0].rowLimit).toBe(250);
      });

      it("tags the live retry, with the LIVE connection's layers and the same id", async () => {
         // Two failures in one: the statement that actually answered the query
         // carried no attribution at all, so the full-cost fallback — the one an
         // operator goes looking for on a bill — landed in the untagged bucket;
         // and the response still returned the id from the bag resolved before
         // the storage attempt, pointing a caller at the statement that FAILED.
         // The layers must come from the live connection: on this tier the store
         // is routinely a different connection, with different enforced
         // properties.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-1",
               connectionMetadata: (connectionName: string) =>
                  connectionName === "live_pg"
                     ? { default: null, enforced: { tenant: "acme" } }
                     : { default: null, enforced: { tenant: "wrong" } },
            },
         );

         const attached = liveRun.firstCall.args[0].queryMetadata;
         expect(attached.tenant).toBe("acme");
         expect(attached.query_id).toBe("corr-1");
         expect(result.queryCorrelationId).toBe("corr-1");
      });

      it("does not record a live-served answer as a storage hit", async () => {
         // The hit rate is the tier's headline KPI; counting a fallback as a hit
         // makes it RISE while the tier is broken.
         const { model, recordStub } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         await model.getQueryResults(undefined, undefined, "run: daily -> x");

         const success = recordStub
            .getCalls()
            .map((c) => c.args[1] as Record<string, string>)
            .find((a) => a["malloy.model.query.status"] === "success");
         expect(success?.["malloy.model.query.served_from"]).toBe(
            "live_fallback",
         );
      });

      it("maps a failure of the live retry like any other query failure", async () => {
         // A broad outage takes the warehouse down alongside the store, so the
         // retry failing is ordinary — and an unmapped throw turns a clean 400
         // into a 500 with no error metric behind it.
         const { model, recordStub } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            liveRunFails: true,
         });

         await expect(
            model.getQueryResults(undefined, undefined, "run: daily -> x"),
         ).rejects.toThrow(BadRequestError);

         const errored = recordStub
            .getCalls()
            .map((c) => c.args[1] as Record<string, string>)
            .find((a) => a["malloy.model.query.status"] === "error");
         expect(errored).toBeDefined();
      });

      it("decides on the SHAPE's bindings, not the package's", async () => {
         // Bindings are pushed package-wide and `freshnessFallback` is per entry,
         // so a mixed set is normal. A query whose shape carries only the `live`
         // binding must still degrade — a `stale_ok` sibling it never touched
         // cannot veto it.
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            packageBindings: [
               binding("daily", "live"),
               binding("other", "stale_ok"),
            ],
            storageFailsAt: "run",
         });

         await model.getQueryResults(undefined, undefined, "run: daily -> x");

         expect(liveRun.calledOnce).toBe(true);
      });

      it("keeps surfacing the error when the shape carries a non-live binding", async () => {
         // `fail` and the `stale_ok` default are fail-closed: the caller asked to
         // hear about it rather than be served a slower answer.
         const { model } = routedModel({
            shapeBindings: [binding("daily", "stale_ok")],
            storageFailsAt: "run",
         });

         await expect(
            model.getQueryResults(undefined, undefined, "run: daily -> x"),
         ).rejects.toThrow(BadRequestError);
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

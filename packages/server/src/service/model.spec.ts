// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
            }): {
               model: Model;
               runStub: sinon.SinonStub;
               wrapStub: sinon.SinonStub;
            } {
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
               const wrapStub = sinon
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
               return { model, runStub, wrapStub };
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

            it("refuses a row overflow before wrapping the result at all", async () => {
               // The test above pins WHICH error a row overflow reports; this one
               // pins where the check sits. `wrapResult` deep-converts every row
               // into Cell objects, an object graph larger than the JSON built
               // from it, and a row overflow is a maxRows + 1 row result by
               // construction. Asserting the stub never ran is the only thing
               // that catches the check drifting below the wrap: the 413 and its
               // message are identical either way.
               process.env.PUBLISHER_MAX_QUERY_ROWS = "10";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "10000";
               const { model, wrapStub } = buildModelWithFakeRun({
                  userLimit: 100,
                  totalRows: 11,
                  wrappedJson: { rows: [] },
               });

               await expect(
                  model.getQueryResults(
                     undefined,
                     undefined,
                     "run: orders -> summary",
                  ),
               ).rejects.toBeInstanceOf(PayloadTooLargeError);
               expect(wrapStub.called).toBe(false);
            });

            it("applies the bigint replacer to the compact shape", async () => {
               // `queryResults.data.value` is raw driver output and DuckDB returns
               // count() as a BigInt, so serializing it without the replacer
               // throws a TypeError, not a RangeError, and escapes as the bare
               // 500 this whole path exists to remove. No other test in the
               // suite fails if the replacer argument is dropped.
               process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
               process.env.PUBLISHER_MAX_RESPONSE_BYTES = "10000";
               const { model } = buildModelWithFakeRun({
                  userLimit: 10,
                  totalRows: 1,
                  wrappedJson: { rows: [] },
                  // Both branches of the replacer: a count inside the safe
                  // integer range stays a JSON number, one past it becomes a
                  // string so its digits survive.
                  compactRows: [{ n: 42n, big: 9007199254740993n }],
               });

               const { serializedResult } = await model.getQueryResults(
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
               expect(serializedResult).toBe(
                  '[{"n":42,"big":"9007199254740993"}]',
               );
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
      /**
       * A malloy `Note`. The `at` location is required, not decoration: without
       * it `Annotations.parseAsTag` throws on `line.at.url`, and every reader of
       * these annotations catches and degrades to "no layer" — so a note without
       * it produces a green test that proves nothing.
       */
      const specNote = (text: string) => ({
         text,
         at: {
            url: "file://mockModel.malloy",
            range: {
               start: { line: 0, character: 0 },
               end: { line: 0, character: 1 },
            },
         },
      });

      function routedModel(opts: {
         shapeBindings: unknown[];
         packageBindings?: unknown[];
         storageFailsAt: "prepare" | "run";
         liveRunFails?: boolean;
         livePreparedLimit?: number;
         /**
          * Raw `##` note texts for the model file, and `#@` note texts per
          * source — the same annotation bundle the build path reads through
          * `PersistSource.annotations`. Without these the modelDef has empty
          * `contents` and the declared model/source layers resolve to nothing,
          * which is a test that proves only the package layer.
          */
         modelNotes?: string[];
         sourceNotes?: Record<string, string[]>;
         /**
          * `#@` notes on a NAMED QUERY of the same name, which `modelDef.contents`
          * holds beside sources. Overwrites the source entry, so a test can prove
          * which of the two a reader picked.
          */
         namedQueryNotes?: Record<string, string[]>;
         /**
          * `##` notes on a DIFFERENT compilation node that this model inherits
          * from — i.e. an import. `modelAnnotations` folds these in and
          * `ownModelNotes` does not, which is the whole difference under test.
          */
         importedModelNotes?: string[];
         /**
          * The source the COMPILED query reads, as `resolveAuthorizeSourceFromRunnable`
          * would resolve it off the prepared query. Distinct from whatever the
          * query TEXT names first.
          */
         compiledRunTarget?: string;
         /** Named queries, for the `queryName` request shape. */
         queries?: { name: string; sourceName: string }[];
      }) {
         const storageErr = new Error("store table missing");
         // Both runnables stub `getPreparedQuery` even though nothing in these
         // tests reads the compiled query: the authorize entry-point walk and
         // the storage-routing row-level pre-check both call it, and a mock
         // that omits it is not "a runnable with an irrelevant method
         // missing" — it is a runnable whose compile THROWS, which the
         // pre-check (correctly) treats as "cannot tell whether this entry
         // point is row-level gated" and refuses to route. `{_query: {}}` is
         // the no-run-target shape: `structRef` is undefined, so the walk
         // resolves no struct, finds no gate, and routing proceeds — which is
         // what these tests are actually about.
         const preparedQueryStub = () => sinon.stub().resolves({ _query: {} });
         const storageRunnable = {
            getPreparedQuery: preparedQueryStub(),
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
         const preparedQuery = opts.compiledRunTarget
            ? {
                 getPreparedQuery: sinon.stub().resolves({
                    _query: { structRef: opts.compiledRunTarget },
                 }),
              }
            : {};
         Object.assign(storageRunnable, preparedQuery);
         const liveRunnable = {
            getPreparedQuery: preparedQueryStub(),
            getPreparedResult: sinon.stub().resolves({
               resultExplore: { limit: opts.livePreparedLimit ?? 0 },
               connectionName: "live_pg",
            }),
            run: liveRun,
            ...preparedQuery,
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
            {
               contents: Object.fromEntries(
                  Object.entries(opts.sourceNotes ?? {}).map(
                     ([name, texts]) => [
                        name,
                        {
                           // `type` is not decoration: `safeSourceTag` admits
                           // only a real SourceDef, so a fixture without one is
                           // a named query as far as malloy's `isSourceDef` is
                           // concerned and contributes no source layer.
                           type: "table",
                           annotations: { notes: texts.map(specNote) },
                        },
                     ],
                  ),
               ),
               ...(opts.namedQueryNotes
                  ? {
                       contents: {
                          ...Object.fromEntries(
                             Object.entries(opts.sourceNotes ?? {}).map(
                                ([name, texts]) => [
                                   name,
                                   {
                                      type: "table",
                                      annotations: {
                                         notes: texts.map(specNote),
                                      },
                                   },
                                ],
                             ),
                          ),
                          ...Object.fromEntries(
                             Object.entries(opts.namedQueryNotes).map(
                                ([name, texts]) => [
                                   `${name}_query`,
                                   {
                                      type: "query",
                                      annotations: {
                                         notes: texts.map(specNote),
                                      },
                                   },
                                ],
                             ),
                          ),
                       },
                    }
                  : {}),
               exports: [],
               queryList: [],
               // The file-level `##` tags come from the modelAnnotations
               // REGISTRY keyed by modelID, not from a bare `annotation` field —
               // that indirection exists so an import's tags can be folded in.
               modelID: "m",
               modelAnnotations:
                  opts.modelNotes || opts.importedModelNotes
                     ? {
                          m: {
                             inheritsFrom: opts.importedModelNotes
                                ? ["file://imported.malloy"]
                                : [],
                             ownNotes: {
                                notes: (opts.modelNotes ?? []).map(specNote),
                             },
                          },
                          ...(opts.importedModelNotes
                             ? {
                                  "file://imported.malloy": {
                                     inheritsFrom: [],
                                     ownNotes: {
                                        notes: opts.importedModelNotes.map(
                                           specNote,
                                        ),
                                     },
                                  },
                               }
                             : {}),
                       }
                     : undefined,
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
            } as any,
            undefined,
            // queries: how a `queryName` request resolves to its source.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            opts.queries as any,
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

      it("tags the source the query RUNS, not the first one its text names", async () => {
         // Malloy executes the LAST `run:`; `extractRunTargetSourceName` reads
         // the FIRST. Tagging off the surface syntax therefore attributed an
         // expensive statement to the cheap source's team and tier — worse than
         // missing attribution, because the bill lands on a source that never
         // ran. The authorize gate already resolves the compiled target for
         // exactly this reason; metadata now reads the same answer.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            compiledRunTarget: "expensive",
            sourceNotes: {
               cheap: ['#@ queryMetadata.tier="bronze"'],
               expensive: ['#@ queryMetadata.tier="platinum"'],
            },
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: cheap -> x\nrun: expensive -> x",
         );

         expect(liveRun.firstCall.args[0].queryMetadata.tier).toBe("platinum");
      });

      it("does not fold an IMPORT's model-file tags into the importer", async () => {
         // `modelAnnotations` folds the import lineage because a file-level
         // `##(authorize)` gate an import could shed would be no gate at all. A
         // tag is not a gate: folding one lets a shared include attribute every
         // importing file's traffic to the include's team, and reports the
         // resulting publish warning against a file that does not contain the
         // line.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            importedModelNotes: ['## queryMetadata.team="platform"'],
            modelNotes: ['## queryMetadata.surface="marts"'],
         });

         await model.getQueryResults(undefined, undefined, "run: daily -> x");

         const attached = liveRun.firstCall.args[0].queryMetadata;
         expect(attached.surface).toBe("marts");
         expect(attached.team).toBeUndefined();
      });

      it("assembles no metadata layers at all when the feature is off", async () => {
         // `mergeQueryMetadata` early-returns on `off`, so everything assembled
         // for it is discarded. The mode is off unless an operator turns it on,
         // so assembling anyway made the DEFAULT deployment pay an annotation
         // walk and a connection lookup on every query for a bag nobody reads.
         // The connection lookup is the observable half.
         process.env.PUBLISHER_QUERY_METADATA = "off";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            modelNotes: ['## queryMetadata.surface="marts"'],
         });
         const connectionMetadata = sinon.stub().returns({
            default: { team: "finance" },
            enforced: null,
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            { connectionMetadata },
         );

         expect(connectionMetadata.called).toBe(false);
         expect(liveRun.firstCall.args[0].queryMetadata).toBeUndefined();
      });

      it("carries the package's declared properties onto a SERVED query", async () => {
         // The gap this closes: declared properties reached materialization
         // statements and nothing else, so a deployment could attribute its
         // builds and not the interactive traffic that is most of its warehouse
         // bill. A served query arrived carrying the platform's context and none
         // of the author's own vocabulary.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-2",
               packageDeclaration: { team: "finance", tier: "bronze" },
               connectionMetadata: () => ({ default: null, enforced: null }),
            },
         );

         const attached = liveRun.firstCall.args[0].queryMetadata;
         expect(attached.team).toBe("finance");
         expect(attached.tier).toBe("bronze");
         // Context still applies on top, and is what distinguishes a served
         // statement from a build of the same source.
         expect(attached.class).toBe("interactive");
      });

      it("lets the request override a declared property, and keeps the rest", async () => {
         // Precedence across the two layers that were never composed together
         // before: a caller's per-request bag is more specific than anything the
         // author declared, but must not evict what it does not mention.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-3",
               request: { tier: "platinum" },
               packageDeclaration: { team: "finance", tier: "bronze" },
               connectionMetadata: () => ({ default: null, enforced: null }),
            },
         );

         const attached = liveRun.firstCall.args[0].queryMetadata;
         expect(attached.tier).toBe("platinum");
         expect(attached.team).toBe("finance");
      });

      it("composes all three declared layers, most specific winning", async () => {
         // The package layer alone is not the claim — a model file's `##` tag and
         // a source's `#@` tag have to reach a served statement too, with the
         // same precedence the build path applies. Both are read through casts
         // into `modelDef`, the shape where a field rename degrades to a silent
         // no-layer, so an empty-`contents` fixture would pass while proving
         // neither.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            modelNotes: [
               '## materialization.queryMetadata.tier="silver"\n',
               '## materialization.queryMetadata.from_model="yes"\n',
            ],
            sourceNotes: {
               daily: ['#@ persist queryMetadata.tier="gold"\n'],
            },
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-4",
               packageDeclaration: { tier: "bronze", team: "finance" },
               connectionMetadata: () => ({ default: null, enforced: null }),
            },
         );

         const attached = liveRun.firstCall.args[0].queryMetadata;
         // source > model file > package, per property.
         expect(attached.tier).toBe("gold");
         // Nothing more specific mentions these, so both survive.
         expect(attached.from_model).toBe("yes");
         expect(attached.team).toBe("finance");
      });

      it("resolves the source layer from a named query, which supplies no sourceName", async () => {
         // The dominant REST and MCP shape: `queryName` alone. `sourceName` is
         // optional on that branch despite what the request-shape error string
         // says, so passing the raw param would drop the declared layer for the
         // call shape most callers use. Resolves through
         // `queries.find(q => q.name === queryName)?.sourceName`.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            queries: [{ name: "daily_view", sourceName: "daily" }],
            sourceNotes: {
               daily: ['#@ persist queryMetadata.tier="gold"\n'],
            },
         });

         await model.getQueryResults(
            undefined,
            "daily_view",
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-7",
               connectionMetadata: () => ({ default: null, enforced: null }),
            },
         );

         expect(liveRun.firstCall.args[0].queryMetadata.tier).toBe("gold");
      });

      it("resolves the source layer from ad-hoc query text", async () => {
         // The other branch of the same resolution: surface syntax on the run
         // target, for a request that names nothing at all.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            sourceNotes: {
               daily: ['#@ persist queryMetadata.tier="gold"\n'],
            },
         });

         await model.getQueryResults(
            undefined,
            undefined,
            // Ad-hoc text whose run target is resolvable from surface syntax.
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-5",
               connectionMetadata: () => ({ default: null, enforced: null }),
            },
         );

         expect(liveRun.firstCall.args[0].queryMetadata.tier).toBe("gold");
      });

      it("refuses a declared property that would forge build identity", async () => {
         // Context only overwrites names it has a VALUE for, and a served query
         // has no `source`, `trigger` or `run_id`. Without the reserved-name rule
         // a model file could stamp `source=orders_daily` on interactive traffic,
         // which in the warehouse's own history reads exactly like a build of
         // that source — the confusion the declared layer was originally withheld
         // to prevent.
         process.env.PUBLISHER_QUERY_METADATA = "on";
         const { model, liveRun } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            sourceNotes: {
               daily: [
                  '#@ persist queryMetadata.source="orders_daily" queryMetadata.run_id="forged" queryMetadata.tier="gold"\n',
               ],
            },
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            {
               correlationId: "corr-6",
               connectionMetadata: () => ({ default: null, enforced: null }),
            },
         );

         const attached = liveRun.firstCall.args[0].queryMetadata;
         expect(attached.source).toBeUndefined();
         expect(attached.run_id).toBeUndefined();
         // The author's own property, which is not a context name, still lands.
         expect(attached.tier).toBe("gold");
         expect(attached.class).toBe("interactive");
      });

      it("reports the model file's own declaration, and parses it once", async () => {
         // The publish gate reads this per model, and `getPackageMetadata()` runs
         // once per package inside listPackages — so recomputing would walk the
         // import closure and re-parse every `##` note on a listing. A compiled
         // model's annotations never change, so the memo needs no invalidation;
         // identity is the cheapest proof it is a memo and not a fresh parse.
         const { model } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            modelNotes: ['## materialization.queryMetadata.tier="silver"\n'],
         });

         const first = model.getDeclaredQueryMetadata();
         expect(first).toEqual({ tier: "silver" });
         expect(model.getDeclaredQueryMetadata()).toBe(first);
      });

      it("lists tagged SOURCES only, never a named query that carries a `#@`", async () => {
         // `modelDef.contents` holds named queries beside sources. Iterating it
         // blind read a query's `#@` as though a source had declared it, so the
         // query's name turned up among the package's tagged sources — and any
         // publish warning about that bag pointed an author at a `source:` that
         // does not exist in the file.
         const { model } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
            sourceNotes: { daily: ['#@ queryMetadata.tier="gold"'] },
            namedQueryNotes: { daily: ['#@ queryMetadata.tier="forged"'] },
         });

         expect(model.getDeclaredSourceQueryMetadata()).toEqual([
            { sourceName: "daily", queryMetadata: { tier: "gold" } },
         ]);
      });

      it("reports no declaration as null, and does not re-derive it", async () => {
         // `null` is a computed answer, not "unknown" — so the memo has to
         // distinguish it from "not yet computed" or every call re-parses.
         const { model } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         expect(model.getDeclaredQueryMetadata()).toBeNull();
         expect(model.getDeclaredQueryMetadata()).toBeNull();
      });

      it("returns servedFrom and an execution time to the caller", async () => {
         // A storage-served answer is byte-identical to a live one, so without
         // these two a caller cannot tell that materialization did anything.
         // `live_fallback` specifically must reach the caller: it is a SUCCESS
         // answered by the warehouse, indistinguishable from a hit at the call
         // site, and a UI that showed it as "from storage" would be lying.
         const { model } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
         );

         expect(result.servedFrom).toBe("live_fallback");
         expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
         expect(Number.isInteger(result.executionTimeMs)).toBe(true);
      });

      it("reports servedFrom null when the query never routed", async () => {
         // The default for every deployment with no storage sources. Null rather
         // than a made-up "live", because "this query had no storage binding to
         // consider" and "storage was considered and declined" are different
         // facts and only the second is worth showing anyone.
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (() => {
               const runnable = {
                  getPreparedResult: sinon.stub().resolves({
                     resultExplore: { limit: 0 },
                     connectionName: "pg",
                  }),
                  run: sinon.stub().resolves({
                     _queryResult: { data: { rawData: [] } },
                     totalRows: 0,
                     data: { value: [] },
                     connectionName: "pg",
                  }),
               };
               return {
                  loadQuery: sinon.stub().returns(runnable),
                  loadRestrictedQuery: sinon.stub().returns(runnable),
               };
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
            })() as any,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            { contents: {}, exports: [], queryList: [] } as any,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
         );
         sinon
            .stub(API.util, "wrapResult")
            .returns({ rows: [] } as unknown as ReturnType<
               typeof API.util.wrapResult
            >);

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
         );

         expect(result.servedFrom).toBeNull();
      });

      it("reports queryCostBytes as null rather than zero when unreported", async () => {
         // Null and 0 mean opposite things here. Every non-BigQuery backend
         // reports nothing, and so does a storage-served query that touched no
         // warehouse — reading either as "this cost zero" is how a savings
         // number gets fabricated out of a backend that simply cannot say.
         const { model } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
         );

         expect(result.queryCostBytes).toBeNull();
      });

      it("keeps unbounded values off the query histogram's labels", async () => {
         // The query TEXT and the returned ROW COUNT are both unbounded label
         // values, and a histogram label multiplies by the bucket count — either
         // one grows the metric for as long as the process serves traffic. They
         // belong on the request log, not here. This pins the exclusion, because
         // re-adding one is a one-line change that looks harmless.
         const { model, recordStub } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         await model.getQueryResults(undefined, undefined, "run: daily -> x");

         for (const attrs of recordStub
            .getCalls()
            .map((c) => c.args[1] as Record<string, unknown>)) {
            expect(attrs["malloy.model.query.query"]).toBeUndefined();
            expect(attrs["malloy.model.query.rows_total"]).toBeUndefined();
         }
      });

      it("labels the query histogram with the environment and package", async () => {
         // Without these the only identity on the metric is a bare model path,
         // which is neither unique across packages nor able to answer "whose
         // queries are being served from storage".
         const { model, recordStub } = routedModel({
            shapeBindings: [binding("daily", "live")],
            storageFailsAt: "run",
         });

         await model.getQueryResults(
            undefined,
            undefined,
            "run: daily -> x",
            undefined,
            undefined,
            undefined,
            undefined,
            { environment: "acme___prod" },
         );

         const success = recordStub
            .getCalls()
            .map((c) => c.args[1] as Record<string, string>)
            .find((a) => a["malloy.model.query.status"] === "success");
         expect(success?.["malloy.environment"]).toBe("acme___prod");
         expect(success?.["malloy.package"]).toBe(packageName);
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

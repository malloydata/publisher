import {
   MalloyError,
   ModelDef,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import type * as Malloy from "@malloydata/malloy-interfaces";
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

   // ------------------------------------------------------------------
   // Source Parameters integration with getQueryResults
   // ------------------------------------------------------------------
   describe("getQueryResults with sourceParameters", () => {
      const minimalModelDef = {
         type: "model",
         name: "test",
         exports: [],
         contents: {},
         queryList: [],
      } as unknown as ModelDef;

      function makeSourceInfos(
         params: Malloy.ParameterInfo[],
         sourceName = "flights",
      ): Malloy.SourceInfo[] {
         return [
            {
               kind: "source",
               name: sourceName,
               schema: { fields: [] },
               parameters: params,
            } as unknown as Malloy.SourceInfo,
         ];
      }

      function param(
         name: string,
         kind: string,
         hasDefault = false,
      ): Malloy.ParameterInfo {
         return {
            name,
            type: { kind } as Malloy.ParameterType,
            default_value: hasDefault
               ? ({
                    kind: "string_literal",
                    string_value: "x",
                 } as Malloy.LiteralValue)
               : undefined,
         };
      }

      it("should throw BadRequestError when a required source parameter is missing", async () => {
         const loadQueryStub = sinon.stub();
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([param("carrier", "string_type")]),
            undefined,
            undefined,
         );

         await expect(
            model.getQueryResults("flights", "by_carrier", undefined, {}),
         ).rejects.toThrow(/Parameter "carrier" is required/);

         expect(loadQueryStub.called).toBe(false);
         sinon.restore();
      });

      it("should throw BadRequestError listing all missing required params", async () => {
         const loadQueryStub = sinon.stub();
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([
               param("carrier", "string_type"),
               param("year", "number_type"),
            ]),
            undefined,
            undefined,
         );

         await expect(
            model.getQueryResults("flights", "by_carrier", undefined, {}),
         ).rejects.toThrow(/Parameters "carrier", "year" are required/);

         sinon.restore();
      });

      it("should silently ignore unknown source parameters", async () => {
         const loadQueryStub = sinon.stub().returns({
            getPreparedResult: sinon.stub().rejects(new Error("stop")),
         });
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([param("carrier", "string_type", true)]),
            undefined,
            undefined,
         );

         try {
            await model.getQueryResults("flights", "by_carrier", undefined, {
               nonexistent: "val",
            });
         } catch {
            // expected — getPreparedResult throws
         }

         expect(loadQueryStub.calledOnce).toBe(true);
         expect(loadQueryStub.firstCall.args[0]).toBe(
            "\nrun: flights->by_carrier",
         );
         sinon.restore();
      });

      it("should inject param clause into loadQuery for named query", async () => {
         const loadQueryStub = sinon.stub().returns({
            getPreparedResult: sinon.stub().rejects(new Error("stop")),
         });
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([param("carrier", "string_type")]),
            undefined,
            undefined,
         );

         try {
            await model.getQueryResults("flights", "by_carrier", undefined, {
               carrier: "AA",
            });
         } catch {
            // expected — getPreparedResult throws
         }

         expect(loadQueryStub.calledOnce).toBe(true);
         expect(loadQueryStub.firstCall.args[0]).toBe(
            '##! experimental.parameters\n\nrun: flights(carrier is "AA")->by_carrier',
         );
         sinon.restore();
      });

      it("should not inject params when sourceParameters is empty", async () => {
         const loadQueryStub = sinon.stub().returns({
            getPreparedResult: sinon.stub().rejects(new Error("stop")),
         });
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([param("carrier", "string_type", true)]),
            undefined,
            undefined,
         );

         try {
            await model.getQueryResults("flights", "by_carrier", undefined, {});
         } catch {
            // expected
         }

         expect(loadQueryStub.calledOnce).toBe(true);
         expect(loadQueryStub.firstCall.args[0]).toBe(
            "\nrun: flights->by_carrier",
         );
         sinon.restore();
      });

      it("should skip param validation when source has no declared params", async () => {
         const loadQueryStub = sinon.stub().returns({
            getPreparedResult: sinon.stub().rejects(new Error("stop")),
         });
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([]),
            undefined,
            undefined,
         );

         try {
            await model.getQueryResults(
               "flights",
               "by_carrier",
               undefined,
               undefined,
            );
         } catch {
            // expected
         }

         expect(loadQueryStub.calledOnce).toBe(true);
         expect(loadQueryStub.firstCall.args[0]).toBe(
            "\nrun: flights->by_carrier",
         );
         sinon.restore();
      });

      it("should convert number params to unquoted literals", async () => {
         const loadQueryStub = sinon.stub().returns({
            getPreparedResult: sinon.stub().rejects(new Error("stop")),
         });
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([param("min_distance", "number_type")]),
            undefined,
            undefined,
         );

         try {
            await model.getQueryResults("flights", "query1", undefined, {
               min_distance: "500",
            });
         } catch {
            // expected
         }

         expect(loadQueryStub.firstCall.args[0]).toBe(
            "##! experimental.parameters\n\nrun: flights(min_distance is 500)->query1",
         );
         sinon.restore();
      });

      it("should convert boolean params correctly", async () => {
         const loadQueryStub = sinon.stub().returns({
            getPreparedResult: sinon.stub().rejects(new Error("stop")),
         });
         const model = new Model(
            packageName,
            mockModelPath,
            {},
            "model",
            { loadQuery: loadQueryStub } as unknown as ModelMaterializer,
            minimalModelDef,
            undefined,
            undefined,
            makeSourceInfos([param("active", "boolean_type")]),
            undefined,
            undefined,
         );

         try {
            await model.getQueryResults("flights", "query1", undefined, {
               active: "true",
            });
         } catch {
            // expected
         }

         expect(loadQueryStub.firstCall.args[0]).toBe(
            "##! experimental.parameters\n\nrun: flights(active is true)->query1",
         );
         sinon.restore();
      });
   });

   // ------------------------------------------------------------------
   // executeNotebookCell with sourceParameters
   // ------------------------------------------------------------------
   describe("executeNotebookCell with sourceParameters", () => {
      it("should return markdown cell without execution", async () => {
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
            [{ type: "markdown", text: "# Hello" }],
            undefined,
         );

         const result = await model.executeNotebookCell(0);
         expect(result.type).toBe("markdown");
         expect(result.text).toBe("# Hello");
         expect(result.result).toBeUndefined();

         sinon.restore();
      });

      it("should throw BadRequestError for out-of-range cell index", async () => {
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
            [{ type: "code", text: "run: x -> { }" }],
            undefined,
         );

         await expect(model.executeNotebookCell(5)).rejects.toThrow(
            /out of range/,
         );
         await expect(model.executeNotebookCell(-1)).rejects.toThrow(
            /out of range/,
         );

         sinon.restore();
      });

      it("should throw compilation error message when no sourceParameters provided", async () => {
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
                  text: "run: flights -> { aggregate: c is count() }",
                  cellCompilationError: new Error(
                     'Parameter "carrier" is required',
                  ),
               },
            ],
            undefined,
         );

         await expect(model.executeNotebookCell(0)).rejects.toThrow(
            /Cell 0 failed to compile.*carrier.*is required/,
         );

         sinon.restore();
      });

      it("should throw compilation error message when cell has compilation error (not parameter-related)", async () => {
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
                  text: "run: flights -> { }",
                  cellCompilationError: new Error("some error"),
               },
            ],
            undefined,
         );

         await expect(model.executeNotebookCell(0)).rejects.toThrow(
            /failed to compile.*some error/,
         );

         sinon.restore();
      });

      it("should require sourceParameters when cell was compiled with stubs", async () => {
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
                  text: "run: flights -> { }",
                  requiresSourceParameters: true,
               },
            ],
            undefined,
         );

         await expect(model.executeNotebookCell(0)).rejects.toThrow(
            /parameterized source/,
         );

         sinon.restore();
      });

      it("should throw when runtime context is missing for recompilation", async () => {
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
                  text: "run: flights -> { }",
                  cellCompilationError: new Error("missing param"),
               },
            ],
            undefined,
            // no runtime, no importBaseURL
         );

         await expect(
            model.executeNotebookCell(0, { carrier: "AA" }),
         ).rejects.toThrow(/missing runtime context/);

         sinon.restore();
      });

      it("should inject param clause into cell text during recompilation", async () => {
         const extendModelStub = sinon.stub();
         const loadFinalQueryStub = sinon.stub();
         const getPreparedResultStub = sinon
            .stub()
            .resolves({ resultExplore: {} });
         const runStub = sinon.stub().resolves({});
         const getPreparedQueryStub = sinon
            .stub()
            .resolves({ _query: { name: "test_q" } });

         extendModelStub.returns({ loadFinalQuery: loadFinalQueryStub });
         loadFinalQueryStub.returns({
            getPreparedResult: getPreparedResultStub,
            run: runStub,
            getPreparedQuery: getPreparedQueryStub,
         });

         const sourceInfos: Malloy.SourceInfo[] = [
            {
               kind: "source",
               name: "flights",
               schema: { fields: [] },
               parameters: [
                  {
                     name: "carrier",
                     type: { kind: "string_type" } as Malloy.ParameterType,
                  },
               ],
            } as unknown as Malloy.SourceInfo,
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
            sourceInfos,
            [
               {
                  type: "code",
                  text: "run: flights -> { aggregate: c is count() }",
                  cellCompilationError: new Error("missing param"),
                  priorModelMaterializer: {
                     extendModel: extendModelStub,
                  } as unknown as ModelMaterializer,
               },
            ],
            undefined,
            sinon.createStubInstance(Runtime),
            new URL("file:///test/"),
         );

         const result = await model.executeNotebookCell(0, { carrier: "AA" });

         expect(extendModelStub.calledOnce).toBe(true);
         const modifiedText = extendModelStub.firstCall.args[0] as string;
         expect(modifiedText).toContain('flights(carrier is "AA")');
         expect(modifiedText).toContain("->");

         expect(result.type).toBe("code");
         expect(result.text).toBe(
            "run: flights -> { aggregate: c is count() }",
         );
         expect(result.queryName).toBe("test_q");

         sinon.restore();
      });

      it("should use runtime.loadModel when priorModelMaterializer is undefined (first cell)", async () => {
         const loadModelStub = sinon.stub();
         const loadFinalQueryStub = sinon.stub();
         const getPreparedResultStub = sinon
            .stub()
            .resolves({ resultExplore: {} });
         const runStub = sinon.stub().resolves({});
         const getPreparedQueryStub = sinon
            .stub()
            .resolves({ _query: { name: "q1" } });

         loadModelStub.returns({ loadFinalQuery: loadFinalQueryStub });
         loadFinalQueryStub.returns({
            getPreparedResult: getPreparedResultStub,
            run: runStub,
            getPreparedQuery: getPreparedQueryStub,
         });

         const mockRuntime = sinon.createStubInstance(Runtime);
         mockRuntime.loadModel.callsFake(loadModelStub);

         const sourceInfos: Malloy.SourceInfo[] = [
            {
               kind: "source",
               name: "flights",
               schema: { fields: [] },
               parameters: [
                  {
                     name: "carrier",
                     type: { kind: "string_type" } as Malloy.ParameterType,
                  },
               ],
            } as unknown as Malloy.SourceInfo,
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
            sourceInfos,
            [
               {
                  type: "code",
                  text: "run: flights -> { aggregate: c is count() }",
                  cellCompilationError: new Error("missing param"),
                  // no priorModelMaterializer — first cell
               },
            ],
            undefined,
            mockRuntime,
            new URL("file:///test/"),
         );

         const result = await model.executeNotebookCell(0, { carrier: "AA" });

         expect(loadModelStub.calledOnce).toBe(true);
         const modifiedText = loadModelStub.firstCall.args[0] as string;
         expect(modifiedText).toContain('flights(carrier is "AA")');
         expect(result.type).toBe("code");

         sinon.restore();
      });

      it("should not modify cell text for sources without parameters", async () => {
         const extendModelStub = sinon.stub();
         const loadFinalQueryStub = sinon.stub();
         extendModelStub.returns({ loadFinalQuery: loadFinalQueryStub });
         loadFinalQueryStub.returns({
            getPreparedResult: sinon.stub().resolves({ resultExplore: {} }),
            run: sinon.stub().resolves({}),
            getPreparedQuery: sinon.stub().resolves({ _query: { name: "q" } }),
         });

         const sourceInfos: Malloy.SourceInfo[] = [
            {
               kind: "source",
               name: "airports",
               schema: { fields: [] },
               // no parameters
            } as unknown as Malloy.SourceInfo,
         ];

         const cellText = "run: airports -> { aggregate: c is count() }";

         const model = new Model(
            packageName,
            "test.malloynb",
            {},
            "notebook",
            undefined,
            undefined,
            undefined,
            undefined,
            sourceInfos,
            [
               {
                  type: "code",
                  text: cellText,
                  cellCompilationError: new Error("some error"),
                  priorModelMaterializer: {
                     extendModel: extendModelStub,
                  } as unknown as ModelMaterializer,
               },
            ],
            undefined,
            sinon.createStubInstance(Runtime),
            new URL("file:///test/"),
         );

         await model.executeNotebookCell(0, { carrier: "AA" });

         const modifiedText = extendModelStub.firstCall.args[0] as string;
         // Text should be unchanged since airports has no parameters
         expect(modifiedText).toBe(cellText);

         sinon.restore();
      });
   });
});

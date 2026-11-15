// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { Model, ModelPolicy } from "../service/model";
import { PACKAGE_LOAD_PROTOCOL_VERSION } from "./protocol";
import {
   checkLoadPackageResult,
   decodeSerializedModel,
   PackageLoadProtocolError,
} from "./protocol_decode";

function result(overrides: Record<string, unknown> = {}) {
   return {
      type: "load-package-result",
      requestId: "job-1",
      protocolVersion: PACKAGE_LOAD_PROTOCOL_VERSION,
      packageMetadata: { name: "pkg" },
      models: [{ modelPath: "m.malloy", modelType: "model" }],
      loadDurationMs: 1,
      timings: {
         compileDurationMs: 0,
         schemaFetchDurationMs: 0,
         schemaFetchCount: 0,
      },
      ...overrides,
   };
}

/** Every optional field set, as a compiled model and a notebook carry them. */
const fullModel = {
   modelPath: "m.malloy",
   modelType: "model",
   modelDef: { contents: {}, exports: [] },
   modelInfo: { entries: [] },
   sourceInfos: [],
   sources: [],
   queries: [],
   filterMap: [["orders", []]],
   givens: [],
   notebookCells: [
      { type: "markdown", text: "# hi" },
      { type: "code", text: "run: x", cellModelDef: {} },
   ],
   modelSourceText: "source: x is duckdb.sql('select 1')",
   dataStyles: {},
   compileDurationMs: 3,
   problems: [],
   compilationError: undefined,
   authorizeWarnings: ["w"],
   renderTagResults: [{ label: "v" }],
   preaggregateCompanion: {},
   entities: plannedEntities(),
};

/** A well-formed entities plan for `m.malloy`, with `overrides`. */
function plannedEntities(overrides: Record<string, unknown> = {}) {
   return {
      status: "planned",
      relativePath: "m.malloy",
      packageRoot: "/pkg",
      provisional: false,
      untaken: [],
      givens: [],
      sourceInfos: [],
      metadata: [{ sourceName: "x", leading: [], trailing: [], joins: [] }],
      entities: [],
      findings: [],
      fingerprintFailures: [],
      durationMs: 1,
      ...overrides,
   };
}

describe("decodeSerializedModel", () => {
   it("returns a well-formed model as the same object, preserving aliasing", () => {
      const shared = { note: "gate" };
      const model = { ...fullModel, modelDef: { a: shared, b: shared } };
      const decoded = decodeSerializedModel(model);
      expect(decoded).toBe(model as never);
      const def = decoded.modelDef as { a: unknown; b: unknown };
      expect(def.a).toBe(def.b);
   });

   it("accepts a failed entities plan", () => {
      expect(() =>
         decodeSerializedModel({
            modelPath: "m.malloy",
            modelType: "model",
            entities: {
               status: "failed",
               relativePath: "m.malloy",
               packageRoot: "/pkg",
               error: "boom",
            },
         }),
      ).not.toThrow();
   });

   it("accepts a minimal model and a failed one", () => {
      expect(() =>
         decodeSerializedModel({
            modelPath: "a.malloynb",
            modelType: "notebook",
         }),
      ).not.toThrow();
      expect(() =>
         decodeSerializedModel({
            modelPath: "a.malloy",
            modelType: "model",
            compilationError: { name: "Error", message: "boom" },
         }),
      ).not.toThrow();
   });

   const malformed: Array<[string, unknown, RegExp]> = [
      ["a non-object", "nope", /must be an object, got string/],
      ["null", null, /must be an object, got null/],
      ["no modelPath", { modelType: "model" }, /has no modelPath/],
      [
         "an unknown modelType",
         { modelPath: "m.malloy", modelType: "view" },
         /modelType "view"; expected "model" or "notebook"/,
      ],
      [
         "a modelDef that is not an object",
         { modelPath: "m.malloy", modelType: "model", modelDef: "x" },
         /modelDef of kind string; expected object/,
      ],
      [
         "sources that are not an array",
         { modelPath: "m.malloy", modelType: "model", sources: {} },
         /sources of kind object; expected array/,
      ],
      [
         "a filterMap entry that is not a pair",
         { modelPath: "m.malloy", modelType: "model", filterMap: [["x"]] },
         /filterMap entry that is not a \[sourceName, filters\[\]\] pair/,
      ],
      [
         "authorizeWarnings that are not strings",
         { modelPath: "m.malloy", modelType: "model", authorizeWarnings: [1] },
         /authorizeWarnings of kind array; expected string\[\]/,
      ],
      [
         "a notebook cell without text",
         {
            modelPath: "n.malloynb",
            modelType: "notebook",
            notebookCells: [{ type: "code" }],
         },
         /notebook cell without a code\/markdown type and text/,
      ],
      [
         "an entities plan with an unknown status",
         {
            modelPath: "m.malloy",
            modelType: "model",
            entities: plannedEntities({ status: "done" }),
         },
         /entities plan with status "done"; expected "planned" or "failed"/,
      ],
      [
         "an entities plan for another model",
         {
            modelPath: "m.malloy",
            modelType: "model",
            entities: plannedEntities({ relativePath: "other.malloy" }),
         },
         /entities plan for "other.malloy"/,
      ],
      [
         "an entities plan whose entities are not an array",
         {
            modelPath: "m.malloy",
            modelType: "model",
            entities: plannedEntities({ entities: {} }),
         },
         /entities plan whose entities is of kind object; expected array/,
      ],
      [
         "an entities plan with a malformed source",
         {
            modelPath: "m.malloy",
            modelType: "model",
            entities: plannedEntities({ metadata: [{ sourceName: "x" }] }),
         },
         /entities plan with a source that is not/,
      ],
      [
         "a failed entities plan without an error",
         {
            modelPath: "m.malloy",
            modelType: "model",
            entities: {
               status: "failed",
               relativePath: "m.malloy",
               packageRoot: "/pkg",
            },
         },
         /failed entities plan without an error/,
      ],
      [
         "a compilationError without a message",
         {
            modelPath: "m.malloy",
            modelType: "model",
            compilationError: { name: "E" },
         },
         /compilationError without a name and message/,
      ],
   ];
   for (const [label, raw, message] of malformed) {
      it(`rejects ${label}`, () => {
         let thrown: unknown;
         try {
            decodeSerializedModel(raw);
         } catch (error) {
            thrown = error;
         }
         expect(thrown).toBeInstanceOf(PackageLoadProtocolError);
         expect((thrown as Error).message).toMatch(message);
         expect((thrown as Error).message).toStartWith(
            "Package-load worker protocol error: ",
         );
      });
   }
});

describe("checkLoadPackageResult", () => {
   it("returns a well-formed result as the same object", () => {
      const msg = result({ models: [fullModel] });
      expect(checkLoadPackageResult(msg)).toBe(msg as never);
   });

   it("rejects another protocol version before reading anything else", () => {
      expect(() =>
         checkLoadPackageResult(
            result({
               protocolVersion: PACKAGE_LOAD_PROTOCOL_VERSION + 1,
               models: "x",
            }),
         ),
      ).toThrow(
         `protocol version ${PACKAGE_LOAD_PROTOCOL_VERSION + 1}, this server ${PACKAGE_LOAD_PROTOCOL_VERSION}`,
      );
   });

   it("rejects a result with no protocol version", () => {
      const msg: Record<string, unknown> = result();
      delete msg.protocolVersion;
      expect(() => checkLoadPackageResult(msg)).toThrow(
         PackageLoadProtocolError,
      );
   });

   it("rejects a malformed envelope", () => {
      expect(() => checkLoadPackageResult(result({ models: {} }))).toThrow(
         /models of kind object; expected array/,
      );
      expect(() =>
         checkLoadPackageResult(result({ packageMetadata: null })),
      ).toThrow(/packageMetadata of kind null; expected object/);
      expect(() =>
         checkLoadPackageResult(result({ timings: undefined })),
      ).toThrow(/missing loadDurationMs or timings/);
   });

   it("rejects a result whose model is malformed", () => {
      expect(() =>
         checkLoadPackageResult(
            result({
               models: [
                  { modelPath: "m.malloy", modelType: "model", givens: 1 },
               ],
            }),
         ),
      ).toThrow(/serialized model "m\.malloy" has givens of kind number/);
   });
});

describe("Model.fromSerialized: decode check", () => {
   const hydrate = (data: unknown) =>
      Model.fromSerialized(
         "pkg",
         "/unused",
         {} as never,
         data as Parameters<typeof Model.fromSerialized>[3],
         { policy: ModelPolicy.standalone() },
      );

   it("hydrates a well-formed model with no modelDef as before", () => {
      const model = hydrate({ modelPath: "m.malloy", modelType: "model" });
      expect(model).toBeInstanceOf(Model);
   });

   it("refuses a malformed model with a protocol error instead of casting it", () => {
      expect(() =>
         hydrate({
            modelPath: "m.malloy",
            modelType: "model",
            sources: "nope",
         }),
      ).toThrow(PackageLoadProtocolError);
      expect(() => hydrate({ modelType: "model" })).toThrow(/has no modelPath/);
   });
});

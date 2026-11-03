// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";

import { ModelNotFoundError, NotQueryableError } from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import { ModelController } from "./model.controller";

/**
 * `getModel` returns the compiled model AND the file's own Malloy text, so a
 * client can show the code beside the compiled view of the same file with one
 * request. The store, environment, package, and model are sinon fakes: this is
 * about the controller's shaping of the response, not compilation.
 */
const SOURCE_TEXT = 'source: flights is duckdb.table("flights.parquet")\n';

function buildController(
   model: {
      getType: () => "model" | "notebook";
      getModel?: sinon.SinonStub;
      assertFileOnSurface?: () => void;
      showsFileText?: (text: string) => boolean;
   },
   getModelFileText: sinon.SinonStub = sinon.stub().resolves(SOURCE_TEXT),
) {
   // The text is read from the PACKAGE on getPackage's lock-free fast path,
   // never through the environment's locked read — the fake environment
   // deliberately has no getModelFileText.
   const fakePackage = {
      getModel: sinon.stub().returns({
         assertFileOnSurface: () => {},
         showsFileText: () => true,
         ...model,
      }),
      getModelFileText,
   };
   const fakeEnv = { getPackage: sinon.stub().resolves(fakePackage) };
   const fakeStore = {
      getEnvironment: sinon.stub().resolves(fakeEnv),
   } as unknown as EnvironmentStore;
   return { controller: new ModelController(fakeStore), getModelFileText };
}

const COMPILED = { modelPath: "flights.malloy", sourceInfos: ["{}"] };

describe("ModelController.getModel", () => {
   afterEach(() => sinon.restore());

   it("omits the sources a caller cannot enter when givens are supplied", async () => {
      const compiled = {
         modelPath: "clinic.malloy",
         sources: [
            { name: "open_src", givens: [] },
            {
               name: "chart",
               givens: [{ name: "ROLES", type: "string[]", secure: true }],
               authorize: ["'clinician' in $ROLES"],
            },
         ],
         queries: [
            { name: "q_open", sourceName: "open_src" },
            { name: "q_chart", sourceName: "chart" },
         ],
         sourceInfos: ['{"name":"open_src"}', '{"name":"chart"}'],
      };
      const { controller } = buildController({
         getType: () => "model",
         getModel: sinon.stub().resolves(compiled),
      });
      const asBilling = await controller.getModel(
         "env",
         "faa",
         "clinic.malloy",
         { givens: { ROLES: ["billing"] } },
      );
      expect(asBilling.sources?.map((s) => s.name)).toEqual(["open_src"]);
      expect(asBilling.queries?.map((q) => q.name)).toEqual(["q_open"]);
      expect(asBilling.sourceInfos).toEqual(['{"name":"open_src"}']);
      const asClinician = await controller.getModel(
         "env",
         "faa",
         "clinic.malloy",
         { givens: { ROLES: ["clinician"] } },
      );
      expect(asClinician.sources?.map((s) => s.name)).toEqual([
         "open_src",
         "chart",
      ]);
      const noGivens = await controller.getModel("env", "faa", "clinic.malloy");
      expect(noGivens.sources?.map((s) => s.name)).toEqual([
         "open_src",
         "chart",
      ]);
   });

   it("returns the compiled model with the file's text alongside, read on the lock-free fast path", async () => {
      const { controller, getModelFileText } = buildController({
         getType: () => "model",
         getModel: sinon.stub().resolves(COMPILED),
      });

      const result = await controller.getModel("env", "faa", "flights.malloy");

      expect(result).toEqual({ ...COMPILED, sourceText: SOURCE_TEXT });
      expect(getModelFileText.calledOnceWithExactly("flights.malloy")).toBe(
         true,
      );
   });

   it("still returns the compiled model when the text cannot be read", async () => {
      const { controller } = buildController(
         { getType: () => "model", getModel: sinon.stub().resolves(COMPILED) },
         sinon.stub().rejects(new Error("EACCES")),
      );

      const result = await controller.getModel("env", "faa", "flights.malloy");

      expect(result).toEqual(COMPILED);
      expect("sourceText" in result).toBe(false);
   });

   it("refuses a file off the surface with the query route's 404, unwrapped", async () => {
      const refusal = new NotQueryableError(
         'No queryable model "orders.malloy".',
      );
      const { controller, getModelFileText } = buildController({
         getType: () => "model",
         getModel: sinon.stub().resolves(COMPILED),
         assertFileOnSurface: () => {
            throw refusal;
         },
      });

      await expect(
         controller.getModel("env", "faa", "orders.malloy"),
      ).rejects.toBe(refusal);
      expect(getModelFileText.called).toBe(false);
   });

   it("withholds the text when the model says it names something unpublished", async () => {
      const showsFileText = sinon.stub().returns(false);
      const { controller } = buildController({
         getType: () => "model",
         getModel: sinon.stub().resolves(COMPILED),
         showsFileText,
      });

      const result = await controller.getModel("env", "faa", "index.malloy");

      expect(result).toEqual(COMPILED);
      // The decision is made on the text itself.
      expect(showsFileText.calledOnceWithExactly(SOURCE_TEXT)).toBe(true);
   });

   it("returns a file off the surface, with its text, when the caller asks to see it", async () => {
      const assertFileOnSurface = sinon
         .stub()
         .throws(new NotQueryableError('No queryable model "orders.malloy".'));
      const showsFileText = sinon.stub().returns(false);
      const { controller } = buildController({
         getType: () => "model",
         getModel: sinon.stub().resolves(COMPILED),
         assertFileOnSurface,
         showsFileText,
      });

      const result = await controller.getModel("env", "faa", "orders.malloy", {
         includeHiddenFilesAndSources: true,
      });

      // The compiled body is the model's own, curated as ever; only the 404
      // and the text's withholding are lifted.
      expect(result).toEqual({ ...COMPILED, sourceText: SOURCE_TEXT });
      expect(assertFileOnSurface.called).toBe(false);
      expect(showsFileText.called).toBe(false);
   });

   it("keeps both checks when includeHiddenFilesAndSources is false", async () => {
      const refusal = new NotQueryableError(
         'No queryable model "orders.malloy".',
      );
      const { controller } = buildController({
         getType: () => "model",
         getModel: sinon.stub().resolves(COMPILED),
         assertFileOnSurface: () => {
            throw refusal;
         },
      });

      await expect(
         controller.getModel("env", "faa", "orders.malloy", {
            includeHiddenFilesAndSources: false,
         }),
      ).rejects.toBe(refusal);
   });

   it("still refuses a notebook before reading anything", async () => {
      const { controller, getModelFileText } = buildController({
         getType: () => "notebook",
      });

      await expect(
         controller.getModel("env", "faa", "README.malloy"),
      ).rejects.toBeInstanceOf(ModelNotFoundError);
      expect(getModelFileText.called).toBe(false);
   });
});

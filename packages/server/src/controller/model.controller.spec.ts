// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";

import { ModelNotFoundError } from "../errors";
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
   model: { getType: () => "model" | "notebook"; getModel?: sinon.SinonStub },
   getModelFileText: sinon.SinonStub = sinon.stub().resolves(SOURCE_TEXT),
) {
   // The text is read from the PACKAGE on getPackage's lock-free fast path,
   // never through the environment's locked read — the fake environment
   // deliberately has no getModelFileText.
   const fakePackage = {
      getModel: sinon.stub().returns(model),
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

   it("still refuses a notebook before reading anything", async () => {
      const { controller, getModelFileText } = buildController({
         getType: () => "notebook",
      });

      await expect(
         controller.getModel("env", "faa", "README.malloynb"),
      ).rejects.toBeInstanceOf(ModelNotFoundError);
      expect(getModelFileText.called).toBe(false);
   });
});

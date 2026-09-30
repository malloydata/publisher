// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import sinon from "sinon";
import { BadRequestError } from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import { CompileController } from "./compile.controller";

const controllerWith = () => {
   const compileSource = sinon.stub().resolves({ problems: [] });
   const store = {
      getEnvironment: async () => ({ compileSource }),
   } as unknown as EnvironmentStore;
   return { controller: new CompileController(store), compileSource };
};

describe("CompileController.compile", () => {
   it("refuses a source that is not a string, before anything compiles", async () => {
      const { controller, compileSource } = controllerWith();
      for (const source of [{ length: 1e12 }, ["run: x"], 42]) {
         await expect(
            controller.compile("env", "pkg", "m.malloy", source),
         ).rejects.toBeInstanceOf(BadRequestError);
      }
      expect(compileSource.called).toBe(false);
   });

   it("passes a string source, and an absent one, through", async () => {
      const { controller, compileSource } = controllerWith();
      await controller.compile("env", "pkg", "m.malloy", "run: x");
      await controller.compile("env", "pkg", "m.malloy", undefined);
      expect(compileSource.firstCall.args[2]).toBe("run: x");
      expect(compileSource.secondCall.args[2]).toBeUndefined();
   });
});

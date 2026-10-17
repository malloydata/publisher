// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import sinon from "sinon";
import { BadRequestError } from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import { CompileController } from "./compile.controller";

const controllerWith = (compiled: object = { problems: [] }) => {
   const compileSource = sinon.stub().resolves(compiled);
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

   it.each([
      ["no problems", [], "success"],
      ["only warnings", ["warn", "info"], "success"],
      ["any error", ["warn", "error"], "error"],
   ])("status with %s is %s", async (_, severities, status) => {
      const problems = severities.map((severity) => ({
         severity,
         message: "m",
      }));
      const { controller } = controllerWith({ problems });
      const result = await controller.compile("env", "pkg", "m.malloy", "x");
      expect(result).toEqual({ status, problems } as typeof result);
   });

   it("passes sql through and adds the legacy kind to a document", async () => {
      const { controller, compileSource } = controllerWith({
         problems: [],
         sql: "SELECT 1",
         document: { layout: "grid", cells: [] },
      });
      const result = await controller.compile(
         "env",
         "pkg",
         "m.malloy",
         "x",
         true,
         { G: 1 },
         "file",
      );
      expect(compileSource.firstCall.args.slice(3)).toEqual([
         true,
         { G: 1 },
         "file",
      ]);
      expect(result.sql).toBe("SELECT 1");
      expect(result.document).toEqual({
         layout: "grid",
         cells: [],
         kind: "dashboard",
      });
   });
});

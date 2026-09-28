// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type GivenValue } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError, BadRequestError } from "../errors";
import { Environment } from "./environment";

// /compile takes given values too (they bind when it builds SQL), so a value a
// `filter<T>` given cannot read is refused there the same way the query route
// refuses it, rather than returned as Malloy's `[object Object]` problem.

const MODEL = `##! experimental.givens

given:
  FLAG :: filter<boolean> is f''
  ROLE :: string

source: orders is duckdb.sql("SELECT 1 as id, true as big") extend {
  measure: c is count()
  view: filtered is { where: big ~ $FLAG; aggregate: c }
}

#(authorize) 'analyst' = $ROLE
source: gated is orders
`;

describe("compileSource checks filter given values", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-compile-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            JSON.stringify({ name: "pkg", description: "compile-givens" }),
         );
         await fs.writeFile(path.join(stagingPath, "model.malloy"), MODEL);
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compile = (givens?: Record<string, GivenValue>) =>
      env.compileSource(
         "pkg",
         "model.malloy",
         "run: orders -> filtered",
         true,
         givens,
      );

   it("refuses a value the given's type cannot read, with the reason", async () => {
      const error = await compile({ FLAG: "asdf" }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toBe(
         "Invalid value for given FLAG (filter<boolean>): Illegal boolean " +
            "filter 'asdf'. Must be one of true,=true,false,=false,null,none. " +
            "Fix: send a filter<boolean> expression, or leave FLAG unset to " +
            "use its default.",
      );
   });

   it("checks the submitted text's givens on a model path not on disk", async () => {
      const error = await env
         .compileSource(
            "pkg",
            "new.malloy",
            "##! experimental.givens\n" +
               "given: NEW_FLAG :: filter<boolean> is f''\n" +
               'source: t is duckdb.sql("SELECT true as big") extend {\n' +
               "  measure: c is count()\n" +
               "}\n" +
               "run: t -> { where: big ~ $NEW_FLAG; aggregate: c }",
            true,
            { NEW_FLAG: "asdf" },
            "file",
         )
         .then(
            () => undefined,
            (e: unknown) => e,
         );
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toStartWith(
         "Invalid value for given NEW_FLAG (filter<boolean>): ",
      );
   });

   it("checks an edit at file scope against the edit's types, not the cached model's", async () => {
      // The edit makes FLAG a filter<number>: a number filter the cached
      // filter<boolean> would refuse now compiles, and `asdf` is refused with
      // the number parser's reason.
      const edited =
         "##! experimental.givens\n" +
         "given: FLAG :: filter<number> is f''\n" +
         'source: orders is duckdb.sql("SELECT 1 as id, 3 as big") extend {\n' +
         "  measure: c is count()\n" +
         "  view: filtered is { where: big ~ $FLAG; aggregate: c }\n" +
         "}\n" +
         "run: orders -> filtered";
      const compileEdit = (givens: Record<string, GivenValue>) =>
         env.compileSource("pkg", "model.malloy", edited, true, givens, "file");

      const { problems, sql } = await compileEdit({ FLAG: ">2" });
      expect(problems.filter((p) => p.severity === "error")).toEqual([]);
      expect(sql).toBeDefined();

      const error = await compileEdit({ FLAG: "asdf" }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toStartWith(
         "Invalid value for given FLAG (filter<number>): ",
      );
   });

   it("denies a caller the gate refuses before judging a value", async () => {
      const compileError = await env
         .compileSource("pkg", "model.malloy", "run: gated -> filtered", true, {
            FLAG: "asdf",
         })
         .then(
            () => undefined,
            (e: unknown) => e,
         );
      expect(compileError).toBeInstanceOf(AccessDeniedError);

      const model = (await env.getPackage("pkg")).getModel("model.malloy")!;
      const queryError = await model
         .getQueryResults(
            undefined,
            undefined,
            "run: gated -> filtered",
            undefined,
            undefined,
            { FLAG: "asdf" },
         )
         .then(
            () => undefined,
            (e: unknown) => e,
         );
      expect(queryError).toBeInstanceOf(AccessDeniedError);
   });

   it("compiles with a value it can read, and with none", async () => {
      for (const givens of [{ FLAG: "true" }, undefined]) {
         const { problems, sql } = await compile(givens);
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
         expect(sql).toBeDefined();
      }
   });
});

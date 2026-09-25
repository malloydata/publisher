// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type GivenValue } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { BadRequestError } from "../errors";
import { Environment } from "./environment";

// /compile takes given values too (they bind when it builds SQL), so a value a
// `filter<T>` given cannot read is refused there the same way the query route
// refuses it, rather than returned as Malloy's `[object Object]` problem.

const MODEL = `##! experimental.givens

given: FLAG :: filter<boolean> is f''

source: orders is duckdb.sql("SELECT 1 as id, true as big") extend {
  measure: c is count()
  view: filtered is { where: big ~ $FLAG; aggregate: c }
}
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

   it("compiles with a value it can read, and with none", async () => {
      for (const givens of [{ FLAG: "true" }, undefined]) {
         const { problems, sql } = await compile(givens);
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
         expect(sql).toBeDefined();
      }
   });
});

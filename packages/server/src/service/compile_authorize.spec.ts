import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError } from "../errors";
import { Environment } from "./environment";

// End-to-end gate on the /compile path (PR4). Exercises environment.compileSource
// through a real installed package, not just the Model primitives — pins that
// the early gate AND the compiled-source backstop fire, the latter REGARDLESS of
// includeSql (a compile-time schema oracle is closed even with no SQL extraction).

const PUBLISHER_JSON = JSON.stringify({
   name: "pkg",
   description: "compile-gate",
});

// `gated` is locked to $ROLE='analyst'; `open_src` is unrestricted.
const MODEL = `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }

source: open_src is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }
`;

describe("compile-path authorize gate (compileSource)", () => {
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
            PUBLISHER_JSON,
         );
         await fs.writeFile(path.join(stagingPath, "model.malloy"), MODEL);
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compile = (source: string, givens?: Record<string, string>) =>
      env.compileSource("pkg", "model.malloy", source, false, givens);

   it("denies a direct gated source without the satisfying given (early gate)", async () => {
      await expect(
         compile("run: gated -> { aggregate: c }"),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a gated source reached via the LAST run: statement (backstop, includeSql=false)", async () => {
      // Regression guard: the early gate only matches the first `run:` (ungated
      // open_src here), so the gated source in the executed final statement is
      // caught only by the compiled-source backstop — which must run even when
      // no SQL is requested.
      await expect(
         compile(
            "run: open_src -> { aggregate: c }\nrun: gated -> { aggregate: c }",
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("allows the gated source when the given satisfies the gate", async () => {
      const { problems } = await compile("run: gated -> { aggregate: c }", {
         ROLE: "analyst",
      });
      expect(problems).toBeDefined();
   });

   it("leaves an ungated source compilable without any given", async () => {
      const { problems } = await compile("run: open_src -> { aggregate: c }");
      expect(problems).toBeDefined();
   });
});

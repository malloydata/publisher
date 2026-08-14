import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError, NotQueryableError } from "../errors";
import { Environment } from "./environment";

// End-to-end gate on the /compile path. Exercises environment.compileSource
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

   it("rejects an authorize annotation in the submitted source", async () => {
      // /compile appends the text to the model, so a caller-declared gate would
      // land alongside the author's — and includeSql makes this the door worth
      // the most to an attacker.
      await expect(
         env.compileSource(
            "pkg",
            "model.malloy",
            `#(authorize) "true"
             source: mine is gated extend {}
             run: mine -> { aggregate: c }`,
            true,
         ),
      ).rejects.toThrow(/authorize` annotation is not permitted/);
   });

   it("denies a gate laundered through an unrelated annotation, with includeSql", async () => {
      // No "authorize" byte in the request: the render tag alone used to move
      // `gated`'s annotations off the struct and hand back its SQL.
      await expect(
         env.compileSource(
            "pkg",
            "model.malloy",
            `# some_render_tag
             source: mine is gated extend {}
             run: mine -> { aggregate: c }`,
            true,
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

describe("compile-path is exempt from the query boundary (compileSource)", () => {
   // /compile is the authoring loop, and the boundary is discovery curation,
   // not access control. Gating compile made a curated package un-authorable:
   // the QA session that set explores + queryableSources: "declared" (HANDOFF
   // CR-5) watched every per-file compile 404 with "Query target is not
   // queryable". The query path keeps the boundary in full (query_boundary
   // .spec.ts); authorize keeps gating compile (the suite above).
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-compileb-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            JSON.stringify({
               name: "pkg",
               explores: ["index.malloy"],
               queryableSources: "declared",
            }),
         );
         await fs.writeFile(
            path.join(stagingPath, "base.malloy"),
            `source: base_source is duckdb.sql("select 1 as id") extend {
  measure: c is count()
}`,
         );
         await fs.writeFile(
            path.join(stagingPath, "index.malloy"),
            `import "base.malloy"
source: helper is duckdb.sql("select 1 as id") extend { measure: hc is count() }
source: customers is duckdb.sql("select 1 as id") extend { measure: c is count() }
export { customers }`,
         );
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("compiles against a non-explores model file (the per-file authoring loop)", async () => {
      const { problems } = await env.compileSource(
         "pkg",
         "base.malloy",
         "run: base_source -> { aggregate: c }",
         false,
      );
      expect(problems).toBeDefined();
   });

   it("compiles text targeting a non-exported source inside the explores file", async () => {
      const { problems } = await env.compileSource(
         "pkg",
         "index.malloy",
         "run: helper -> { aggregate: hc }",
         false,
      );
      expect(problems).toBeDefined();
   });

   it("the QUERY path still denies the same hidden targets (exemption is compile-only)", async () => {
      const pkg = await env.getPackage("pkg");
      await expect(
         pkg
            .getModel("index.malloy")!
            .getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: hc }",
            ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });
});

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

describe("compile exemption is not an existence oracle (compileSource)", () => {
   // The exemption must not let /compile distinguish "this gated source exists"
   // from "no such name". A source that is BOTH boundary-hidden and
   // #(authorize)-gated has to answer with the boundary's generic 404, the same
   // as the query surface — otherwise an unauthorized caller enumerates the
   // hidden namespace one 403 at a time. A gated source the boundary does NOT
   // hide keeps its informative 403.
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-oracle-"));
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
         // Hidden file (not in explores) holding a gated source.
         await fs.writeFile(
            path.join(stagingPath, "secret.malloy"),
            `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: hidden_gated is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }`,
         );
         // Listed file with a gated source that IS exported (visible). It also
         // imports the hidden file, so the evasion probes below can resolve
         // hidden_gated in a LISTED model's namespace; the import does not
         // export it, so it stays boundary-hidden.
         await fs.writeFile(
            path.join(stagingPath, "index.malloy"),
            `##! experimental.givens
import "secret.malloy"

#(authorize) "$ROLE = 'analyst'"
source: visible_gated is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }

source: customers is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }
export { customers, visible_gated }`,
         );
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("answers 404, not 403, for a source that is hidden AND gated", async () => {
      // Pre-exemption this was a NotQueryableError because the boundary ran
      // first; the exemption must preserve the OUTCOME for a hidden target even
      // though the boundary no longer blocks compilation itself.
      await expect(
         env.compileSource(
            "pkg",
            "secret.malloy",
            "run: hidden_gated -> { aggregate: c }",
            false,
         ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });

   it("masks it with includeSql too (the door worth the most to an attacker)", async () => {
      await expect(
         env.compileSource(
            "pkg",
            "secret.malloy",
            "run: hidden_gated -> { aggregate: c }",
            true,
         ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });

   it("keeps the informative 403 for a gated source the boundary does NOT hide", async () => {
      await expect(
         env.compileSource(
            "pkg",
            "index.malloy",
            "run: visible_gated -> { aggregate: c }",
            false,
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("a multi-statement decoy cannot keep the 403 that names the hidden source", async () => {
      // The early text gate resolves only the FIRST run: statement (the
      // curated decoy), so converting the denial on surface syntax alone let
      // this probe through with `Access denied for source "hidden_gated"` —
      // the verbatim existence proof the mask exists to withhold. The
      // conversion settles the COMPILED run target instead.
      await expect(
         env.compileSource(
            "pkg",
            "index.malloy",
            "run: customers -> { aggregate: c }\nrun: hidden_gated -> { aggregate: c }",
            false,
         ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });

   it("a derivation alias over the hidden source is masked too", async () => {
      // `probe` is the caller's own alias, so the denial names it rather than
      // hidden_gated — but a 403 here still separates exists-and-gated from
      // nonexistent (which fails as a compile error), so it enumerates the
      // hidden namespace all the same. The compiled-target conversion walks
      // the same derivation rule as the query surface: probe derives from a
      // non-curated source, so the query surface answers 404, and so must this.
      await expect(
         env.compileSource(
            "pkg",
            "index.malloy",
            "source: probe is hidden_gated extend {}\nrun: probe -> { aggregate: c }",
            false,
         ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });

   it("a derivation alias over the VISIBLE gated source keeps its 403", async () => {
      // The guard against over-tightening: deriving from a curated gated
      // source is admitted by the query surface (derivesFromCurated), so the
      // informative denial must survive the conversion.
      await expect(
         env.compileSource(
            "pkg",
            "index.malloy",
            "source: mine is visible_gated extend {}\nrun: mine -> { aggregate: c }",
            false,
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("still compiles the hidden source once the given satisfies its gate", async () => {
      // The mask is only on the denial path — authoring against a hidden file
      // stays possible for a caller who passes the gate.
      const { problems } = await env.compileSource(
         "pkg",
         "secret.malloy",
         "run: hidden_gated -> { aggregate: c }",
         false,
         { ROLE: "analyst" },
      );
      expect(problems).toBeDefined();
   });
});

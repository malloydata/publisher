import { type GivenValue } from "@malloydata/malloy";
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

// `gated` is locked to $ROLE='analyst'; `open_src` is unrestricted; `row_gated`
// is locked by a ROW-FIELD condition (`org_id`), not just a given.
const MODEL = `##! experimental.givens

given:
  ROLE :: string
  GROUPS :: number[]

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }

source: open_src is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }

#(authorize) "org_id in $GROUPS"
source: row_gated is duckdb.sql("SELECT 1 as x, 1 as org_id") extend { measure: c is count() }
`;

/**
 * MODEL with every `#(authorize)` line removed.
 *
 * The tests below submit a caller edit that has dropped the author's gate, and
 * assert the on-disk gate denies anyway. They must drop ALL of them: a leftover
 * gate byte in caller-submitted text is refused up front by
 * `assertNoCallerAuthorizeAnnotation` (a BadRequestError), which is a different
 * refusal than the one under test and would pass for the wrong reason.
 */
const withoutGates = (model: string): string =>
   model
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("#(authorize)"))
      .join("\n");

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

   const compile = (source: string, givens?: Record<string, GivenValue>) =>
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

   it("denies the gated source even when the given satisfies the gate — /compile has no recompile step", async () => {
      // Every gate is a row filter now, and `/compile`'s backstop
      // (`assertAuthorizedForRunnable`, called with no `recompile` option)
      // has nothing to apply that filter to — it is a probe, not a query
      // execution. `Model.authorizeAndBindRunnable`'s own doc: "A row-level
      // gate with no `options.recompile` denies". This used to admit when
      // `$ROLE = 'analyst'` was given-only (a whole-source boolean with a
      // real admit/deny answer, no filter to apply); the collapse to one
      // gate concept removes that shortcut, so a satisfied given no longer
      // matters here.
      await expect(
         compile("run: gated -> { aggregate: c }", { ROLE: "analyst" }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies the gated source when the given does NOT satisfy the gate", async () => {
      await expect(
         compile("run: gated -> { aggregate: c }", { ROLE: "nobody" }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("CRITICAL — a row-level gate still denies even with a satisfying given, when the compile path cannot graft it", async () => {
      // `row_gated`'s condition reads `org_id`, a real column. The same
      // "independently recompiled model" shape that defeats
      // `resolveGraftTarget` here has no fallback to fall back TO — every
      // gate is a row filter now, and a filter with nowhere to attach
      // rejects outright, so this must fail to compile and deny — never
      // admit a caller whose given would satisfy the row condition, since
      // there is no scope here to graft the row filter onto at all.
      await expect(
         compile("run: row_gated -> { aggregate: c }", { GROUPS: [1] }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

   // The `file`-scope backstop discovers a run target's gate by walking the
   // COMPILED RUNNABLE's own struct (`resolveRunTargetStruct`), which reflects
   // whatever text the caller submitted. A caller who strips the
   // `#(authorize)` annotation from that text compiles a struct with no gate
   // of its own — so the walk alone finds nothing. `Model.
   // collectAuthorizeEntryPointGates` also folds in `gateModel`'s own
   // on-disk `entryPointGatesBySource` entries for the run target's source
   // name, which the caller's submitted text cannot edit, so the gate is
   // still found and — being row-level with no `recompile` step to apply a
   // filter to — still denies.
   it("denies when the submitted edit deletes the gate (file scope)", async () => {
      const withoutGate = withoutGates(MODEL);
      await expect(
         env.compileSource(
            "pkg",
            "model.malloy",
            `${withoutGate}\nrun: gated -> { aggregate: c }`,
            false,
            undefined,
            "file",
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a gated final run after an open decoy, with the gate stripped (file scope)", async () => {
      const withoutGate = withoutGates(MODEL);
      await expect(
         env.compileSource(
            "pkg",
            "model.malloy",
            `${withoutGate}
run: open_src -> { aggregate: c }
run: gated -> { aggregate: c }`,
            false,
            undefined,
            "file",
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("a brand-new model path cannot bypass an imported source gate", async () => {
      await expect(
         env.compileSource(
            "pkg",
            "scratch.malloy",
            `import "model.malloy"
run: gated -> { aggregate: c }`,
            true,
            undefined,
            "file",
         ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });

   // Same fold-in as the two tests above, for a file added since the last
   // package load: `Environment.compileSource` compiles that on-disk file
   // fresh as an ephemeral `gateModel` (`Model.create`, reading the real
   // file, not the caller's substituted text), so its
   // `entryPointGatesBySource` is still authoritative and still denies.
   it("denies for a file added since the last load, with its gate stripped, regardless of ROLE", async () => {
      await fs.writeFile(
         path.join(rootDir, "env", "pkg", "stale.malloy"),
         MODEL,
      );
      const submitted = `${withoutGates(MODEL)}\nrun: gated -> { aggregate: c }`;

      await expect(
         env.compileSource(
            "pkg",
            "stale.malloy",
            submitted,
            false,
            undefined,
            "file",
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);

      await expect(
         env.compileSource(
            "pkg",
            "stale.malloy",
            submitted,
            false,
            { ROLE: "analyst" },
            "file",
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

   it("denies (masked as NotQueryableError) the hidden source even when the given satisfies its gate", async () => {
      // Every gate is a row filter now, and `/compile` has no recompile step
      // to apply one to (see the identical case in "compile-path authorize
      // gate" above) — a satisfied given no longer admits here, the same
      // accepted narrowing as the given-only-to-row-level collapse elsewhere.
      await expect(
         env.compileSource(
            "pkg",
            "secret.malloy",
            "run: hidden_gated -> { aggregate: c }",
            false,
            { ROLE: "analyst" },
         ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });
});

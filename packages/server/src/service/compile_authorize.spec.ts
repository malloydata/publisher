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

source: gated is duckdb.sql("SELECT 1 as x") extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

source: open_src is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }

source: row_gated is duckdb.sql("SELECT 1 as x, 1 as org_id") extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS
}

source: always_true is duckdb.sql("SELECT 1 as x") extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is 1 = 1
}

source: always_false is duckdb.sql("SELECT 1 as x") extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}
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

   it("ADMITS the gated source at APPEND scope when every given the gate reads is supplied — the authoring loop", async () => {
      // Scope "append" compiles the caller's text against a VIRTUAL model, so
      // the run target's `SourceDef` belongs to a different `ModelDef` than
      // the gate model's — but `collectAuthorizeEntryPointGates`'s fold-in
      // (`model.ts`) replaces that ephemeral entry with the ON-DISK "gated"
      // entry by CONTENT (same label/exprs/selfContained), whose `struct` IS
      // a `this.modelDef.contents` value. `resolveGraftTarget` then resolves
      // it directly, and the `checkOnly` decidable escape admits: every given
      // the gate reads (`ROLE`) was supplied. Denying here protected nothing
      // the query path doesn't already enforce (a query still gets FILTERED
      // rows, never raw ones) while making a gated source un-authorable.
      const { problems } = await compile("run: gated -> { aggregate: c }", {
         ROLE: "analyst",
      });
      expect(problems).toEqual([]);
   });

   it("ADMITS at APPEND scope even when the given does NOT satisfy the gate — /compile decides on PRESENCE, not the value", async () => {
      // The `checkOnly` decidable escape only asks whether the caller
      // engaged with the gate (supplied SOME value for every given it
      // reads), not whether that value would pass — the real row filter is
      // evaluated against the actual value at RUN time, never here. `/compile`
      // returns a schema, not rows, so admitting on presence alone reveals
      // nothing a query wouldn't already answer with (possibly empty)
      // filtered rows.
      const { problems } = await compile("run: gated -> { aggregate: c }", {
         ROLE: "nobody",
      });
      expect(problems).toEqual([]);
   });

   it("ADMITS a row-field gate (`org_id`) at APPEND scope once its given is supplied", async () => {
      // `row_gated`'s condition reads `org_id`, a real column — the fold-in
      // still resolves the on-disk twin for it exactly as it does for the
      // given-only "gated" case above, so the same decidable escape applies:
      // `GROUPS` was supplied, so this compiles without running the query.
      const { problems } = await compile("run: row_gated -> { aggregate: c }", {
         GROUPS: [1],
      });
      expect(problems).toEqual([]);
   });

   it("ADMITS a gate referencing NO given (`authorized is 1 = 1`) with no given supplied at all", async () => {
      // Routed defect fix: `givenNames.length === 0` is decidable by
      // construction — there is no caller value left to wait on, since
      // `/compile` executes nothing. Before the fix this denied (the
      // dimension form's `literalAtoms` was hardcoded empty, so
      // `constantTrue` could never be true), while the query path admitted
      // every row for the identical gate — an inconsistency between the two
      // enforcement points for the exact same access rule.
      const { problems } = await compile(
         "run: always_true -> { aggregate: c }",
      );
      expect(problems).toEqual([]);
   });

   it("ADMITS a gate referencing NO given that is constant `false` — /compile decides on PRESENCE, not the value, same as a supplied-but-wrong given", async () => {
      // `givenNames.length === 0` is decidable regardless of which way the
      // gate itself resolves — /compile never runs the query, so there is
      // no row-truth to check here either way. The deny-everyone kill
      // switch is a QUERY-path guarantee (a real run grafts `where: false`
      // and gets zero rows, pinned in
      // `row_level_authorize.integration.spec.ts`), not a `/compile` one.
      const { problems } = await compile(
         "run: always_false -> { aggregate: c }",
      );
      expect(problems).toEqual([]);
   });

   it("leaves an ungated source compilable without any given", async () => {
      const { problems } = await compile("run: open_src -> { aggregate: c }");
      expect(problems).toEqual([]);
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
   it("denies for a file added since the last load, with its gate stripped, when the caller supplies no given", async () => {
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

      // A caller who DOES supply every given the on-disk gate reads is
      // compiling their own authoring loop, and compile never runs the query
      // — see `Model.authorizeAndBindRunnable`'s `checkOnly` branch. The gate
      // still applies in full on the query path.
      await expect(
         env.compileSource(
            "pkg",
            "stale.malloy",
            submitted,
            false,
            { ROLE: "analyst" },
            "file",
         ),
      ).resolves.toBeDefined();
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

source: hidden_gated is duckdb.sql("SELECT 1 as x") extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}`,
         );
         // Listed file with a gated source that IS exported (visible). It also
         // imports the hidden file, so the evasion probes below can resolve
         // hidden_gated in a LISTED model's namespace; the import does not
         // export it, so it stays boundary-hidden.
         await fs.writeFile(
            path.join(stagingPath, "index.malloy"),
            `##! experimental.givens
import "secret.malloy"

source: visible_gated is duckdb.sql("SELECT 1 as x") extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

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

   it("ADMITS the hidden source once the gate's given is supplied — denyHiddenAsNotQueryable scrubs a denial, it is not an access check", async () => {
      // `denyHiddenAsNotQueryable` (environment.ts) runs the gate first and
      // only converts to `NotQueryableError` when the gate itself threw
      // `AccessDeniedError`. That is intended: the query boundary
      // (`explores`/`queryableSources`, the *what* axis) deliberately does
      // NOT apply to `/compile` — a boundary-enforcing `/compile` made a
      // curated package un-authorable (QA HANDOFF CR-5 had every per-file
      // compile 404 the moment `queryableSources: "declared"` was set). Only
      // `#(authorize)` (the *who* axis) gates `/compile`, and it still
      // applies here in full. Once the fold-in (model.ts) lets the on-disk
      // gate's `checkOnly` decidable escape admit on a supplied `$ROLE`, the
      // gate succeeds, `convert()` never runs, and this resolves — the
      // caller is authorized, so there is nothing left for the 404 mask to
      // scrub.
      const { problems, sql } = await env.compileSource(
         "pkg",
         "secret.malloy",
         "run: hidden_gated -> { aggregate: c }",
         false,
         { ROLE: "analyst" },
      );
      // The disclosure surface: the schema resolved with no diagnostics
      // (compile succeeded — the caller now knows `hidden_gated` exists and
      // is queryable), and `sql` stays absent because this call passed
      // `includeSql: false`, not because the gate blocked it.
      expect(problems).toEqual([]);
      expect(sql).toBeUndefined();
   });

   it("with includeSql AND a non-satisfying given, returns the hidden source's UNGRAFTED SQL — the widest point of the accepted trade", async () => {
      // Same admission as above (presence, not value, decides), but now with
      // `includeSql: true` and `ROLE: "nobody"` — a value that would fail the
      // gate at run time. `/compile` never runs the query, so there is no row
      // filter to apply here even for a value that would have failed one:
      // the returned SQL is the plain compiled query, with no `$ROLE`
      // reference at all. This is the actual shape of the residual the
      // product owner accepted, not just that some string was returned.
      const { problems, sql } = await env.compileSource(
         "pkg",
         "secret.malloy",
         "run: hidden_gated -> { aggregate: c }",
         true,
         { ROLE: "nobody" },
      );
      expect(problems).toEqual([]);
      expect(sql).toContain("FROM (SELECT 1 as x) as base");
      expect(sql).not.toMatch(/ROLE|analyst|nobody/i);
   });
});

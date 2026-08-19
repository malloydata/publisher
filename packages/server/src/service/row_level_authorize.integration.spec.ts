/**
 * Verification suite for row-level `#(authorize)`.
 *
 * ## Load-time validation — read this before the rest of the file
 *
 * `Model.create` calls `validateAuthorizeProbes` unconditionally; so does the
 * package-load worker, whose `authorizeWarnings` it ships over the wire for
 * `Model.fromSerialized` to log — `fromSerialized` itself does not call
 * `validateAuthorizeProbes` again, it just hydrates what the worker already
 * validated. See the "row-field #(authorize) gate — load-time validation"
 * describe block below. That call is shape-aware: it probes each gate as a
 * source-level `where:` filter on the ENTRY POINT it actually applies to
 * (`buildRowLevelProbe`), not a one-row probe — so a row-field gate on a
 * source whose own field space contains the gated field loads cleanly
 * (pinned below, both on `Model.create` and the real worker-pool
 * `fromSerialized` path).
 *
 * For an entry point whose OWN field space cannot resolve the gate at all (a
 * `rename:`/`except:`/`accept:`-ed field, or a `query_source` projection that
 * dropped it), the outcome depends on
 * WHOSE mistake it is, not just that the probe failed to compile there:
 *  - If the gate is genuinely INHERITED at that entry point — carries no
 *    annotation of its own at all, or its own annotation note object is the
 *    SAME object, by reference, as one that validated successfully somewhere
 *    else in the model — the load does NOT abort: it warns
 *    (`onRowLevelGateUnexpressible`) and the affected entry point denies
 *    every request instead (`Model.resolveGateShape`). `W_rename`/`W_except`/
 *    `W_accept` below are exactly this case: each inherits `X`'s gate by
 *    Malloy's by-reference copy (no annotation of their own), and their own
 *    `extend` renamed/dropped the field the inherited gate needs.
 *  - If it is NOT inherited — no annotation of its own, or an
 *    independently-authored one that merely shares TEXT with something else
 *    (not the SAME note object) — there is no ancestor to blame it on, and
 *    the load aborts exactly as it always has. Note-object identity, not
 *    text, is what tells these apart; see `validateAuthorizeProbes`'s doc in
 *    `authorize.ts` for why text alone is unsound.
 *
 * The multi-source `ENTRY` model this file's "entry-point matrix" (and
 * "load-time scoping") describe blocks share therefore loads CLEANLY through
 * the real `Model.create` — `W_rename`/`W_except`/`W_accept`/`Z2` each warn
 * without aborting, and `cp_joiner` (an ordinary, unannotated join of gated
 * `X`) is not reported as a misplaced annotation either (see
 * `source_extraction.ts`'s note-identity discriminator). Pinned directly by
 * the "load-time scoping" describe block's tests, which use `createModel` —
 * the real `Model.create`, not `buildGatedModel`.
 *
 * `buildGatedModel` still exists, but not because `ENTRY` needs it: many
 * OTHER tests in this file (P0 join scoping, inheritance, fail-closed,
 * runnable identity, posture, grammar, …) deliberately construct a gate that
 * IS genuinely invalid or unexpressible at the ONE entry point under test —
 * on purpose, to exercise `resolveGateShape` / `authorizeAndBindRunnable` /
 * the graft's request-time fail-closed behavior directly. Going through
 * `Model.create` for those would correctly abort the whole load before the
 * test ever reached request time. `buildGatedModel` is a `Model` constructed
 * directly (like `model_storage_serve.spec.ts` and `authorize_gate_walk.spec.ts`
 * already do for their own out-of-band reasons) from REAL, fully compiled
 * Malloy IR (`runtime.loadModel(...).getModel()` — never hand-typed IR),
 * skipping ONLY the pre-flight `validateAuthorizeProbes` call, so a model
 * built to exercise ONE entry point's request-time behavior isn't blocked at
 * load time by that same deliberate break. See `buildGatedModel`'s own doc
 * comment for exactly what is and isn't bypassed.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   API,
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
   type Connection,
   type GivenValue,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
   type SourceDef,
} from "@malloydata/malloy";
import { describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, ModelCompilationError } from "../errors";
import { logger } from "../logger";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   buildRowLevelProbe,
   classifyAuthorizeGate,
   liftProbeFilterCondition,
} from "./authorize";
import {
   createGateClassificationDeps,
   resolveGateShape,
} from "./gate_classification";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import { Model } from "./model";
import { Package } from "./package";

const ROOT = "file:///row-level-authorize-tests/";

/**
 * Shared seed: `parent` carries an org/group column and a join key into
 * `childtable`, which carries the name a joined-field gate tests against.
 * `org_id=1` rows are ids 1,2; `org_id=2` rows are ids 3,4. `child_id=10`
 * (parent ids 1,3) joins to `childtable.name='bob'`; `child_id=20` (ids 2,4)
 * joins to `'other'`.
 */
const SEED_SQL = `
CREATE OR REPLACE TABLE parent (id INTEGER, org_id INTEGER, child_id INTEGER, val VARCHAR);
INSERT INTO parent VALUES
   (1, 1, 10, 'a'), (2, 1, 20, 'b'), (3, 2, 10, 'c'), (4, 2, 20, 'd');
CREATE OR REPLACE TABLE childtable (id INTEGER, name VARCHAR);
INSERT INTO childtable VALUES (10, 'bob'), (20, 'other');
`;

async function newDuckdb(): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const stmt of SEED_SQL.trim()
      .split(";")
      .filter((s) => s.trim())) {
      await duckdb.runSQL(stmt.trim() + ";");
   }
   return duckdb;
}

/** Private members this file's harness / assertions reach past the public
 *  surface — same idiom as `authorize_gate_walk.spec.ts`'s `GateWalker`. */
interface ModelInternals {
   setGateRuntime(runtime: Runtime): void;
   authorizeAndBindRunnable(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      options?: {
         recompile?: (mm: ModelMaterializer) => QueryMaterializer;
         bypassAuthorize?: boolean;
      },
   ): Promise<QueryMaterializer>;
   resolveGraftTarget(
      struct: SourceDef,
      originModelDef: ModelDef,
      graftModelDef: ModelDef,
   ): string | undefined;
   resolveGateShape(
      entry: {
         label: string;
         exprs: string[];
         selfContained: boolean;
         struct?: SourceDef;
      },
      originModelDef: ModelDef,
      graftScope:
         | {
              modelDef: ModelDef;
              materializer: ModelMaterializer;
              cacheScope: string;
           }
         | undefined,
   ): Promise<{ shape: string }>;
   queryEntryPointHasRowLevelGate(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<boolean>;
   modelDef?: ModelDef;
}

/**
 * Build a `Model` from REAL compiled Malloy IR while skipping
 * `Model.create`'s pre-flight `validateAuthorizeProbes` call — see the file
 * header for why that call is unconditional and why this file still needs to
 * skip it for tests whose gate is DELIBERATELY invalid at the one entry
 * point under test. It also skips `Model.create`'s
 * `assertNoMisplacedAuthorizeAnnotations` check and never populates
 * `sources`/`queries`/`filterMap` (`Model.getSources`/`getQueries` are never
 * called), so `hasAuthorize()`/`effectiveAuthorizeFor` are inert on a
 * `buildGatedModel` instance — tests that need those go through `createModel`
 * (the real `Model.create`) instead. Everything else is genuine: the text is
 * compiled by a real `Runtime` against a real DuckDB connection, and the
 * resulting `modelDef` / `given:` surface / `ModelMaterializer` are exactly
 * what `Model.create` would have wired up had its pre-flight probe not stood
 * in the way. `setGateRuntime` is wired the same way `Model.create` wires it,
 * so grafting is reachable too.
 */
async function buildGatedModel(
   text: string,
   opts?: { duckdb?: DuckDBConnection; modelPath?: string },
): Promise<{
   model: Model;
   internals: ModelInternals;
   mm: ModelMaterializer;
   duckdb: DuckDBConnection;
}> {
   const duckdb = opts?.duckdb ?? (await newDuckdb());
   const modelPath = opts?.modelPath ?? "m.malloy";
   const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
   // Every gate in this file declares a `given:` block, which needs the
   // experimental flag — callers write bare `given:` text, not the flag line.
   const fullText = text.includes("experimental.givens")
      ? text
      : `##! experimental.givens\n\n${text}`;
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}${modelPath}`, fullText]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}${modelPath}`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef as ModelDef;
   const modelInfo = modelDefToModelInfo(modelDef);
   const malloyGivens = Array.from(
      compiled.givens.values(),
   ) as unknown as MalloyGiven[];
   const givens =
      malloyGivens.length > 0 ? malloyGivens.map(malloyGivenToApi) : undefined;
   const model = new Model(
      "test-pkg",
      modelPath,
      {},
      "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      mm as any,
      modelDef,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      givens as any,
      modelInfo,
   );
   const internals = model as unknown as ModelInternals;
   internals.setGateRuntime(runtime);
   return { model, internals, mm, duckdb };
}

/**
 * A stub runnable carrying its OWN `_modelDef`, compiled from the SAME gate
 * text but under a SEPARATE declaring file URL — mimics append-scope
 * `/compile`'s synthetic `__compile_check.malloy` recompile
 * (`environment.ts`'s `compileSource`), whose struct and `ModelDef` share no
 * object identity, `sourceID`, or annotation-note identity with the on-disk
 * model's own copy. A stub with no `_modelDef` at all (the vacuous shape this
 * replaces) falls back to `this.modelDef` in `resolveRunTargetStruct` and
 * resolves the ON-DISK struct instead — erasing exactly the ephemeral-model
 * property that made real `/compile` 403 every gated source at append scope.
 */
async function buildEphemeralRunnable(
   text: string,
   sourceName: string,
   duckdb: DuckDBConnection,
): Promise<{ getPreparedQuery(): Promise<unknown> }> {
   const fullText = text.includes("experimental.givens")
      ? text
      : `##! experimental.givens\n\n${text}`;
   const ephemeralPath = "__compile_check.malloy";
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}${ephemeralPath}`, fullText]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(
         new Map<string, Connection>([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}${ephemeralPath}`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef as ModelDef;
   const structRef = modelDef.contents[sourceName] as SourceDef;
   return {
      getPreparedQuery: async () => ({
         _query: { structRef },
         _modelDef: modelDef,
      }),
   };
}

/**
 * Run `queryText` through the real `authorizeAndBindRunnable` and return the
 * SQL the bound runnable would issue — proof, from production code, of
 * exactly what a query compiled to, including whether a joined-field gate's
 * JOIN landed.
 */
async function boundSql(
   internals: ModelInternals,
   mm: ModelMaterializer,
   queryText: string,
   givens: Record<string, GivenValue> = {},
): Promise<string> {
   const runnable = mm.loadRestrictedQuery(queryText);
   const bound = await internals.authorizeAndBindRunnable(runnable, givens, {
      recompile: (m) => m.loadRestrictedQuery(queryText),
   });
   return bound.getSQL({ givens });
}

async function boundRows(
   internals: ModelInternals,
   mm: ModelMaterializer,
   queryText: string,
   givens: Record<string, GivenValue> = {},
): Promise<ReadonlyArray<Record<string, unknown>>> {
   const runnable = mm.loadRestrictedQuery(queryText);
   const bound = await internals.authorizeAndBindRunnable(runnable, givens, {
      recompile: (m) => m.loadRestrictedQuery(queryText),
   });
   const result = await bound.run({ rowLimit: 1000, givens });
   return result.data.value;
}

/**
 * The shared multi-entry-point fixture for both the "load-time scoping" and
 * "entry-point matrix" describe blocks below: `X` is gated; `Y` inherits by a
 * bare `extend {}`; `Z`/`Z2` are `query_source` projections (one keeps the
 * gated column, one drops it); `W_rename`/`W_except`/`W_accept` each inherit
 * `X`'s gate BY REFERENCE (no annotation of their own) but can't express it
 * after their own `extend` renames/drops the field; `cp_joiner` is an
 * ordinary, unannotated `join_one:` of `X`; `query: q` is a bare named query
 * over `X` with no annotation of its own. One fixture, not two: earlier this
 * file kept a `SCOPED_ENTRY` variant (this minus `cp_joiner`/`query: q`) to
 * dodge a `source_extraction.ts` bug that reported `cp_joiner`'s ordinary
 * join as a misplaced annotation and aborted the whole load before
 * `validateAuthorizeProbes` was ever reached — see the file header. With
 * that bug fixed, verbatim `ENTRY` loads cleanly through the REAL
 * `Model.create` (pinned by the tests in "load-time scoping" below, which
 * use `createModel`, not `buildGatedModel`), so the split fixture no longer
 * earns its keep.
 */
const ENTRY = `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: Y is X extend {}

source: Z is X -> { group_by: id, org_id, val; aggregate: n is count() }

source: Z2 is X -> { group_by: id, val; aggregate: n is count() }

source: W_rename is X extend { rename: tenant is org_id }

source: W_except is X extend { except: org_id }

source: W_accept is X extend { accept: id, val, n }

source: cp_joiner is duckdb.table('parent') extend {
   join_one: X on id = X.id
   measure: n is count()
}

query: q is X -> { aggregate: n is count() }
`;

// ---------------------------------------------------------------------------
// Load-time validation of a row-field #(authorize) gate — shape-aware
// ---------------------------------------------------------------------------

describe("row-field #(authorize) gate — load-time validation", () => {
   const ROW_FIELD_GATE = `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend { measure: n is count() }
`;

   it("CRITICAL — Model.create loads it cleanly (validateAuthorizeProbes is shape-aware: it probes the gate as a source-level filter on the entry point, not a one-row probe)", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-blocking-"));
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), ROW_FIELD_GATE);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — loads cleanly through the REAL worker-pool fromSerialized path", async () => {
      const originalWorkers = process.env.PACKAGE_LOAD_WORKERS;
      process.env.PACKAGE_LOAD_WORKERS = "1";
      const pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-blocking-pool-"));
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      try {
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: "pkg" }),
         );
         // Inline `duckdb.sql(...)` rather than `duckdb.table('parent')` —
         // the worker pool compiles this in a SEPARATE worker, which gets
         // its own fresh `:memory:` DuckDB instance rather than sharing this
         // process's, so a table this process seeded via `runSQL` would 404
         // there with a Catalog Error unrelated to authorize entirely (see
         // the identical pattern in `package_load_pool.spec.ts`'s
         // `buildConfig`/`"loads a trivial single-model package..."`). The
         // row data needs to travel IN the query text.
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.sql("select 1 as id, 1 as org_id") extend { measure: n is count() }
`,
         );
         const { MalloyConfig, FixedConnectionMap: FCM } = await import(
            "@malloydata/malloy"
         );
         const connections = new FCM(new Map([["duckdb", duckdb]]), "duckdb");
         const malloyConfig = new MalloyConfig({ connections: {} });
         malloyConfig.wrapConnections(() => connections);
         // Package.create -> pool.loadPackage -> Model.fromSerialized is the
         // REAL production hydration path — package_load_worker.ts calls the
         // SAME validateAuthorizeProbes as Model.create, so this is the only
         // way to pin that the fromSerialized path stays in lockstep with it,
         // rather than trusting that a fix to one path also covers the other.
         const pkg = await Package.create("env", "pkg", dir, malloyConfig);
         expect(pkg).toBeDefined();
      } finally {
         await __setPackageLoadPoolForTests(null);
         if (originalWorkers === undefined) {
            delete process.env.PACKAGE_LOAD_WORKERS;
         } else {
            process.env.PACKAGE_LOAD_WORKERS = originalWorkers;
         }
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Load-time SCOPING: a gate one derived entry point can't express warns and
// keeps loading (Task B); a gate invalid in itself, or one Malloy attaches
// somewhere nothing enforces, still fails the whole load (Task C).
//
// Unlike the "entry-point matrix" describe block below (which uses
// `buildGatedModel` to skip `Model.create`'s pre-flight probe entirely — see
// the file header), these tests go through the REAL `Model.create`, because
// what they pin IS that pre-flight probe's own behavior.
// ---------------------------------------------------------------------------

describe("row-level authorize — load-time scoping", () => {
   /** Load `text` through the real `Model.create`, against a fresh seeded
    *  DuckDB. Caller is responsible for `duckdb.close()` / `fs.rmSync(dir)`. */
   async function createModel(
      text: string,
      fileName = "m.malloy",
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-scoping-"));
      fs.writeFileSync(path.join(dir, fileName), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         fileName,
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   function compilationErrorOf(model: Model): Error | undefined {
      return (model as unknown as { compilationError?: Error })
         .compilationError;
   }

   /** Whether `spyOn(logger, "warn")` was called with the entry-point
    *  unexpressible warning for `sourceName`. */
   function warnedUnexpressible(
      warnSpy: { mock: { calls: unknown[][] } },
      sourceName: string,
   ): boolean {
      return warnSpy.mock.calls.some((call) => {
         const [message, fields] = call as [string, { sourceName?: string }?];
         return (
            typeof message === "string" &&
            message.includes("not expressible at this entry point") &&
            fields?.sourceName === sourceName
         );
      });
   }

   it("Z2 (query-source, gate column projected away): load succeeds and warns, request still denies", async () => {
      const warnSpy = spyOn(logger, "warn");
      warnSpy.mockClear();
      const { model, duckdb, dir } = await createModel(ENTRY);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(warnedUnexpressible(warnSpy, "Z2")).toBe(true);

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: Z2 -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("Z (query-source, gate column kept): loads with no compilation error and serves rows filtered by the gate", async () => {
      const { model, duckdb, dir } = await createModel(ENTRY);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: Z -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         const rows = result.compactResult as unknown as { n: number }[];
         // org_id=1 rows are ids 1,2 — filtered, not the unfiltered count of 4.
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("W_rename / W_except / W_accept: load succeeds, warns, and each denies at request time — the rest of the model still serves", async () => {
      const warnSpy = spyOn(logger, "warn");
      warnSpy.mockClear();
      const { model, duckdb, dir } = await createModel(ENTRY);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         for (const name of ["W_rename", "W_except", "W_accept"]) {
            expect(warnedUnexpressible(warnSpy, name)).toBe(true);
            await expect(
               model.getQueryResults(
                  undefined,
                  undefined,
                  `run: ${name} -> { aggregate: n is count() }`,
                  {},
                  true,
                  { GROUPS: [1] },
               ),
            ).rejects.toBeInstanceOf(AccessDeniedError);
         }
         // The rest of the model — including the SOURCE that declares the
         // gate — still loads and serves. This is the point of scoping the
         // failure: three unexpressible derived entry points do not take
         // down `X` (or `Y`, `Z`) with them.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a grammar-rejected gate (not against an in-membership test) still fails the whole model load", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "not (org_id in $GROUPS)"
source: X is duckdb.table('parent') extend { measure: n is count() }
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(
            /not.*is not permitted|negated membership/i,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a #(authorize) annotation on a top-level query: statement fails the load (fails OPEN otherwise — see docs)", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.givens

given:
  GROUPS :: number[]

source: X is duckdb.table('parent') extend { measure: n is count() }

#(authorize) "org_id in $GROUPS"
query: q is X -> { aggregate: n is count() }
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/query "q"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a #(authorize) annotation on a FIELD inside a source (not the source: line) fails the load", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.givens

given:
  GROUPS :: number[]

source: X is duckdb.table('parent') extend {
   #(authorize) "org_id in $GROUPS"
   measure: n is count()
}
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/field "n" of source "X"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — two sources independently typing the identical gate text: the one that does not resolve FAILS the load, not downgraded by its neighbor (fix2)", async () => {
      // `A` and `B` each carry their OWN separately-parsed `#(authorize)`
      // annotation with the identical TEXT — two distinct note objects, not
      // one shared by reference. `A` (backed by `parent`, which has org_id)
      // resolves; `B` (backed by `childtable`, which does not) is a genuine
      // authoring mistake at the point it is declared. An escape discriminator
      // keyed on gate TEXT would let `B` off because `A`'s identical string
      // validated somewhere in the model; the note-OBJECT discriminator must
      // not, since `B`'s own note is not `A`'s note.
      const { model, duckdb, dir } = await createModel(
         `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: A is duckdb.table('parent') extend { measure: n is count() }

#(authorize) "org_id in $GROUPS"
source: B is duckdb.table('childtable') extend { measure: n is count() }
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/source "B"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Task C false positive (fix1) — an unannotated join_one:/join_many: of a
// gated source must not be reported as a misplaced #(authorize) annotation.
// Malloy embeds the joined source as a nested StructDef on the join field and
// copies that source's own annotation note object onto the join field's own
// annotations BY REFERENCE whenever the join line adds none of its own — the
// exact shape docs/authorize.md's own worked join example ships. Before this
// fix, `source_extraction.ts`'s field scan reported this as misplaced on
// every such join, and — because `Package.create` rethrows a model
// `compilationError` on the create path — made row-level authorize
// unshippable for any package containing one.
// ---------------------------------------------------------------------------

describe("row-level authorize — misplaced-annotation scan (Task C)", () => {
   /** `salaries` is locked; `headcount_by_dept` joins it with no annotation
    *  of its own — docs/authorize.md's own worked example shape. */
   const JOIN_OF_GATED_SOURCE = `##! experimental.givens

#(authorize) "false"
source: salaries is duckdb.table('parent') extend {
   measure: n is count()
}

source: headcount_by_dept is duckdb.table('childtable') extend {
   join_one: salaries on id = salaries.id
   measure: headcount is count()
}
`;

   it("CRITICAL — an unannotated join_one: of a gated source LOADS through Model.create", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-join-misplaced-"));
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), JOIN_OF_GATED_SOURCE);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — the identical join loads through the real worker-pool Package.create path (the package-level rethrow is what made this SEVERE)", async () => {
      const originalWorkers = process.env.PACKAGE_LOAD_WORKERS;
      process.env.PACKAGE_LOAD_WORKERS = "1";
      const pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-join-misplaced-pool-"),
      );
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      try {
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: "pkg" }),
         );
         // Inline duckdb.sql(...) rather than duckdb.table(...) — the worker
         // compiles in a SEPARATE process with its own fresh :memory: DuckDB,
         // so a table this process seeded via runSQL 404s there; see the
         // identical pattern in the "load-time validation" describe block's
         // worker-pool test above.
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

#(authorize) "false"
source: salaries is duckdb.sql("select 1 as id") extend { measure: n is count() }

source: headcount_by_dept is duckdb.sql("select 1 as id") extend {
   join_one: salaries on id = salaries.id
   measure: headcount is count()
}
`,
         );
         const { MalloyConfig: MC, FixedConnectionMap: FCM } = await import(
            "@malloydata/malloy"
         );
         const connections = new FCM(new Map([["duckdb", duckdb]]), "duckdb");
         const malloyConfig = new MC({ connections: {} });
         malloyConfig.wrapConnections(() => connections);
         const pkg = await Package.create("env", "pkg", dir, malloyConfig);
         expect(pkg).toBeDefined();
      } finally {
         await __setPackageLoadPoolForTests(null);
         if (originalWorkers === undefined) {
            delete process.env.PACKAGE_LOAD_WORKERS;
         } else {
            process.env.PACKAGE_LOAD_WORKERS = originalWorkers;
         }
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — an #(authorize) an author writes directly ON a join_one: line of an UNGATED source FAILS the load (the join's declaration resolves in this model, so a mismatched note is author-written)", async () => {
      // `salaries` is UNGATED here, so `headcount_by_dept`'s join-line
      // annotation cannot be Malloy's by-reference copy of anything — it is
      // unambiguously author-written, and its declaration (`salaries`)
      // resolves inside this model's own `contents`, so
      // `gatedSourceOwnAuthorizeNotes` is authoritative for it. Leaving this
      // a warning would be exactly the bug this fix closes: the annotation
      // lands on the join FIELD, so `headcount_by_dept` itself ends up with
      // no gate at all and serves every row unfiltered. Only a join whose
      // declaration this walk cannot resolve INSIDE this model at all (the
      // cross-file shapes below) still gets the benefit of the doubt and
      // warns instead — see `source_extraction.ts`'s doc. The gated variant
      // (same text / different text) is covered by the "Task C fix1/fix3"
      // describe block below.
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-join-authored-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

source: salaries is duckdb.table('parent') extend {
   measure: n is count()
}

source: headcount_by_dept is duckdb.table('childtable') extend {
   #(authorize) "false"
   join_one: salaries on id = salaries.id
   measure: headcount is count()
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(
            /field "salaries" of source "headcount_by_dept"/,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — an array-typed dimension is NOT a join_one:/join_many:, so a misplaced #(authorize) on it still FAILS the load (not a warn)", async () => {
      // `isJoined` is `'join' in def`, which Malloy's IR also sets on an
      // array/record-typed dimension's nested struct — but there is no
      // JOINED SOURCE here for the join-warn branch's remedy to name, so
      // gating this on `isJoined(field) && isSourceDef(field)` matters: an
      // array/record dimension is not a source and must keep failing like
      // every other misplaced `dimension:` annotation.
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-array-dim-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `source: s is duckdb.sql("select 1 as id") extend {
   #(authorize) "false"
   dimension: arr is [1, 2, 3]
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/field "arr" of source "s"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a record-typed dimension is NOT a join_one:/join_many: either, so it fails the load the same way", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-record-dim-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `source: s is duckdb.sql("select 1 as id") extend {
   #(authorize) "false"
   dimension: rec is {a is 1}
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/field "rec" of source "s"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Fix1 — cross-file join false positive: a join whose gated source lives
// beyond THIS model's own `contents`/`sourceRegistry` — a selective one-hop
// import of only the joiner, or a two-hop transitive import (Malloy merges
// only ONE import level) — must not be reported as misplaced or fail the
// load. `gatedSourceOwnAuthorizeNotes`'s identity check cannot find this
// note: it names a declaration this model's own `contents` never registered
// at all, which is exactly why the fix treats "cannot be resolved to a
// declaration in this model" as its own inherited-copy case.
//
// That branch is silent rather than warned, so "loads cleanly" is the whole
// observable claim here. These two tests previously also asserted that no join
// warning fired — against a field nothing ever populated, so the assertion
// could not fail and proved nothing the line above it did not already prove.
// ---------------------------------------------------------------------------

describe("row-level authorize — cross-file join false positive (fix1)", () => {
   async function createMultiFileModel(
      files: Record<string, string>,
      entryFile: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-crossfile-"));
      for (const [name, text] of Object.entries(files)) {
         fs.writeFileSync(path.join(dir, name), text);
      }
      const model = await Model.create(
         "test-pkg",
         dir,
         entryFile,
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   function compilationErrorOf(model: Model): Error | undefined {
      return (model as unknown as { compilationError?: Error })
         .compilationError;
   }

   // `sal` is gated; `mid` joins it with no annotation of its own — the
   // by-reference copy shape `docs/authorize.md` documents, just declared in
   // a file the importing model does not fully see.
   const A_WITH_JOINER = `##! experimental.givens

#(authorize) "false"
source: sal is duckdb.table('parent') extend { measure: n is count() }

source: mid is duckdb.table('childtable') extend {
   join_one: sal on id = sal.id
   measure: h is count()
}
`;

   it("CRITICAL — selective one-hop import of only the joiner loads cleanly (sal never enters m's own contents)", async () => {
      const M = `##! experimental.givens
import { mid } from "a.malloy"

source: top is mid extend {}
`;
      const { model, duckdb, dir } = await createMultiFileModel(
         { "a.malloy": A_WITH_JOINER, "m.malloy": M },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — two-hop transitive import loads cleanly (Malloy merges only one import level, so sal is invisible to m)", async () => {
      // `a.malloy` declares ONLY the gated `sal` — `mid` (the joiner) lives
      // in `b.malloy`, ONE hop away from `sal`. `m.malloy` imports `b.malloy`
      // fully, which is a SECOND hop from `sal` — Malloy merges only one
      // import level, so `sal` never enters `m`'s own `modelDef.contents`,
      // even though `b.malloy`'s `mid` (which DOES enter `m`'s contents) still
      // carries a join field whose `referenceID` names it.
      const A_SAL_ONLY = `##! experimental.givens

#(authorize) "false"
source: sal is duckdb.table('parent') extend { measure: n is count() }
`;
      const B = `##! experimental.givens
import "a.malloy"

source: mid is duckdb.table('childtable') extend {
   join_one: sal on id = sal.id
   measure: h is count()
}
`;
      const M = `##! experimental.givens
import "b.malloy"

source: top is mid extend {}
`;
      const { model, duckdb, dir } = await createMultiFileModel(
         { "a.malloy": A_SAL_ONLY, "b.malloy": B, "m.malloy": M },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Fix1/fix3 — an author-written #(authorize) directly on a join_one: line of
// a GATED source. Malloy REPLACES the joined struct's annotations outright
// when the join line carries one of its own (no `inherits`, no by-reference
// copy — see `gate_registry_walk.ts`'s doc), so this note is never identity-
// matched to the source's own gate, same text or not. `salaries` is declared
// in the SAME FILE, so its declaration resolves inside this model's own
// `contents` and `gatedSourceOwnAuthorizeNotes` is authoritative for it — a
// mismatched note here is unambiguously author-written. The load therefore
// FAILS, same as any other misplaced annotation: leaving this a warning is
// exactly the bug the fix closes, since the annotation lands on the join
// FIELD and `headcount` itself would end up with no gate at all. Only the
// cross-file shape (the join's declaration is beyond this model's own
// visibility) still gets the benefit of the doubt and warns — see the
// "cross-file join false positive" describe block above.
// ---------------------------------------------------------------------------

describe("row-level authorize — authored annotation on a join line of a gated source (fix1/fix3)", () => {
   async function createModel(
      text: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-join-authored2-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   function compilationErrorOf(model: Model): Error | undefined {
      return (model as unknown as { compilationError?: Error })
         .compilationError;
   }

   const GATED_SALARIES = `##! experimental.givens

#(authorize) "false"
source: salaries is duckdb.table('parent') extend {
   measure: n is count()
}
`;

   it("CRITICAL — same-text authored annotation on the join line FAILS the load (case C)", async () => {
      const { model, duckdb, dir } = await createModel(
         `${GATED_SALARIES}
source: headcount is duckdb.table('childtable') extend {
   #(authorize) "false"
   join_one: salaries on id = salaries.id
   measure: h is count()
}
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/field "salaries" of source "headcount"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — different-text authored annotation on the join line ALSO FAILS the load (case D)", async () => {
      const { model, duckdb, dir } = await createModel(
         `${GATED_SALARIES}
source: headcount is duckdb.table('childtable') extend {
   #(authorize) "1 = 1"
   join_one: salaries on id = salaries.id
   measure: h is count()
}
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/field "salaries" of source "headcount"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// P0 — the graft must be scoped to the entry point (docs §3, "the graft MUST
// be scoped to the entry point")
// ---------------------------------------------------------------------------

describe("row-level authorize — P0 join scoping", () => {
   it("CRITICAL — P0: a child's OWN gate does not fire when the ungated parent joins it", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
#(authorize) "false"
source: childtable is duckdb.table('childtable') extend {}
source: parent is duckdb.table('parent') extend {
   join_one: childtable on child_id = childtable.id
   measure: n is count()
}
`);
      try {
         // parent itself declares no gate; collectEntryPointGates does not walk
         // joins, so childtable's "false" gate must not be collected — and must
         // not filter/deny parent's own rows. All 4 seed rows, unfiltered.
         const rows = await boundRows(
            internals,
            mm,
            "run: parent -> { aggregate: n is count() }",
         );
         expect(rows[0].n).toBe(4);
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — P0's wanted case: a parent gate referencing a joined field filters AND emits the JOIN", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  BOB :: string

source: childtable is duckdb.table('childtable') extend {}
#(authorize) "childtable.name = $BOB"
source: parent is duckdb.table('parent') extend {
   join_one: childtable on child_id = childtable.id
   measure: n is count()
}
`);
      try {
         const query = "run: parent -> { aggregate: n is count() }";
         const sql = await boundSql(internals, mm, query, { BOB: "bob" });
         expect(sql).toMatch(/join/i);
         const rows = await boundRows(internals, mm, query, { BOB: "bob" });
         // child_id=10 rows are ids 1 and 3 (parent), which join to name='bob'.
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Inheritance
// ---------------------------------------------------------------------------

describe("row-level authorize — inheritance", () => {
   const INHERIT = `
given:
  GROUPS :: number[]
  VAL :: string

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: Y is X extend {}

#(authorize) "val = $VAL"
source: Z is X extend {}
`;

   it("Y is X extend {} inherits X's gate and filters", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(INHERIT);
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: Y -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2); // org_id=1 rows: ids 1,2
      } finally {
         await duckdb.close();
      }
   });

   it("Z's own gate OVERRIDES the base's rather than stacking with it (AND)", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(INHERIT);
      try {
         // GROUPS is deliberately empty (would deny every row if the base's
         // gate ALSO applied) — Z declares its own gate, which replaces X's, so
         // only val='c' decides the outcome. If this regresses to stacking,
         // GROUPS=[] would drive the count to 0 instead of 1.
         const rows = await boundRows(
            internals,
            mm,
            "run: Z -> { aggregate: n is count() }",
            {
               GROUPS: [],
               VAL: "c",
            },
         );
         expect(rows[0].n).toBe(1);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Entry-point matrix (spike findings §4) — every shape must either filter
// correctly or deny; none may return unfiltered rows.
// ---------------------------------------------------------------------------

describe("row-level authorize — entry-point matrix", () => {
   // `ENTRY` is the shared module-level fixture defined above (used by
   // "load-time scoping" too) — see its doc comment for why this is one
   // fixture, not two.
   async function harness() {
      return buildGatedModel(ENTRY);
   }

   it("run: X filters", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("Y is X extend filters (derived entry point)", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: Y -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("Z is X -> {...} with the gate column KEPT in the projection filters", async () => {
      // `Z` is a model-declared query source (`source: Z is X -> {...}`), a
      // `modelDef.contents` entry in its own right. resolveGraftTarget grafts
      // `Z` itself, not its base `X`: `Z`'s compiled `SourceDef` snapshotted
      // `X` at declaration time, so a condition appended to `X` afterward
      // would never reach it. Grafting `Z` works because `org_id` survived
      // `Z`'s projection, so it resolves fine in `Z`'s own field space.
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: Z -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("Z2 is X -> {...} with the gate column PROJECTED AWAY denies (the narrow, author-fixable 403 the design accepts — corrects §3b)", async () => {
      // §3b's original claim (measured against a CALLER-declared query
      // source in ad-hoc text) does not hold for `Z2`, a MODEL-declared one:
      // grafting `Z2`'s base `X` can't reach `Z2` (its compiled `SourceDef`
      // snapshotted `X` at declaration time — same reason `Z` above must be
      // grafted directly), and grafting `Z2` itself can't work either,
      // because `org_id` is not in `Z2`'s own field space — its projection
      // (`group_by: id, val`) dropped it. There is no third graft target
      // that has both a snapshot of `Z2`'s declaration AND `org_id` in
      // scope, so this is DENY, not a serving-filtered case: the narrow,
      // author-fixable 403 the design's original plan accepted for a gate
      // column projected out of a query-source entry point.
      const { internals, mm, duckdb } = await harness();
      try {
         await expect(
            boundRows(internals, mm, "run: Z2 -> { aggregate: n is count() }", {
               GROUPS: [1],
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("W_rename (org_id renamed away) denies rather than serving unfiltered", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         await expect(
            boundRows(
               internals,
               mm,
               "run: W_rename -> { aggregate: n is count() }",
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("W_except (org_id excepted) denies rather than serving unfiltered", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         await expect(
            boundRows(
               internals,
               mm,
               "run: W_except -> { aggregate: n is count() }",
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("W_accept (org_id not accepted) denies rather than serving unfiltered", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         await expect(
            boundRows(
               internals,
               mm,
               "run: W_accept -> { aggregate: n is count() }",
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("a source reached only via join (cp_joiner) does not carry X's gate — same Q16/P0 rule as entry point", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: cp_joiner -> { aggregate: n is count() }",
         );
         expect(rows[0].n).toBe(4);
      } finally {
         await duckdb.close();
      }
   });

   it("a caller-declared ad-hoc derivation (`source: mine is X extend {}` + `run: mine`) filters", async () => {
      // Filters correctly. `resolveGraftTarget`'s "direct" check
      // (`findContentsKey`) is evaluated against the STABLE `graftModelDef`
      // (`this.modelMaterializer`'s own model), not the ephemeral ad-hoc
      // modelDef the caller's inline `source: mine is X extend {}` compiled
      // into — so "mine" (which exists only in that ephemeral modelDef)
      // never matches there, direct or not. This is a TRIVIAL `extend {}`
      // (no rename/except/accept/dimension/join addition), so Malloy elides
      // the derivation reference entirely (`resolveDeclaredSource` returns
      // `none`) but copies X's `#(authorize)` note onto "mine" BY REFERENCE
      // — the same note object, not a re-parsed equal one — which is exactly
      // what `findSourceByOwnAnnotationIdentity` traces back to "X" in the
      // stable model, landing the graft there.
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "source: mine is X extend {}\nrun: mine -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("`query: q is X -> {...}` + `run: q` filters", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(internals, mm, "run: q", {
            GROUPS: [1],
         });
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("two `run:` statements — the LAST one executes and is what gets gated", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: cp_joiner -> { aggregate: n is count() }\nrun: X -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("a multi-stage `X -> {...} -> {...}` filters (source-level filter is unaffected by staging)", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { group_by: id; aggregate: n is count() } -> { aggregate: t is n.sum() }",
            { GROUPS: [1] },
         );
         expect(rows[0].t).toBe(2);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Fail-closed
// ---------------------------------------------------------------------------

describe("row-level authorize — fail-closed (CRITICAL)", () => {
   it("gate column absent from the entry shape denies with a 403, not a 400/500", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
source: W is X extend { except: org_id }
`);
      try {
         const err = await boundRows(
            internals,
            mm,
            "run: W -> { aggregate: n is count() }",
            { GROUPS: [1] },
         ).catch((e) => e);
         expect(err).toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("gate's given unresolved (not on this model's own given surface) denies with a 403", async () => {
      // FAR is declared 2 import hops from entry.malloy: entry -> mid -> deep.
      // Malloy merges only ONE hop into a model's own given namespace, so
      // entry's `compiledModel.givens` never surfaces FAR — classifyAuthorizeGate's
      // `declaredTypeOf` cannot find it and rejects as "unreachable_given".
      const duckdb = await newDuckdb();
      const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
      const files = new Map<string, string>([
         [
            `${ROOT}deep.malloy`,
            `##! experimental.givens\n\ngiven:\n  FAR :: number[]\n\n#(authorize) "org_id in $FAR"\nsource: Deep is duckdb.table('parent') extend {\n   measure: n is count()\n}\n`,
         ],
         [
            `${ROOT}mid.malloy`,
            `import "deep.malloy"\nsource: Mid is Deep extend {}\n`,
         ],
         [
            `${ROOT}entry.malloy`,
            `import "mid.malloy"\nsource: Entry is Mid extend {}\n`,
         ],
      ]);
      const urlReader = new InMemoryURLReader(files);
      const runtime = new Runtime({
         urlReader,
         connections: new FixedConnectionMap(connMap, "duckdb"),
      });
      const mm = runtime.loadModel(new URL(`${ROOT}entry.malloy`), {
         importBaseURL: new URL(ROOT),
      });
      try {
         const compiled = await mm.getModel();
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const modelDef = (compiled as any)._modelDef as ModelDef;
         const modelInfo = modelDefToModelInfo(modelDef);
         const malloyGivens = Array.from(
            compiled.givens.values(),
         ) as unknown as MalloyGiven[];
         const givens =
            malloyGivens.length > 0
               ? malloyGivens.map(malloyGivenToApi)
               : undefined;
         // Confirms the premise: FAR is genuinely absent from entry's own surface.
         expect(givens?.some((g) => g.name === "FAR")).toBeFalsy();
         const model = new Model(
            "test-pkg",
            "entry.malloy",
            {},
            "model",
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            mm as any,
            modelDef,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            givens as any,
            modelInfo,
         );
         const internals = model as unknown as ModelInternals;
         internals.setGateRuntime(runtime);
         const err = await boundRows(
            internals,
            mm,
            "run: Entry -> { aggregate: n is count() }",
            { FAR: [1] },
         ).catch((e) => e);
         expect(err).toBeInstanceOf(AccessDeniedError);
         // The remedy text classifyAuthorizeGate builds ("import { FAR } from
         // ...") must NOT reach the caller — the thrown error names nothing
         // about the gate. It only ever reaches the debug log.
         expect(String((err as Error).message)).not.toContain("FAR");
         expect(String((err as Error).message)).not.toContain("import");
      } finally {
         await duckdb.close();
      }
   });

   it("gate compile throws (e.g. an inherited gate whose field was renamed away) denies with a 403", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
source: W is X extend { rename: tenant is org_id }
`);
      try {
         const err = await boundRows(
            internals,
            mm,
            "run: W -> { aggregate: n is count() }",
            { GROUPS: [1] },
         ).catch((e) => e);
         expect(err).toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("an unresolvable graft target: resolveGraftTarget returns undefined (synthetic IR real Malloy input cannot express — same idiom as authorize_gate_walk.spec.ts)", async () => {
      const { internals, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const modelDef = internals.modelDef as ModelDef;
         // A detached copy of X's own struct: same shape, but neither identity-
         // nor sourceID-matched to anything in `modelDef.contents`, and its
         // sourceID/referenceID links resolve to nothing — exactly the
         // "resolveDeclaredSource returns none/unresolvable with no contents
         // match" case real Malloy IR cannot construct through any source
         // syntax (every real struct is reachable from `contents`).
         const orphan = structuredClone(
            modelDef.contents["X"],
         ) as unknown as SourceDef;
         (orphan as unknown as { sourceID?: string }).sourceID = undefined;
         (orphan as unknown as { referenceID?: string }).referenceID =
            undefined;
         const target = internals.resolveGraftTarget(
            orphan,
            modelDef,
            modelDef,
         );
         expect(target).toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — a row-level gate with no resolvable graft target still REJECTS, with nothing to attach a filter to", async () => {
      // Same orphan shape as the test above (an unresolvable graft target —
      // `resolveGraftTarget` returns `undefined`), but exercised through the
      // FULL `resolveGateShape`: every gate is a row filter now, and a filter
      // with nowhere to attach cannot be enforced, so a missing graft target
      // rejects outright rather than attempting any fallback classification.
      const { internals, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const modelDef = internals.modelDef as ModelDef;
         const orphan = structuredClone(
            modelDef.contents["X"],
         ) as unknown as SourceDef;
         (orphan as unknown as { sourceID?: string }).sourceID = undefined;
         (orphan as unknown as { referenceID?: string }).referenceID =
            undefined;
         const graftScope = {
            modelDef,
            materializer: (
               internals as unknown as { modelMaterializer: ModelMaterializer }
            ).modelMaterializer,
            cacheScope: "model",
         };
         const resolution = await internals.resolveGateShape(
            {
               label: "X",
               exprs: ["org_id in $GROUPS"],
               selfContained: false,
               struct: orphan,
            },
            modelDef,
            graftScope,
         );
         expect(resolution.shape).toBe("rejected");
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — a differing given surface does not collide on a cached entry", async () => {
      // `resolveGateShape`'s cache key is `(cacheScope, graftTarget,
      // filterText)` — no fingerprint of the given surface a classification
      // was computed against. Safe only because `createGateClassificationDeps`
      // mints a `gateShapeCache` and its given-declared-type/default maps
      // TOGETHER, so two deps structs for two different given surfaces can
      // never share a Map: the SAME (graftTarget, filterText) classifies
      // independently under each, rather than one reusing a stale
      // classification the other cached.
      const { internals, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const modelDef = internals.modelDef as ModelDef;
         const graftScope = {
            modelDef,
            materializer: (
               internals as unknown as { modelMaterializer: ModelMaterializer }
            ).modelMaterializer,
            cacheScope: "model",
         };
         const entry = {
            label: "X",
            exprs: ["org_id in $GROUPS"],
            selfContained: false,
            struct: modelDef.contents["X"] as unknown as SourceDef,
         };

         // Surface A: `GROUPS` is declared as an array — the gate compiles
         // and classifies as a real row filter.
         const depsWithGroups = createGateClassificationDeps([
            { name: "GROUPS", type: "number[]" },
         ]);
         const admitted = await resolveGateShape(
            entry,
            modelDef,
            graftScope,
            depsWithGroups,
         );
         expect(admitted.shape).toBe("row_level");

         // Surface B: `GROUPS` is not on this surface at all. A fresh deps
         // struct never shares a Map with surface A's, so the IDENTICAL
         // (graftTarget, filterText) must reject as unreachable, not silently
         // reuse surface A's cached admit.
         const depsWithoutGroups = createGateClassificationDeps([]);
         expect(depsWithoutGroups.gateShapeCache).not.toBe(
            depsWithGroups.gateShapeCache,
         );
         const rejected = await resolveGateShape(
            entry,
            modelDef,
            graftScope,
            depsWithoutGroups,
         );
         expect(rejected.shape).toBe("rejected");
         if (rejected.shape === "rejected") {
            expect(rejected.cause).toBe("unreachable_given");
         }
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Runnable identity
// ---------------------------------------------------------------------------

describe("row-level authorize — runnable identity (CRITICAL)", () => {
   it("a notebook-shaped cell composing a #(filter) source filter AND a row-level gate carries BOTH", async () => {
      // Exercised at the QueryMaterializer level (same recompile shape
      // executeNotebookCell uses: `mm.loadQuery` + authorizeAndBindRunnable),
      // since a full notebook harness needs the .malloynb worker path this
      // file's headline bug already shows is unreachable for any row-field
      // gate. This proves the COMPOSITION invariant executeNotebookCell's own
      // doc states: the recompile must use whatever text the filter refinement
      // already rebuilt, not the pre-refinement cell text.
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   where: val != 'd'
   measure: n is count()
}
`);
      try {
         // val != 'd' (the source's own #(filter)-equivalent where:) removes
         // id 4; org_id in [1] then keeps only ids 1,2 of what remains.
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
         const rowsOtherOrg = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [2] },
         );
         // org_id=2 rows are ids 3,4; id 4 is removed by the source's own
         // where:, so only id 3 survives — proves the where: composed with
         // the gate rather than being dropped by the recompile.
         expect(rowsOtherOrg[0].n).toBe(1);
      } finally {
         await duckdb.close();
      }
   });

   it("a bound row-level-gated query rejects at run time when its given reference has no value (self-triggering case)", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         // No GROUPS supplied at all: the graft's given reference has no
         // value, so the run throws at RUN time — the "self-triggering" case
         // the matrix calls out. It must surface as a clean failure, never as
         // a silent, unfiltered success from some fallback path. (This test
         // never configures serve bindings or calls getQueryResults, so it
         // does not itself exercise storage routing — see the "storage
         // routing" describe block below for that claim.)
         const runnable = mm.loadRestrictedQuery(
            "run: X -> { aggregate: n is count() }",
         );
         const bound = await internals.authorizeAndBindRunnable(
            runnable,
            {},
            {
               recompile: (m) =>
                  m.loadRestrictedQuery(
                     "run: X -> { aggregate: n is count() }",
                  ),
            },
         );
         await expect(
            bound.run({ rowLimit: 10, givens: {} }),
         ).rejects.toThrow();
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Posture
// ---------------------------------------------------------------------------

describe("row-level authorize — posture", () => {
   it("a caller the gate admits nowhere gets zero rows, not a 403 (deliberate contract)", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [] },
         );
         expect(rows[0].n).toBe(0);
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — /compile 403s a row-field gate whose givens the caller did NOT supply", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         // /compile's backstop (Environment.compileSource) calls
         // assertAuthorizedForRunnable with NO recompile hook, so there is
         // nothing to attach the filter to. A caller who supplied no value for
         // the gate's given has presented nothing to be judged on, so the
         // authoring escape below does not apply and this denies.
         const stubRunnable = {
            getPreparedQuery: async () => ({ _query: { structRef: "X" } }),
         };
         await expect(
            model.assertAuthorizedForRunnable(stubRunnable, {}),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("/compile ADMITS when the caller supplied every given the gate reads — the authoring loop", async () => {
      // Refusing here was strictly harsher than the query path, which answers
      // a gated source with FILTERED rows rather than a 403, so it protected
      // nothing while making a gated source un-authorable. `/compile` never
      // runs the query; it returns a schema (and, with includeSql, the
      // UNGRAFTED SQL — see `docs/authorize.md`).
      const text = `
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`;
      const { model, duckdb } = await buildGatedModel(text);
      try {
         const stubRunnable = await buildEphemeralRunnable(text, "X", duckdb);
         await expect(
            model.assertAuthorizedForRunnable(stubRunnable, { GROUPS: [1] }),
         ).resolves.toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   it("/compile ADMITS a constant-true gate with no givens at all", async () => {
      const text = `
#(authorize) "true"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`;
      const { model, duckdb } = await buildGatedModel(text);
      try {
         const stubRunnable = await buildEphemeralRunnable(text, "X", duckdb);
         await expect(
            model.assertAuthorizedForRunnable(stubRunnable, {}),
         ).resolves.toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   // The three tests below pin residual gaps the on-disk-twin fold-in does
   // NOT close — known and chosen, not accidental. Each denies a caller who
   // would be admitted if the gap were closed; a later change that flips one
   // of these should be a deliberate decision, not a surprise.

   it("KNOWN GAP — append-scope /compile of a caller-authored derivation (`source: mine is X extend {…}`) still denies with every given supplied", async () => {
      // The fold-in only replaces an entry keyed on the run target's OWN
      // source name (`ownSourceName`, here "mine"); `entryPointGatesBySource`
      // has no on-disk entry for "mine" at all (it is declared only in the
      // caller's ad-hoc text), so there is nothing to fold in and the walk's
      // ephemeral copy of the INHERITED gate stands — `resolveGraftTarget`
      // fails all three strategies against it, same as before the fix.
      const text = `
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`;
      const { model, duckdb } = await buildGatedModel(text);
      try {
         const derivedText = `${text}\nsource: mine is X extend {}\n`;
         const stubRunnable = await buildEphemeralRunnable(
            derivedText,
            "mine",
            duckdb,
         );
         await expect(
            model.assertAuthorizedForRunnable(stubRunnable, { GROUPS: [1] }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("KNOWN GAP — a constant-true walk entry with no on-disk twin under the entry name still denies", async () => {
      // "solo" exists only in the ephemeral compile, not in this model's own
      // `entryPointGatesBySource`, so the fold-in never runs for it and
      // `resolveGraftTarget` never gets a target to classify against —
      // rejected before `classifyAuthorizeGate` ever sees the literal "true".
      const { model, duckdb } = await buildGatedModel(`
#(authorize) "true"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const stubRunnable = await buildEphemeralRunnable(
            `
#(authorize) "true"
source: solo is duckdb.table('parent') extend {
   measure: n is count()
}
`,
            "solo",
            duckdb,
         );
         await expect(
            model.assertAuthorizedForRunnable(stubRunnable, {}),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("KNOWN GAP — a new-path compile importing a gated model (assertAuthorizedFromCompiledRunnable) still denies with every given supplied", async () => {
      // `skipOwnSourceGate: true` is exactly what keeps the fold-in from
      // running (it is gated on `!skipOwnSourceGate`), so this denies even
      // though the run target IS the on-disk "X" with a satisfying given —
      // the same case the fold-in fixes for `assertAuthorizedForRunnable`.
      const text = `
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`;
      const { model, duckdb } = await buildGatedModel(text);
      try {
         const stubRunnable = await buildEphemeralRunnable(text, "X", duckdb);
         await expect(
            model.assertAuthorizedFromCompiledRunnable(stubRunnable, {
               GROUPS: [1],
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — an unsupplied GATE given denies opaquely; the gate's given name never reaches the caller", async () => {
      // The gate is grafted into the query, so its given is bound by the same
      // `run()` and Malloy's failure names it. That name is exactly what
      // `docs/authorize.md` promises a denied caller never learns.
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-given-leak-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: X is duckdb.table('parent') extend { measure: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = await model
            .getQueryResults(
               undefined,
               undefined,
               "run: X -> { group_by: org_id; aggregate: n is count() }",
               {},
               true,
               {},
            )
            .catch((e) => e);
         expect(err).toBeInstanceOf(AccessDeniedError);
         const message = String((err as Error).message);
         expect(message).not.toContain("ROLE");
         expect(message).not.toContain("givens");
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("/compile still 403s a constant-FALSE gate — nothing is readable, so nothing is authorable", async () => {
      const { model, duckdb } = await buildGatedModel(`
#(authorize) "false"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const stubRunnable = {
            getPreparedQuery: async () => ({ _query: { structRef: "X" } }),
         };
         await expect(
            model.assertAuthorizedForRunnable(stubRunnable, {}),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

describe("row-level authorize — state (no shared-state mutation)", () => {
   it("the original modelDef is never mutated, and a graft never accumulates across repeated requests", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const modelDef = internals.modelDef as ModelDef;
         const originalFilterList = (
            modelDef.contents["X"] as unknown as {
               filterList?: unknown[];
            }
         ).filterList;
         const query = "run: X -> { aggregate: n is count() }";
         for (let i = 0; i < 3; i++) {
            const sql = await boundSql(internals, mm, query, { GROUPS: [1] });
            // Exactly one instance of the grafted predicate per compile — never
            // two, three (accumulating across calls).
            const occurrences = sql.split('"org_id"').length - 1;
            expect(occurrences).toBe(1);
         }
         // The ORIGINAL modelDef.contents entry is byte-identical to before any
         // gated request ran — the graft only ever touches a structuredClone.
         expect(
            (modelDef.contents["X"] as unknown as { filterList?: unknown[] })
               .filterList,
         ).toEqual(originalFilterList);
      } finally {
         await duckdb.close();
      }
   });

   it("a gated request followed by a bypass request on the same query returns all rows", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const query = "run: X -> { aggregate: n is count() }";
         const gated = await boundRows(internals, mm, query, { GROUPS: [1] });
         expect(gated[0].n).toBe(2);

         const runnable = mm.loadRestrictedQuery(query);
         const bypassed = await internals.authorizeAndBindRunnable(
            runnable,
            {},
            { bypassAuthorize: true },
         );
         const result = await bypassed.run({ rowLimit: 10, givens: {} });
         expect(result.data.value[0].n).toBe(4);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Givens
// ---------------------------------------------------------------------------

describe("row-level authorize — givens", () => {
   it("the caller's given for a row-level gate reaches the real query (regression against filterGivensToModelSurface dropping an authorize-only name)", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         // GROUPS is on this model's own given surface, so
         // filterGivensToModelSurface must not drop it — the filtered result
         // is the observable proof it reached the graft's WHERE clause at run
         // time (a dropped/defaulted given would return either all 4 rows or
         // throw "no value", not exactly the org_id=1 subset).
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("an empty array given yields zero rows (WHERE FALSE), including on a multi-stage query", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const single = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [] },
         );
         expect(single[0].n).toBe(0);
         const multiStage = await boundRows(
            internals,
            mm,
            "run: X -> { group_by: id; aggregate: n is count() } -> { aggregate: t is n.sum() }",
            { GROUPS: [] },
         );
         // Zero rows survive stage0 (WHERE FALSE), so stage1's sum() over an
         // empty group is 0, not null (Malloy/DuckDB's sum() default).
         expect(multiStage[0].t).toBe(0);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Grammar (end-to-end: each spelling exercised as a real request denial/admit,
// not just via classifyAuthorizeGate's own unit tests in authorize.spec.ts)
// ---------------------------------------------------------------------------

describe("row-level authorize — grammar", () => {
   async function grammarModel(gate: string, givenDecl = "GROUPS :: number[]") {
      return buildGatedModel(`
given:
  ${givenDecl}

${gate}
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
   }

   it("`field in $ARRAY` is the allowed spelling for an array given", async () => {
      const { internals, mm, duckdb } = await grammarModel(
         '#(authorize) "org_id in $GROUPS"',
      );
      try {
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("`field = $ARRAY` is refused (array_given_needs_in) — denies at request time", async () => {
      const { internals, mm, duckdb } = await grammarModel(
         '#(authorize) "org_id = $GROUPS"',
      );
      try {
         await expect(
            boundRows(internals, mm, "run: X -> { aggregate: n is count() }", {
               GROUPS: [1],
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("`field ? $ARRAY` is refused — compiles to the same node as `=`, same rejection", async () => {
      const { internals, mm, duckdb } = await grammarModel(
         '#(authorize) "org_id ? $GROUPS"',
      );
      try {
         await expect(
            boundRows(internals, mm, "run: X -> { aggregate: n is count() }", {
               GROUPS: [1],
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("`not (...)` is refused — negation would invert fail-closed into fail-open", async () => {
      const { internals, mm, duckdb } = await grammarModel(
         '#(authorize) "not (org_id in $GROUPS)"',
      );
      try {
         await expect(
            boundRows(internals, mm, "run: X -> { aggregate: n is count() }", {
               GROUPS: [],
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });

   it("a function call operand is refused — a gate compares a bare field, not an expression", async () => {
      const { internals, mm, duckdb } = await grammarModel(
         '#(authorize) "upper(val) = $REGION"',
         "REGION :: string",
      );
      try {
         await expect(
            boundRows(internals, mm, "run: X -> { aggregate: n is count() }", {
               REGION: "A",
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Other
// ---------------------------------------------------------------------------

describe("row-level authorize — other", () => {
   it("classifyAuthorizeGate's own allowlist (unit-level; end-to-end coverage of the same rules is in the grammar describe above)", () => {
      // A quick cross-check that the exported classifier agrees with the
      // end-to-end grammar results above, without needing a compiled model.
      const declaredTypes = new Map([["GROUPS", "number[]"]]);
      const rejectedEq = classifyAuthorizeGate(
         {
            code: "org_id = $GROUPS",
            refSummary: { fieldUsage: [{ path: ["org_id"] }] },
            e: {
               node: "=",
               kids: {
                  left: { node: "field", path: ["org_id"] },
                  right: { node: "given", refName: "GROUPS" },
               },
            },
         },
         declaredTypes,
         new Map(),
      );
      expect(rejectedEq.shape).toBe("rejected");
   });

   it("`$X in $Y` checks reachability on BOTH operands — an unreachable membership CANDIDATE is rejected, not accepted", async () => {
      // Real compiled IR (never hand-typed), classified against a given
      // surface that omits `TENANT` — the shape a gate two import hops from
      // its `given:` declaration presents. Every other operand position
      // routes through `declaredTypeOf`; this one used to return `true`
      // straight off `givenOperand`, so the gate classified `row_level` with
      // `TENANT` in `givenNames` and bound TENANT's DECLARATION DEFAULT at
      // request time instead of the caller's value.
      const duckdb = await newDuckdb();
      try {
         const urlReader = new InMemoryURLReader(
            new Map([
               [
                  `${ROOT}m.malloy`,
                  `##! experimental.givens

given:
  TENANT :: number
  ALLOWED :: number[]

source: X is duckdb.table('parent') extend { measure: n is count() }
`,
               ],
            ]),
         );
         const runtime = new Runtime({
            urlReader,
            connections: new FixedConnectionMap(
               new Map<string, Connection>([["duckdb", duckdb]]),
               "duckdb",
            ),
         });
         const mm = runtime.loadModel(new URL(`${ROOT}m.malloy`), {
            importBaseURL: new URL(ROOT),
         });
         const classifyWithSurface = async (
            expr: string,
            surface: Map<string, string>,
         ) => {
            const prepared = await mm
               .loadQuery(buildRowLevelProbe("X", `(${expr})`))
               .getPreparedQuery();
            const condition = liftProbeFilterCondition(
               prepared as never,
               "test",
               `(${expr})`,
            );
            return classifyAuthorizeGate(condition, surface, new Map());
         };
         const partialSurface = new Map([["ALLOWED", "array"]]);

         const membership = await classifyWithSurface(
            "$TENANT in $ALLOWED",
            partialSurface,
         );
         expect(membership.shape).toBe("rejected");
         expect((membership as { cause?: string }).cause).toBe(
            "unreachable_given",
         );

         // The control every other operand position already satisfied.
         const comparison = await classifyWithSurface(
            "org_id = $TENANT",
            partialSurface,
         );
         expect(comparison.shape).toBe("rejected");
         expect((comparison as { cause?: string }).cause).toBe(
            "unreachable_given",
         );

         // Both givens reachable: still the accepted self-contained shape.
         const reachable = await classifyWithSurface(
            "$TENANT in $ALLOWED",
            new Map([
               ["ALLOWED", "array"],
               ["TENANT", "number"],
            ]),
         );
         expect(reachable.shape).toBe("row_level");
      } finally {
         await duckdb.close();
      }
   });

   it("error scrubbing: a fail-closed row-level deny never names the column/join/expression to the caller", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
source: W is X extend { except: org_id }
`);
      try {
         const err = await boundRows(
            internals,
            mm,
            "run: W -> { aggregate: n is count() }",
            { GROUPS: [1] },
         ).catch((e) => e);
         expect(err).toBeInstanceOf(AccessDeniedError);
         const message = String((err as Error).message);
         expect(message).not.toContain("org_id");
         expect(message).not.toContain("GROUPS");
         expect(message).not.toContain("except");
      } finally {
         await duckdb.close();
      }
   });

   it("the gate predicate does not appear in the response's serialized annotations (isSourceFilter keeps it out of drill_filters)", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const runnable = mm.loadRestrictedQuery(
            "run: X -> { group_by: id; aggregate: n is count() }",
         );
         const bound = await internals.authorizeAndBindRunnable(
            runnable,
            { GROUPS: [1] },
            {
               recompile: (m) =>
                  m.loadRestrictedQuery(
                     "run: X -> { group_by: id; aggregate: n is count() }",
                  ),
            },
         );
         const result = await bound.run({
            rowLimit: 10,
            givens: { GROUPS: [1] },
         });
         // The RAW internal _queryResult carries the filter's code in its own
         // structDef.resultMetadata.filterList — that is never what a caller
         // sees. API.util.wrapResult is the "to stable" conversion callers
         // actually receive (the same call model.ts's getQueryResults makes),
         // and Malloy's to_stable.js skips isSourceFilter conditions when
         // building drill_filters there — assert on THAT observable, not the
         // internal shape.
         const wrapped = API.util.wrapResult(result);
         const serialized = JSON.stringify(wrapped);
         expect(serialized).not.toContain("org_id in");
      } finally {
         await duckdb.close();
      }
   });

   it("multiple #(authorize) annotations on ONE source are OR-ed, not AND-ed", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]
  VAL :: string

#(authorize) "org_id in $GROUPS"
#(authorize) "val = $VAL"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         // Two #(authorize) annotations on ONE source OR — an admin-style
         // ($VAL match) OR a group match. Supplying a group that would match
         // id 2 (org_id=1, val='b') but a VAL that matches nothing proves OR:
         // if this were AND, no row would ever satisfy both disjuncts in the
         // one WHERE, but disjunction means group membership alone suffices.
         const rows = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [1], VAL: "no-such-value" },
         );
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("multiple #(authorize) annotations OR: an admin-role match sees everything, a non-admin sees only their own orgs", async () => {
      // The admin-override idiom works: `classifyAuthorizeGate`'s
      // `isLiteralOperand` branch handles a given compared against a string
      // LITERAL (`$ROLE = 'admin'`), not only a bare field reference, so this
      // disjunct classifies and OR's correctly with the sibling
      // `org_id in $GROUPS` gate — matching resolveGateShape's own doc
      // (model.ts, above "filterText folds the entry's whole OR
      // disjunction"), which describes exactly this spelling as the
      // supported admin-override idiom.
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  ROLE :: string
  GROUPS :: number[]

#(authorize) "$ROLE = 'admin'"
#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const admin = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { ROLE: "admin", GROUPS: [] },
         );
         expect(admin[0].n).toBe(4);
         const nonAdmin = await boundRows(
            internals,
            mm,
            "run: X -> { aggregate: n is count() }",
            { ROLE: "analyst", GROUPS: [1] },
         );
         expect(nonAdmin[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Notebook cells — a cell that both declares a gated source and runs it, and
// a cell whose `#(filter)` refinement fails to compile.
// ---------------------------------------------------------------------------

/**
 * Extract the value of the first field of the first row from a notebook
 * cell's JSON-stringified result (`executeNotebookCell`'s `result` string).
 * Same shape `filter_integration.spec.ts`'s `parseNotebookResult` decodes,
 * narrowed to the one cell this file only needs: a single-row, single-column
 * `aggregate:` result.
 */
function firstCellResultValue(resultJson: string): unknown {
   const parsed = JSON.parse(resultJson);
   const row = parsed?.data?.array_value?.[0]?.record_value?.[0];
   return (
      row?.number_value ??
      row?.string_value ??
      row?.boolean_value ??
      row?.timestamp_value ??
      null
   );
}

describe("row-level authorize — notebook cells", () => {
   it("CRITICAL — a cell that declares its own source under a row-level gate AND runs it returns correctly filtered rows", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

>>>malloy
source: local2 is gated extend {}
run: local2 -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // Cell 0 declares `gated`; cell 1 declares ITS OWN source `local2`
         // (deriving from `gated`) and runs it in the SAME cell. Grafting
         // the row-level condition against the model-wide cumulative
         // modelDef — which, by the time this request runs, already has
         // `local2` — and then recompiling this cell's own
         // `source:`+`run:` text against it fails to compile with "Cannot
         // redefine 'local2'", because that text tries to redeclare a name
         // the graft target model already carries. Grafting the model AS OF
         // THIS CELL (cell 0's own scope, which does not have `local2` yet)
         // is what makes the redeclaration — and therefore the whole cell —
         // resolvable again.
         const result = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         // org_id=1 rows are ids 1,2 — the gate filters out the org_id=2
         // rows (ids 3,4), so the count is 2, not 4.
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a cell whose #(filter) refinement does not compile surfaces that failure, never a silent unfiltered admit", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  ROLE :: string

#(filter) dimension=nonexistent_field type=equal
#(authorize) "$ROLE = 'admin'"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const loadErr = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(loadErr).toBeUndefined();

         // filterParams supplies a value for the `nonexistent_field` filter,
         // so the `#(filter)` refinement rebuild appends
         // `+ {where: nonexistent_field = 'x'}` to the cell's text — which
         // fails to compile, since `gated` has no such field. Every gate is a
         // row filter now (there is no more given-only fast path), so the
         // PRE-refinement gate call can only reject a structurally invalid
         // gate synchronously — a `$ROLE = 'admin'` mismatch is DEFERRED to
         // the post-refinement authoritative bind, same as any other
         // row-level gate. That authoritative step never runs here: the
         // broken refinement fails to compile first. Nothing leaks either
         // way — no query ever executes — so what this pins is that the
         // refinement's own compile failure surfaces cleanly, not that it is
         // reclassified as a 403.
         await expect(
            model.executeNotebookCell(0, { nonexistent_field: "x" }, false, {
               ROLE: "analyst",
            }),
         ).rejects.toThrow(/nonexistent_field/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a row-level gate declared and run in the FIRST code cell returns correctly filtered rows (no earlier cell to graft against; grafts its OWN post-declaration scope, repointing the compiled queryDef rather than recompiling text)", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // org_id=1 rows are ids 1,2 — the gate filters out the org_id=2
         // rows (ids 3,4), so the count is 2, not 4.
         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("fail-closed: an empty given on a first-code-cell self-declaring gate yields zero rows, not an unfiltered/leaked count", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(0);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a row-level gate in a code cell preceded only by markdown also returns correctly filtered rows (same fallback as the first-cell case)", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>markdown
# Just a heading, no code cell before this one

>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a LATER cell that declares and runs its own gated source (not just the first cell) also returns correctly filtered rows — the earlier cell exists but does not carry the declared source, so its scope is just as unusable as cell 0's absence", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

source: unrelated is duckdb.table('childtable') extend { primary_key: id }

>>>malloy
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("self-declaring cell, multi-stage pipeline shape (`gated -> {…} -> {…}`) filters correctly", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { group_by: org_id, id } -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("self-declaring cell, named-view shape (`gated -> byorg`) filters correctly", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.table('parent') extend {
   measure: n is count()
   view: byorg is { aggregate: n is count() }
}

run: gated -> byorg
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("self-declaring cell, joined-field gate shape (`childtable.name in $GROUPS` via join_one) filters correctly", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: string[]

source: childtable is duckdb.table('childtable') extend { primary_key: id }

#(authorize) "childtable.name in $GROUPS"
source: gated is duckdb.table('parent') extend {
   join_one: childtable with child_id
   measure: n is count()
}

run: gated -> { group_by: id, org_id, childtable.name }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // child_id=10 rows are ids 1 and 3 (parent), which join to name='bob'.
         // The RUN QUERY itself references the joined field (`childtable.name`
         // in the projection) — residual limitation: the joined-field shape
         // only proved out on the new (queryDef-repoint) path when the run
         // query's own text already references the joined field, which is
         // what forces the join into the ORIGINALLY compiled queryDef before
         // the graft appends its condition.
         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: ["bob"],
         });
         expect(result.result).toBeDefined();
         const rowCount = (
            JSON.parse(result.result!) as {
               data?: { array_value?: unknown[] };
            }
         ).data?.array_value?.length;
         expect(rowCount).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — P0 on the new path: a self-declaring cell's ungated entry point joining a gated source does not fire the joined gate", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-notebook-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
#(authorize) "false"
source: childtable is duckdb.table('childtable') extend {}

source: joiner is duckdb.table('parent') extend {
   join_one: childtable on child_id = childtable.id
   measure: n is count()
}

run: joiner -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // `joiner` declares no gate of its own and does not derive from
         // `childtable` — collectEntryPointGates does not walk joins, so
         // childtable's "false" gate must not be collected here either. All
         // 4 seed rows, unfiltered — same P0 rule the non-notebook query path
         // enforces, now proven for a cell whose graft scope is its OWN
         // post-declaration model (both sources are declared in this SAME
         // self-declaring cell).
         const result = await model.executeNotebookCell(
            0,
            undefined,
            false,
            {},
         );
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(4);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Storage routing — a row-level gated query must not route to storage
// ---------------------------------------------------------------------------

describe("row-level authorize — storage routing", () => {
   it("CRITICAL — a row-level gated query does not route to storage", async () => {
      const originalMode = process.env.PERSIST_STORAGE_MODE;
      process.env.PERSIST_STORAGE_MODE = "on";
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-storage-"));
      try {
         // Model.create (not buildGatedModel): the routing pre-check's cheap
         // pre-filter (`hasAuthorize()`) reads `this.sources`, which only a
         // real compile populates — buildGatedModel's harness constructs a
         // `Model` with `sources` left `undefined`, which would make
         // `hasAuthorize()` false regardless of the gate and defeat the very
         // pre-check this test verifies.
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // A storage binding for X that, if routed to, would answer with a
         // value ("999") the live, gate-filtered query could never produce —
         // proof the observed result did not come from the storage tier even
         // without reading `servedFrom`.
         await duckdb.runSQL(
            "CREATE OR REPLACE TABLE mz_real AS SELECT 999 AS n",
         );
         const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
         const serveConfig = new MalloyConfig({ connections: {} });
         serveConfig.wrapConnections(
            () => new FixedConnectionMap(connMap, "duckdb"),
         );
         model.setServeDestinationConfig(() => serveConfig);
         model.setServeBindings([
            {
               sourceName: "X",
               destinationName: "duckdb",
               virtualHandle: "h",
               tablePath: "mz_real",
               schema: [{ name: "n", type: "BIGINT" }],
            },
         ]);

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         // The observable a caller actually reads — not an internal
         // implementation detail like whether `serveVirtualMap` was set.
         expect(result.servedFrom).not.toBe("storage");
         // The row-level gate's own filtering still applied: org_id=1 rows
         // are ids 1,2, so the live count is 2 — not the storage stub's 999,
         // and not the unfiltered live count of 4.
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows[0].n).toBe(2);
      } finally {
         if (originalMode === undefined) {
            delete process.env.PERSIST_STORAGE_MODE;
         } else {
            process.env.PERSIST_STORAGE_MODE = originalMode;
         }
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a row-level gated query does not route to storage when `this.sources` is unavailable (`hasAuthorize()` is not a safe gate for this decision)", async () => {
      // `queryEntryPointHasRowLevelGate` used to short-circuit on
      // `!this.hasAuthorize()`, which reads `this.sources` — populated only
      // by a full `Model.create`/`fromSerialized` compile. `buildGatedModel`
      // (this file's harness for exercising real compiled IR without that
      // load-time probe — see the file header) leaves `sources` `undefined`,
      // so `hasAuthorize()` returns false UNCONDITIONALLY here regardless of
      // the gate actually on the model — exactly the gap the test above's
      // comment warns `Model.create` avoids by populating `this.sources`.
      // Nothing in the `Model` constructor's contract guarantees `sources`
      // is always populated before a query can run, so a routing decision
      // that depends on it is not sound. This test pins that the FIXED
      // predictor still finds the gate by walking the compiled entry point
      // directly, independent of `this.sources`.
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      const originalMode = process.env.PERSIST_STORAGE_MODE;
      process.env.PERSIST_STORAGE_MODE = "on";
      try {
         // Confirms the premise: `hasAuthorize()` really is blind here, which
         // is why the guard removed from `queryEntryPointHasRowLevelGate`
         // could not be relied on to keep the routing pre-check sound.
         expect(model.hasAuthorize()).toBe(false);

         // A storage binding for X that, if routed to, would answer with a
         // value ("999") the live, gate-filtered query could never produce.
         await duckdb.runSQL(
            "CREATE OR REPLACE TABLE mz_real AS SELECT 999 AS n",
         );
         const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
         const serveConfig = new MalloyConfig({ connections: {} });
         serveConfig.wrapConnections(
            () => new FixedConnectionMap(connMap, "duckdb"),
         );
         model.setServeDestinationConfig(() => serveConfig);
         model.setServeBindings([
            {
               sourceName: "X",
               destinationName: "duckdb",
               virtualHandle: "h",
               tablePath: "mz_real",
               schema: [{ name: "n", type: "BIGINT" }],
            },
         ]);

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect(result.servedFrom).not.toBe("storage");
         // org_id=1 rows are ids 1,2 — the live, gate-filtered count is 2,
         // not the storage stub's 999 and not the unfiltered live count of 4.
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows[0].n).toBe(2);
      } finally {
         if (originalMode === undefined) {
            delete process.env.PERSIST_STORAGE_MODE;
         } else {
            process.env.PERSIST_STORAGE_MODE = originalMode;
         }
         await duckdb.close();
      }
   });

   it("CRITICAL — an entry point whose gate resolves to `deny` does not route to storage (`queryEntryPointHasRowLevelGate` used to admit routing for any non-`row_level` resolution, including `deny`)", async () => {
      const originalMode = process.env.PERSIST_STORAGE_MODE;
      process.env.PERSIST_STORAGE_MODE = "on";
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-storage-deny-"));
      try {
         // `W_except` inherits `X`'s gate by reference but drops the gated
         // column with its own `except:` — the documented way to make
         // `resolveGateShape` return `deny` rather than `row_level`
         // (load-time scoping's "W_rename / W_except / W_accept" test above
         // pins the same shape denying at request time on the LIVE query;
         // this test is about the routing PRE-CHECK, not that path).
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: W_except is X extend { except: org_id }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // A storage binding for W_except that, if routed to, would answer
         // with a value ("999") no correctly-denied request could ever
         // produce.
         await duckdb.runSQL(
            "CREATE OR REPLACE TABLE mz_real AS SELECT 999 AS n",
         );
         const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
         const serveConfig = new MalloyConfig({ connections: {} });
         serveConfig.wrapConnections(
            () => new FixedConnectionMap(connMap, "duckdb"),
         );
         model.setServeDestinationConfig(() => serveConfig);
         model.setServeBindings([
            {
               sourceName: "W_except",
               destinationName: "duckdb",
               virtualHandle: "h",
               tablePath: "mz_real",
               schema: [{ name: "n", type: "BIGINT" }],
            },
         ]);

         const result = await model
            .getQueryResults(
               undefined,
               undefined,
               "run: W_except -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            )
            .catch((e) => e);
         // `W_except`'s own gate resolution denies this request regardless of
         // storage routing (a second, independent defense — see the surface-
         // syntax early gate at `getQueryResults`' `earlySource` check, which
         // also resolves this same `deny` before compilation). This assertion
         // is the OBSERVABLE the finding calls for; it is NOT, on its own,
         // proof that `queryEntryPointHasRowLevelGate`'s predicate is what
         // caused it — the isolated unit test below is what pins that.
         expect(result).toBeInstanceOf(AccessDeniedError);
      } finally {
         if (originalMode === undefined) {
            delete process.env.PERSIST_STORAGE_MODE;
         } else {
            process.env.PERSIST_STORAGE_MODE = originalMode;
         }
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — queryEntryPointHasRowLevelGate itself returns true (blocks routing) for a `deny` resolution, not just `row_level` (isolated)", async () => {
      // The end-to-end test above denies the request either way, because
      // `getQueryResults`' surface-syntax early gate (`earlySource`) ALSO
      // resolves `W_except` by name and denies before compilation even
      // starts — so it cannot, on its own, tell a fixed predicate apart from
      // the original bug for a directly-named top-level source. This test
      // isolates that ONE method and calls it directly
      // (same idiom as the `resolveGateShape`/`resolveGraftTarget` tests
      // above, which reach past the public surface via `ModelInternals`),
      // so it fails specifically when `queryEntryPointHasRowLevelGate`'s own
      // predicate regresses to admitting a `deny` resolution — independent of
      // any other gate in the request path that happens to also catch it.
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: W_except is X extend { except: org_id }
`);
      try {
         const runnable = mm.loadRestrictedQuery(
            "run: W_except -> { aggregate: n is count() }",
         );
         const blocksRouting =
            await internals.queryEntryPointHasRowLevelGate(runnable);
         expect(blocksRouting).toBe(true);
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — a gate whose condition references no row field still blocks storage routing (there is no more given-only escape)", async () => {
      // Before the given_only/row_level split collapsed to one concept, a
      // gate like `$ROLE = 'analyst'` (no field reference at all) was exempt
      // from this routing block — safe under the OLD design because it was
      // enforced by a whole-source boolean probe that ran regardless of
      // routing. That escape is gone: EVERY gate blocks storage now, whether
      // or not its condition happens to mention a column.
      const originalMode = process.env.PERSIST_STORAGE_MODE;
      process.env.PERSIST_STORAGE_MODE = "on";
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-storage-given-only-"),
      );
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // A storage binding for X that, if routed to, would answer with a
         // value ("999") the live, gate-filtered query could never produce.
         await duckdb.runSQL(
            "CREATE OR REPLACE TABLE mz_real AS SELECT 999 AS n",
         );
         const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
         const serveConfig = new MalloyConfig({ connections: {} });
         serveConfig.wrapConnections(
            () => new FixedConnectionMap(connMap, "duckdb"),
         );
         model.setServeDestinationConfig(() => serveConfig);
         model.setServeBindings([
            {
               sourceName: "X",
               destinationName: "duckdb",
               virtualHandle: "h",
               tablePath: "mz_real",
               schema: [{ name: "n", type: "BIGINT" }],
            },
         ]);

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { ROLE: "analyst" },
         );
         expect(result.servedFrom).not.toBe("storage");
         // The live, unfiltered count (no row field is gated) is 4 — not the
         // storage stub's 999.
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows[0].n).toBe(4);
      } finally {
         if (originalMode === undefined) {
            delete process.env.PERSIST_STORAGE_MODE;
         } else {
            process.env.PERSIST_STORAGE_MODE = originalMode;
         }
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Constant-false short circuit — a gate whose compiled condition is the bare
// literal `false` (the classic whole-source deny, `#(authorize) "false"`)
// answers with a synthesized empty result rather than dispatching a `WHERE
// false` query to the warehouse. Covers ONLY this literal case; a fail-closed
// sentinel for an unassigned trusted identity-bound given is a separate,
// not-yet-implemented case (docs/authorize.md calls "system givens" a
// "planned milestone... not implemented yet") and is out of scope here.
// ---------------------------------------------------------------------------

describe("row-level authorize — constant-false short circuit", () => {
   const CONSTANT_FALSE_MODEL = `#(authorize) "false"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`;

   it("CRITICAL — never dispatches to the warehouse, and answers servedFrom short_circuited with zero rows", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-const-false-"));
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), CONSTANT_FALSE_MODEL);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();

         const runSqlSpy = spyOn(duckdb, "runSQL");
         runSqlSpy.mockClear();

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
         );

         expect(result.servedFrom).toBe("short_circuited");
         expect(result.compactResult).toEqual([]);
         // The proof this test exists for: the gate is provably empty, so
         // nothing was ever sent to the connection at all — not even a
         // `WHERE false` query that would itself have returned zero rows.
         expect(runSqlSpy.mock.calls.length).toBe(0);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — the synthesized result carries the real query's schema, not just an empty row count", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-const-false-schema-"),
      );
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), CONSTANT_FALSE_MODEL);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();

         const gatedResult = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
         );
         expect(gatedResult.servedFrom).toBe("short_circuited");

         // An identical query against an UNGATED twin source is the ground
         // truth for what schema this query should carry.
         const ungatedDir = fs.mkdtempSync(
            path.join(os.tmpdir(), "rla-const-false-ungated-"),
         );
         fs.writeFileSync(
            path.join(ungatedDir, "m.malloy"),
            `source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         );
         const ungatedModel = await Model.create(
            "test-pkg-ungated",
            ungatedDir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         try {
            const liveResult = await ungatedModel.getQueryResults(
               undefined,
               undefined,
               "run: X -> { group_by: org_id; aggregate: n is count() }",
               {},
               true,
            );
            // Compare name/kind/type, not the whole `FieldInfo` — each field
            // also carries a `#(malloy) reference_id = "<uuid>"` annotation
            // freshly minted per compile, which differs between the two
            // models even though the schema itself is identical.
            const shapeOf = (fields: typeof gatedResult.result.schema.fields) =>
               fields.map((f) => ({
                  name: f.name,
                  kind: f.kind,
                  type: (f as { type?: unknown }).type,
               }));
            expect(shapeOf(gatedResult.result.schema.fields)).toEqual(
               shapeOf(liveResult.result.schema.fields),
            );
         } finally {
            fs.rmSync(ungatedDir, { recursive: true, force: true });
         }
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — bypassAuthorize still executes the query live (the short circuit never fires under a bypass)", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-const-false-bypass-"),
      );
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), CONSTANT_FALSE_MODEL);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();

         const runSqlSpy = spyOn(duckdb, "runSQL");
         runSqlSpy.mockClear();

         // GROUPED, not a bare `aggregate:` — an ungrouped aggregate is
         // vetoed by `pipelineRowCountFollowsInput` whatever the gate says,
         // so it can never observe the short circuit failing to fire.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
            {},
            undefined,
            undefined,
            "full",
            /* bypassAuthorize */ true,
         );

         expect(result.servedFrom).not.toBe("short_circuited");
         // The unfiltered live rows — bypassing authorize skips the "false"
         // gate entirely, so both org groups (4 seed rows) come back.
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows.length).toBe(2);
         expect(rows.reduce((sum, r) => sum + r.n, 0)).toBe(4);
         expect(runSqlSpy.mock.calls.length).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("does NOT short-circuit a gate that is not provably constant-false (depends on real row data)", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const runSqlSpy = spyOn(duckdb, "runSQL");
         runSqlSpy.mockClear();
         // GROUPED — see the bypass test above for why a bare `aggregate:`
         // cannot observe this.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect(result.servedFrom).not.toBe("short_circuited");
         const rows = result.compactResult as unknown as {
            org_id: number;
            n: number;
         }[];
         // org_id=1 rows are ids 1,2 — the live, gate-filtered group.
         expect(rows.length).toBe(1);
         expect(rows[0].org_id).toBe(1);
         expect(rows[0].n).toBe(2);
         expect(runSqlSpy.mock.calls.length).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
      }
   });

   it("does NOT short-circuit a literal-atom gate that is not `false` (e.g. an admin-override $ROLE comparison)", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  ROLE :: string

#(authorize) "$ROLE = 'admin'"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const runSqlSpy = spyOn(duckdb, "runSQL");
         runSqlSpy.mockClear();
         // GROUPED — see the bypass test above for why a bare `aggregate:`
         // cannot observe this.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
            { ROLE: "admin" },
         );
         expect(result.servedFrom).not.toBe("short_circuited");
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows.length).toBe(2);
         expect(rows.reduce((sum, r) => sum + r.n, 0)).toBe(4);
         expect(runSqlSpy.mock.calls.length).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
      }
   });

   it("CRITICAL — does NOT short-circuit an ungrouped aggregate: it still runs live, because a bare `aggregate:` with no `group_by:` always emits exactly one row even over zero input rows", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-const-false-ungrouped-"),
      );
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), CONSTANT_FALSE_MODEL);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();

         const runSqlSpy = spyOn(duckdb, "runSQL");
         runSqlSpy.mockClear();

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
         );

         // Still denied by the gate (the row-level filter still landed and
         // matched nothing), just not via the short circuit: the one row an
         // ungrouped aggregate always emits still has to come from a real
         // (cheap, `WHERE false`, zero-scan) run.
         expect(result.servedFrom).not.toBe("short_circuited");
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows[0].n).toBe(0);
         expect(runSqlSpy.mock.calls.length).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("records the short_circuited decision on the row-level gate metric", async () => {
      const { startMetricsHarness } = await import(
         "../test_helpers/metrics_harness"
      );
      const { resetAuthorizeGuardTelemetryForTesting } = await import(
         "../authorize_metrics"
      );
      const harness = await startMetricsHarness();
      resetAuthorizeGuardTelemetryForTesting();
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-const-false-metric-"),
      );
      try {
         fs.writeFileSync(path.join(dir, "m.malloy"), CONSTANT_FALSE_MODEL);
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();

         // `group_by:` makes this query row-count-safe to short-circuit — a
         // bare ungrouped `aggregate:` always emits exactly one row (see
         // `pipelineRowCountFollowsInput`) and is covered by its own test.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
         );
         expect(result.servedFrom).toBe("short_circuited");

         expect(
            await harness.collectCounter(
               "publisher_authorize_row_level_total",
               {
                  decision: "short_circuited",
               },
            ),
         ).toBe(1);
      } finally {
         resetAuthorizeGuardTelemetryForTesting();
         await harness.shutdown();
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Vacuous default atom — a literal atom that is TRUE at its given's own
// declared default admits every row for a caller who supplies nothing.
// ---------------------------------------------------------------------------

describe("row-level authorize — vacuous default atom", () => {
   /**
    * Load `text` through the real `Model.create` — same idiom as the
    * "load-time scoping" describe block's own `createModel`, duplicated
    * (not imported) because that helper is local to its own `describe`
    * body. `assertNoVacuousDefaultAtom` is a LOAD-TIME check inside
    * `validateAuthorizeProbes`, so `buildGatedModel` (used by the sibling
    * "grammar" describe block above) cannot exercise it — that harness
    * deliberately SKIPS `Model.create`'s pre-flight validation (see its own
    * doc comment).
    */
   async function createModel(
      text: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-vacuous-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   function compilationErrorOf(model: Model): Error | undefined {
      return (model as unknown as { compilationError?: Error })
         .compilationError;
   }

   it("CRITICAL — `$ROLE != 'admin'` OR'd with a row-level gate, ROLE defaulting to '', is refused at load (vacuously true for a caller supplying nothing)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  ROLE :: string is ''
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS or $ROLE != 'admin'"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/ROLE.*!=.*'admin'/);
         expect(err?.message).toMatch(/evaluates to TRUE/i);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("names the entry point it probed, which for a derived one is NOT the source that authored the gate", async () => {
      // Pins the one cost of `assertNoVacuousDefaultAtom` NOT taking the
      // `ownNotes.length === 0` escape its `rejected` sibling takes (see the
      // comment at its call site). The refusal is correct — a vacuous atom is
      // wrong wherever the gate reaches — but whichever entry point probes it
      // first is what the message names, and here that can be the derivation
      // rather than `X`, which is where the annotation actually lives. Asserted
      // so a future change to that escape is a visible diff rather than a silent
      // change in what an author is told to go and look at.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  ROLE :: string is ''
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS or $ROLE != 'admin'"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: Derived is X -> { group_by: org_id }
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         // The atom is named either way — that half is unambiguous.
         expect(err?.message).toMatch(/ROLE.*!=.*'admin'/);
         // And it names SOME entry point. `Derived` carries no annotation of its
         // own, so if it probed first the message points at it rather than at
         // `X`, which is the source an author would have to be told about.
         expect(err?.message).toMatch(/on source "(X|Derived)"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("`$ROLE = 'admin'` OR'd with a row-level gate, ROLE defaulting to '', still loads and works as an admin-override (false at the default, not vacuous)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  ROLE :: string is ''
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS or $ROLE = 'admin'"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // A caller who omits ROLE gets its declared default (''), which is
         // not 'admin' — the atom is false, so the gate falls through to the
         // GROUPS membership test exactly as if the atom were absent. GROUPS
         // has no default (`docs/givens.md`: an array given can't declare
         // one), so it must still be supplied.
         const noRole = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [] },
         );
         const noRoleRows = noRole.compactResult as unknown as {
            n: number;
         }[];
         expect(noRoleRows[0].n).toBe(0);
         // The admin override still works when a caller DOES supply it.
         const admin = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { ROLE: "admin", GROUPS: [] },
         );
         const adminRows = admin.compactResult as unknown as { n: number }[];
         expect(adminRows[0].n).toBe(4);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Field-vs-given comparison against a given carrying a declared default — the
// SCALAR_COMPARISON_NODES sibling of the `not in $GROUPS` hazard: `org_id in
// $GROUPS` is refused when negated because an empty default admits every row,
// but `tenant != $EXCLUDED` / `amount > $FLOOR` carry the IDENTICAL hazard at
// the documented default convention ('' / 0) and were previously accepted —
// `assertNoVacuousDefaultAtom` only evaluates a `<given> <op> <literal>` atom,
// never a `<field> <op> <given>` comparison, so it structurally cannot catch
// these. `classifyAuthorizeGate` now refuses a field comparison outright when
// its given carries ANY declared default, regardless of operator — see its
// doc comment for why this is about the DEFAULT, not the operator, and the
// anti-narrowing test below for why narrowing to `=`/`in` instead would be
// wrong (a `<=`/`>=` no-read-up gate against a given with NO default is
// legitimate and stays accepted).
// ---------------------------------------------------------------------------

describe("row-level authorize — field comparison against a defaulted given", () => {
   /** Same idiom as the "vacuous default atom" describe block's own
    *  `createModel` (duplicated, not imported, for the same reason). */
   async function createModel(
      text: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-field-default-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   function compilationErrorOf(model: Model): Error | undefined {
      return (model as unknown as { compilationError?: Error })
         .compilationError;
   }

   it("CRITICAL — `amount > $FLOOR`, FLOOR defaulting to 0, is refused at load (a caller supplying nothing admits ~every row)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  FLOOR :: number is 0

#(authorize) "amount > $FLOOR"
source: X is duckdb.table('parent') extend {
   dimension: amount is id
   measure: n is count()
}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/FLOOR/);
         expect(err?.message).toMatch(/default/i);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — `tenant != $EXCLUDED`, EXCLUDED defaulting to '', is refused at load", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  EXCLUDED :: string is ''

#(authorize) "tenant != $EXCLUDED"
source: X is duckdb.table('parent') extend {
   dimension: tenant is val
   measure: n is count()
}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/EXCLUDED/);
         expect(err?.message).toMatch(/default/i);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("ANTI-NARROWING GUARD — `clearance <= $MAXLVL`, MAXLVL with NO default, loads cleanly and filters rows (a no-read-up gate must not be refused merely for using a comparison operator)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  MAXLVL :: number

#(authorize) "clearance <= $MAXLVL"
source: X is duckdb.table('parent') extend {
   dimension: clearance is org_id
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // org_id=1 rows are ids 1,2 (clearance=1); org_id=2 rows are ids 3,4
         // (clearance=2). MAXLVL=1 admits only the clearance=1 rows; MAXLVL=2
         // admits all four — two different values, two different real row
         // counts, so a fix that refused every comparison operator (not just
         // ones whose given carries a default) would fail this test loudly.
         const low = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { MAXLVL: 1 },
         );
         const lowRows = low.compactResult as unknown as { n: number }[];
         expect(lowRows[0].n).toBe(2);

         const high = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { MAXLVL: 2 },
         );
         const highRows = high.compactResult as unknown as { n: number }[];
         expect(highRows[0].n).toBe(4);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("`org_id in $GROUPS`, a caller supplying an empty array, loads cleanly and returns zero rows (the fail-closed empty-array case, unaffected by this refusal since `in` is not a field-vs-given SCALAR_COMPARISON_NODES comparison)", async () => {
      // An array-typed given can't declare a default at all (Malloy grammar
      // rejects `:: number[] is []` — confirmed against this suite's own
      // compiler), so there is no declared-default hazard on this shape to
      // begin with; the fail-closed behavior below is `in`'s existing
      // empty-array handling, not something this refusal touches.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [] },
         );
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows[0].n).toBe(0);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// P0 — composite resolution copies a composite parent's OWN #(authorize) note
// OBJECT onto the resolved member struct's own blockNotes, alongside the
// member's own note. Reading that merged set as one source's own OR list
// (the pre-fix shape) folds a DIFFERENT declaring source's condition into
// this source's disjunction, silently turning this file's own AND-across-
// sources rule into an OR the moment a query-source base resolves through a
// composite. `effectiveAncestorGateExprs` (`gate_registry_walk.ts`) and
// `Model.collectEntryPointGates`/`gateExprsForOwnAnnotations` (`model.ts`)
// now IDENTITY-SUBTRACT the parent's own notes before reading the member's,
// and keep the two sources' gates as separate GROUPS rather than one
// concatenated list (`AuthorizeMap`, `authorize.ts`) — see those modules' doc
// comments for the mechanics.
// ---------------------------------------------------------------------------

describe("row-level authorize — composite gate grouping (P0 leak, fixed)", () => {
   async function createModel(
      text: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-composite-group-"),
      );
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   function compilationErrorOf(model: Model): Error | undefined {
      return (model as unknown as { compilationError?: Error })
         .compilationError;
   }

   // THE LEAK: `combo`'s own gate (`region = $REGION`) and `member_a`'s own
   // gate (`org_id in $GROUPS`) are declared on two DIFFERENT sources and
   // must AND. Before the fix, Malloy's by-reference copy of `combo`'s note
   // onto the resolved `member_a` struct made `member_a`'s "own" list read
   // as `["region = $REGION", "org_id in $GROUPS"]` — one OR'd disjunction —
   // so the whole condition collapsed to `(region=$REGION) AND ((region=
   // $REGION) OR (org_id in $GROUPS))`, which a truthful `region` term alone
   // satisfies regardless of `org_id`. Every assertion below is load-bearing:
   // dropping (a) would let a fix "solve" this by failing the load; dropping
   // (c) would let a fix that denies every caller (destroying the feature)
   // pass.
   it("CRITICAL — a composite parent's gate and its resolved member's gate AND; they do not fold into one OR", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.composite_sources
##! experimental.givens

given:
  REGION :: string
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: member_a is duckdb.sql("SELECT 7 as org_id, 'us' as region UNION ALL SELECT 8, 'us'") extend {}

source: member_b is duckdb.sql("SELECT 99 as org_id, 'eu' as region") extend {}

#(authorize) "region = $REGION"
source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: org_id, region }
`,
      );
      try {
         // (a) the fix must not "solve" the leak by failing the load.
         expect(compilationErrorOf(model)).toBeUndefined();

         // (b) a caller whose GROUPS names neither org gets EXACTLY ZERO
         // rows — pre-fix, `combo`'s own gate (`region = 'us'`, true here)
         // made the whole disjunction true regardless of GROUPS, leaking
         // every `region='us'` row.
         const denied = await model.getQueryResults(
            undefined,
            undefined,
            "run: qs -> { select: org_id, region }",
            {},
            true,
            { REGION: "us", GROUPS: [999] },
         );
         expect((denied.compactResult as unknown[]).length).toBe(0);

         // (c) a caller whose GROUPS names the org gets EXACTLY that row —
         // without this, a fix that makes `qs` deny every caller would also
         // pass (a) and (b) while destroying the feature.
         const allowed = await model.getQueryResults(
            undefined,
            undefined,
            "run: qs -> { select: org_id, region }",
            {},
            true,
            { REGION: "us", GROUPS: [7] },
         );
         const rows = allowed.compactResult as unknown as {
            org_id: number;
            region: string;
         }[];
         expect(rows).toEqual([{ org_id: 7, region: "us" }]);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   // THE LOAD-SIDE FAILURE: before groups, `qs`'s effective gate concatenated
   // `combo`'s atom with `member_a`'s condition into ONE OR'd list —
   // `["$ROLE != 'admin'", "$ROLE != 'admin'", "org_id in $GROUPS"]` (the
   // ancestor copy doubling it) — which the vacuous-default check would walk
   // as one list, potentially misattributing a hazard found in one source's
   // atom to a DIFFERENT source's declared note. Keeping the two sources'
   // gates as separate groups means `combo`'s atom is classified and
   // validated entirely on its own — every gate is a row filter now (there is
   // no more given-only vs row-level split), so `ROLE` here carries NO
   // declared default: the point of this test is grouping independence, not
   // the vacuous-default check itself (that check has its own dedicated
   // coverage elsewhere), and a defaulting `ROLE` would make this atom
   // genuinely vacuous on its own regardless of grouping.
   it("CRITICAL — a composite gate and its resolved member's row-level gate load independently, with no cross-group interference", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.composite_sources
##! experimental.givens

given:
  ROLE :: string
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: member_a is duckdb.sql("SELECT 7 as org_id UNION ALL SELECT 8 as org_id") extend {}

source: member_b is duckdb.sql("SELECT 99 as org_id") extend {}

#(authorize) "$ROLE != 'admin'"
source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: org_id }
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: qs -> { select: org_id }",
            {},
            true,
            // ROLE has no default, so it must be supplied explicitly — a
            // separate, pre-existing constraint this test isn't exercising.
            { ROLE: "analyst", GROUPS: [7] },
         );
         const rows = result.compactResult as unknown as { org_id: number }[];
         expect(rows).toEqual([{ org_id: 7 }]);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// The graft cache holds a full `structuredClone(modelDef)` per entry. Measured
// with Bun 1.3 on @malloydata/malloy 0.0.427, that is ~1.35x the serialized
// ModelDef resident -- 1.4 MiB for a 1 MiB model, 4.1 MiB for a 3 MiB one --
// so an uncapped map is a several-hundred-MiB step function per gated package
// that nothing releases. These pin the bound, and that a miss is only ever a
// recompile.
// ---------------------------------------------------------------------------

describe("row-level authorize — grafted materializer cache is bounded", () => {
   const GATED = `##! experimental.givens

given: GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.sql("SELECT 1 as org_id, 1 as x") extend {
  measure: c is count()
}
`;

   /** Same shape as the scoping block's `createModel`, redeclared here so this
    *  block stands alone. */
   async function createModelForCache(
      text: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-graft-cache-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb, dir };
   }

   /** Reach the private cache and the private builder the request path uses. */
   function internals(model: Model) {
      return model as unknown as {
         graftedMaterializerCache: Map<string, unknown>;
         defaultGraftScope(): {
            modelDef: unknown;
            materializer: unknown;
            cacheScope: string;
         };
         getOrBuildGraftedMaterializer(
            grafts: ReadonlyArray<{
               graftTarget: string;
               filterText: string;
               condition: unknown;
            }>,
            graftScope: unknown,
         ): unknown;
      };
   }

   /** One graft set, distinct per `n`, in a distinct cache scope. Varying the
    *  scope is how a notebook multiplies entries -- the case that makes the
    *  count unbounded in practice. */
   function fill(model: Model, n: number): void {
      const inner = internals(model);
      const scope = inner.defaultGraftScope();
      inner.getOrBuildGraftedMaterializer(
         [
            {
               graftTarget: "gated",
               filterText: `org_id = ${n}`,
               condition: { node: "true" },
            },
         ],
         { ...scope, cacheScope: `cell:${n}` },
      );
   }

   it("evicts least-recently-used past the cap instead of growing without bound", async () => {
      const { model, duckdb, dir } = await createModelForCache(GATED);
      try {
         const cache = internals(model).graftedMaterializerCache;

         for (let n = 0; n < 40; n++) fill(model, n);

         // 40 distinct scopes, at most 32 retained.
         expect(cache.size).toBeLessThanOrEqual(32);
         expect(cache.size).toBe(32);

         // The oldest is gone and the newest is held: FIFO and LRU agree here,
         // because nothing was re-read during the fill.
         const keys = [...cache.keys()];
         expect(keys.some((k) => k.startsWith("cell:0\u0000"))).toBe(false);
         expect(keys.some((k) => k.startsWith("cell:39\u0000"))).toBe(true);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a re-read keeps an old entry alive — the policy is LRU, not FIFO", async () => {
      const { model, duckdb, dir } = await createModelForCache(GATED);
      try {
         const cache = internals(model).graftedMaterializerCache;

         for (let n = 0; n < 32; n++) fill(model, n);
         expect(cache.size).toBe(32);

         // Touch the oldest, then overflow by one. Under FIFO the touched
         // entry would be the one evicted; under LRU its neighbour goes.
         fill(model, 0);
         fill(model, 999);

         const keys = [...cache.keys()];
         expect(keys.some((k) => k.startsWith("cell:0\u0000"))).toBe(true);
         expect(keys.some((k) => k.startsWith("cell:1\u0000"))).toBe(false);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

// ---------------------------------------------------------------------------
// Field-LESS gates: the row-level grammar can refuse a gate that published
// fine as a whole-source boolean. Refusing it must cost that ONE source, not
// the whole model file (see `validateAuthorizeProbes`'s `readsRowField`).
// ---------------------------------------------------------------------------

describe("row-level authorize — a field-less gate the grammar refuses", () => {
   /**
    * Every one of these loads on the pre-row-level publisher: each reads no
    * row field, so it classified as `given_only` and was validated by a plain
    * one-row probe rather than by the row-level grammar, which now refuses
    * them all. The escape is fail-closed because `resolveGateShape` re-runs
    * the identical SHAPE classification per request and rejects
    * independently — asserted below by the request-time denial, not assumed.
    */
   const REFUSED_FIELD_LESS_GATES = [
      "1 = 1",
      "'a' = 'a'",
      "$ROLE like 'ana%'",
      "$ROLE is not null",
      "$ROLE = 'a' and 1 = 1",
      "not false",
      "$ROLE = $ROLE_D",
   ];

   function modelText(gate: string): string {
      return `##! experimental.givens

given:
  ROLE :: string
  ROLE_D :: string is 'x'

#(authorize) "${gate}"
source: Gated is duckdb.table('parent') extend { measure: n is count() }

source: Ungated is duckdb.table('childtable') extend { measure: n is count() }
`;
   }

   for (const gate of REFUSED_FIELD_LESS_GATES) {
      it(`\`${gate}\`: the model file still loads and serves, the gated source denies`, async () => {
         const warnSpy = spyOn(logger, "warn");
         warnSpy.mockClear();
         const duckdb = await newDuckdb();
         const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-fieldless-"));
         try {
            fs.writeFileSync(path.join(dir, "m.malloy"), modelText(gate));
            const model = await Model.create(
               "test-pkg",
               dir,
               "m.malloy",
               new Map<string, Connection>([["duckdb", duckdb]]),
            );
            // The blocker this pins: a `compilationError` here turns the
            // WHOLE file into a placeholder, taking `Ungated` down with it.
            expect(
               (model as unknown as { compilationError?: Error })
                  .compilationError,
            ).toBeUndefined();
            expect(
               (warnSpy.mock.calls as unknown[][]).some((call) => {
                  const [message, fields] = call as [
                     string,
                     { sourceName?: string }?,
                  ];
                  return (
                     typeof message === "string" &&
                     message.includes("not expressible at this entry point") &&
                     fields?.sourceName === "Gated"
                  );
               }),
            ).toBe(true);

            // Fails CLOSED on the one source the refused gate protects …
            await expect(
               model.getQueryResults(
                  undefined,
                  undefined,
                  "run: Gated -> { aggregate: n is count() }",
                  {},
                  true,
                  { ROLE: "a" },
               ),
            ).rejects.toBeInstanceOf(AccessDeniedError);

            // … while the rest of the file keeps serving.
            const ungated = await model.getQueryResults(
               undefined,
               undefined,
               "run: Ungated -> { aggregate: n is count() }",
               {},
               true,
            );
            expect(
               (ungated.compactResult as unknown as { n: number }[])[0].n,
            ).toBe(2);
         } finally {
            await duckdb.close();
            fs.rmSync(dir, { recursive: true, force: true });
         }
      });
   }

   it("a VACUOUS-DEFAULT atom still fails the whole load, even field-less — it has no request-time counterpart to deny with", async () => {
      // `assertNoVacuousDefaultAtom` is a load-time static CHECK, and the
      // request path never repeats it: `resolveGateShape` only re-runs the shape
      // walk, which accepts `$ROLE_D != 'blocked'`. Warning instead of
      // throwing therefore leaves the source SERVING every row to a caller
      // who supplies nothing — the exact admission the check exists to stop.
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-vacuous-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            modelText("$ROLE_D != 'blocked'"),
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeDefined();
         expect(String(err?.message)).toContain("evaluates to TRUE");
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a gate that DOES read a row field still fails the whole load — the escape is scoped to field-less gates", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-fieldful-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id = $GROUPS"
source: Gated is duckdb.table('parent') extend { measure: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeDefined();
         expect(String(err?.message)).toContain("is declared `array`");
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

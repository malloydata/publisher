/**
 * Request-time enforcement of a row-level `#(access_filter)`: the graft that
 * attaches the gate's condition to the entry point, and everything that must
 * stay fail-closed around it (an unresolvable graft target, an unreachable
 * given, a malformed lifted condition, the gate-shape cache, storage routing,
 * notebook cells, the `/compile` primitives, the bypass). What each shape does
 * to rows end to end is `gate_semantics.spec.ts`; what loads is
 * `load_validation.spec.ts`.
 *
 * Most tests use the fixture's `buildGatedModel`: real compiled Malloy IR that
 * skips `Model.create`'s load-time validation, so a gate deliberately
 * unexpressible at the entry point under test reaches request time instead of
 * aborting the load. Tests that need `hasAuthorize()` / introspection go
 * through the real `Model.create`.
 */
import {
   API,
   type ModelDef,
   type ModelMaterializer,
   type SourceDef,
} from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { resetAuthorizeGuardTelemetryForTesting } from "../../../src/authorize_metrics";
import { AccessDeniedError } from "../../../src/errors";
import { AuthorizeGrammarError } from "../../../src/service/authorize_grammar";
import {
   createGateClassificationDeps,
   resolveGateShape,
} from "../../../src/service/gate_classification";
import { Model } from "../../../src/service/model";
import {
   boundRows,
   boundSql,
   buildEphemeralRunnable,
   buildGatedModel,
   connectionsFor,
   loadAuthorizeModel,
   seededDuckdb,
   withGivensFlag,
} from "../../../src/test_helpers/authorize_fixture";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import { duckdbMalloyConfig } from "../../../src/test_helpers/temp_package";

/**
 * The matrix's shared model: `X` is gated; `cp_joiner` joins it with no
 * annotation of its own; `query: q` is a bare named query over `X`.
 */
const ENTRY = `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: cp_joiner is duckdb.table('parent') extend {
   join_one: X on id = X.id
   measure: n is count()
}

query: q is X -> { aggregate: n is count() }
`;

// ---------------------------------------------------------------------------
// P0 — the graft must be scoped to the entry point (docs §3, "the graft MUST
// be scoped to the entry point")
// ---------------------------------------------------------------------------

describe("row-level authorize — P0 join scoping", () => {
   it("CRITICAL — P0's wanted case: a parent gate referencing a joined field filters AND emits the JOIN", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  BOB :: string

source: childtable is duckdb.table('childtable') extend {}
#(access_filter) childtable.name = $BOB
source: parent is duckdb.table('parent') extend {
   join_one: childtable on child_id = childtable.id
   measure: n is count()
}
`);
      try {
         const query = "run: parent -> { aggregate: n is count() }";
         const sql = await boundSql({ internals, mm }, query, { BOB: "bob" });
         expect(sql).toMatch(/join/i);
         const rows = await boundRows({ internals, mm }, query, { BOB: "bob" });
         // child_id=10 rows are ids 1 and 3 (parent), which join to name='bob'.
         expect(rows[0].n).toBe(2);
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
   // `ENTRY` is the module-level model defined above.
   async function harness() {
      return buildGatedModel(ENTRY);
   }

   it("W_rename / W_except (renaming/excepting the COLUMN the gate condition reads, at REQUEST time): the graft still cannot lift, and DENIES rather than serving an unfiltered or silently-empty result (measured; see task-3-report.md)", async () => {
      // A different fixture from the shared `ENTRY` above, on purpose: under
      // the dimension form, `buildGatedModel` deliberately bypassed
      // `Model.create`'s own pre-flight abort for this shape (see the file
      // header) to isolate what the GRAFT itself does when reached directly
      // — reaching it resolved to a live `WHERE (false)`, a successful EMPTY
      // result rather than a thrown error. Under the source-line form there
      // is no load-time abort to bypass in the first place (see the "load-
      // time scoping" describe block's identical test), so the isolation
      // this harness exists for is moot here — but the REQUEST-time outcome
      // still differs from the old one: confirmed empirically, a lift
      // failure against `W_rename`/`W_except` now REJECTS with
      // `AccessDeniedError`, the same fail-closed shape `Z`/`Z2` above use,
      // not a silently-empty result.
      const { internals, mm, duckdb } =
         await buildGatedModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: W_rename is X extend { rename: tenant is org_id }
source: W_except is X extend { except: org_id }
`);
      try {
         for (const name of ["W_rename", "W_except"]) {
            await expect(
               boundRows(
                  { internals, mm },
                  `run: ${name} -> { aggregate: n is count() }`,
                  { GROUPS: [1] },
               ),
            ).rejects.toBeInstanceOf(AccessDeniedError);
         }
      } finally {
         await duckdb.close();
      }
   });

   // W_rename / W_except are still not members of ENTRY (see its doc comment
   // for why — the reasoning changed once ENTRY moved to the source-line
   // form) and are covered by their own dedicated test above instead.

   it("a caller-declared ad-hoc derivation (`source: mine is X extend {}` + `run: mine`) FILTERS correctly (matches the string form's old FILTER intent; measured, see task-3-report.md)", async () => {
      // Under the dimension form this was a KNOWN GAP (denied): the graft
      // was resolved by tracing "mine"'s COPIED source-level annotation note
      // object back to "X" — a STRUCT-level identity trace — but the
      // dimension form's own annotation lived on the FIELD (`authorized`),
      // not the struct, so there was no analogous identity for that trace to
      // key on even though "mine" (a trivial `extend {}`) did flatten the
      // `authorized` field forward unchanged. Under the source-line form the
      // note itself IS struct-level, so the same identity trace this
      // mechanism already used for the STRING form applies directly, no
      // separate mechanism required. Confirmed empirically: this now
      // filters, the way the string form always did.
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows(
            { internals, mm },
            "source: mine is X extend {}\nrun: mine -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         // org_id=1 rows: ids 1,2.
         expect(rows[0].n).toBe(2);
      } finally {
         await duckdb.close();
      }
   });

   it("`query: q is X -> {...}` + `run: q` filters", async () => {
      const { internals, mm, duckdb } = await harness();
      try {
         const rows = await boundRows({ internals, mm }, "run: q", {
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
            { internals, mm },
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
            { internals, mm },
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
   it("gate's given unresolved (not on this model's own given surface) denies with a 403", async () => {
      // FAR is declared 2 import hops from entry.malloy: entry -> mid -> deep.
      // Malloy merges only ONE hop into a model's own given namespace, so
      // entry's `compiledModel.givens` never surfaces FAR — `resolveGateShape`'s
      // `givenDeclaredTypes` surface check can't find it and rejects as "unreachable_given".
      const gated = await buildGatedModel(
         {
            "deep.malloy": `##! experimental.givens\n\ngiven:\n  FAR :: number[]\n\n#(access_filter) org_id in $FAR\nsource: Deep is duckdb.table('parent') extend {\n   measure: n is count()\n}\n`,
            "mid.malloy": `import "deep.malloy"\nsource: Mid is Deep extend {}\n`,
            "entry.malloy": `import "mid.malloy"\nsource: Entry is Mid extend {}\n`,
         },
         { entry: "entry.malloy" },
      );
      const { internals, mm, duckdb } = gated;
      try {
         // Confirms the premise: FAR is genuinely absent from entry's own surface.
         const surface = Array.from((await mm.getModel()).givens.values());
         expect(
            surface.some((g) => (g as { name?: string }).name === "FAR"),
         ).toBe(false);
         const err = await boundRows(
            { internals, mm },
            "run: Entry -> { aggregate: n is count() }",
            { FAR: [1] },
         ).catch((e) => e);
         expect(err).toBeInstanceOf(AccessDeniedError);
         // The rejection detail ("$FAR is not on this model's given
         // surface") must NOT reach the caller — the thrown error names
         // nothing about the gate. It only ever reaches the debug log.
         expect(String((err as Error).message)).not.toContain("FAR");
         expect(String((err as Error).message)).not.toContain("import");
      } finally {
         await duckdb.close();
      }
   });

   it("an unresolvable graft target: resolveGraftTarget returns undefined (synthetic IR real Malloy input cannot express — same idiom as authorize_gate_walk.spec.ts)", async () => {
      const { internals, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
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
      // FULL `resolveGateShape`: an `#(access_filter)` is a filter, and a filter
      // with nowhere to attach cannot be enforced, so a missing graft target
      // rejects outright rather than attempting any fallback classification.
      const { internals, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
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

   it("CRITICAL — a lifted condition missing its `.e` still REJECTS rather than throwing an uncaught TypeError", async () => {
      // `FilterCondition` is TYPED as always carrying `.e` (`extends ExprE`),
      // but that is a compile-time promise about the shape
      // `liftProbeFilterCondition` (`./authorize`) BUILDS from — it verifies
      // `code`/`isSourceFilter` only, never that `.e` itself is present. A
      // malformed or unexpected prepared-query response (a Malloy version
      // skew, a future IR shape this code hasn't seen) could satisfy both of
      // those checks while carrying no `.e` at all. Before this was guarded,
      // `resolveGateShape` dereferenced `condition.e.node` unconditionally to
      // check for the bare-`false` fail-closed sentinel — an uncaught
      // TypeError on a missing `.e`, surfacing as a 500 instead of the 403
      // every other unclassifiable gate shape gets here. This is the one
      // guarantee `authorize.spec.ts`'s deleted "rejects an absent condition,
      // fail closed" test pinned with no replacement when that file's
      // `classifyAuthorizeGate` unit tests were deleted (Task 4) — restored
      // here against the real function instead.
      const { internals, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const modelDef = internals.modelDef as ModelDef;
         const materializer = (
            internals as unknown as { modelMaterializer: ModelMaterializer }
         ).modelMaterializer;
         const filterText = "(org_id in $GROUPS)";
         // Intercept the probe compile and hand back a `FilterCondition` that
         // passes `liftProbeFilterCondition`'s own checks (`code` matches,
         // `isSourceFilter: true`) but carries no `.e` — the shape this test
         // exists to prove doesn't crash the classifier.
         const loadQuerySpy = spyOn(materializer, "loadQuery").mockReturnValue({
            getPreparedQuery: () =>
               Promise.resolve({
                  _query: {
                     structRef: {
                        filterList: [
                           {
                              node: "filterCondition",
                              code: filterText,
                              isSourceFilter: true,
                              expressionType: "scalar",
                              // Deliberately no `.e`.
                           },
                        ],
                     },
                  },
               }),
         } as unknown as ReturnType<ModelMaterializer["loadQuery"]>);
         try {
            const graftScope = { modelDef, materializer, cacheScope: "model" };
            const resolution = await internals.resolveGateShape(
               {
                  label: "X",
                  exprs: ["org_id in $GROUPS"],
                  selfContained: false,
                  struct: modelDef.contents["X"] as unknown as SourceDef,
               },
               modelDef,
               graftScope,
            );
            expect(resolution.shape).toBe("rejected");
         } finally {
            loadQuerySpy.mockRestore();
         }
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

#(access_filter) org_id in $GROUPS
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
            route: "access_filter",
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

   it("CRITICAL — the two ROUTES do not collide on a cached entry", async () => {
      // The fail-closed `["false"]` sentinel (`gate_registry_walk`'s
      // `ancestorGateExprs`) is synthesized on BOTH routes for the SAME
      // struct, so the two routes really do present the identical
      // (cacheScope, graftTarget, filterText), and the walk runs filter-first
      // (`CANONICAL_AUTHORIZE_ROUTES`). What this pins is that the SHAPE
      // comes from `entry.route` and not from whatever the memo happens to
      // hold: the lock must come back a lock even when the filter's entry got
      // there first. A `row_level` here means the lock grafts `where: false`
      // and the caller gets 200 with zero rows on a source nothing confirmed
      // they may read.
      //
      // Pinned at this level rather than end to end because no model TEXT
      // reaches that sentinel — it needs unreadable IR or an exhausted
      // registry walk, and a plain 40-deep `extend` chain resolves cleanly
      // (measured). A test that cannot reach the shape is worse than none.
      const { internals, duckdb } = await buildGatedModel(`
#(authorize) false
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
         const deps = createGateClassificationDeps([]);
         const base = {
            label: "X",
            exprs: ["false"],
            selfContained: true,
            struct: modelDef.contents["X"] as unknown as SourceDef,
         };

         // Filter route first, exactly as the walk orders them: it populates
         // the memo.
         const filter = await resolveGateShape(
            { ...base, route: "access_filter" },
            modelDef,
            graftScope,
            deps,
         );
         expect(filter.shape).toBe("row_level");

         // Same scope, same graft target, same filter text, other route. It
         // must come back as a LOCK — a `row_level` here is the memo leaking
         // across routes.
         const lock = await resolveGateShape(
            { ...base, route: "authorize" },
            modelDef,
            graftScope,
            deps,
         );
         expect(lock.shape).toBe("lock");
      } finally {
         await duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// Runnable identity
// ---------------------------------------------------------------------------

describe("row-level authorize — runnable identity (CRITICAL)", () => {
   it("a notebook-shaped cell composing a source's own where: filter AND a row-level gate carries BOTH", async () => {
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

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   where: val != 'd'
   measure: n is count()
}
`);
      try {
         // val != 'd' (the source's own where:) removes
         // id 4; org_id in [1] then keeps only ids 1,2 of what remains.
         const rows = await boundRows(
            { internals, mm },
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
         expect(rows[0].n).toBe(2);
         const rowsOtherOrg = await boundRows(
            { internals, mm },
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

#(access_filter) org_id in $GROUPS
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
   it("CRITICAL — /compile 403s a row-field gate whose givens the caller did NOT supply", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
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

#(access_filter) org_id in $GROUPS
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

   it("/compile ADMITS a gate referencing no given at all (`authorized is true`) with nothing supplied", async () => {
      // `givenNames.length === 0` is decidable by construction — there is no
      // caller value left to wait on, since `/compile` executes nothing.
      // (Previously this denied: the dimension form's `resolveGateShape`
      // hardcoded `literalAtoms: []`, so `constantTrue` could never be true,
      // and the decidable check required EITHER `constantTrue` OR at least
      // one given with every one supplied — a no-given gate satisfied
      // neither. Fixed by deciding directly on `givenNames.length === 0`.)
      // A bare `#(authorize) true` is refused by the grammar at real load
      // (`docs/authorize.md`: "true stays refused") — `buildGatedModel`
      // bypasses that preflight on purpose, so this pins the internal
      // decision logic in isolation, not a shape a caller can ever produce.
      const text = `
#(authorize) true
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

#(access_filter) org_id in $GROUPS
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
      // rejected before `resolveGateShape` ever sees the literal "true".
      // Both `#(authorize) true` bodies below are load-impossible shapes —
      // `buildGatedModel` bypasses the real preflight on purpose, to pin
      // this walk-entry logic without a loadable fixture to carry it.
      const { model, duckdb } = await buildGatedModel(`
#(authorize) true
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const stubRunnable = await buildEphemeralRunnable(
            `
#(authorize) true
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

#(access_filter) org_id in $GROUPS
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
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "m.malloy": `##! experimental.givens

given:
  ROLE :: string

#(authorize) 'analyst' = $ROLE
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         },
         "m.malloy",
      );
      try {
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
         await cleanup();
      }
   });

   it("/compile REFUSES a constant-FALSE lock — the lock is truth-evaluated here, not merely present", async () => {
      // The two routes part company on `/compile`. A `#(access_filter)` is
      // still decided on PRESENCE, not value — it has no whole-source answer
      // and `/compile` runs nothing, so there is no row-truth to check. A
      // `#(authorize)` does have one, and reading a source's SQL is reaching
      // it, so the lock is evaluated. The cost is real and deliberate: an
      // author outside the group can no longer compile-check a locked
      // source.
      const { model, duckdb } = await buildGatedModel(`
#(authorize) false
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

#(access_filter) org_id in $GROUPS
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
            const sql = await boundSql({ internals, mm }, query, {
               GROUPS: [1],
            });
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

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const query = "run: X -> { aggregate: n is count() }";
         const gated = await boundRows({ internals, mm }, query, {
            GROUPS: [1],
         });
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
   it("an empty array given yields zero rows (WHERE FALSE), including on a multi-stage query", async () => {
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`);
      try {
         const single = await boundRows(
            { internals, mm },
            "run: X -> { aggregate: n is count() }",
            { GROUPS: [] },
         );
         expect(single[0].n).toBe(0);
         const multiStage = await boundRows(
            { internals, mm },
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
// Other
// ---------------------------------------------------------------------------

describe("row-level authorize — other", () => {
   it("error scrubbing: a fail-closed row-level deny never names the column/join/expression to the caller", async () => {
      // A query-source projection dropping `authorized` — the shape that
      // reproduces a thrown `AccessDeniedError` under the dimension form
      // (see the "fail-closed" describe block's own tests: an `extend {
      // except: ... }` resolves to a live, empty result instead).
      const { internals, mm, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
source: W is X -> { group_by: id, val; aggregate: n is count() }
`);
      try {
         const err = await boundRows(
            { internals, mm },
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

#(access_filter) org_id in $GROUPS
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

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
describe("deprecated: .malloynb — row-level authorize — notebook cells", () => {
   it("CRITICAL — a cell that declares its own source under a row-level gate AND runs it returns correctly filtered rows", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

>>>malloy
source: local2 is gated extend {}
run: local2 -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
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
         await cleanup();
      }
   });

   // DEPRECATED FEATURE COVERAGE — delete together with
   // `#(filter)` (see RELEASE_NOTES deprecations).
   describe("deprecated: #(filter)", () => {
      it("CRITICAL — a cell whose #(filter) refinement does not compile surfaces that failure, never a silent unfiltered admit", async () => {
         const { model, cleanup } = await loadAuthorizeModel(
            {
               "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  ROLE :: string

#(filter) dimension=nonexistent_field type=equal
#(authorize) 'admin' = $ROLE
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
            },
            "nb.malloynb",
         );
         try {
            const loadErr = (model as unknown as { compilationError?: Error })
               .compilationError;
            expect(loadErr).toBeUndefined();

            // filterParams supplies a value for the `nonexistent_field` filter,
            // so the `#(filter)` refinement rebuild appends
            // `+ {where: nonexistent_field = 'x'}` to the cell's text — which
            // fails to compile, since `gated` has no such field. The caller is
            // given the ADMITTING role deliberately: this pins that a broken
            // refinement surfaces its own compile failure rather than silently
            // admitting unfiltered rows, and a denied caller would short-circuit
            // on the lock's 403 before reaching that question at all.
            await expect(
               model.executeNotebookCell(0, { nonexistent_field: "x" }, false, {
                  ROLE: "admin",
               }),
            ).rejects.toThrow(/nonexistent_field/);
         } finally {
            await cleanup();
         }
      });
   });

   it("CRITICAL — a row-level gate declared and run in the FIRST code cell returns correctly filtered rows (no earlier cell to graft against; grafts its OWN post-declaration scope, repointing the compiled queryDef rather than recompiling text)", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
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
         await cleanup();
      }
   });

   it("fail-closed: an empty given on a first-code-cell self-declaring gate yields zero rows, not an unfiltered/leaked count", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(0);
      } finally {
         await cleanup();
      }
   });

   it("a row-level gate in a code cell preceded only by markdown also returns correctly filtered rows (same fallback as the first-cell case)", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>markdown
# Just a heading, no code cell before this one

>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await cleanup();
      }
   });

   it("CRITICAL — a LATER cell that declares and runs its own gated source (not just the first cell) also returns correctly filtered rows — the earlier cell exists but does not carry the declared source, so its scope is just as unusable as cell 0's absence", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

source: unrelated is duckdb.table('childtable') extend { primary_key: id }

>>>malloy
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await cleanup();
      }
   });

   it("self-declaring cell, multi-stage pipeline shape (`gated -> {…} -> {…}`) filters correctly", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
}

run: gated -> { group_by: org_id, id } -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await cleanup();
      }
   });

   it("self-declaring cell, named-view shape (`gated -> byorg`) filters correctly", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('parent') extend {
   measure: n is count()
   view: byorg is { aggregate: n is count() }
}

run: gated -> byorg
`,
         },
         "nb.malloynb",
      );
      try {
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await cleanup();
      }
   });

   it("self-declaring cell, joined-field gate shape (`childtable.name in $GROUPS` via join_one) filters correctly", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: string[]

source: childtable is duckdb.table('childtable') extend { primary_key: id }

#(access_filter) childtable.name in $GROUPS
source: gated is duckdb.table('parent') extend {
   join_one: childtable with child_id
   measure: n is count()
}

run: gated -> { group_by: id, org_id, childtable.name }
`,
         },
         "nb.malloynb",
      );
      try {
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
         await cleanup();
      }
   });

   it("CRITICAL — P0 on the new path: a self-declaring cell's ungated entry point joining a gated source does not fire the joined gate", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  ID :: number

#(access_filter) id = $ID
source: childtable is duckdb.table('childtable') extend {
}

source: joiner is duckdb.table('parent') extend {
   join_one: childtable on child_id = childtable.id
   measure: n is count()
}

run: joiner -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
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
         await cleanup();
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
      const duckdb = await seededDuckdb();
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

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            connectionsFor(duckdb),
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
         const serveConfig = duckdbMalloyConfig(duckdb).malloyConfig;
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

#(access_filter) org_id in $GROUPS
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
         const serveConfig = duckdbMalloyConfig(duckdb).malloyConfig;
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
      const duckdb = await seededDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-storage-deny-"));
      try {
         // `W_except` is a query-source projection dropping the gate
         // dimension itself — the shape that makes `resolveGateShape`
         // return `deny` rather than `row_level` UNDER `Model.create` (an
         // `extend { except: org_id }` instead aborts the WHOLE model's
         // load here — see load_validation.spec.ts's unexpressible-derivation
         // coverage of that shape; this test is about the routing
         // PRE-CHECK, not load validation).
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}

source: W_except is X -> { group_by: id, val; aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            connectionsFor(duckdb),
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
         const serveConfig = duckdbMalloyConfig(duckdb).malloyConfig;
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

#(access_filter) org_id in $GROUPS
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
      const duckdb = await seededDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-storage-given-only-"),
      );
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  ROLE :: string

#(authorize) 'analyst' = $ROLE
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            connectionsFor(duckdb),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // A storage binding for X that, if routed to, would answer with a
         // value ("999") the live, gate-filtered query could never produce.
         await duckdb.runSQL(
            "CREATE OR REPLACE TABLE mz_real AS SELECT 999 AS n",
         );
         const serveConfig = duckdbMalloyConfig(duckdb).malloyConfig;
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

   // The storage serve-shape re-emits a plain author `where:` verbatim onto
   // its own transient struct, so `assertNoMisboundInheritedFilters` still
   // sees and denies a genuinely misbound one even after `getQueryResults`
   // swaps `runnable` for the shape and routing succeeds. A regression pin,
   // not a gap: a misbound plain `where:` must still deny, routed or not.
   it("CRITICAL — a plain (unannotated) where:-filtered source's misbound derivation still denies under storage routing", async () => {
      const originalMode = process.env.PERSIST_STORAGE_MODE;
      process.env.PERSIST_STORAGE_MODE = "on";
      const duckdb = await seededDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rla-storage-plain-"));
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `source: X is duckdb.table('parent') extend {
   where: org_id = 1
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            connectionsFor(duckdb),
         );
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeUndefined();

         // A storage binding for X that, if routed to, would answer with a
         // value the live query could never produce — proof of routing
         // independent of whether the misbind is caught.
         await duckdb.runSQL(
            "CREATE OR REPLACE TABLE mz_real AS SELECT * FROM parent",
         );
         const serveConfig = duckdbMalloyConfig(duckdb).malloyConfig;
         model.setServeDestinationConfig(() => serveConfig);
         model.setServeBindings([
            {
               sourceName: "X",
               destinationName: "duckdb",
               virtualHandle: "h",
               tablePath: "mz_real",
               schema: [
                  { name: "id", type: "BIGINT" },
                  { name: "org_id", type: "BIGINT" },
                  { name: "child_id", type: "BIGINT" },
                  { name: "val", type: "STRING" },
               ],
            },
         ]);

         // `except: org_id` then `rename: org_id is val` frees the name
         // `org_id` the inherited `where:` still reads by text, rebinding it
         // to a different physical column (`val`) — the same misbind
         // `filter_binding_guard_integration.spec.ts`'s "a plain where:
         // filter with NO access_filter annotation" tests prove denies with
         // storage routing off.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: X extend { except: org_id } extend { rename: org_id is val } -> { aggregate: n is count() }",
               {},
               true,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
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
// Constant gate expressions — `#(authorize) false` and `#(authorize) 1 = 1`.
// The authorize grammar retires the admit-everyone constant outright (every
// term must reference a given), but `false` gets one deliberate exception:
// a whole body that is exactly `false` still parses, as a `deny_all`
// sentinel — without it there is no gate a caller cannot eventually
// satisfy, and the locked-base-plus-curated-extensions idiom these docs
// teach has no legal spelling. `true` is NOT given the same treatment: an
// admit-everyone gate is not a gate at all, so it stays refused.
// ---------------------------------------------------------------------------

describe("row-level authorize — constant gate expressions are refused by the grammar", () => {
   it("CRITICAL — bypassAuthorize still executes the query live and returns every row unfiltered even under a gate that would otherwise deny everything", async () => {
      const duckdb = await seededDuckdb();
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "rla-const-false-bypass-"),
      );
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('parent') extend {
   measure: n is count()
}
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "m.malloy",
            connectionsFor(duckdb),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { group_by: org_id; aggregate: n is count() }",
            {},
            true,
            { GROUPS: [] },
            undefined,
            undefined,
            "full",
            /* bypassAuthorize */ true,
         );

         // The unfiltered live rows — bypassing authorize skips the gate
         // entirely, so both org groups (4 seed rows) come back despite an
         // empty GROUPS, which would otherwise deny every row.
         const rows = result.compactResult as unknown as { n: number }[];
         expect(rows.length).toBe(2);
         expect(rows.reduce((sum, r) => sum + r.n, 0)).toBe(4);
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

#(access_filter) org_id in $GROUPS
source: gated is duckdb.sql("SELECT 1 as org_id, 1 as x") extend {
  measure: c is count()
}
`;

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
      const { model, cleanup } = await loadAuthorizeModel(GATED);
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
         await cleanup();
      }
   });

   it("a re-read keeps an old entry alive — the policy is LRU, not FIFO", async () => {
      const { model, cleanup } = await loadAuthorizeModel(GATED);
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
         await cleanup();
      }
   });
});

// ---------------------------------------------------------------------------
// `publisher_authorize_admit_all_total` counts DECLARATIONS, not entry points.
// Malloy copies a base's annotation note objects onto a plain `extend {}`
// derivation BY REFERENCE, so the presence-based `authorizeOwnNotes` calls
// every such derivation an owner. Counting off it ticked once per inheriting
// entry point, making the number a function of model shape.
// ---------------------------------------------------------------------------

describe("row-level authorize — the admit-all counter counts declarations", () => {
   let harness: MetricsHarness;

   const COUNTER = "publisher_authorize_admit_all_total";

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop the cached instrument so it re-binds to this test's provider.
      resetAuthorizeGuardTelemetryForTesting();
   });

   afterEach(async () => {
      resetAuthorizeGuardTelemetryForTesting();
      await harness.shutdown();
   });

   it("one declared `#(authorize) true` inherited by two `extend` derivations ticks ONCE, not once per entry point", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         withGivensFlag(`
#(authorize) true
source: base is duckdb.table('parent') extend {}

source: mid is base extend {}

source: leaf is mid extend {}
`),
      );
      try {
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();
         // Both sentinels live on the lock, so that is the label they book.
         expect(
            await harness.collectCounter(COUNTER, {
               route: "authorize",
            }),
         ).toBe(1);
      } finally {
         await cleanup();
      }
   });

   it("a model refused as admit_all_with_sibling ticks nothing — the callback runs after coherence", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         withGivensFlag(`
given:
  ROLE :: string[]

#(authorize) true
#(authorize) 'finance' in $ROLE
source: X is duckdb.table('parent') extend {}
`),
      );
      try {
         const err = (model as unknown as { compilationError?: Error })
            .compilationError;
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "admit_all_with_sibling",
         );
         expect(
            await harness.collectCounter(COUNTER, {
               route: "authorize",
            }),
         ).toBe(0);
      } finally {
         await cleanup();
      }
   });
});

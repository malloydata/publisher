// Compile-time `#(authorize)` gate classification for persist sources:
// `classifyPersistSourceGate` (`./build_plan`) calls `gate_classification.ts`'s
// standalone functions directly against a real compiled
// `{modelDef, materializer}` pair — no `Model` instance, no network/warehouse
// access. Pinned against the real compiler (like `materialization_eligibility.spec.ts`)
// rather than hand-built stubs, since the classification reads compiled IR
// shapes (annotations, given surface, join embedding) a stub would drift from.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type {
   ModelDef,
   ModelMaterializer,
   PersistSource,
} from "@malloydata/malloy";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { classifyPersistSourceGate } from "./build_plan";
import {
   createGateClassificationDeps,
   type GateClassificationDeps,
} from "./gate_classification";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import { assertColocatedPersistNotAuthorizeGated } from "./materialization_eligibility";

const ROOT = "file:///gate-classify/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

/** Compile a model, returning its persist sources plus the classification
 *  inputs `compilePackageBuildPlan` builds around each model. */
async function compileModel(model: string): Promise<{
   modelDef: ModelDef;
   materializer: ModelMaterializer;
   deps: GateClassificationDeps;
   sources: Record<string, PersistSource>;
}> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const materializer = runtime.loadModel(new URL(`${ROOT}m.malloy`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await materializer.getModel();
   const sources: Record<string, PersistSource> = {};
   for (const source of Object.values(compiled.getBuildPlan().sources)) {
      sources[source.name] = source;
   }
   const givens = Array.from(
      compiled.givens.values(),
   ) as unknown as MalloyGiven[];
   const deps = createGateClassificationDeps(
      givens.map(malloyGivenToApi),
      "m.malloy",
   );
   return { modelDef: compiled._modelDef, materializer, deps, sources };
}

describe("classifyPersistSourceGate", () => {
   it("classifies a plain row-level gate as row_level and attributed", async () => {
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#@ persist name="gated"
source: gated is base -> { select: org_id } extend {
   #(authorize)
   internal dimension: authorized is org_id = $ORG
}
`,
      );
      const outcome = await classifyPersistSourceGate(
         sources.gated,
         modelDef,
         materializer,
         deps,
         "m.malloy",
      );
      expect(outcome).toEqual({
         classification: "row_level",
         attributed: true,
      });
   });

   it("records rejected when a query-source derivation inherits an ancestor's gate dimension whose given is off THIS deps surface", async () => {
      // Replaces the old string form's "two AND'd groups, one rejects" case:
      // that shape OR'd two independently-authored `#(authorize)` annotations
      // on one source, which the dimension form cannot express at all — G4
      // refuses a source declaring more than one gate dimension (see
      // `gate_dimension_integration.spec.ts`'s "more than one annotated
      // dimension" test). What still needs pinning under the dimension form
      // is that `derived`'s inherited copy of `base`'s gate (carried in via
      // the query-source derivation, same mechanism `collectEntryPointGates`
      // uses for every other inheritance case in this file) is re-checked
      // against THIS CALL's own given surface, not the compiling model's, and
      // is rejected rather than silently admitted when the given the
      // inherited gate references isn't on it (`unreachable_given` — see
      // `resolveGateShape`'s doc for why the deps struct, not the compiled
      // model, is the actual given surface used at classification time).
      const { modelDef, materializer, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as org_id") extend {
   #(authorize)
   internal dimension: authorized is org_id = $ORG
}

#@ persist name="derived"
source: derived is base extend {}
`,
      );
      const restrictedDeps = createGateClassificationDeps([]);
      const outcome = await classifyPersistSourceGate(
         sources.derived,
         modelDef,
         materializer,
         restrictedDeps,
         "m.malloy",
      );
      expect(outcome.classification).toBe("rejected");
   });

   it("leaves attributed:false for a gate reachable only through a join (no own entry-point gate)", async () => {
      // `joiner` declares no gate of its own; `collectEntryPointGates` does
      // not trace joins, so its classification is vacuously `row_level` (no
      // group to reject) — `attributed` is what still catches `locked`'s
      // gate, which `referencesAuthorize`'s deep walk finds through the join.
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

#(authorize) "org_id = $ORG"
source: locked is duckdb.sql("select 1 as org_id")

#@ persist name="joiner"
source: joiner is duckdb.sql("select 1 as x") extend {
   join_one: locked on x = locked.org_id
}
`,
      );
      const outcome = await classifyPersistSourceGate(
         sources.joiner,
         modelDef,
         materializer,
         deps,
         "m.malloy",
      );
      expect(outcome).toEqual({
         classification: "row_level",
         attributed: false,
      });
   });

   it("leaves attributed:false when the entry point's own gate is accompanied by a join-only gate", async () => {
      // Distinct from the join-only case above: `joiner` HAS its own valid
      // entry-point gate (classifies `row_level` on its own), but the deep
      // walk ALSO finds `locked`'s gate through the join — a note the
      // entry-point walk never sees, so `attributed` must still be false.
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number
  DEPT :: number

source: locked is duckdb.sql("select 1 as dept_id") extend {
   #(authorize)
   internal dimension: authorized is dept_id = $DEPT
}

#@ persist name="joiner"
source: joiner is duckdb.sql("select 1 as x, 1 as org_id") extend {
   #(authorize)
   internal dimension: authorized is org_id = $ORG
   join_one: locked on x = locked.dept_id
}
`,
      );
      const outcome = await classifyPersistSourceGate(
         sources.joiner,
         modelDef,
         materializer,
         deps,
         "m.malloy",
      );
      expect(outcome).toEqual({
         classification: "row_level",
         attributed: false,
      });
   });

   it("pins {row_level, attributed:true} for a joined copy of the entry point's own extend-inherited gate — note-identity cannot see the join", async () => {
      // `derived` writes no annotation of its own; `extend`ing `locked`
      // unannotated copies `locked`'s note object BY REFERENCE onto
      // `derived`'s own annotations (so `collectEntryPointGates` sees it as
      // an own entry-point gate — `classification: "row_level"`). `derived`
      // ALSO joins a second copy of `locked`, carrying the SAME note object.
      // `isAuthorizeAttributedToEntryPoint` dedupes by object identity, so
      // the joined copy contributes no note the no-join walk doesn't already
      // have via the extend-inherited copy — `attributed` comes out `true`,
      // indistinguishable from a source with no join-carried gate at all.
      //
      // This is safe ONLY IF the colocated relaxation this classification
      // feeds grafts the row filter at serve time exactly as the live query
      // path does — i.e. the artifact grants nothing a live query would not.
      // It is not safe merely because `attributed` reads `true` here.
      //
      // It DOES hold for this exact shape: `derived` is itself a top-level
      // `modelDef.contents` entry (the `#@ persist name="derived"` source),
      // and `entry.struct` from `collectEntryPointGates` (above) is `derived`'s
      // OWN compiled struct — the same object. `resolveGraftTarget`'s FIRST
      // branch (`findContentsKey` by identity) therefore resolves the graft
      // target to `"derived"` directly, with no ancestor walk involved, for
      // BOTH a live query of `derived` and a colocated-served one: the graft
      // mechanism (`Model.buildGraftedMaterializer`) operates on `modelDef`,
      // not on whether the entry point's rows come from a live recompute or a
      // same-connection table substitution. So the row filter lands on
      // `derived` either way, identically. `assertColocatedPersistNotAuthorizeGated`
      // (`./materialization_eligibility`) admits this shape given this exact
      // `{classification: "row_level", attributed: true}` outcome.
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: locked is duckdb.sql("select 1 as org_id, 1 as x") extend {
   #(authorize)
   internal dimension: authorized is org_id = $ORG
}

#@ persist name="derived"
source: derived is locked extend {
   join_one: other is locked on x = other.x
}
`,
      );
      const outcome = await classifyPersistSourceGate(
         sources.derived,
         modelDef,
         materializer,
         deps,
         "m.malloy",
      );
      expect(outcome).toEqual({
         classification: "row_level",
         attributed: true,
      });
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(
            sources.derived,
            sources.derived.name,
            "persist",
            outcome,
         ),
      ).not.toThrow();
   });

   it("records rejected for a persist source over a composite entry point whose own `select:` projects the gated member's gate field away", async () => {
      // `comp` is a query_source whose base is `compose(member_a, member_b)`.
      // Only `member_a` carries the gate; Malloy resolves the composite to
      // ONE concrete member per query and copies that member's own notes
      // onto `query.compositeResolvedSourceDef` (see
      // `malloy_annotation_invariants.spec.ts`), which `collectEntryPointGates`
      // reads as the entry point's OWN gate — discovery finds it with no join
      // or deep walk involved.
      //
      // Under the OLD string form this classified `row_level`/`attributed:
      // true`: the gate was a source-level annotation carrying literal
      // expression TEXT (`"org_id in $GROUPS"`), grafted independently of
      // which fields `comp`'s own `-> { select: … }` happened to keep. The
      // dimension form instead grafts a reference to the `authorized` FIELD
      // itself (`where: (authorized)`) at the ENTRY POINT's own compiled
      // struct — and `authorized` is `internal`, so a `select:` that does not
      // name it (this one only keeps `org_id`/`amount`) drops it from
      // `comp`'s own fields the same way any other unselected field is
      // dropped; `internal` also makes it unselectable even from within
      // `comp`'s own pipeline (confirmed: naming it explicitly in `select:`
      // is a compile error, "'authorized' is internal"). The lift then fails
      // ("'authorized' is not defined") and `resolveGateShape` rejects rather
      // than guessing — fails CLOSED, not open, so this is a coverage gap
      // (a composite entry point narrowed by `select:` cannot be classified
      // row-level and so cannot be colocated-served), not a security one.
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental { persistence composite_sources givens }

given:
  GROUPS :: number[]

source: member_a is duckdb.sql("select 7 as org_id, 1 as amount") extend {
   #(authorize)
   internal dimension: authorized is org_id in $GROUPS
}

source: member_b is duckdb.sql("select 99 as org_id, 2 as amount")

source: combo is compose(member_a, member_b)

#@ persist name="comp"
source: comp is combo -> { select: org_id, amount }
`,
      );
      const outcome = await classifyPersistSourceGate(
         sources.comp,
         modelDef,
         materializer,
         deps,
         "m.malloy",
      );
      expect(outcome).toEqual({
         classification: "rejected",
         attributed: true,
      });
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(
            sources.comp,
            sources.comp.name,
            "persist",
            outcome,
         ),
      ).toThrow(/authorize/i);
   });

   it("records the fail-closed outcome when classification throws", async () => {
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#@ persist name="gated"
source: gated is base -> { select: org_id } extend {
   #(authorize)
   internal dimension: authorized is org_id = $ORG
}
`,
      );
      const source = sources.gated;
      // Simulate an introspection failure — `_sourceDef` is the first thing
      // `classifyPersistSourceGate` reads.
      Object.defineProperty(source, "_sourceDef", {
         get(): never {
            throw new Error("simulated introspection failure");
         },
      });
      const outcome = await classifyPersistSourceGate(
         source,
         modelDef,
         materializer,
         deps,
         "m.malloy",
      );
      expect(outcome).toEqual({
         classification: "rejected",
         attributed: false,
      });
   });
});

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
#(authorize) "org_id = $ORG"
source: gated is base -> { select: org_id }
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

   it("records rejected when one of two AND'd gate groups rejects, preserving OR within the accepting group", async () => {
      // `derived`'s OWN group OR's two valid row-level comparisons together
      // (resolveGateShape's own `gateFilterText` fold — not re-tested here);
      // `base`'s group (carried in via the query-source derivation, per
      // `collectEntryPointGates`) compares a field against a bare literal,
      // which `classifyAuthorizeGate` rejects (`no_given_reference`). The two
      // groups are AND'd, so the second's rejection must win regardless of
      // the first's shape.
      //
      // `base`'s literal-only gate could not survive a real local package
      // load — `validateAuthorizeProbes` rejects a field-vs-literal
      // comparison at load time. This fixture reaches `classifyPersistSourceGate`
      // directly, skipping that validation, so the shape tested here is one
      // load validation would in fact catch UNLESS `base` lived in a model
      // this one only transitively imports: load validation covers only the
      // importING model's own entry points (`authorize.ts`'s
      // `validateAuthorizeProbes` doc), so a cross-model import is the one
      // way this shape reaches compile time for real.
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number
  ORG2 :: number

#(authorize) "org_id = 999"
source: base is duckdb.sql("select 1 as org_id")

#@ persist name="derived"
#(authorize) "org_id = $ORG"
#(authorize) "org_id = $ORG2"
source: derived is base -> { select: org_id }
`,
      );
      const outcome = await classifyPersistSourceGate(
         sources.derived,
         modelDef,
         materializer,
         deps,
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

#(authorize) "dept_id = $DEPT"
source: locked is duckdb.sql("select 1 as dept_id")

#@ persist name="joiner"
#(authorize) "org_id = $ORG"
source: joiner is duckdb.sql("select 1 as x, 1 as org_id") extend {
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
      // `assertColocatedPersistNotAuthorizeGated` (`./materialization_eligibility`)
      // still refuses this exact shape today — its `referencesAuthorize` walk
      // has no notion of `attributed` and finds the gate regardless of how it
      // got there.
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

#(authorize) "org_id = $ORG"
source: locked is duckdb.sql("select 1 as org_id, 1 as x")

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
         assertColocatedPersistNotAuthorizeGated(sources.derived),
      ).toThrow();
   });

   it("records the fail-closed outcome when classification throws", async () => {
      const { modelDef, materializer, deps, sources } = await compileModel(
         `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#@ persist name="gated"
#(authorize) "org_id = $ORG"
source: gated is base -> { select: org_id }
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import { tempBuildPlanPackages } from "../test_helpers/incremental_test_harness";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { computeSourceEntityId } from "./build_plan";
import {
   compilePackageBuildPlan,
   computePackageBuildPlan,
} from "./build_plan_compile";
import { setEventLoopYieldBudgetMsForTesting } from "./event_loop_yield";
import { Model } from "./model";

/**
 * Compiling a package's build plan on the main thread (build_plan_compile.ts):
 * the real-compiler main path, its failure paths, notebook/flag skipping and
 * cancellation, gate-outcome recording, and the projected plan's colocated
 * eligibility.
 */

const ONE_PERSIST_SOURCE = `##! experimental.persistence

source: orders is duckdb.sql("select 1 as id, 'a' as category")

#@ persist name="by_category"
source: by_category is orders -> { group_by: category; aggregate: n is count() }
`;

describe("compilePackageBuildPlan (real compiler)", () => {
   const packages = tempBuildPlanPackages();
   afterEach(() => packages.cleanup());

   it(
      "compiles a model's persist source into a graph, with its connection, digest and model path",
      async () => {
         const pkg = await packages.create(ONE_PERSIST_SOURCE);

         const compiled = await compilePackageBuildPlan(pkg);

         expect(compiled.builds.map((b) => b.connectionName)).toEqual([
            "duckdb",
         ]);
         const [[sourceID, source]] = Object.entries(compiled.sources);
         expect(source.name).toBe("by_category");
         expect(compiled.sourceModelPaths).toEqual({ [sourceID]: "m.malloy" });
         expect([...compiled.connections.keys()]).toEqual(["duckdb"]);
         expect(compiled.connectionDigests.duckdb).toEqual(expect.any(String));
         // The digest is what the source's content address is built from.
         expect(
            computeSourceEntityId(source, compiled.connectionDigests),
         ).toEqual(expect.any(String));
      },
      { timeout: 20000 },
   );

   it(
      "rejects with the compile error when a model does not compile",
      async () => {
         const pkg = await packages.create(`##! experimental.persistence
source: broken is duckdb.sql("select 1 as id") extend { dimension: x is no_such_field }
`);

         await expect(compilePackageBuildPlan(pkg)).rejects.toThrow(
            /no_such_field/,
         );
      },
      { timeout: 20000 },
   );

   describe("a connection that does not resolve", () => {
      let harness: MetricsHarness;
      afterEach(async () => {
         resetMaterializationTelemetryForTesting();
         await harness?.shutdown();
      });

      it(
         "still returns the plan, without that connection's digest, and counts the skip",
         async () => {
            harness = await startMetricsHarness();
            resetMaterializationTelemetryForTesting();
            const pkg = await packages.create(ONE_PERSIST_SOURCE, {
               getMalloyConnection: async () => {
                  throw new Error("no such connection");
               },
            });

            const compiled = await compilePackageBuildPlan(pkg);

            expect(compiled.builds).toHaveLength(1);
            expect(compiled.connections.size).toBe(0);
            expect(compiled.connectionDigests).not.toHaveProperty("duckdb");
            expect(
               await harness.collectCounter(
                  "publisher_materialization_connection_digest_skipped_total",
                  {},
               ),
            ).toBe(1);
         },
         { timeout: 20000 },
      );
   });
});

describe("compilePackageBuildPlan", () => {
   // DEPRECATED FEATURE COVERAGE — delete together with
   // `.malloynb` notebooks (see RELEASE_NOTES deprecations).
   describe("deprecated: .malloynb notebooks", () => {
      it("skips .malloynb notebooks without compiling them", async () => {
         // A notebook would throw on its `>>>` cell delimiter if compiled as a
         // flat model, aborting the whole package plan; it must be skipped.
         const getModelRuntime = sinon.stub(Model, "getModelRuntime");
         try {
            const pkg = {
               getModelPaths: () => ["notes.malloynb"],
               getPackagePath: () => "/test",
               getMalloyConfig: () => ({}),
               getMalloyConnection: async () => ({}),
            } as unknown as Parameters<typeof compilePackageBuildPlan>[0];

            const compiled = await compilePackageBuildPlan(pkg);

            expect(compiled.builds).toEqual([]);
            expect(getModelRuntime.called).toBe(false);
         } finally {
            getModelRuntime.restore();
         }
      });
   });

   it("stops on a cancel that lands while it yields, before compiling the next model", async () => {
      const getModelRuntime = sinon.stub(Model, "getModelRuntime");
      // Yield at every step, so the first model's yield is a real turn.
      const budget = setEventLoopYieldBudgetMsForTesting(0);
      try {
         const pkg = {
            getModelPaths: () => ["m.malloy"],
            getPackagePath: () => "/test",
            getMalloyConfig: () => ({}),
            getMalloyConnection: async () => ({}),
         } as unknown as Parameters<typeof compilePackageBuildPlan>[0];
         const controller = new AbortController();
         // Queued before the plan's own yield, so it runs during that yield.
         setImmediate(() => controller.abort());

         await expect(
            compilePackageBuildPlan(pkg, controller.signal),
         ).rejects.toThrow("Build cancelled");
         expect(getModelRuntime.called).toBe(false);
      } finally {
         setEventLoopYieldBudgetMsForTesting(budget);
         getModelRuntime.restore();
      }
   });

   it("skips a model lacking ##! experimental.persistence instead of aborting the package", async () => {
      // getBuildTargets() THROWS on a model without the flag (it does not
      // return empty), so a header-less non-persist model — e.g. an imported
      // base that only defines raw sources — must be skipped, or it would abort
      // the whole package plan and drop the persist source in the sibling model.
      const fakeModel = (hasFlag: boolean, sourceIDs: string[]) => ({
         hasFlag,
         sourceIDs,
         modelAnnotations: {
            parseAsTag: () => ({ tag: { has: () => hasFlag } }),
         },
         givens: new Map(),
         _modelDef: { contents: {} },
      });
      const models: Record<string, ReturnType<typeof fakeModel>> = {
         "base.malloy": fakeModel(false, []), // header-less: must be skipped
         "agg.malloy": fakeModel(true, ["daily@agg"]),
      };
      const getBuildTargets = async (model: ReturnType<typeof fakeModel>) => {
         if (!model.hasFlag) {
            throw new Error(
               "Model must have ##! experimental.persistence to use _walkPersistSources()",
            );
         }
         const sources = model.sourceIDs.map((sourceID) => ({
            sourceID,
            name: sourceID.split("@")[0],
            connectionName: "duckdb",
         }));
         return {
            connections: [
               {
                  connectionName: "duckdb",
                  targets: [{ buildId: "b", dependsOn: [], sources }],
               },
            ],
            tagParseLog: [],
            unbuildable: [],
         };
      };
      const getModelRuntime = sinon
         .stub(Model, "getModelRuntime")
         .callsFake((async (_path: unknown, modelPath: unknown) => ({
            runtime: {
               loadModel: () => ({
                  getModel: async () => models[modelPath as string],
               }),
               getBuildTargets,
            },
            modelURL: new URL(`file:///${modelPath}`),
            importBaseURL: new URL("file:///"),
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
         })) as any);
      try {
         const pkg = {
            getModelPaths: () => ["base.malloy", "agg.malloy"],
            getPackagePath: () => "/test",
            getMalloyConfig: () => ({}),
            getMalloyConnection: async () => ({ getDigest: async () => "dig" }),
         } as unknown as Parameters<typeof compilePackageBuildPlan>[0];

         // Would throw here before the guard (base.malloy's getBuildTargets).
         const compiled = await compilePackageBuildPlan(pkg);

         // The header-less model is skipped; the persist source in agg survives.
         expect(compiled.builds).toHaveLength(1);
         expect(compiled.builds[0].order).toEqual(["daily@agg"]);
      } finally {
         getModelRuntime.restore();
      }
   });
});

// Real-compiler coverage of `compilePackageBuildPlan`'s gate-classification
// wiring, over an actual temp-dir package (like `tests/component/query_boundary.component.spec.ts`'s
// `makeMalloyConfig` pattern), rather than the stubbed `Model.getModelRuntime`
// above. `classifyPersistSourceGate` (`classifyPersistSourceGate.spec.ts`) is
// otherwise only ever called directly; nothing else asserts that
// `compilePackageBuildPlan`'s two call sites (the ordinary persist-source loop
// and the pre-aggregation rollup branch) actually populate
// `CompiledBuildPlan.sourceGateOutcomes`.
describe("compilePackageBuildPlan populates sourceGateOutcomes", () => {
   const packages = tempBuildPlanPackages();
   afterEach(() => packages.cleanup());

   it(
      "records a row_level/attributed outcome for an ordinary #@ persist source's own gate",
      async () => {
         const pkg = await packages.create(`##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#(access_filter) org_id = $ORG
#@ persist name="gated"
source: gated is base -> { select: org_id } extend {}
`);

         const compiled = await compilePackageBuildPlan(pkg);

         const [sourceID] = Object.keys(compiled.sources).filter(
            (id) => compiled.sources[id].name === "gated",
         );
         expect(sourceID).toBeDefined();
         expect(compiled.sourceGateOutcomes?.[sourceID]).toEqual({
            classification: "row_level",
            attributed: true,
         });
      },
      { timeout: 20000 },
   );

   it(
      "classifies a legacy quoted-string #(authorize) gate REJECTED when the classifier reaches it with no load preflight to have refused it first",
      async () => {
         // `compilePackageBuildPlan` has no `Model.create`/package-load-worker
         // preflight, so a legacy quoted-string gate (`#(authorize) "org_id =
         // 999"`) reaches this classifier with its payload intact.
         // `parseAuthorizeAnnotation` returns it completely verbatim, quotes
         // included (see its doc) — compiled as an ordinary Malloy
         // expression, that payload is a STRING LITERAL, not a boolean, so
         // `resolveGateShape`'s probe fails to lift it and the outcome is
         // `rejected`. This is the invariant `build_plan.spec.ts`'s
         // `colocatedSourceEligibility` "keys eligibility..." test protects
         // at the eligibility layer; this test pins the classification
         // itself, one level down.
         const pkg = await packages.create(`##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

#(authorize) "org_id = 999"
source: base is duckdb.sql("select 1 as org_id")

#@ persist name="gated"
source: gated is base -> { select: org_id }
`);

         const compiled = await compilePackageBuildPlan(pkg);

         const [sourceID] = Object.keys(compiled.sources).filter(
            (id) => compiled.sources[id].name === "gated",
         );
         expect(sourceID).toBeDefined();
         // `attributed: true` here — the gate is genuinely the entry point's
         // own (via query-source derivation from `base`), not reached
         // through a join. It is the CLASSIFICATION, not the attribution,
         // that the legacy quoted form flips to `rejected`; either alone
         // (`rejected` OR `!attributed`) already refuses colocated
         // eligibility (`materialization_eligibility.ts`), so this pins the
         // specific mechanism findings-r2 is about rather than the
         // coarser "somehow refused" shape.
         expect(compiled.sourceGateOutcomes?.[sourceID]).toEqual({
            classification: "rejected",
            attributed: true,
         });
      },
      { timeout: 20000 },
   );

   it(
      "records a row_level/attributed outcome for a synthesized #@ preaggregate rollup, from the DIFFERENT {model, materializer} pair the rollup was compiled with",
      async () => {
         // No `#(authorize)` gate anywhere in this fixture, so the vacuous
         // (no entry-point gate) case applies: `row_level` because there is
         // no group to reject, `attributed: true` because both the deep and
         // no-join walks find nothing. This is still a real assertion, not a
         // vacuous one — if the rollup branch classified against the wrong
         // model/materializer pair (see `preaggregation_compile.ts:105-107`)
         // or degraded to blanket refusal, this would come back `rejected`.
         const pkg =
            await packages.create(`##! experimental { persistence composite_sources }

source: orders is duckdb.sql("""
  SELECT 1 AS order_id, 10 AS amount, 'A' AS category
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`);

         const compiled = await compilePackageBuildPlan(pkg);

         const rollupEntries = Object.entries(
            compiled.sourceGateOutcomes ?? {},
         );
         expect(rollupEntries).toHaveLength(1);
         const [, outcome] = rollupEntries[0];
         expect(outcome).toEqual({
            classification: "row_level",
            attributed: true,
         });
      },
      { timeout: 20000 },
   );
});

describe("computePackageBuildPlan", () => {
   it("returns a null plan and no dropped sources when the package declares no persist sources", async () => {
      const pkg = {
         getModelPaths: () => [],
         getPackagePath: () => "/test",
         getMalloyConfig: () => ({}),
         getMalloyConnection: async () => ({}),
      } as unknown as Parameters<typeof computePackageBuildPlan>[0];

      const { plan, droppedPersistSources } =
         await computePackageBuildPlan(pkg);
      expect(plan).toBeNull();
      expect(droppedPersistSources).toEqual([]);
   });

   describe("colocatedSourceEligibility", () => {
      const packages = tempBuildPlanPackages();
      afterEach(() => packages.cleanup());

      it(
         "keys eligibility so a same-NAMED source in a DIFFERENT model is judged independently — one eligible, one refused",
         async () => {
            // Two models each declare a persist source named "s". model_a's is
            // row-level and attributed (colocated-eligible); model_b's carries a
            // gate that classifies REJECTED (a literal-vs-field comparison). A
            // name-keyed positive check ("is `s` eligible anywhere in the
            // package?") would incorrectly authorize model_b's binding off
            // model_a's eligibility. Keying by each source's own sourceEntityId
            // (computed from ITS OWN connection digest + SQL, never looked up by
            // name) cannot make that mistake — the two sources are examined,
            // and recorded, independently.
            const pkg = await packages.create({
               "model_a.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base_a is duckdb.sql("select 1 as org_id")

#(access_filter) org_id = $ORG
#@ persist name="s"
source: s is base_a -> { select: org_id } extend {}
`,
               "model_b.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG2 :: number

#(authorize) "org_id = 999"
source: base_b is duckdb.sql("select 2 as org_id")

#@ persist name="s"
source: s is base_b -> { select: org_id }
`,
            });

            const compiled = await compilePackageBuildPlan(pkg);
            const { colocatedSourceEligibility } =
               await computePackageBuildPlan(pkg);

            const sourceIDA = Object.keys(compiled.sources).find(
               (id) =>
                  compiled.sources[id].name === "s" && id.includes("model_a"),
            );
            const sourceIDB = Object.keys(compiled.sources).find(
               (id) =>
                  compiled.sources[id].name === "s" && id.includes("model_b"),
            );
            expect(sourceIDA).toBeDefined();
            expect(sourceIDB).toBeDefined();

            const idA = computeSourceEntityId(
               compiled.sources[sourceIDA as string],
               compiled.connectionDigests,
            );
            const idB = computeSourceEntityId(
               compiled.sources[sourceIDB as string],
               compiled.connectionDigests,
            );
            // Distinct content ⇒ distinct entity ids, which is what makes this a
            // real collision-avoidance test rather than a tautology.
            expect(idA).not.toBe(idB);
            expect(colocatedSourceEligibility.eligibleEntityIds.has(idA)).toBe(
               true,
            );
            expect(colocatedSourceEligibility.eligibleEntityIds.has(idB)).toBe(
               false,
            );
         },
         { timeout: 20000 },
      );

      it(
         "a refusal wins a real sourceEntityId collision — identical compiled SQL, different gates",
         async () => {
            // `sourceEntityId` is `makeBuildId(connectionDigest, getSQL())`,
            // which excludes annotation bytes: model_a's `s` and model_b's `s`
            // compile to the exact same SQL on the same connection, so they
            // collide on the same id despite model_a's gate being eligible
            // (row-level, attributed to the entry point) and model_b's being
            // refused (the gate is reachable only through a join, so it is
            // unattributed). A positive-eligibility check that lets either
            // source's outcome win nondeterministically is fail-open; the
            // refusal must win regardless of iteration order. model_a uses
            // the current unquoted form deliberately — the legacy quoted
            // form (`#(authorize) "x = $ORG"`) would itself classify
            // `rejected` (see the legacy-quoted-gate tests below), which
            // would collapse this into a refused/refused collision and lose
            // the eligible/refused distinction this test exists to exercise.
            const pkg = await packages.create({
               "model_a.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as x")

#@ persist name="s"
#(access_filter) x = $ORG
source: s is base -> { select: x }
`,
               "model_b.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

#(access_filter) x = 1
source: locked is duckdb.sql("select 1 as x")

#@ persist name="s"
source: s is duckdb.sql("select 1 as x") extend {
   join_one: locked on 1 = 1
} -> { select: x }
`,
            });

            const compiled = await compilePackageBuildPlan(pkg);
            const { colocatedSourceEligibility } =
               await computePackageBuildPlan(pkg);

            const sourceIDA = Object.keys(compiled.sources).find(
               (id) =>
                  compiled.sources[id].name === "s" && id.includes("model_a"),
            );
            const sourceIDB = Object.keys(compiled.sources).find(
               (id) =>
                  compiled.sources[id].name === "s" && id.includes("model_b"),
            );
            expect(sourceIDA).toBeDefined();
            expect(sourceIDB).toBeDefined();

            const idA = computeSourceEntityId(
               compiled.sources[sourceIDA as string],
               compiled.connectionDigests,
            );
            const idB = computeSourceEntityId(
               compiled.sources[sourceIDB as string],
               compiled.connectionDigests,
            );
            // Same id: this is the real collision, not a tautology.
            expect(idA).toBe(idB);
            expect(colocatedSourceEligibility.refused[idA]).toBeDefined();
            expect(colocatedSourceEligibility.eligibleEntityIds.has(idA)).toBe(
               false,
            );
         },
         { timeout: 20000 },
      );

      it(
         "one unreadable source costs the package that source's binding, not its whole colocated tier",
         async () => {
            // The colocated gate deliberately admits a given-referencing source
            // (materialization_eligibility.spec.ts's mz_colocated_given), and
            // `bad`'s given has no default, so getSQL() — and every content
            // address derived from it — throws. An escaping throw would leave
            // colocatedSourceEligibility undefined, and colocated is the default
            // tier: bindColocatedServeManifest would then drop EVERY binding for
            // the package, reverting `good` to live recompute too.
            const pkg = await packages.create({
               "m.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: base is duckdb.sql("select 1 as org_id, 2 as amount")

#@ persist name="good"
source: good is base -> { select: org_id, amount }

#@ persist name="bad"
source: bad is base -> { where: org_id = $ORG; select: org_id, amount }
`,
            });

            const { plan, colocatedSourceEligibility } =
               await computePackageBuildPlan(pkg);

            expect(
               Object.values(plan?.sources ?? {}).map((s) => s.name),
            ).toEqual(["good"]);
            // `good` keeps a usable binding; `bad` contributes neither an
            // eligibility nor a refusal, since it has no id to key either under.
            expect(colocatedSourceEligibility.eligibleEntityIds.size).toBe(1);
            const [good] = Object.values(plan?.sources ?? {});
            expect(
               colocatedSourceEligibility.eligibleEntityIds.has(
                  good.sourceEntityId,
               ),
            ).toBe(true);
         },
         { timeout: 20000 },
      );
   });
});

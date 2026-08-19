import type { PersistSource } from "@malloydata/malloy";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import * as sinon from "sinon";
import type { BuildGraph as MalloyBuildGraph } from "@malloydata/malloy";
import {
   type BuildPlanPackage,
   compilePackageBuildPlan,
   computeSourceEntityId,
   computePackageBuildPlan,
   deriveAnnotationFields,
   deriveBuildPlan,
   flattenDependsOn,
   iterGraphSources,
   projectToPublicColumns,
   resolveFreshness,
   resolveQueryMetadata,
   resolvePackageConnections,
} from "./build_plan";
import { MaterializationEligibilityError } from "../errors";
import { fakeSource } from "./materialization_test_fixtures";
import { Model } from "./model";

describe("flattenDependsOn", () => {
   it("maps nested dependsOn entries to a flat sourceID list", () => {
      expect(
         flattenDependsOn({
            dependsOn: [{ sourceID: "a" }, { sourceID: "b" }],
         }),
      ).toEqual(["a", "b"]);
   });
});

describe("iterGraphSources", () => {
   it("yields resolvable sources in dependency order, skipping missing ones", () => {
      const a = fakeSource({ name: "a", sourceEntityId: "ba" });
      const b = fakeSource({ name: "b", sourceEntityId: "bb" });
      const graph = {
         connectionName: "duckdb",
         nodes: [
            [{ sourceID: "a@m", dependsOn: [] }],
            [
               { sourceID: "missing@m", dependsOn: [] },
               { sourceID: "b@m", dependsOn: [] },
            ],
         ],
      } as unknown as MalloyBuildGraph;

      const names = [...iterGraphSources(graph, { "a@m": a, "b@m": b })].map(
         (s) => s.name,
      );
      expect(names).toEqual(["a", "b"]);
   });

   it("walks each root's nested dependsOn tree, deps before dependents", () => {
      // root -> mid -> leaf, with only `root` at the graph's node level — this
      // mirrors malloy getBuildPlan(): terminal persist sources are the nodes,
      // and every transitive persist dependency is nested in dependsOn. All
      // three must be yielded (so all get built), leaf-first so a downstream
      // build reads its upstream's freshly materialized table.
      const root = fakeSource({ name: "root", sourceEntityId: "br" });
      const mid = fakeSource({ name: "mid", sourceEntityId: "bm" });
      const leaf = fakeSource({ name: "leaf", sourceEntityId: "bl" });
      const graph = {
         connectionName: "duckdb",
         nodes: [
            [
               {
                  sourceID: "root@m",
                  dependsOn: [
                     {
                        sourceID: "mid@m",
                        dependsOn: [{ sourceID: "leaf@m", dependsOn: [] }],
                     },
                  ],
               },
            ],
         ],
      } as unknown as MalloyBuildGraph;

      const names = [
         ...iterGraphSources(graph, {
            "root@m": root,
            "mid@m": mid,
            "leaf@m": leaf,
         }),
      ].map((s) => s.name);
      expect(names).toEqual(["leaf", "mid", "root"]);
   });

   it("deduplicates a shared (diamond) dependency across roots", () => {
      // r1 and r2 both depend on `shared`; it must be yielded exactly once and
      // before both dependents.
      const r1 = fakeSource({ name: "r1", sourceEntityId: "b1" });
      const r2 = fakeSource({ name: "r2", sourceEntityId: "b2" });
      const shared = fakeSource({ name: "shared", sourceEntityId: "bs" });
      const graph = {
         connectionName: "duckdb",
         nodes: [
            [
               {
                  sourceID: "r1@m",
                  dependsOn: [{ sourceID: "shared@m", dependsOn: [] }],
               },
               {
                  sourceID: "r2@m",
                  dependsOn: [{ sourceID: "shared@m", dependsOn: [] }],
               },
            ],
         ],
      } as unknown as MalloyBuildGraph;

      const names = [
         ...iterGraphSources(graph, {
            "r1@m": r1,
            "r2@m": r2,
            "shared@m": shared,
         }),
      ].map((s) => s.name);
      expect(names).toEqual(["shared", "r1", "r2"]);
   });
});

describe("deriveAnnotationFields", () => {
   it("returns all key=value fields of the #@ persist annotation", () => {
      const source = {
         annotations: {
            parseAsTag: () => ({
               tag: {
                  *entries() {
                     yield ["name", { text: () => "engaged_events" }];
                     yield ["realization", { text: () => "COPY" }];
                  },
               },
            }),
         },
      } as unknown as PersistSource;

      expect(deriveAnnotationFields(source)).toEqual({
         name: "engaged_events",
         realization: "COPY",
      });
   });

   it("degrades to {} when the annotation is absent or unparseable", () => {
      const source = {
         annotations: {
            parseAsTag: () => {
               throw new Error("no @ annotation");
            },
         },
      } as unknown as PersistSource;

      expect(deriveAnnotationFields(source)).toEqual({});
   });
});

describe("projectToPublicColumns", () => {
   // A source whose PUBLIC surface (intrinsic atomic fields) is `cols` — i.e. any
   // `except:`-ed / access-restricted column is already absent here, as Malloy
   // reflects it. deriveColumns reads exactly this.
   const sourceWithPublicCols = (cols: string[]): PersistSource =>
      ({
         dialectName: "postgres",
         _explore: {
            intrinsicFields: cols.map((name) => ({
               name,
               isAtomicField: () => true,
               type: "string",
            })),
         },
      }) as unknown as PersistSource;

   it("wraps the build SQL to project only the source's public columns", () => {
      const src = sourceWithPublicCols(["order_date", "amount"]); // `region` hidden → absent
      const out = projectToPublicColumns(
         src,
         "SELECT order_date, region, amount FROM t",
      );
      // Outer projection lists ONLY the public columns; the hidden one is dropped.
      expect(out).toMatch(/^SELECT\b/);
      expect(out).toContain("order_date");
      expect(out).toContain("amount");
      expect(out).toContain(
         "FROM (SELECT order_date, region, amount FROM t) AS __public",
      );
      // `region` must not appear in the OUTER projection (before the subquery).
      const outerProjection = out.slice(0, out.indexOf("FROM ("));
      expect(outerProjection).not.toContain("region");
   });

   // Fails closed, like the rest of the eligibility surface: a public surface we
   // can't determine refuses the build rather than widening to everything getSQL
   // projects (which is where the hidden columns are).
   it("refuses the build when the field list can't be read", () => {
      const noExplore = {} as unknown as PersistSource;
      expect(() => projectToPublicColumns(noExplore, "SELECT 1")).toThrow(
         /public column surface could not be determined/,
      );
   });

   it("refuses the build when the source exposes no public atomic columns", () => {
      // Everything getSQL projects is hidden — the worst case to materialize.
      expect(() =>
         projectToPublicColumns(sourceWithPublicCols([]), "SELECT 1"),
      ).toThrow(/no public atomic columns/);
   });

   it("refuses with an eligibility error (422), naming the source", () => {
      const src = { ...sourceWithPublicCols([]), name: "daily" };
      try {
         projectToPublicColumns(src as unknown as PersistSource, "SELECT 1");
         throw new Error("expected a refusal");
      } catch (err) {
         expect(err).toBeInstanceOf(MaterializationEligibilityError);
         expect((err as Error).message).toContain("'daily'");
      }
   });
});

describe("computeSourceEntityId", () => {
   it("delegates to PersistSource.makeBuildId with the connection digest and SQL", () => {
      const makeBuildId = sinon.stub().returns("computed-id");
      const source = {
         connectionName: "duckdb",
         makeBuildId,
         getSQL: () => "SELECT 7",
      } as unknown as PersistSource;

      const id = computeSourceEntityId(source, { duckdb: "dig-1" });

      expect(id).toBe("computed-id");
      expect(makeBuildId.calledOnceWithExactly("dig-1", "SELECT 7")).toBe(true);
   });
});

describe("resolvePackageConnections", () => {
   it("resolves each unique name once and omits failures", async () => {
      const getMalloyConnection = sinon.stub();
      getMalloyConnection.withArgs("ok").resolves({ id: "ok-conn" });
      getMalloyConnection.withArgs("bad").rejects(new Error("nope"));

      const map = await resolvePackageConnections({ getMalloyConnection }, [
         "ok",
         "ok",
         "bad",
      ]);

      expect(map.has("ok")).toBe(true);
      expect(map.has("bad")).toBe(false);
      // "ok" requested twice but resolved once (dedupe).
      expect(getMalloyConnection.withArgs("ok").callCount).toBe(1);
   });
});

describe("deriveBuildPlan", () => {
   it("projects graphs and sources into the wire build plan", () => {
      const orders = fakeSource({
         name: "orders",
         sourceEntityId: "bid-orders",
         sql: "SELECT 1",
      });
      const plan = deriveBuildPlan(
         [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "orders@m", dependsOn: [] }]],
            },
         ] as unknown as Parameters<typeof deriveBuildPlan>[0],
         { "orders@m": orders },
         { duckdb: "dig" },
      );

      expect(plan.graphs[0].connectionName).toBe("duckdb");
      expect(plan.sources["orders@m"]).toMatchObject({
         name: "orders",
         connectionName: "duckdb",
         sourceEntityId: "bid-orders",
         sql: "SELECT 1",
         columns: [],
      });
   });

   it("reports declared refresh verbatim (null when unset) and does not emit sharing/schedule", () => {
      // `refresh` is a metadata pass-through; `sharing`/`schedule` were retired
      // from the contract and must not be emitted as typed fields (they stay in
      // the raw annotationFields for the publish-time validator to detect).
      const declared = fakeSource({
         name: "declared",
         sourceEntityId: "bid-d",
         annotationFields: {
            name: "d_table",
            sharing: "private",
            refresh: "incremental",
         },
      });
      const unset = fakeSource({ name: "unset", sourceEntityId: "bid-u" });
      const plan = deriveBuildPlan(
         [
            {
               connectionName: "duckdb",
               nodes: [
                  [
                     { sourceID: "declared@m", dependsOn: [] },
                     { sourceID: "unset@m", dependsOn: [] },
                  ],
               ],
            },
         ] as unknown as Parameters<typeof deriveBuildPlan>[0],
         { "declared@m": declared, "unset@m": unset },
         { duckdb: "dig" },
      );

      expect(plan.sources["declared@m"].refresh).toBe("incremental");
      // Retired typed fields are absent from the wire projection.
      expect(
         (plan.sources["declared@m"] as Record<string, unknown>).sharing,
      ).toBeUndefined();
      expect(
         (plan.sources["declared@m"] as Record<string, unknown>).schedule,
      ).toBeUndefined();
      // The raw annotation map still carries every field (so the validator can
      // reject a source-level sharing/schedule at publish).
      expect(plan.sources["declared@m"].annotationFields).toEqual({
         name: "d_table",
         sharing: "private",
         refresh: "incremental",
      });
      // Unset refresh is null on the wire.
      expect(plan.sources["unset@m"].refresh).toBeNull();
   });

   it("honors the sourceNames filter", () => {
      const a = fakeSource({ name: "a", sourceEntityId: "bid-a" });
      const b = fakeSource({ name: "b", sourceEntityId: "bid-b" });
      const plan = deriveBuildPlan(
         [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "a@m", dependsOn: [] }]],
            },
         ] as unknown as Parameters<typeof deriveBuildPlan>[0],
         { "a@m": a, "b@m": b },
         { duckdb: "dig" },
         ["a"],
      );

      expect(Object.keys(plan.sources)).toEqual(["a@m"]);
   });

   it("carries the per-source package-relative modelPath", () => {
      const a = fakeSource({ name: "a", sourceEntityId: "bid-a" });
      const b = fakeSource({ name: "b", sourceEntityId: "bid-b" });
      const plan = deriveBuildPlan(
         [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "a@m", dependsOn: [] }]],
            },
         ] as unknown as Parameters<typeof deriveBuildPlan>[0],
         { "a@m": a, "b@m": b },
         { duckdb: "dig" },
         undefined,
         { "a@m": "rollup.malloy" },
      );

      // Mapped source gets its model path; an unmapped source stays undefined.
      expect(plan.sources["a@m"].modelPath).toBe("rollup.malloy");
      expect(plan.sources["b@m"].modelPath).toBeUndefined();
   });
});

describe("resolveFreshness", () => {
   it("reports source-level freshness verbatim", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         freshnessSchedule: {
            freshness: { window: "1h", fallback: "stale_ok" },
         },
      });
      expect(resolveFreshness(source, null)).toEqual({
         window: "1h",
         fallback: "stale_ok",
      });
   });

   it("returns null when unset at every level", () => {
      const source = fakeSource({ name: "s", sourceEntityId: "bid" });
      expect(resolveFreshness(source, null)).toBeNull();
   });

   it("falls back to model-file then package per field (most-specific-wins)", () => {
      // freshness.window from source, freshness.fallback from model-file.
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         freshnessSchedule: { freshness: { window: "1h" } },
         modelFreshnessSchedule: { freshness: { fallback: "fail" } },
      });
      const pkg = {
         schedule: null,
         freshness: { window: "24h", fallback: "live" as const },
      };
      expect(resolveFreshness(source, pkg)).toEqual({
         window: "1h",
         fallback: "fail",
      });
   });

   it("inherits the package freshness when the source and model are unset", () => {
      const source = fakeSource({ name: "s", sourceEntityId: "bid" });
      const pkg = { schedule: null, freshness: { window: "24h" } };
      expect(resolveFreshness(source, pkg)).toEqual({ window: "24h" });
   });

   it("drops an invalid fallback rather than defaulting it", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         freshnessSchedule: { freshness: { window: "1h", fallback: "bogus" } },
      });
      expect(resolveFreshness(source, null)).toEqual({ window: "1h" });
   });

   it("reads the model-file `materialization` envelope", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         modelMaterialization: { freshness: { freshness: { window: "12h" } } },
      });
      expect(resolveFreshness(source, null)).toEqual({ window: "12h" });
   });

   it("prefers the envelope over the deprecated bare model-file form", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         modelFreshnessSchedule: { freshness: { window: "48h" } },
         modelMaterialization: { freshness: { freshness: { window: "12h" } } },
      });
      expect(resolveFreshness(source, null)).toEqual({ window: "12h" });
   });

   it("still reads a bare model-file knob the envelope does not declare", () => {
      // A package published before the envelope existed keeps resolving, and the
      // envelope does not hide the knobs it says nothing about.
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         modelFreshnessSchedule: { freshness: { fallback: "fail" } },
         modelMaterialization: { freshness: { freshness: { window: "12h" } } },
      });
      expect(resolveFreshness(source, null)).toEqual({
         window: "12h",
         fallback: "fail",
      });
   });
});

describe("resolveQueryMetadata", () => {
   it("returns null when no layer declares anything", () => {
      const source = fakeSource({ name: "s", sourceEntityId: "bid" });
      expect(resolveQueryMetadata(source, null)).toBeNull();
      expect(
         resolveQueryMetadata(source, {
            schedule: null,
            freshness: null,
            queryMetadata: null,
         }),
      ).toBeNull();
   });

   it("reads the source's `#@ persist queryMetadata.*` properties", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         queryMetadata: { team: "finance", workload: "orders" },
      });
      expect(resolveQueryMetadata(source, null)).toEqual({
         team: "finance",
         workload: "orders",
      });
   });

   it("resolves most-specific-wins PER PROPERTY across all four layers", () => {
      // Package declares team+tier, the model-file envelope overrides tier and
      // adds one of its own, the source overrides only workload: every property
      // nothing more specific overrides has to survive.
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         queryMetadata: { workload: "orders" },
         modelMaterialization: {
            queryMetadata: { tier: "gold", surface: "marts" },
         },
      });
      expect(
         resolveQueryMetadata(source, {
            schedule: null,
            freshness: null,
            queryMetadata: { team: "finance", tier: "bronze" },
         }),
      ).toEqual({
         team: "finance",
         tier: "gold",
         surface: "marts",
         workload: "orders",
      });
   });

   it("prefers the CANONICAL model-file form over the deprecated envelope", () => {
      // Inverted deliberately. The envelope used to win, mirroring freshness —
      // but freshness migrates the other way (there the envelope is canonical
      // and the bare form legacy). For query metadata the bare `##
      // queryMetadata.*` is what an author is now told to write, so the envelope
      // winning meant the spelling we deprecate silently overrode the one we
      // recommend, in a file that declares both.
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         modelQueryMetadata: { tier: "bronze", legacy: "kept" },
         modelMaterialization: { queryMetadata: { tier: "gold", old: "seen" } },
      });
      expect(resolveQueryMetadata(source, null)).toEqual({
         tier: "bronze",
         legacy: "kept",
         // A property only the envelope declares still resolves — the
         // deprecated home keeps working, it just stops overriding.
         old: "seen",
      });
   });

   it("keeps a contract-violating property for the validator to report", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         queryMetadata: { "team.name": "finance" },
      });
      expect(resolveQueryMetadata(source, null)).toEqual({
         "team.name": "finance",
      });
   });

   it("does not confuse the scalar `#@ persist` fields beside it", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         annotationFields: { name: "s_table", refresh: "full" },
         queryMetadata: { team: "finance" },
      });
      expect(resolveQueryMetadata(source, null)).toEqual({ team: "finance" });
      expect(deriveAnnotationFields(source)).toEqual({
         name: "s_table",
         refresh: "full",
      });
   });
});

describe("deriveBuildPlan freshness", () => {
   it("projects the resolved per-source freshness onto the plan (no schedule/sharing)", () => {
      const source = fakeSource({
         name: "s",
         sourceEntityId: "bid",
         annotationFields: { name: "s_table" },
         freshnessSchedule: { freshness: { window: "1h" } },
      });
      const plan = deriveBuildPlan(
         [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "s@m", dependsOn: [] }]],
            },
         ] as unknown as Parameters<typeof deriveBuildPlan>[0],
         { "s@m": source },
         { duckdb: "dig" },
         undefined,
         undefined,
         {
            schedule: null,
            freshness: { window: "24h", fallback: "live" as const },
         },
      );

      // Source window wins over the package default; package fallback fills the
      // unset source fallback.
      expect(plan.sources["s@m"].freshness).toEqual({
         window: "1h",
         fallback: "live",
      });
      // Retired fields are not emitted.
      expect(
         (plan.sources["s@m"] as Record<string, unknown>).schedule,
      ).toBeUndefined();
      expect(
         (plan.sources["s@m"] as Record<string, unknown>).sharing,
      ).toBeUndefined();
   });
});

describe("compilePackageBuildPlan", () => {
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

         expect(compiled.graphs).toEqual([]);
         expect(getModelRuntime.called).toBe(false);
      } finally {
         getModelRuntime.restore();
      }
   });

   it("skips a model lacking ##! experimental.persistence instead of aborting the package", async () => {
      // getBuildPlan() THROWS on a model without the flag (it does not return
      // empty), so a header-less non-persist model — e.g. an imported base that
      // only defines raw sources — must be skipped, or it would abort the whole
      // package plan and drop the persist source in the sibling model.
      const fakeModel = (hasFlag: boolean, graphs: MalloyBuildGraph[]) => ({
         modelAnnotations: {
            parseAsTag: () => ({ tag: { has: () => hasFlag } }),
         },
         getBuildPlan: () => {
            if (!hasFlag) {
               throw new Error(
                  "Model must have ##! experimental.persistence to use getBuildPlan()",
               );
            }
            return { graphs, sources: {}, tagParseLog: [] };
         },
      });
      const models: Record<string, ReturnType<typeof fakeModel>> = {
         "base.malloy": fakeModel(false, []), // header-less: must be skipped
         "agg.malloy": fakeModel(true, [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "daily@agg", dependsOn: [] }]],
            },
         ] as unknown as MalloyBuildGraph[]),
      };
      const getModelRuntime = sinon
         .stub(Model, "getModelRuntime")
         .callsFake((async (_path: unknown, modelPath: unknown) => ({
            runtime: {
               loadModel: () => ({
                  getModel: async () => models[modelPath as string],
               }),
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

         // Would throw here before the guard (base.malloy's getBuildPlan).
         const compiled = await compilePackageBuildPlan(pkg);

         // The header-less model is skipped; the persist source in agg survives.
         expect(compiled.graphs).toHaveLength(1);
         expect(compiled.graphs[0].nodes[0][0].sourceID).toBe("daily@agg");
      } finally {
         getModelRuntime.restore();
      }
   });
});

// Real-compiler coverage of `compilePackageBuildPlan`'s gate-classification
// wiring, over an actual temp-dir package (like `query_boundary.spec.ts`'s
// `makeMalloyConfig` pattern), rather than the stubbed `Model.getModelRuntime`
// above. `classifyPersistSourceGate` (`classifyPersistSourceGate.spec.ts`) is
// otherwise only ever called directly; nothing else asserts that
// `compilePackageBuildPlan`'s two call sites (the ordinary persist-source loop
// and the pre-aggregation rollup branch) actually populate
// `CompiledBuildPlan.sourceGateOutcomes`.
describe("compilePackageBuildPlan populates sourceGateOutcomes", () => {
   let rootDir: string;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-buildplan-"),
      );
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   async function realPackage(
      modelText: string,
      modelFile = "m.malloy",
   ): Promise<BuildPlanPackage> {
      await fs.writeFile(path.join(rootDir, modelFile), modelText);
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);
      return {
         getModelPaths: () => [modelFile],
         getPackagePath: () => rootDir,
         getMalloyConfig: () => malloyConfig,
         getMalloyConnection: async () => duckdb,
      };
   }

   it(
      "records a row_level/attributed outcome for an ordinary #@ persist source's own gate",
      async () => {
         const pkg = await realPackage(`##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#@ persist name="gated"
#(authorize) "org_id = $ORG"
source: gated is base -> { select: org_id }
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
            await realPackage(`##! experimental { persistence composite_sources }

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
      let rootDir: string;

      beforeEach(async () => {
         rootDir = await fs.mkdtemp(
            path.join(os.tmpdir(), "publisher-buildplan-colocated-"),
         );
      });

      afterEach(async () => {
         await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
      });

      async function realPackageMulti(
         files: Record<string, string>,
      ): Promise<BuildPlanPackage> {
         for (const [modelFile, modelText] of Object.entries(files)) {
            await fs.writeFile(path.join(rootDir, modelFile), modelText);
         }
         const duckdb = new DuckDBConnection("duckdb", ":memory:");
         const connections = new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         );
         const malloyConfig = new MalloyConfig({ connections: {} });
         malloyConfig.wrapConnections(() => connections);
         return {
            getModelPaths: () => Object.keys(files),
            getPackagePath: () => rootDir,
            getMalloyConfig: () => malloyConfig,
            getMalloyConnection: async () => duckdb,
         };
      }

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
            const pkg = await realPackageMulti({
               "model_a.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base_a is duckdb.sql("select 1 as org_id")

#@ persist name="s"
#(authorize) "org_id = $ORG"
source: s is base_a -> { select: org_id }
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
            // refusal must win regardless of iteration order.
            const pkg = await realPackageMulti({
               "model_a.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

source: base is duckdb.sql("select 1 as x")

#@ persist name="s"
#(authorize) "x = $ORG"
source: s is base -> { select: x }
`,
               "model_b.malloy": `##! experimental.persistence
##! experimental.givens

given:
  ORG :: number

#(authorize) "x = 1"
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
   });
});

// `BuildPlan.refusedSources`: a SEPARATE collection from `PersistSourcePlan`,
// computed from the tier-appropriate assert post-relaxation (see
// `deriveBuildPlan`'s doc). Real-compiler coverage, like the describe blocks
// above, because the whole point is the exact IR shape (getSQL() actually
// throwing for a given with no default) rather than a stubbed approximation.
describe("BuildPlan.refusedSources", () => {
   let rootDir: string;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-buildplan-refused-"),
      );
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   async function realPackage(
      modelText: string,
      modelFile = "m.malloy",
   ): Promise<BuildPlanPackage> {
      await fs.writeFile(path.join(rootDir, modelFile), modelText);
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);
      return {
         getModelPaths: () => [modelFile],
         getPackagePath: () => rootDir,
         getMalloyConfig: () => malloyConfig,
         getMalloyConnection: async () => duckdb,
      };
   }

   it(
      "reports a package where EVERY persist source is refused as a completed plan carrying refusedSources, not a thrown error",
      async () => {
         // Both sources are storage-tier (declare `storage=`) and both are
         // ineligible: `mz_given` references a given, `mz_free` declares an
         // unbound parameter. Classified from the compiled source's IR alone
         // (Parameter.value/refSummary.givenUsage), independent of whether
         // `getSQL()` itself would succeed for this particular shape — see
         // the next test for the shape where it demonstrably does not.
         const pkg = await realPackage(`##! experimental.persistence
##! experimental.parameters
##! experimental.givens

given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")

#@ persist name="mz_given" storage="dest"
source: mz_given is base -> { where: tenant = $tenant; aggregate: c is count() }

#@ persist name="mz_free" storage="dest"
source: mz_free(threshold::number) is base -> { aggregate: c is count() }
`);

         const { plan } = await computePackageBuildPlan(pkg);

         expect(plan).not.toBeNull();
         expect(Object.keys(plan?.sources ?? {})).toHaveLength(0);
         const refused = Object.values(plan?.refusedSources ?? {});
         expect(refused).toHaveLength(2);
         const byName = Object.fromEntries(refused.map((r) => [r.name, r]));
         expect(byName.mz_given).toMatchObject({
            tier: "storage",
            reason: "given",
         });
         expect(byName.mz_given.message).toMatch(/given/i);
         expect(byName.mz_free).toMatchObject({
            tier: "storage",
            reason: "free_parameter",
         });
         expect(byName.mz_free.message).toMatch(/unbound parameter/i);
      },
      { timeout: 20000 },
   );

   it(
      "represents a given-referencing refusal without SQL or a content address (the reason a separate collection exists)",
      async () => {
         // Refused on the compiled source's IR alone (a non-empty given-usage
         // summary), BEFORE `getSQL()`/`computeSourceEntityId` are ever
         // called — the codepath that constructs `PersistSourcePlan` (which
         // requires both) is never reached for this source at all, which is
         // the property a free-parameter or given-referencing source needs:
         // neither is guaranteed to survive those calls in general.
         const pkg = await realPackage(`##! experimental.persistence
##! experimental.givens

given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")

#@ persist name="mz_given" storage="dest"
source: mz_given is base -> { where: tenant = $tenant; aggregate: c is count() }
`);

         const { plan } = await computePackageBuildPlan(pkg);

         expect(plan).not.toBeNull();
         expect(Object.keys(plan?.sources ?? {})).toHaveLength(0);
         const [refused] = Object.values(plan?.refusedSources ?? {});
         expect(refused).toMatchObject({
            name: "mz_given",
            tier: "storage",
            reason: "given",
         });
         // No SQL/content-address fields leak onto the refused entry — it is
         // a genuinely different wire shape, not `PersistSourcePlan` with two
         // fields blanked out.
         expect(refused).not.toHaveProperty("sql");
         expect(refused).not.toHaveProperty("sourceEntityId");
      },
      { timeout: 20000 },
   );

   it(
      "does NOT report a colocated gated source as refused once the row-level relaxation admits it (the storage-rules SourceEligibility.refused trap)",
      async () => {
         // Plain `#@ persist` (no `storage=`): the entry point's own
         // `#(authorize)` gate classifies row_level + attributed, so the
         // colocated relaxation admits it. The OLD `SourceEligibility.refused`
         // (computed with the unconditional storage-tier assert) would report
         // this same source as `refused: authorize` — refusedSources must not
         // repeat that mistake now that it can actually build.
         const pkg = await realPackage(`##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#@ persist name="gated"
#(authorize) "org_id = $ORG"
source: gated is base -> { select: org_id }
`);

         const { plan } = await computePackageBuildPlan(pkg);

         expect(plan).not.toBeNull();
         expect(Object.keys(plan?.refusedSources ?? {})).toHaveLength(0);
         const [entry] = Object.values(plan?.sources ?? {});
         expect(entry).toMatchObject({ name: "gated" });
         expect(entry.sourceEntityId).toBeTruthy();
         expect(entry.sql).toBeTruthy();
      },
      { timeout: 20000 },
   );

   it(
      "reports a gated preaggregate rollup as BOTH synthesized (sources) and refused (refusedSources, tier preaggregate)",
      async () => {
         // Rollups group away the gate column, so unlike colocated there is no
         // row-level admission here: the pre-aggregation gate refuses this
         // rollup unconditionally. Synthesis is unaffected by that refusal (see
         // preaggregation_seams.spec.ts's "gate stops materialization, not
         // synthesis" test), so the rollup still lands in `sources` — the plan
         // now also has to say it will never materialize.
         const pkg =
            await realPackage(`##! experimental { persistence composite_sources givens }

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (10, 'A', 1),
    (20, 'A', 2),
    (30, 'B', 1)
  ) AS t(amount, category, org_id)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`);

         const { plan } = await computePackageBuildPlan(pkg);

         expect(plan).not.toBeNull();
         const [source] = Object.values(plan?.sources ?? {});
         expect(source).toMatchObject({ origin: "preaggregate" });
         const [refused] = Object.values(plan?.refusedSources ?? {});
         expect(refused).toMatchObject({
            tier: "preaggregate",
            reason: "authorize",
            sourceID: source.sourceID,
         });
      },
      { timeout: 20000 },
   );
});

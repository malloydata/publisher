import type { PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import type { BuildGraph as MalloyBuildGraph } from "@malloydata/malloy";
import {
   compilePackageBuildPlan,
   computeSourceEntityId,
   computePackageBuildPlan,
   deriveAnnotationFields,
   deriveBuildPlan,
   flattenDependsOn,
   iterGraphSources,
   projectToPublicColumns,
   resolveFreshness,
   resolvePackageConnections,
} from "./build_plan";
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

   it("fails open — returns the build SQL unchanged when columns can't be derived", () => {
      const noExplore = {} as unknown as PersistSource; // deriveColumns throws → []
      expect(projectToPublicColumns(noExplore, "SELECT 1")).toBe("SELECT 1");
      const empty = sourceWithPublicCols([]);
      expect(projectToPublicColumns(empty, "SELECT 1")).toBe("SELECT 1");
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

describe("computePackageBuildPlan", () => {
   it("returns null when the package declares no persist sources", async () => {
      const pkg = {
         getModelPaths: () => [],
         getPackagePath: () => "/test",
         getMalloyConfig: () => ({}),
         getMalloyConnection: async () => ({}),
      } as unknown as Parameters<typeof computePackageBuildPlan>[0];

      expect(await computePackageBuildPlan(pkg)).toBeNull();
   });
});

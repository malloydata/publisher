import type { PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import type { BuildGraph as MalloyBuildGraph } from "@malloydata/malloy";
import {
   compilePackageBuildPlan,
   computeBuildId,
   computePackageBuildPlan,
   deriveAnnotationFields,
   deriveBuildPlan,
   flattenDependsOn,
   iterGraphSources,
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
      const a = fakeSource({ name: "a", buildId: "ba" });
      const b = fakeSource({ name: "b", buildId: "bb" });
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
      const root = fakeSource({ name: "root", buildId: "br" });
      const mid = fakeSource({ name: "mid", buildId: "bm" });
      const leaf = fakeSource({ name: "leaf", buildId: "bl" });
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
      const r1 = fakeSource({ name: "r1", buildId: "b1" });
      const r2 = fakeSource({ name: "r2", buildId: "b2" });
      const shared = fakeSource({ name: "shared", buildId: "bs" });
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

describe("computeBuildId", () => {
   it("delegates to PersistSource.makeBuildId with the connection digest and SQL", () => {
      const makeBuildId = sinon.stub().returns("computed-id");
      const source = {
         connectionName: "duckdb",
         makeBuildId,
         getSQL: () => "SELECT 7",
      } as unknown as PersistSource;

      const id = computeBuildId(source, { duckdb: "dig-1" });

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
         buildId: "bid-orders",
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
         buildId: "bid-orders",
         sql: "SELECT 1",
         columns: [],
      });
   });

   it("honors the sourceNames filter", () => {
      const a = fakeSource({ name: "a", buildId: "bid-a" });
      const b = fakeSource({ name: "b", buildId: "bid-b" });
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
      const a = fakeSource({ name: "a", buildId: "bid-a" });
      const b = fakeSource({ name: "b", buildId: "bid-b" });
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

import type { PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   computeBuildId,
   computePackageBuildPlan,
   deriveBuildPlan,
   flattenDependsOn,
   resolvePackageConnections,
} from "./build_plan";
import { fakeSource } from "./materialization_test_fixtures";

describe("flattenDependsOn", () => {
   it("maps nested dependsOn entries to a flat sourceID list", () => {
      expect(
         flattenDependsOn({
            dependsOn: [{ sourceID: "a" }, { sourceID: "b" }],
         }),
      ).toEqual(["a", "b"]);
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

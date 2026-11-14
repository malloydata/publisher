// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { makeDigest, type PersistSource } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   collectModelBuildPlan,
   computeSourceEntityId,
   deriveAnnotationFields,
   deriveBuildPlan,
   emptyBuildPlanParts,
   plannedSources,
   projectToPublicColumns,
   resolveFreshness,
   resolveQueryMetadata,
   resolvePackageConnections,
   sourceBuildsOf,
   sourcesInBuildOrder,
   type SourceBuild,
} from "./materialization/plan/build_plan";
import { computePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import { MaterializationEligibilityError } from "../errors";
import {
   compilePersistSources,
   duckdbTestConnections,
   loadTestRuntime,
   tempBuildPlanPackages,
} from "../test_helpers/incremental_test_harness";
import { fakeSource } from "../test_helpers/materialization_test_fixtures";
import { installSettings } from "../test_helpers/settings_env";

describe("source builds from getBuildTargets", () => {
   // a <- b <- c, plus `a_ext`, which extends `a` without changing its SQL and
   // so shares its table.
   const CHAIN = `##! experimental.persistence
source: base is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
#@ persist
source: a is base -> { group_by: category; aggregate: total is amount.sum() }
source: a_ext is a extend { dimension: label is concat('x', category) }
#@ persist
source: b is a -> { group_by: category; aggregate: t2 is total.sum() }
#@ persist
source: c is b -> { group_by: category; aggregate: t3 is t2.sum() }
`;

   async function buildsOf(model: string) {
      const { connections } = duckdbTestConnections();
      const { runtime, materializer } = loadTestRuntime(connections, model);
      const targets = await runtime.getBuildTargets(
         await materializer.getModel(),
      );
      return { builds: sourceBuildsOf(targets), targets };
   }

   const name = (sourceID: string) => sourceID.split("@")[0];

   it("orders every source after the tables it reads", async () => {
      const { builds } = await buildsOf(CHAIN);
      expect(builds).toHaveLength(1);
      const order = builds[0].order.map(name);
      expect(order.sort()).toEqual(["a", "a_ext", "b", "c"].sort());
      const at = (n: string) => builds[0].order.map(name).indexOf(n);
      expect(at("a")).toBeLessThan(at("b"));
      expect(at("a_ext")).toBeLessThan(at("b"));
      expect(at("b")).toBeLessThan(at("c"));
   });

   it("gives every source every table it reads, and an extension none", async () => {
      // The wire graphs are a host's only view of these edges, for a rerun's
      // upstream closure, freshness inheritance and lineage. `b -> a` sits
      // below the first level, where a walk of roots alone loses it.
      const { builds } = await buildsOf(CHAIN);
      const deps = Object.fromEntries(
         Object.entries(builds[0].dependsOn).map(([id, on]) => [
            name(id),
            on.map(name).sort(),
         ]),
      );
      expect(deps).toEqual({
         a: [],
         a_ext: [],
         b: ["a", "a_ext"],
         c: ["b"],
      });
   });

   it("projects those edges into the wire graphs", async () => {
      const { builds, targets } = await buildsOf(CHAIN);
      const sources = Object.fromEntries(
         plannedSources(targets).map((src) => [src.sourceID, src]),
      );
      const plan = deriveBuildPlan(builds, sources, { duckdb: "dig" });
      const edges = plan.graphs.flatMap((g) =>
         g.nodes
            .flat()
            .flatMap((n) =>
               (n.dependsOn ?? []).map((d) => `${name(d)}>${name(n.sourceID)}`),
            ),
      );
      expect(edges.sort()).toEqual(["a>b", "a_ext>b", "b>c"]);
   });

   it("builds a dependency two sources share once, before both", async () => {
      const { builds } = await buildsOf(`##! experimental.persistence
source: base is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
#@ persist
source: shared is base -> { group_by: category; aggregate: total is amount.sum() }
#@ persist
source: r1 is shared -> { group_by: category; aggregate: t1 is total.sum() }
#@ persist
source: r2 is shared -> { group_by: category; aggregate: t2 is total.max() }
`);
      const order = builds[0].order.map(name);
      expect(order.filter((n) => n === "shared")).toHaveLength(1);
      expect(order[0]).toBe("shared");
      expect(order.slice(1).sort()).toEqual(["r1", "r2"]);
      const deps = Object.fromEntries(
         Object.entries(builds[0].dependsOn).map(([id, on]) => [
            name(id),
            on.map(name),
         ]),
      );
      expect(deps).toEqual({ shared: [], r1: ["shared"], r2: ["shared"] });
   });

   it("puts a source that cannot compile last, with no table to read", async () => {
      const { builds, targets } =
         await buildsOf(`##! experimental { persistence givens }
given: TENANT :: string
source: base is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
#@ persist
source: scoped is base -> { where: category = $TENANT; group_by: category }
#@ persist
source: open is base -> { group_by: category }
`);
      expect(targets.unbuildable.map((u) => u.source.name)).toEqual(["scoped"]);
      expect(builds[0].order.map(name)).toEqual(["open", "scoped"]);
      expect(builds[0].dependsOn[builds[0].order[1]]).toEqual([]);
   });

   it("walks only the sources a plan holds", () => {
      const a = fakeSource({ name: "a", sourceEntityId: "ba" });
      const b = fakeSource({ name: "b", sourceEntityId: "bb" });
      const build: SourceBuild = {
         connectionName: "duckdb",
         order: ["a@m", "missing@m", "b@m"],
         dependsOn: {},
      };
      expect(
         [...sourcesInBuildOrder(build, { "a@m": a, "b@m": b })].map(
            (src) => src.name,
         ),
      ).toEqual(["a", "b"]);
   });
});

describe("detectDroppedPersistSources under getBuildTargets", () => {
   it("still reports a dropped source whose #@ persist line carries an @env. reference", async () => {
      // Malloy decides `persist` over every line; the backstop must agree, or
      // the annotation becomes a silent no-op.
      const { duckdb, connections } = duckdbTestConnections();
      await duckdb.runSQL("CREATE OR REPLACE TABLE t_env AS SELECT 1 AS a");
      const { runtime, materializer } = loadTestRuntime(
         connections,
         `##! experimental.persistence
#@ persist name=@env.PUBLISHER_SPEC_UNSET_VAR
source: filtered is duckdb.table('t_env') extend { where: a > 0 }
`,
      );
      const parts = emptyBuildPlanParts();
      await collectModelBuildPlan(parts, {
         modelPath: "m.malloy",
         packagePath: "/test",
         materializer,
         malloyModel: await materializer.getModel(),
         runtime,
         getRuntime: () => {
            throw new Error("no pre-aggregation in this model");
         },
      });
      expect(parts.droppedPersistSources).toEqual([
         { name: "filtered", modelPath: "m.malloy" },
      ]);
   });

   it("reports a persisted filtered pass-through of a table, which the walk does not plan", async () => {
      const { duckdb, connections } = duckdbTestConnections();
      await duckdb.runSQL("CREATE TABLE t AS SELECT 1 AS a");
      const { runtime, materializer } = loadTestRuntime(
         connections,
         `##! experimental.persistence
#@ persist
source: filtered is duckdb.table('t') extend { where: a > 0 }
#@ persist
source: rolled is duckdb.table('t') -> { group_by: a }
`,
      );
      const parts = emptyBuildPlanParts();
      await collectModelBuildPlan(parts, {
         modelPath: "m.malloy",
         packagePath: "/test",
         materializer,
         malloyModel: await materializer.getModel(),
         runtime,
         getRuntime: () => {
            throw new Error("no pre-aggregation in this model");
         },
      });

      expect(Object.values(parts.sources).map((src) => src.name)).toEqual([
         "rolled",
      ]);
      expect(parts.droppedPersistSources).toEqual([
         { name: "filtered", modelPath: "m.malloy" },
      ]);
   });
});

describe("deriveAnnotationFields", () => {
   it("returns all key=value fields of the #@ persist annotation", () => {
      const source = {
         annotations: {
            forRoute: () => [],
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
            forRoute: () => [],
            parseAsTag: () => {
               throw new Error("no @ annotation");
            },
         },
      } as unknown as PersistSource;

      expect(deriveAnnotationFields(source)).toEqual({});
   });
});

describe("#@ annotations never read the server's environment", () => {
   // The tag parser hydrates `@env.NAME` from `process.env`, and both of these
   // reach a client: `annotationFields` on the build plan, and the metadata
   // attached to every build statement.
   const SECRET_VAR = "PUBLISHER_SPEC_ENV_SECRET";
   const SECRET = "s3cr3t-from-the-server-environment";
   const model = `##! experimental.persistence
source: raw is duckdb.sql("""SELECT 1 AS x""")
#@ persist name=@env.${SECRET_VAR}
source: leaky_name is raw -> { select: * }
#@ persist name="kept"
#@ queryMetadata { owner=@env.${SECRET_VAR} }
source: leaky_metadata is raw -> { select: * }
`;

   afterEach(() => {
      delete process.env[SECRET_VAR];
   });

   it("drops the #@ route of a source with an @env. line, and the source still plans", async () => {
      process.env[SECRET_VAR] = SECRET;
      const { connections } = duckdbTestConnections();
      const { sources } = await compilePersistSources(connections, model);

      // Malloy still plans both: it reads only whether `persist` is present.
      expect(Object.keys(sources).sort()).toEqual([
         "leaky_metadata",
         "leaky_name",
      ]);
      expect(deriveAnnotationFields(sources.leaky_name)).toEqual({});
      // One `@env.` line drops the source's whole `#@` route, `name=` included.
      expect(deriveAnnotationFields(sources.leaky_metadata)).toEqual({});
      for (const source of Object.values(sources)) {
         const read = JSON.stringify([
            deriveAnnotationFields(source),
            resolveQueryMetadata(source, null),
            resolveFreshness(source, null),
         ]);
         expect(read).not.toContain(SECRET);
      }
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
   it("hashes the connection digest with its build target's SQL, as the compiler keys a BuildID", () => {
      const source = { connectionName: "duckdb" } as unknown as PersistSource;
      plannedSources({
         connections: [
            {
               connectionName: "duckdb",
               targets: [{ sql: "SELECT 7", sources: [source] }],
            },
         ],
         unbuildable: [],
      } as unknown as Parameters<typeof plannedSources>[0]);

      expect(computeSourceEntityId(source, { duckdb: "dig-1" })).toBe(
         makeDigest("dig-1", "SELECT 7"),
      );
      // The digest is the connection's: the same SQL elsewhere is another
      // table.
      expect(computeSourceEntityId(source, { duckdb: "dig-2" })).not.toBe(
         computeSourceEntityId(source, { duckdb: "dig-1" }),
      );
   });

   it("refuses a source that maps onto no build target", () => {
      const source = {
         name: "loose",
         connectionName: "duckdb",
      } as unknown as PersistSource;
      expect(() => computeSourceEntityId(source, { duckdb: "dig-1" })).toThrow(
         /no build target/,
      );
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
   it("reports a join to a given-scoped persisted source as joinedTerms, apart from strippedTerms", async () => {
      const { restore } = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      });
      try {
         const { connections } = duckdbTestConnections();
         const { sources } = await compilePersistSources(
            connections,
            `##! experimental { persistence givens }
given:
  ORG_ID :: number is 1
  USER_ID :: number is 7
source: raw is duckdb.sql("""SELECT * FROM (VALUES (1,10)) AS t(org_id, opp_id)""")
source: grants_raw is duckdb.sql("""SELECT * FROM (VALUES (1,7,10)) AS g(org_id, user_id, opp_id)""")
#@ persist name="grants" storage=lake
source: grants is grants_raw -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}
#@ persist name="opps" storage=lake
source: opps is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is g.opp_id is not null
}`,
         );
         const bySourceID = Object.fromEntries(
            Object.values(sources).map((src) => [src.sourceID, src]),
         );
         const plan = deriveBuildPlan(
            [
               {
                  connectionName: "duckdb",
                  order: Object.keys(bySourceID),
                  dependsOn: {},
               },
            ],
            bySourceID,
            { duckdb: "dig" },
         );
         const opps = Object.values(plan.sources).find(
            (p) => p.name === "opps",
         );
         expect(opps?.strippedTerms).toEqual([
            { code: "org_id = $ORG_ID", givens: ["ORG_ID"] },
         ]);
         expect(opps?.joinedTerms).toEqual([
            {
               alias: "g",
               source: "grants",
               terms: [
                  {
                     code: "org_id = $ORG_ID and user_id = $USER_ID",
                     givens: ["ORG_ID", "USER_ID"],
                  },
               ],
            },
         ]);
         const grants = Object.values(plan.sources).find(
            (p) => p.name === "grants",
         );
         expect(grants?.joinedTerms).toBeUndefined();
      } finally {
         restore();
      }
   });

   it("projects builds and sources into the wire build plan", () => {
      const orders = fakeSource({
         name: "orders",
         sourceEntityId: "bid-orders",
         sql: "SELECT 1",
      });
      const plan = deriveBuildPlan(
         [{ connectionName: "duckdb", order: ["orders@m"], dependsOn: {} }],
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
               order: ["declared@m", "unset@m"],
               dependsOn: {},
            },
         ],
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
               order: ["a@m"],
               dependsOn: {},
            },
         ],
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
               order: ["a@m"],
               dependsOn: {},
            },
         ],
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
               order: ["s@m"],
               dependsOn: {},
            },
         ],
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

// `BuildPlan.refusedSources`: a SEPARATE collection from `PersistSourcePlan`,
// computed from the tier-appropriate assert post-relaxation (see
// `deriveBuildPlan`'s doc). Real-compiler coverage, like the describe blocks
// above, because the whole point is the exact IR shape (getSQL() actually
// throwing for a given with no default) rather than a stubbed approximation.
describe("BuildPlan.refusedSources", () => {
   const packages = tempBuildPlanPackages();
   afterEach(() => packages.cleanup());

   it(
      "refuses a source whose #@ route reads the server's environment, instead of building it into the warehouse",
      async () => {
         // Publisher drops that source's whole `#@` route, so it reads no
         // `storage=` while Malloy still plans the source. Built, it would land
         // in the source warehouse; refused, it serves live.
         const pkg = await packages.create(`##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount")

#@ persist name="ok"
source: ok is base -> { aggregate: c is count() }

#@ persist name="leaky" storage="dest"
#@ queryMetadata { owner=@env.PUBLISHER_SPEC_UNSET_VAR }
source: leaky is base -> { aggregate: c is count() }
`);

         const { plan } = await computePackageBuildPlan(pkg);

         expect(Object.values(plan?.sources ?? {}).map((s) => s.name)).toEqual([
            "ok",
         ]);
         const [refused] = Object.values(plan?.refusedSources ?? {});
         expect(refused).toMatchObject({
            name: "leaky",
            reason: "env_reference",
         });
         expect(refused.message).toMatch(/@env\./);
      },
      { timeout: 20000 },
   );

   it(
      "reports a package where EVERY persist source is refused as a completed plan carrying refusedSources, not a thrown error",
      async () => {
         // Both sources are storage-tier (declare `storage=`) and both are
         // ineligible: `mz_given` references a given, `mz_free` declares an
         // unbound parameter. Classified from the compiled source's IR alone
         // (Parameter.value/refSummary.givenUsage), independent of whether
         // `getSQL()` itself would succeed for this particular shape — see
         // the next test for the shape where it demonstrably does not.
         const pkg = await packages.create(`##! experimental.persistence
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
            reason: "given_in_persisted_query",
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
         const pkg = await packages.create(`##! experimental.persistence
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
            reason: "given_in_persisted_query",
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
         // `#(access_filter)` gate classifies row_level + attributed, so the
         // colocated relaxation admits it. The OLD `SourceEligibility.refused`
         // (computed with the unconditional storage-tier assert) would report
         // this same source as `refused: authorize` — refusedSources must not
         // repeat that mistake now that it can actually build.
         const pkg = await packages.create(`##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: base is duckdb.sql("select 1 as org_id")

#(access_filter) org_id = $ORG
#@ persist name="gated"
source: gated is base -> { select: org_id } extend {}
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
      "plans two models' rollups of a same-named source as two tables, so neither collides",
      async () => {
         // Under the v1 cutover a `#@ preaggregate` line is the rollup leg's
         // declaration: each rollup is named for its base, grain and declaring
         // model, and planned in its declaring model's companion, so
         // `orders` pre-aggregated by `category` in two models with different
         // contents is two names and two addresses. The collision the
         // composite's model-blind name made (and refused) cannot happen.
         const rollup = (
            amount: string,
         ) => `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT * FROM (VALUES (${amount}, 'A')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;
         const pkg = await packages.create({
            "a.malloy": rollup("10"),
            "b.malloy": rollup("99"),
         });

         const { plan } = await computePackageBuildPlan(pkg);

         const rollups = Object.values(plan?.sources ?? {}).filter(
            (s) => s.origin === "preaggregate",
         );
         expect(rollups.map((r) => r.modelPath).sort()).toEqual([
            "a.malloy",
            "b.malloy",
         ]);
         expect(new Set(rollups.map((r) => r.name)).size).toBe(2);
         expect(new Set(rollups.map((r) => r.sourceEntityId)).size).toBe(2);
         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
      },
      { timeout: 20000 },
   );

   it(
      "plans one rollup for a source index.malloy re-exports, and refuses neither (#7096)",
      async () => {
         // The recommended curation: the source's own file declares the
         // rollup, and `index.malloy` imports and re-exports the source. The
         // rollup leg plans a rollup only in the model that declares its
         // base, so it is listed once, under that model.
         const pkg = await packages.create({
            "jaffle.malloy": `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT * FROM (VALUES (10, 'A'), (5, 'B')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
            "index.malloy": `import "jaffle.malloy"
export { orders }
`,
         });

         const { plan } = await computePackageBuildPlan(pkg);

         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
         const rollups = Object.values(plan?.sources ?? {}).filter(
            (s) => s.origin === "preaggregate",
         );
         expect(rollups.map((r) => r.modelPath)).toEqual(["jaffle.malloy"]);
      },
      { timeout: 20000 },
   );

   it(
      "plans a renamed import's rollup as the declaring model's, so it is one table",
      async () => {
         const pkg = await packages.create({
            "jaffle.malloy": `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
            "report.malloy": `import { o is orders } from "jaffle.malloy"
source: notes is duckdb.sql("SELECT 1 AS id")
export { notes }
`,
         });

         const { plan } = await computePackageBuildPlan(pkg);

         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
         const rollups = Object.values(plan?.sources ?? {}).filter(
            (s) => s.origin === "preaggregate",
         );
         // Once, under the declaring model, and named for `orders`, not the
         // alias `o`.
         expect(rollups.map((r) => r.modelPath)).toEqual(["jaffle.malloy"]);
         expect(rollups[0].name).toMatch(/^orders__rollup__category__/);
      },
      { timeout: 20000 },
   );

   it(
      "does not let an import aliased to another model's source name collide with that model's rollup",
      async () => {
         const rollupOver = (
            table: string,
         ) => `##! experimental { persistence composite_sources }
source: SRC is duckdb.sql("""SELECT * FROM (VALUES (${table}, 'A')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;
         const pkg = await packages.create({
            "a.malloy": rollupOver("10").replace("SRC", "orders"),
            "crm.malloy": rollupOver("99").replace("SRC", "customers"),
            // `orders` here is crm's `customers`, not a.malloy's `orders`.
            "report.malloy": `import { orders is customers } from "crm.malloy"
source: notes is duckdb.sql("SELECT 1 AS id")
export { notes }
`,
         });

         const { plan } = await computePackageBuildPlan(pkg);

         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
      },
      { timeout: 20000 },
   );

   it(
      "plans a storage rollup over a storage= base, which the rollup leg builds over the stored base (#6293)",
      async () => {
         // The composite could neither build nor serve this, so it was left
         // out with a warning. The rollup leg reads the line as its own
         // declaration and plans it over the stored base (the real fix for
         // #6293, phase 4), so nothing is left out.
         const pkg =
            await packages.create(`##! experimental { persistence composite_sources }
#@ persist storage="lake"
source: orders is duckdb.sql("""SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)""") -> { select: * } extend {
  #@ preaggregate grain="category" storage="lake"
  measure: total is amount.sum()
}
`);

         const { plan, rollupsOverStoredBase } =
            await computePackageBuildPlan(pkg);

         const entries = Object.values(plan?.sources ?? {});
         expect(
            entries.filter((s) => s.origin === "preaggregate"),
         ).toHaveLength(1);
         expect(entries.map((s) => s.name)).toContain("orders");
         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
         expect(rollupsOverStoredBase ?? []).toEqual([]);
         // Every graph node is planned or refused.
         const nodes = (plan?.graphs ?? []).flatMap((g) =>
            g.nodes.flat().map((n) => n.sourceID),
         );
         for (const id of nodes) {
            expect(Object.keys(plan?.sources ?? {})).toContain(id);
         }
      },
      { timeout: 20000 },
   );

   it(
      "plans both models' storage rollups, one over a stored base, with no collision",
      async () => {
         const pkg = await packages.create({
            "a.malloy": `##! experimental { persistence composite_sources }
#@ persist storage="lake"
source: orders is duckdb.sql("""SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)""") -> { select: * } extend {
  #@ preaggregate grain="category" storage="lake"
  measure: total is amount.sum()
}
`,
            "b.malloy": `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT * FROM (VALUES (99, 'A')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category" storage="lake"
  measure: total is amount.sum()
}
`,
         });

         const { plan } = await computePackageBuildPlan(pkg);

         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
         const rollups = Object.values(plan?.sources ?? {}).filter(
            (s) => s.origin === "preaggregate",
         );
         // Each model's rollup is its own table: a.malloy's over its stored
         // base, b.malloy's over the warehouse.
         expect(rollups.map((r) => r.modelPath).sort()).toEqual([
            "a.malloy",
            "b.malloy",
         ]);
         expect(new Set(rollups.map((r) => r.name)).size).toBe(2);
      },
      { timeout: 20000 },
   );

   it(
      "still plans a storage rollup over a base that is not storage-persisted",
      async () => {
         const pkg =
            await packages.create(`##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category" storage="lake"
  measure: total is amount.sum()
}
`);
         const { plan } = await computePackageBuildPlan(pkg);
         expect(
            Object.values(plan?.sources ?? {}).filter(
               (s) => s.origin === "preaggregate",
            ),
         ).toHaveLength(1);
      },
      { timeout: 20000 },
   );

   it(
      "still plans a rollup that only one model generates",
      async () => {
         const pkg = await packages.create({
            "a.malloy": `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
            "b.malloy": `source: other is duckdb.sql("SELECT 1 AS x")`,
         });

         const { plan } = await computePackageBuildPlan(pkg);

         expect(
            Object.values(plan?.sources ?? {}).filter(
               (s) => s.origin === "preaggregate",
            ),
         ).toHaveLength(1);
         expect(plan?.refusedSources ?? {}).toEqual({});
      },
      { timeout: 20000 },
   );

   it(
      "plans no rollup over a gated base, and says why in a finding rather than a refusal",
      async () => {
         // Rollups group away the gate column. The composite synthesized the
         // rollup anyway and then refused it (tier preaggregate). The rollup
         // leg never plans it: neither a source nor a refusal, and the
         // package's finding says the measure is served live.
         const pkg =
            await packages.create(`##! experimental { persistence composite_sources givens }

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
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

         const { plan, annotationFindings } =
            await computePackageBuildPlan(pkg);

         // Nothing planned, so no plan at all.
         expect(Object.values(plan?.sources ?? {})).toEqual([]);
         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
         expect(annotationFindings.map((f) => f.code)).toContain(
            "preaggregate-access-filter",
         );
      },
      { timeout: 20000 },
   );
});

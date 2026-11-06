// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package resource and what moved off it, over HTTP against the real
 * Express app and the real worker-pool load:
 *
 * - `GET …/entities`, `GET …/build-plan`, `GET …/classification` and
 *   `GET …/embedding-index` read the loaded package.
 * - `GET …/status` reads the package's serving state without loading it.
 * - With `PUBLISHER_PACKAGE_INLINE_FIELDS=off` a package response carries only
 *   the package's own fields, and its `GET /status` entry only what a reconcile
 *   reads. With it on (the default) the moved fields are still inline.
 * - A package whose `#(index)` or required `#(filter)` an index cannot honor
 *   loads, with a coded warning the compile dry run reports too.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../../src/api";
import { resetPackageLoadMetricsForTesting } from "../../../src/package_load_metrics";
import { startMetricsHarness } from "../../../src/test_helpers/metrics_harness";
import { type ResourcesEnv, startResourcesEnv } from "./resources_harness";

type ModelEntities = components["schemas"]["ModelEntities"];
type ApiPackage = components["schemas"]["Package"];

const ENV_NAME = "package-resources-env";
const INLINE = "PUBLISHER_PACKAGE_INLINE_FIELDS";
const savedInline = process.env[INLINE];

const ROWS = `duckdb.sql("select * from (values (1, 'web', 'us'), (2, 'store', 'eu')) as t(id, channel, region)")`;

const ORDERS = `##! experimental.givens

#(secure)
given: ROLES :: string[]

#(doc) Orders in every channel.
#(filter) name=channel dimension=channel type=equal required
source: orders is ${ROWS} extend {
  #(index)
  dimension: region_label is concat('r-', region)
  measure: order_count is count()
}
`;

const HELPER = `source: helper is ${ROWS}
`;

/** Fields that moved off Package to a route. */
const MOVED = [
   "buildPlan",
   "agents",
   "secureGivens",
   "embeddingIndex",
   "classification",
   "status",
   "loaded",
   "archiveStatus",
   "servedRevision",
   "sourceContentSha",
   "manifestBindingStatus",
   "manifestEntryCount",
   "boundManifestUri",
   "storageServeBindings",
] as const;

/** What a slim `GET /status` entry may carry (PackageStatusEntry). */
const STATUS_ENTRY_FIELDS = new Set([
   "resource",
   "name",
   "versionId",
   "latestVersion",
   "loaded",
   "archiveStatus",
   "status",
   "manifestBindingStatus",
   "boundManifestUri",
]);

describe("the package resource and its sub-resources", () => {
   let r: ResourcesEnv;
   const get = (rel: string) => r.get(rel);
   const publish = (name: string, location: string) =>
      r.publish(name, location);
   const writePackage = (
      name: string,
      files: Record<string, string>,
      manifest?: Record<string, unknown>,
   ) => r.writePackage(name, files, manifest);
   const pkgUrl = (name: string, sub = "") => r.pkgUrl(name, sub);

   async function statusEntry(name: string): Promise<Record<string, unknown>> {
      const [entry] = await r.statusEntries(name);
      expect(entry).toBeDefined();
      return entry!;
   }

   beforeAll(async () => {
      r = await startResourcesEnv(ENV_NAME);
      const location = writePackage(
         "shop",
         { "orders.malloy": ORDERS, "helper.malloy": HELPER },
         { explores: ["orders.malloy"] },
      );
      expect((await publish("shop", location)).status).toBe(200);
   }, 180_000);

   afterEach(() => {
      if (savedInline === undefined) delete process.env[INLINE];
      else process.env[INLINE] = savedInline;
   });

   afterAll(() => r.stop());

   it("GET …/entities reports the surface by default, and every model with includeHiddenFilesAndSources", async () => {
      const surface = await get(pkgUrl("shop", "/entities"));
      expect(surface.status).toBe(200);
      expect(
         (surface.json as unknown as ModelEntities[]).map((m) => m.modelPath),
      ).toEqual(["orders.malloy"]);

      const { status, json } = await get(
         pkgUrl("shop", "/entities?includeHiddenFilesAndSources=true"),
      );
      expect(status).toBe(200);
      const models = json as unknown as ModelEntities[];
      expect(models.map((m) => m.modelPath)).toEqual([
         "helper.malloy",
         "orders.malloy",
      ]);
      const orders = models.find((m) => m.modelPath === "orders.malloy")!;
      expect(orders.error).toBeUndefined();
      expect(orders.givens?.map((g) => [g.name, g.type, g.secure])).toEqual([
         ["ROLES", "string[]", true],
      ]);
      const source = orders.sources!.find((s) => s.name === "orders")!;
      expect(source.queryable).toBe(true);
      expect(source.doc).toBe("Orders in every channel.");
      expect(source.filters?.map((f) => [f.name, f.required])).toEqual([
         ["channel", true],
      ]);
      const regionLabel = orders.fields!.find(
         (f) => f.name === "region_label",
      )!;
      expect(regionLabel.kind).toBe("dimension");
      expect(regionLabel.indexFingerprint?.sqlHash).toMatch(/^[0-9a-f]{64}$/);
      expect(regionLabel.indexFingerprint?.requiredFilterDimensions).toEqual([
         "channel",
      ]);
      const helper = models.find((m) => m.modelPath === "helper.malloy")!;
      expect(helper.sources!.find((s) => s.name === "helper")!.queryable).toBe(
         false,
      );
      // The whole response, as a load computes it from the rebuilt models.
      expect(models).toMatchSnapshot();
   }, 60_000);

   it("GET …/entities withholds a locked source unless includeHiddenFilesAndSources is set", async () => {
      const location = writePackage("lockbox", {
         "index.malloy": `#(authorize) false
source: vault is ${ROWS}
source: lobby is ${ROWS}
`,
      });
      expect((await publish("lockbox", location)).status).toBe(200);
      const sourcesOf = async (query: string) => {
         const models = (await get(pkgUrl("lockbox", `/entities${query}`)))
            .json as unknown as ModelEntities[];
         return {
            sources: models.flatMap((m) => m.sources ?? []).map((s) => s.name),
            fieldSources: [
               ...new Set(
                  models.flatMap((m) => m.fields ?? []).map((f) => f.source),
               ),
            ],
         };
      };
      expect(await sourcesOf("")).toEqual({
         sources: ["lobby"],
         fieldSources: ["lobby"],
      });
      expect(
         (await sourcesOf("?includeHiddenFilesAndSources=true")).sources.sort(),
      ).toEqual(["lobby", "vault"]);
   }, 60_000);

   it("GET …/entities limits a surface file to its export closure, and withholds a FALSE-locked source and its nested joins", async () => {
      const location = writePackage(
         "closure",
         {
            "helper.malloy": `source: helper is ${ROWS}\n`,
            "main.malloy": `import "helper.malloy"
#(authorize) FALSE
source: vault is ${ROWS} extend {
  join_one: lining is ${ROWS} extend { join_one: deeper is ${ROWS} on id = deeper.id } on id = lining.id
}
source: shown is ${ROWS} extend {
  join_one: helper on id = helper.id
}
`,
         },
         { explores: ["main.malloy"] },
      );
      expect((await publish("closure", location)).status).toBe(200);
      const models = (await get(pkgUrl("closure", "/entities")))
         .json as unknown as ModelEntities[];
      expect(models.map((m) => m.modelPath)).toEqual(["main.malloy"]);
      const roots = (names: string[]) =>
         [...new Set(names.map((n) => n.split(".")[0]))].sort();
      const main = models[0]!;
      expect(roots((main.sources ?? []).map((s) => s.name))).toEqual(["shown"]);
      expect(roots((main.fields ?? []).map((f) => f.source))).toEqual([
         "shown",
      ]);
      expect(roots((main.sourceSchemas ?? []).map((s) => s.name))).toEqual([
         "shown",
      ]);

      const all = (
         await get(
            pkgUrl("closure", "/entities?includeHiddenFilesAndSources=true"),
         )
      ).json as unknown as ModelEntities[];
      const allMain = all.find((m) => m.modelPath === "main.malloy")!;
      expect(roots((allMain.sources ?? []).map((s) => s.name))).toEqual(
         expect.arrayContaining(["helper", "shown", "vault"]),
      );
   }, 60_000);

   it("GET …/build-plan and GET …/embedding-index read the loaded package", async () => {
      const plan = await get(pkgUrl("shop", "/build-plan"));
      expect(plan.status).toBe(200);
      // The package declares no persist source: an empty plan.
      expect(plan.json).toEqual({ graphs: [], sources: {} });

      const index = await get(pkgUrl("shop", "/embedding-index"));
      expect(index.status).toBe(200);
      expect(index.json.status).toBeString();

      expect((await get(pkgUrl("nope", "/build-plan"))).status).toBe(404);
   }, 60_000);

   it("GET …/classification reads the loaded package, and 404s for an unknown one", async () => {
      const { status, json } = await get(pkgUrl("shop", "/classification"));
      expect(status).toBe(200);
      // Nothing in the package is classified.
      expect(json).toEqual({ sources: [] });
      expect((await get(pkgUrl("nope", "/classification"))).status).toBe(404);
   }, 60_000);

   it("GET …/status reports the serving state, and 404s for an unknown package", async () => {
      const { status, json } = await get(pkgUrl("shop", "/status"));
      expect(status).toBe(200);
      expect(json.name).toBe("shop");
      expect(json.resource).toBe(`/api/v0${pkgUrl("shop")}`);
      expect(json.status).toMatchObject({ serving: true, loading: false });
      expect(json.servedRevision).toBeString();
      expect(json.sourceContentSha).toBeString();
      expect(json.manifestBindingStatus).toBe("unbound");
      expect(json.manifestEntryCount).toBe(0);
      // A serving state, never the package's own fields.
      expect("warnings" in json).toBe(false);
      expect("description" in json).toBe(false);
      expect((await get(pkgUrl("nope", "/status"))).status).toBe(404);
   }, 60_000);

   it("each response's field set, with inline fields off and on", async () => {
      const shapes: Record<string, string[]> = {};
      for (const mode of ["off", "on"] as const) {
         process.env[INLINE] = mode;
         const pkg = (await get(pkgUrl("shop"))).json;
         const listed = (
            (await get(`/environments/${ENV_NAME}/packages`))
               .json as unknown as Record<string, unknown>[]
         ).find((p) => p.name === "shop")!;
         shapes[`${mode}: package`] = Object.keys(pkg).sort();
         shapes[`${mode}: listing`] = Object.keys(listed).sort();
         shapes[`${mode}: /status entry`] = Object.keys(
            await statusEntry("shop"),
         ).sort();
         shapes[`${mode}: …/status`] = Object.keys(
            (await get(pkgUrl("shop", "/status"))).json,
         ).sort();
      }
      delete process.env[INLINE];
      expect(shapes).toMatchSnapshot();
   }, 60_000);

   it("with inline fields off, the package carries only its own fields and the /status entry only what a reconcile reads", async () => {
      process.env[INLINE] = "off";
      const { status, json } = await get(pkgUrl("shop"));
      expect(status).toBe(200);
      const pkg = json as ApiPackage;
      expect(pkg.name).toBe("shop");
      for (const field of MOVED) expect(field in pkg).toBe(false);
      expect("entities" in pkg).toBe(false);

      const listed = (await get(`/environments/${ENV_NAME}/packages`))
         .json as unknown as ApiPackage[];
      const shop = listed.find((p) => p.name === "shop")!;
      for (const field of MOVED) expect(field in shop).toBe(false);

      const entry = await statusEntry("shop");
      expect(entry.status).toMatchObject({ serving: true });
      expect(entry.manifestBindingStatus).toBe("unbound");
      for (const field of Object.keys(entry)) {
         expect(STATUS_ENTRY_FIELDS.has(field)).toBe(true);
      }
   }, 60_000);

   it("with inline fields on (the default), the moved fields are still on the package and its /status entry", async () => {
      delete process.env[INLINE];
      const entry = await statusEntry("shop");
      expect(entry.servedRevision).toBeString();
      expect(entry.sourceContentSha).toBeString();
      // The package's own fields too, as before: more than a slim entry.
      expect(
         Object.keys(entry).some((field) => !STATUS_ENTRY_FIELDS.has(field)),
      ).toBe(true);
      const pkg = (await get(pkgUrl("shop"))).json as ApiPackage;
      expect(pkg.status).toMatchObject({ serving: true });
      expect(pkg.servedRevision).toBeString();
      expect(pkg.manifestBindingStatus).toBeString();
      expect("buildPlan" in pkg).toBe(true);
      expect(pkg.secureGivens?.map((g) => g.name)).toEqual(["ROLES"]);
      expect(pkg.embeddingIndex?.status).toBeString();
      // Never inline: entities has only its own route.
      expect("entities" in pkg).toBe(false);
      // Absent when nothing published is classified, as in 0.12.0.
      expect("classification" in pkg).toBe(false);
   }, 60_000);

   it("a classified package's summary is at GET …/classification, and inline on the package read only while inline fields are on", async () => {
      const location = writePackage("labeled", {
         "index.malloy": `source: people is ${ROWS} extend {
  #(classification) "pii"
  dimension: channel_label is concat('c-', channel)
  #(classification) "none"
  measure: people_count is count()
}
`,
      });
      expect((await publish("labeled", location)).status).toBe(200);
      const summary = (await get(pkgUrl("labeled", "/classification"))).json;
      expect((summary.sources as unknown[]).length).toBeGreaterThan(0);

      delete process.env[INLINE];
      expect((await get(pkgUrl("labeled"))).json.classification).toEqual(
         summary,
      );
      const listed = (
         (await get(`/environments/${ENV_NAME}/packages`))
            .json as unknown as Record<string, unknown>[]
      ).find((p) => p.name === "labeled")!;
      expect("classification" in listed).toBe(false);

      process.env[INLINE] = "off";
      expect("classification" in (await get(pkgUrl("labeled"))).json).toBe(
         false,
      );
   }, 60_000);

   it("a package with an #(index) on a measure loads, with a coded warning the compile dry run reports too", async () => {
      const harness = await startMetricsHarness();
      resetPackageLoadMetricsForTesting();
      try {
         const location = writePackage("measure_index", {
            "bad.malloy": `source: bad is ${ROWS} extend {
  #(index)
  measure: n is count()
}
`,
         });
         expect((await publish("measure_index", location)).status).toBe(200);
         const pkg = (await get(pkgUrl("measure_index"))).json as ApiPackage;
         const warning = pkg.warnings?.find(
            (w) => w.code === "entities-not-a-dimension",
         );
         expect(warning).toMatchObject({
            model: "bad.malloy",
            severity: "warn",
         });
         expect(warning?.message).toContain("is not a dimension");
         expect(
            await harness.collectCounter(
               "publisher_package_entities_findings_total",
               { reason: "not_a_dimension" },
            ),
         ).toBe(1);
         const phases = (status: string) =>
            harness.collectHistogram("malloy_package_load_compile_duration", {
               status,
            });
         expect((await phases("success")).count).toBe(1);

         const dryRun = await r.post(
            pkgUrl("measure_index", "/models/bad.malloy/compile"),
            { scope: "package" },
         );
         expect(JSON.stringify(dryRun.json)).toContain(
            "entities-not-a-dimension",
         );
      } finally {
         resetPackageLoadMetricsForTesting();
         await harness.shutdown();
      }
   }, 120_000);

   it("a package with two required filters on one dimension (a range) loads with no warning", async () => {
      const location = writePackage("range_filter", {
         "range.malloy": `#(filter) name=min dimension=id type=greater_than required
#(filter) name=max dimension=id type=less_than required
source: ranged is ${ROWS}
`,
      });
      expect((await publish("range_filter", location)).status).toBe(200);
      const pkg = (await get(pkgUrl("range_filter"))).json as ApiPackage;
      expect(
         (pkg.warnings ?? []).filter((w) => w.code?.startsWith("entities-")),
      ).toEqual([]);
   }, 120_000);
});

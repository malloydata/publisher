// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package resource and what moved off it, over HTTP against the real
 * Express app and the real worker-pool load:
 *
 * - `GET …/entities`, `GET …/build-plan` and `GET …/embedding-index` read the
 *   loaded package, and a read pinned to a load (`servedRevision`) is refused
 *   with a 409 once the package has reloaded.
 * - `GET …/status` reads the package's serving state without loading it.
 * - With `PUBLISHER_PACKAGE_INLINE_FIELDS=off` a package response carries only
 *   the package's own fields, and its `GET /status` entry only what a reconcile
 *   reads. With it on (the default) the moved fields are still inline.
 * - A package whose `#(index)` or required `#(filter)` an index cannot honor is
 *   refused with a 400.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../../src/api";
import { type ResourcesEnv, startResourcesEnv } from "./resources_harness";

type PackageEntities = components["schemas"]["PackageEntities"];
type PackageBuildPlan = components["schemas"]["PackageBuildPlan"];
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

   it("GET …/entities reports every model's entities, off-surface files included", async () => {
      const { status, json } = await get(pkgUrl("shop", "/entities"));
      expect(status).toBe(200);
      const body = json as unknown as PackageEntities;
      expect(body.servedRevision).toBeString();
      expect(body.models.map((m) => m.modelPath)).toEqual([
         "helper.malloy",
         "orders.malloy",
      ]);
      const orders = body.models.find((m) => m.modelPath === "orders.malloy")!;
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
      const helper = body.models.find((m) => m.modelPath === "helper.malloy")!;
      expect(helper.sources!.find((s) => s.name === "helper")!.queryable).toBe(
         false,
      );
      // The whole response, as a load computes it from the rebuilt models.
      expect(body.models).toMatchSnapshot();
   }, 60_000);

   it("GET …/build-plan and GET …/embedding-index read the loaded package", async () => {
      const plan = await get(pkgUrl("shop", "/build-plan"));
      expect(plan.status).toBe(200);
      const body = plan.json as unknown as PackageBuildPlan;
      expect(body.servedRevision).toBeString();
      expect("buildPlan" in body).toBe(true);

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

   it("an empty servedRevision pins nothing", async () => {
      for (const sub of ["/entities", "/build-plan"]) {
         expect(
            (await get(pkgUrl("shop", `${sub}?servedRevision=`))).status,
         ).toBe(200);
      }
   }, 60_000);

   it("a read pinned to a load is served while that load serves, and refused once the package reloads", async () => {
      const { json } = await get(pkgUrl("shop", "/entities"));
      const revision = (json as unknown as PackageEntities).servedRevision;
      expect((await statusEntry("shop")).servedRevision).toBe(revision);

      for (const sub of ["/entities", "/build-plan"]) {
         const pinned = await get(
            pkgUrl("shop", `${sub}?servedRevision=${revision}`),
         );
         expect(pinned.status).toBe(200);
         expect(pinned.json.servedRevision).toBe(revision);
      }

      expect((await get(pkgUrl("shop", "?reload=true"))).status).toBe(200);
      for (const sub of ["/entities", "/build-plan"]) {
         const stale = await get(
            pkgUrl("shop", `${sub}?servedRevision=${revision}`),
         );
         expect(stale.status).toBe(409);
         expect(stale.json.reason).toBe("REVISION_CHANGED");
      }
      const fresh = await get(pkgUrl("shop", "/entities"));
      expect(fresh.json.servedRevision).not.toBe(revision);
   }, 120_000);

   it("GET …/status reports the serving state, and 404s for an unknown package", async () => {
      const { status, json } = await get(pkgUrl("shop", "/status"));
      expect(status).toBe(200);
      expect(json.name).toBe("shop");
      expect(json.resource).toBe(`/api/v0${pkgUrl("shop")}`);
      expect(json.status).toMatchObject({ serving: true, loading: false });
      expect(json.servedRevision).toBe(
         (await get(pkgUrl("shop", "/entities"))).json.servedRevision,
      );
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
      expect("classification" in pkg).toBe(false);

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
      // Never inline: these have only their own routes.
      expect("entities" in pkg).toBe(false);
      expect("classification" in pkg).toBe(false);
   }, 60_000);

   it("a package with an #(index) on a measure is refused with a 400 naming it", async () => {
      const location = writePackage("bad_index", {
         "bad.malloy": `source: bad is ${ROWS} extend {
  #(index)
  measure: n is count()
}
`,
      });
      const created = await publish("bad_index", location);
      expect(created.status).toBe(400);
      expect(JSON.stringify(created.json)).toContain("is not a dimension");
   }, 120_000);

   it("a package with two required filters on one dimension is refused with a 400", async () => {
      const location = writePackage("bad_filter", {
         "bad.malloy": `#(filter) name=a dimension=channel type=equal required
#(filter) name=b dimension=channel type=equal required
source: bad is ${ROWS}
`,
      });
      const created = await publish("bad_filter", location);
      expect(created.status).toBe(400);
      expect(JSON.stringify(created.json)).toContain(
         "multiple required filters",
      );
   }, 120_000);
});

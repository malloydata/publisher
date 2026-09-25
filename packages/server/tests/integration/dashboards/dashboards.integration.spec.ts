// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * E2E coverage for dashboard discovery: the `/dashboards` list and
 * `/dashboards/{name}` manifest endpoints, against a real package whose
 * `dashboards/` directory exercises each form — a single-query dashboard, one
 * relying on the doc-comment title fallback with `autorun=false` and a
 * filter-literal starting value, a composite (`## artifact { tiles=… }`), and a
 * shared include that must not be listed.
 *
 * Running a dashboard needs no dashboard-specific endpoint, so the last test
 * proves the manifest's names are directly runnable through the ordinary query
 * endpoint with `givens`.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_NAME = "dashboards-test-env";
const PACKAGE_NAME = "dashboards-test";
// A second package in the same env with no dashboards/ directory at all, to pin
// that the list endpoint answers with [] rather than erroring.
const NO_DASHBOARDS_PACKAGE = "html-data-apps-nopublic";
// A third package whose dashboards/ is deliberately broken, for the load-time
// lint. Kept apart from PACKAGE_NAME so that one can assert the opposite: a
// well-formed package produces no dashboard warnings at all.
const LINT_PACKAGE = "dashboards-lint";
// A fourth package that curates its query surface, to pin that discovery
// honours it: one dashboard is in `explores` and one is not.
const CURATED_PACKAGE = "dashboards-curated";
// A fifth package with `explores` declared but `queryableSources: "all"`, so the
// query boundary is inert. Its composite dashboard is import-only, which under
// "declared" would mean every tile 404s, and here means nothing of the sort.
const OPEN_PACKAGE = "dashboards-open";
// A sixth package whose surface comes from an index.malloy rather than a key.
// Its dashboard is withheld for the same reason the curated package's is, but
// the author has no 'explores' to fix it in, so the remedy must differ.
const CONVENTION_PACKAGE = "dashboards-convention";

const fixtureDir = path.resolve(__dirname, "../../fixtures/dashboards-test");
const noDashboardsFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/html-data-apps-nopublic",
);
const lintFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/dashboards-lint",
);
const curatedFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/dashboards-curated",
);
const openFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/dashboards-open",
);
const conventionFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/dashboards-convention",
);

interface DashboardItem {
   resource?: string;
   packageName?: string;
   name?: string;
   path?: string;
   title?: string;
   description?: string;
   error?: string;
}

interface GivenSpec {
   name?: string;
   type?: string;
   label?: string;
   control?: string;
   rangeMin?: number;
   rangeMax?: number;
   suggest?: { query?: string; source?: string; dimension?: string };
   default?: string;
}

interface DashboardManifest extends DashboardItem {
   query?: string;
   tiles?: { query?: string; givenNames?: string[] }[];
   dashboardColumns?: number;
   startingGivens?: Record<string, string>;
   autorun?: boolean;
   givens?: GivenSpec[];
}

describe("Dashboard discovery (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const apiUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}${sub}`;

   const getManifest = async (name: string): Promise<DashboardManifest> => {
      const res = await fetch(apiUrl(`/dashboards/${name}`));
      expect(res.status).toBe(200);
      return (await res.json()) as DashboardManifest;
   };

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [
               { name: PACKAGE_NAME, location: fixtureDir },
               {
                  name: NO_DASHBOARDS_PACKAGE,
                  location: noDashboardsFixtureDir,
               },
               { name: LINT_PACKAGE, location: lintFixtureDir },
               { name: CURATED_PACKAGE, location: curatedFixtureDir },
               { name: CONVENTION_PACKAGE, location: conventionFixtureDir },
               { name: OPEN_PACKAGE, location: openFixtureDir },
            ],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
            );
            if (res.ok) break;
         } catch {
            // not ready yet
         }
         await new Promise((r) => setTimeout(r, 500));
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   // ── the list endpoint ────────────────────────────────────────────

   it("lists exactly the artifact-tagged files, skipping shared includes", async () => {
      const res = await fetch(apiUrl("/dashboards"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as unknown;
      expect(Array.isArray(body)).toBe(true);

      const dashboards = body as DashboardItem[];
      // The exact set: `_shared.malloy` carries no artifact tag, so it is an
      // include and must not appear.
      expect(dashboards.map((d) => d.name).sort()).toEqual([
         "combined",
         "grid",
         "overview",
         "regions",
         "tiled",
      ]);

      const overview = dashboards.find((d) => d.name === "overview");
      expect(overview).toMatchObject({
         packageName: PACKAGE_NAME,
         name: "overview",
         path: "dashboards/overview.malloy",
         title: "Business Overview",
         description: "Order health at a glance.",
      });
      expect(overview?.resource).toBe(
         `/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/dashboards/overview`,
      );
      expect(overview?.error).toBeUndefined();
   });

   it("lists an empty array for a package with no dashboards/ directory", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${NO_DASHBOARDS_PACKAGE}/dashboards`,
      );
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual([]);
   });

   it("404s an unknown package", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/no-such-pkg/dashboards`,
      );
      expect(res.status).toBe(404);
   });

   it("501s a versionId, which the whole API reserves but does not implement", async () => {
      // Publisher has no package versioning. Every route declaring `versionId`
      // rejects it outright, and 501 is what the spec documents for that — the
      // caller asked for a feature the server does not have, which is not an
      // internal failure.
      for (const sub of ["/dashboards", "/dashboards/overview"]) {
         const res = await fetch(apiUrl(`${sub}?versionId=v1`));
         expect(res.status).toBe(501);
         expect(((await res.json()) as { message?: string }).message).toContain(
            "Version IDs not implemented",
         );
      }
   });

   // ── the manifest endpoint ────────────────────────────────────────

   it("returns the manifest of a single-query dashboard, grid width included", async () => {
      const manifest = await getManifest("overview");
      expect(manifest).toMatchObject({
         name: "overview",
         title: "Business Overview",
         query: "overview",
         dashboardColumns: 6,
         autorun: true,
      });
      expect(manifest.tiles).toBeUndefined();
   });

   it("derives the control row from the givens the query references", async () => {
      const manifest = await getManifest("overview");
      const specs = manifest.givens ?? [];
      // Only BRAND and MIN_AMOUNT are referenced; REGION and UNUSED are
      // declared on the model but must not surface as controls here.
      expect(specs.map((s) => s.name).sort()).toEqual(["BRAND", "MIN_AMOUNT"]);

      expect(specs.find((s) => s.name === "BRAND")).toMatchObject({
         type: "filter<string>",
         label: "Brand",
         control: "select",
         suggest: { source: "orders", dimension: "brand" },
         // Unwrapped: the fixture declares `f'Nike'`, and the manifest
         // publishes the body the query endpoint takes.
         default: "Nike",
      });
      expect(specs.find((s) => s.name === "MIN_AMOUNT")).toMatchObject({
         type: "filter<number>",
         label: "Minimum amount",
         rangeMin: 0,
         rangeMax: 500,
      });
   });

   it("falls back to the doc comment for a title, and honors autorun + starting values", async () => {
      const manifest = await getManifest("regions");
      expect(manifest.title).toBe("Orders by region");
      expect(manifest.autorun).toBe(false);
      // Written in the file as the bare filter literal `f'US'`; the manifest
      // carries the run shape the query endpoint accepts.
      expect(manifest.startingGivens).toEqual({ REGION: "US" });
      expect(manifest.givens?.map((s) => s.name)).toEqual(["REGION"]);
      expect(manifest.givens?.[0]).toMatchObject({
         control: "multiselect",
         suggest: { query: "region_suggest", dimension: "region" },
      });
   });

   it("returns a composite dashboard's tiles and grid width", async () => {
      const manifest = await getManifest("combined");
      expect(manifest).toMatchObject({
         name: "combined",
         title: "Combined",
         dashboardColumns: 4,
         autorun: true,
      });
      expect(manifest.query).toBeUndefined();
      // Each tile carries the givens it actually references, so a viewer can
      // re-run only the tiles a changed control affects.
      expect(manifest.tiles).toEqual([
         { query: "orders -> by_brand", givenNames: ["BRAND"] },
         { query: "orders -> by_region", givenNames: ["REGION"] },
         { query: "orders -> totals", givenNames: [] },
      ]);
      // The control row is the union across tiles.
      expect(manifest.givens?.map((s) => s.name)).toEqual(["BRAND", "REGION"]);
   });

   // The per-tile layout, off the VIEW each tile names rather than off the tile
   // entry, which is what lets one view lay out the same as a tile and as a
   // `nest:`. Asserted end to end because the reader walks a compiled ModelDef:
   // the unit tests hand it annotations directly and cannot show that a real
   // compile puts them where it looks.
   it("carries each tile's label, colspan and break off its view", async () => {
      const manifest = await getManifest("tiled");
      expect(manifest).toMatchObject({ dashboardColumns: 12 });
      expect(manifest.tiles).toEqual([
         {
            query: "tiles -> order_tile",
            givenNames: ["BRAND"],
            label: "Orders",
            colspan: 6,
         },
         {
            query: "tiles -> revenue_tile",
            givenNames: ["BRAND"],
            label: "Revenue",
            colspan: 6,
         },
         {
            query: "tiles -> brand_tile",
            givenNames: ["BRAND"],
            label: "By brand",
            colspan: 6,
            break: true,
         },
         {
            query: "tiles -> region_tile",
            givenNames: ["BRAND"],
            label: "By region",
            colspan: 6,
         },
      ]);
   });

   // Every tile is a standalone query and the renderer reads colspan and break
   // only for the children of a `# dashboard` nest, so without Publisher owning
   // those two tag names each tile answers "Unknown render tag 'colspan'".
   it("runs a laid-out tile with no spurious render warning", async () => {
      const res = await fetch(apiUrl("/models/dashboards/tiled.malloy/query"), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ query: "run: tiles -> brand_tile" }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { renderLogs?: unknown[] };
      expect(body.renderLogs).toBeUndefined();
   });

   it("404s an unknown slug, and a dashboards/ file that is only an include", async () => {
      expect((await fetch(apiUrl("/dashboards/nope"))).status).toBe(404);
      // `_shared.malloy` compiles as a model but is not a dashboard.
      expect((await fetch(apiUrl("/dashboards/_shared"))).status).toBe(404);
   });

   it("400s a malformed environment name", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/bad%20name/packages/${PACKAGE_NAME}/dashboards`,
      );
      expect(res.status).toBe(400);
   });

   // ── the manifest is directly runnable ────────────────────────────

   it("runs a dashboard through the ordinary query endpoint with its givens", async () => {
      // The point of having no dashboard-specific run endpoint: everything the
      // manifest names is runnable on the governed query path as-is.
      const manifest = await getManifest("overview");
      const res = await fetch(apiUrl(`/models/${manifest.path}/query`), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            queryName: manifest.query,
            givens: { BRAND: "Nike" },
            compactJson: true,
         }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         result?: string;
         renderLogs?: { message?: string }[];
      };
      const rows = JSON.parse(body.result ?? "[]") as {
         brand_name: string;
      }[];
      expect(rows.map((r) => r.brand_name)).toEqual(["Nike"]);
      // The artifact tag shares the `#` namespace with the renderer's tags, so
      // without filtering, every dashboard query would answer with a spurious
      // "Unknown render tag 'artifact'" warning.
      expect(
         (body.renderLogs ?? []).map((log) => log.message ?? ""),
      ).not.toContain("Unknown render tag 'artifact' on field 'root'");
   });

   it("delivers # drill tags to the browser on the clicked field", async () => {
      // Drill has no endpoint of its own: the browser resolves a click by
      // reading the tag off the field it clicked, which only works because
      // Malloy carries a dimension's annotations into the result schema. That
      // property is pinned against the compiler in
      // src/service/drill_probe.spec.ts; this checks the whole served response
      // still carries it, since a serialization step between here and there
      // would break drill everywhere at once.
      const manifest = await getManifest("overview");
      const res = await fetch(apiUrl(`/models/${manifest.path}/query`), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ queryName: manifest.query }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         result?: string;
         renderLogs?: { message?: string }[];
      };
      const schema = (
         JSON.parse(body.result ?? "{}") as {
            schema?: {
               fields?: { name: string; annotations?: { value: string }[] }[];
            };
         }
      ).schema;
      const brand = (schema?.fields ?? []).find((f) => f.name === "brand_name");
      expect((brand?.annotations ?? []).map((a) => a.value)).toContain(
         "# drill { to=overview given=BRAND }\n",
      );
      // And it arrives without a render warning: `drill` is Publisher's tag,
      // not one the renderer knows, so it would otherwise be reported as
      // unknown on every field that makes a cell clickable.
      expect(
         (body.renderLogs ?? []).map((log) => log.message ?? ""),
      ).not.toContain("Unknown render tag 'drill' on field 'brand_name'");
   });

   it("accepts the filter syntax the SDK's controls produce", async () => {
      // The select and slider controls do not send what the user picked, they
      // send filter syntax built from it (`encodeFilterList`, `encodeAtLeast`
      // in the SDK). That translation is only correct if Malloy reads it the
      // way the control means it, which nothing in the SDK can verify — so it
      // is pinned here, where a real compile either accepts it or does not.
      const manifest = await getManifest("overview");
      const run = (givens: Record<string, string>) =>
         fetch(apiUrl(`/models/${manifest.path}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               queryName: manifest.query,
               givens,
               compactJson: true,
            }),
         });
      const brandsFrom = async (res: Response) => {
         expect(res.status).toBe(200);
         const body = (await res.json()) as { result?: string };
         return (
            JSON.parse(body.result ?? "[]") as {
               brand_name: string;
               total_amount: number;
            }[]
         ).map((row) => row.brand_name);
      };

      // A multiselect: comma-joined values mean "any of these".
      expect(await brandsFrom(await run({ BRAND: "Nike, Levi's" }))).toEqual([
         "Nike",
         "Levi's",
      ]);

      // An empty filter is how a cleared control says "All", and must not read
      // as "matches the empty string".
      expect(await brandsFrom(await run({ BRAND: "" }))).toEqual([
         "Nike",
         "Levi's",
      ]);

      // A slider: `>= N` on a filter<number>. Only order 3 (Levi's, 50) is
      // below 100, so Levi's total drops from 550 to 500 while Nike's stands.
      // BRAND cleared explicitly. The fixture gives it a real default (`f'Nike'`)
      // so the manifest has a non-empty `default` to publish, and this assertion
      // is about MIN_AMOUNT, so it must not inherit a brand filter. It used to
      // pass only because BRAND's default happened to be empty.
      const res = await run({ MIN_AMOUNT: ">= 100", BRAND: "" });
      expect(res.status).toBe(200);
      const rows = JSON.parse(
         ((await res.json()) as { result?: string }).result ?? "[]",
      ) as { brand_name: string; total_amount: number }[];
      expect(
         Object.fromEntries(
            rows.map((row) => [row.brand_name, row.total_amount]),
         ),
      ).toEqual({ Nike: 1000, "Levi's": 500 });
   });

   it("runs each composite tile with only the givens that tile references", async () => {
      const manifest = await getManifest("combined");
      const controlValues: Record<string, string> = {
         BRAND: "Nike",
         REGION: "US",
      };
      for (const tile of manifest.tiles ?? []) {
         const givens = Object.fromEntries(
            (tile.givenNames ?? []).map((name) => [name, controlValues[name]]),
         );
         const res = await fetch(apiUrl(`/models/${manifest.path}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               query: `run: ${tile.query}`,
               givens,
               compactJson: true,
            }),
         });
         expect(res.status).toBe(200);
         const body = (await res.json()) as { result?: string };
         expect(JSON.parse(body.result ?? "[]").length).toBeGreaterThan(0);
      }
   });

   it("accepts a surfaced given a tile does not reference, and rejects an unsurfaced one", async () => {
      // Bindability follows the entry file's given surface, not what the tile
      // references: a surfaced-but-unused given is ignored, while a name the file
      // never imported fails closed. That is what makes the per-tile lists safe
      // as re-run scoping only. It is NOT a statement that the control row is
      // the model's surface: the row is the union over tiles, widened to the
      // surfaced set only when a tile cannot be resolved. This fixture's union
      // happens to equal its surface, which is why the two are easy to conflate
      // here.
      const manifest = await getManifest("combined");
      const run = (givens: Record<string, string>) =>
         fetch(apiUrl(`/models/${manifest.path}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               query: "run: orders -> by_brand",
               givens,
               compactJson: true,
            }),
         });

      expect((await run({ REGION: "US" })).status).toBe(200);

      const unsurfaced = await run({ NOT_IMPORTED: "x" });
      expect(unsurfaced.status).toBeGreaterThanOrEqual(400);
      expect(await unsurfaced.text()).toContain("unknown given");
   });

   // ── the notebook surface, which shares all of the above ──────────

   /**
    * A notebook and a dashboard are two presentations of one machine, so the
    * facts a control row is built from have to reach both. These pin the
    * notebook half: the control contract on `Source.givens`, the `autorun`
    * flag, and a value surviving the shared codec on the notebook-cell path.
    */
   describe("a notebook gets the same parameter contract", () => {
      const notebookUrl = (sub: string) =>
         apiUrl(`/notebooks/orders-since.malloynb${sub}`);

      it("carries each given's control contract on the notebook's sources", async () => {
         const res = await fetch(notebookUrl(""));
         expect(res.status).toBe(200);
         const body = (await res.json()) as {
            autorun?: boolean;
            sources?: { givens?: GivenSpec[] }[];
         };

         const givens = new Map(
            (body.sources ?? [])
               .flatMap((source) => source.givens ?? [])
               .map((given) => [given.name, given]),
         );

         // The same presentation the dashboard manifest reports, because it is
         // read off the declaration rather than off either surface.
         expect(givens.get("BRAND")).toMatchObject({
            label: "Brand",
            control: "select",
            suggest: { source: "orders", dimension: "brand" },
         });
         expect(givens.get("MIN_AMOUNT")).toMatchObject({
            rangeMin: 0,
            rangeMax: 500,
         });
         expect(givens.get("SINCE")).toMatchObject({
            type: "date",
            label: "Ordered since",
         });
      });

      it("reports autorun=false from the file-level tag", async () => {
         const batched = (await (await fetch(notebookUrl(""))).json()) as {
            autorun?: boolean;
         };
         expect(batched.autorun).toBe(false);

         // And an untagged notebook defaults to running on every change.
         const plain = (await (
            await fetch(apiUrl("/notebooks/brands.malloynb"))
         ).json()) as { autorun?: boolean };
         expect(plain.autorun).toBe(true);
      });

      // Both surfaces encode a Date through the SDK's `givensToRequest`, and
      // nothing in the SDK can check that the server reads what it sends, so
      // the wire form is pinned from this side. It matters because the three
      // time types take three spellings and each rejects the other two.
      const runSince = async (since: string) =>
         fetch(
            notebookUrl(
               `/cells/3?givens=${encodeURIComponent(
                  JSON.stringify({ SINCE: since }),
               )}`,
            ),
         );

      it("reads a date given in the bare form the shared codec sends", async () => {
         const countSince = async (since: string) => {
            const res = await runSince(since);
            expect(res.status).toBe(200);
            const body = (await res.json()) as { result?: string };
            const cell = JSON.parse(body.result ?? "{}") as {
               data?: {
                  array_value?: {
                     record_value?: { number_value?: number }[];
                  }[];
               };
            };
            return cell.data?.array_value?.[0]?.record_value?.[0]?.number_value;
         };

         // Six orders in the fixture; two are ordered on or after 2024-03-01.
         expect(await countSince("2024-01-01")).toBe(6);
         expect(await countSince("2024-03-01")).toBe(2);
      });

      /**
       * The manifest says `startingGivens` is "in the shape the query endpoint
       * accepts", so the value it publishes must survive being sent straight
       * back. A MOTLY date literal arrives as a `Date` and `Tag.text()` renders
       * one with `toISOString()`, which is exactly the spelling the next test
       * proves is refused. The fixture writes the literal form deliberately;
       * quoting it, as it used to, dodged the path entirely.
       */
      /**
       * `default` has to be usable AS a default. A filter given is declared as
       * the literal `f'Nike'` and the query endpoint takes the body `Nike`, so
       * publishing the literal made this a field that silently matches zero rows
       * when a client substitutes it, with no error to search for.
       *
       * Both halves in one test: the filter given must be unwrapped, and the
       * plain string whose default READS like a literal must not be, because
       * only a filter-typed given carries the wrapper.
       */
      it("publishes a default the query endpoint accepts, unwrapped only for filter givens", async () => {
         const manifest = await getManifest("overview");
         const byName = Object.fromEntries(
            (manifest.givens ?? []).map((g) => [g.name, g]),
         );
         expect(byName["BRAND"]?.default).toBe("Nike");

         // And it round-trips: the advertised default actually runs and matches.
         const res = await fetch(apiUrl(`/models/${manifest.path}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               queryName: manifest.query,
               givens: { BRAND: byName["BRAND"]?.default },
               compactJson: true,
            }),
         });
         expect(res.status).toBe(200);
         const rows = JSON.parse(
            ((await res.json()) as { result: string }).result,
         ) as unknown[];
         expect(rows.length).toBeGreaterThan(0);
      });

      it("publishes a date starting given the query endpoint will accept", async () => {
         const res = await fetch(apiUrl("/notebooks/orders-start.malloynb"));
         expect(res.status).toBe(200);
         const nb = (await res.json()) as {
            startingGivens?: Record<string, string>;
         };
         expect(nb.startingGivens?.SINCE).toBe("2024-03-01");
         // And it round-trips: the published value runs.
         const run = await runSince(nb.startingGivens?.SINCE ?? "");
         expect(run.status).toBe(200);
      });

      it("rejects a full ISO timestamp for a date given", async () => {
         // The reason `givensToRequest` needs the declared type at all: a
         // blanket toISOString() lands here, not on a result.
         const res = await runSince("2024-03-01T00:00:00.000Z");
         expect(res.status).toBe(400);
         const body = (await res.json()) as { message?: string };
         expect(body.message).toContain("YYYY-MM-DD");
      });
   });

   /**
    * A notebook row in a package listing carries a human title, resolved the
    * way a dashboard's is plus the notebook-only heading step. Asserted on the
    * served response rather than the resolver, because the point of the feature
    * is that a listing stops showing filenames.
    */
   describe("notebook titles in a package listing", () => {
      const listNotebooks = async (
         packageName: string,
      ): Promise<{ path?: string; title?: string; description?: string }[]> => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${packageName}/notebooks`,
         );
         expect(res.status).toBe(200);
         return (await res.json()) as {
            path?: string;
            title?: string;
            description?: string;
         }[];
      };

      it("prefers an explicit ## title= over everything below it", async () => {
         const notebooks = await listNotebooks(PACKAGE_NAME);
         const since = notebooks.find(
            (n) => n.path === "orders-since.malloynb",
         );
         expect(since).toMatchObject({
            title: "Orders in a window",
            description: "Order counts from a date the reader picks.",
         });
      });

      it("falls back to the first markdown heading, so an untagged notebook still has a title", async () => {
         const notebooks = await listNotebooks(PACKAGE_NAME);
         const brands = notebooks.find((n) => n.path === "brands.malloynb");
         expect(brands?.title).toBe("Brands");
         // Nothing to describe it: the heading is a title, not a doc comment.
         expect(brands?.description).toBeUndefined();
      });

      it("takes the doc comment ahead of the heading", async () => {
         const notebooks = await listNotebooks(LINT_PACKAGE);
         const shipping = notebooks.find((n) => n.path === "shipping.malloynb");
         // A title is one line and a description is not. `docCommentText` joins
         // a multi-line comment with newlines on purpose, since that route
         // carries markdown, so the title takes only the first line; using the
         // whole comment put an embedded newline into a single-line field.
         //
         // And the description is the REST of the comment, not all of it. It
         // used to repeat the line the title had already taken, which published
         // the same words twice on any surface rendering both.
         expect(shipping).toMatchObject({
            title: "Carrier volumes",
            description: "Shipments per carrier, refreshed nightly.",
         });
         expect(shipping?.title ?? "").not.toContain("\n");
      });
   });

   // ── the load-time lint ───────────────────────────────────────────

   describe("load-time lint", () => {
      const packageWarnings = async (
         packageName: string,
      ): Promise<
         {
            model?: string;
            subject?: string;
            message?: string;
            severity?: string;
         }[]
      > => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${packageName}`,
         );
         expect(res.status).toBe(200);
         const body = (await res.json()) as {
            warnings?: {
               model?: string;
               target?: string;
               message?: string;
               severity?: string;
            }[];
         };
         return body.warnings ?? [];
      };

      it("says nothing about a well-formed package", async () => {
         expect(await packageWarnings(PACKAGE_NAME)).toEqual([]);
      });

      /**
       * A broken shared include fails the reload outright rather than
       * half-loading the package, so the previously-compiled dashboards keep
       * serving unchanged and no phantom appears.
       *
       * This pins the surrounding contract, NOT the guard in
       * `claimsToBeADashboard`. Measured while writing it: `?reload=true`
       * answers 424 here and logs "Preserving existing package directory after failed
       * load", and `Package.create` aborts on the first model error, so the
       * branch that lists an uncompilable dashboard with its error is not
       * reachable from either ordinary load path. It is reachable only from
       * `Package.reloadAllModels` (materialization refresh, manifest rebind),
       * which keeps per-model placeholders. The guard is therefore defensive,
       * and deliberately not claimed here as pinned.
       */
      it("fails the reload rather than inventing a dashboard from a broken include", async () => {
         const include = path.resolve(
            "publisher_data",
            ENV_NAME,
            LINT_PACKAGE,
            "dashboards/_shared.malloy",
         );
         const listUrl = `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}`;
         try {
            await fs.writeFile(
               include,
               "// No artifact tag: a shared include, and it does not compile.\n" +
                  "source: oops is duckdb.table('data/orders.csv') extend {\n" +
                  "   dimension: bad is\n" +
                  "}\n",
            );
            const reload = await fetch(`${listUrl}?reload=true`);
            expect(reload.status).toBe(424);
            const res = await fetch(`${listUrl}/dashboards`);
            expect(res.status).toBe(200);
            const dashboards = (await res.json()) as DashboardItem[];
            // Unchanged, and above all no `_shared`.
            expect(dashboards.map((d) => d.name).sort()).toEqual([
               "a#b",
               "broken",
               "overview",
               "p%q",
               "v1.2",
            ]);
         } finally {
            await fs.rm(include, { force: true });
            await fetch(`${listUrl}?reload=true`);
         }
      });

      /**
       * A name outside the documented pattern is SERVED and only noted. The
       * route is a plain Express param, so `GET .../dashboards/v1.2` resolves;
       * measured, not assumed. Withholding it broke working dashboards for any
       * team that versions a filename, and then made the drill lint call a real
       * dashboard "not a dashboard in this package".
       */
      it("serves a dashboard whose name is outside the documented pattern", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}/dashboards/v1.2`,
         );
         expect(res.status).toBe(200);
         expect((await res.json()) as { name?: string }).toMatchObject({
            name: "v1.2",
         });
      });

      /**
       * The published link must be FOLLOWABLE. A name is a filename basename,
       * so it can carry a `#`, which opens a URL fragment: published raw,
       * `.../dashboards/a#b` makes a client ask for `.../dashboards/a` and get
       * a 404. Serving the dashboard was the right call; publishing its name
       * unencoded was not, and the old code hid that by withholding it.
       */
      it("publishes a followable URL for a name that is hostile in one", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}/dashboards`,
         );
         const hash = ((await res.json()) as DashboardItem[]).find(
            (d) => d.name === "a#b",
         );
         expect(hash?.resource).toContain("/dashboards/a%23b");
         expect(hash?.resource).not.toContain("/dashboards/a#b");

         // And following exactly what was published resolves.
         const followed = await fetch(`${baseUrl}${hash?.resource ?? ""}`);
         expect(followed.status).toBe(200);
         expect((await followed.json()) as { name?: string }).toMatchObject({
            name: "a#b",
         });
      });

      /**
       * A percent is a different and worse failure mode than a hash. Raw, the
       * param cannot be decoded at all, so the request never reaches a handler
       * and no 404 is possible. This case is why encoding is the fix and "the
       * route matches it" was too broad a conclusion to draw from one dot.
       */
      it("publishes a followable URL for a name containing a percent", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}/dashboards`,
         );
         const pct = ((await res.json()) as DashboardItem[]).find(
            (d) => d.name === "p%q",
         );
         expect(pct?.resource).toContain("/dashboards/p%25q");
         const followed = await fetch(`${baseUrl}${pct?.resource ?? ""}`);
         expect(followed.status).toBe(200);
         expect((await followed.json()) as { name?: string }).toMatchObject({
            name: "p%q",
         });
      });

      /**
       * Sending the name RAW is what the encoding avoids, and it fails before
       * routing: Express cannot decode the param, so no handler runs. Measured
       * rather than assumed, because the obvious guess is 400 and it is not one.
       *
       * Read what this does and does not pin. It exercises NO dashboard code:
       * `decode_param` throws during layer matching, so `getDashboard` never
       * runs and no regression in lookup, gating or payload is visible here.
       * What it pins is that a trailing-param route matches at all and that the
       * app-level handler maps an unclassified error to 500, and that the
       * dashboards route is not somehow special among its neighbours.
       *
       * `models` and `notebooks` are the comparison because they take a param in
       * the same position. Their routes are wildcards where this one is
       * `:dashboardName`, so the bodies match only while both captures are the
       * single segment `p%q`; a multi-segment capture would diverge for reasons
       * unrelated to dashboards. The package route behaves the same way and is
       * simply not expressible through the helper below.
       *
       * Expected to go red when the middleware is fixed to return 400, which is
       * the point of writing it down.
       */
      it("answers a raw, undecodable name exactly as its neighbours do", async () => {
         const raw = (suffix: string) =>
            fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}/${suffix}/p%q`,
            );
         const onDashboards = await raw("dashboards");
         expect(onDashboards.status).toBe(500);
         const body = await onDashboards.text();
         for (const neighbour of ["models", "notebooks"]) {
            const other = await raw(neighbour);
            expect(other.status).toBe(onDashboards.status);
            expect(await other.text()).toBe(body);
         }
      });

      it("notes the unconventional name without refusing to serve it", async () => {
         const messages = (await packageWarnings(LINT_PACKAGE)).map(
            (w) => w.message ?? "",
         );
         expect(messages).toContainEqual(
            expect.stringContaining(
               '"v1.2" is outside the conventional dashboard name',
            ),
         );
         // And the drill pointing at it resolves, because it is real and
         // reachable, so there is no finding about it.
         expect(messages).not.toContainEqual(
            expect.stringContaining('targets "v1.2"'),
         );
      });

      it("still serves the broken package's dashboards", async () => {
         // The lint is advisory: a bad tile costs you that tile, not the
         // dashboard or the package.
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}/dashboards`,
         );
         expect(res.status).toBe(200);
         const dashboards = (await res.json()) as DashboardItem[];
         expect(dashboards.map((d) => d.name).sort()).toEqual([
            "a#b",
            "broken",
            "overview",
            "p%q",
            "v1.2",
         ]);
      });

      it("reports each finding once, against the file or dimension it is on", async () => {
         const warnings = await packageWarnings(LINT_PACKAGE);
         const messages = warnings.map((w) => w.message ?? "");
         // Derived up front on purpose: bun's toMatchObject substitutes the
         // asymmetric matchers into the received object, so reading `warnings`
         // again after one of the assertions below would see a matcher where the
         // message used to be.
         const find = (needle: string) =>
            warnings.find((w) => (w.message ?? "").includes(needle));
         const malformed = find("treated as a shared include");
         const missingView = find('no view "missing_view"');
         const drillWarnings = warnings.filter((w) =>
            (w.message ?? "").includes("# drill"),
         );

         expect(messages).toContainEqual(
            expect.stringContaining(
               'source "orders" has no view "missing_view"',
            ),
         );
         expect(messages).toContainEqual(
            expect.stringContaining('no source "ghost" in this file'),
         );
         expect(messages).toContainEqual(
            expect.stringContaining(
               "# dashboard { columns=… } must be a positive integer",
            ),
         );
         // The other half of the one-spelling change, and the reason the
         // enumeration lint exists: nothing reads `dashboard_columns` any more,
         // so without this the grid silently falls back to the default width.
         expect(messages).toContainEqual(
            expect.stringContaining(
               "`dashboard_columns` in the artifact tag does nothing in Publisher",
            ),
         );
         expect(messages).toContainEqual(
            expect.stringContaining(
               'given "REGION", which this file does not import',
            ),
         );
         expect(messages).toContainEqual(
            expect.stringContaining('suggests options from query "nowhere"'),
         );
         expect(messages).toContainEqual(
            expect.stringContaining(
               '# drill on orders.region_name targets "no_such_dashboard"',
            ),
         );
         // Reachable only from a notebook, and checked all the same: drill is
         // declared on a model dimension, so the scan covers every model rather
         // than only the files in dashboards/.
         expect(messages).toContainEqual(
            expect.stringContaining(
               '# drill on shipping.carrier_name targets "ghost"',
            ),
         );
         // to=self with no given to land the clicked value in, on any surface.
         expect(messages).toContainEqual(
            expect.stringContaining(
               "# drill on shipping.warehouse has to=self, but no model in " +
                  'this package declares a given "warehouse"',
            ),
         );
         // The self drill that names a declared given is silent, so the rule
         // is not just "every self drill warns".
         expect(messages).not.toContainEqual(
            expect.stringContaining("shipping.ships_from"),
         );
         expect(messages).toContainEqual(
            expect.stringContaining(
               'Custom dashboard components are not supported, so "dashboards/orphan.jsx" is ignored',
            ),
         );
         // The refinement tile is legal Malloy and must not be warned about.
         expect(messages).not.toContainEqual(
            expect.stringContaining("by_brand + { limit: 2 }"),
         );
         // `# drill { to=["overview"] }` resolves, so it is silent.
         expect(messages).not.toContainEqual(
            expect.stringContaining("orders.brand_name"),
         );

         // The silent case: a tag that does not parse is discarded whole, so the
         // file quietly stops being a dashboard.
         expect(malformed).toMatchObject({
            model: "dashboards/malformed.malloy",
            subject: "malformed",
            severity: "error",
         });

         // A drill is declared on a model dimension, not in a dashboard, so it
         // is reported once for the package rather than per importing file, and
         // names no model.
         // `orders.unconventional_target` drills at `v1.2`, which IS served, so
         // it correctly produces no finding.
         expect(drillWarnings.map((w) => w.subject).sort()).toEqual([
            "orders.region_name",
            "shipping.carrier_name",
            "shipping.warehouse",
         ]);
         for (const warning of drillWarnings) {
            expect(warning.severity).toBe("error");
            expect(warning.model).toBeUndefined();
         }

         // Findings that belong to a file name it, so an author knows where to
         // go.
         expect(missingView).toMatchObject({
            model: "dashboards/broken.malloy",
            subject: "broken",
            severity: "error",
         });
      });
   });

   /**
    * Publisher does not run author-written dashboard components. A sandboxed
    * JSX surface was built and then cut (docs/malloyyo-dashboards-design.md
    * §"Custom JSX components"), so what is asserted here is its absence: a
    * .jsx in dashboards/ must not become a served, compiled, executable asset.
    */
   describe("custom dashboard components (not supported)", () => {
      it("serves no frame or bundle for a dashboard", async () => {
         for (const path of [
            "/dashboards/overview/frame",
            "/dashboards/overview/bundle.js",
         ]) {
            const res = await fetch(apiUrl(path));
            // Whether an unmatched API path 404s or falls through to the SPA is
            // not the point; that it never answers with the frame document or a
            // compiled component is.
            const body = await res.text();
            expect(body).not.toContain("__DASHBOARD__");
            expect(body).not.toContain("__DASH_RUNTIME__");
         }
      });

      it("does not serve the sandbox vendor runtime", async () => {
         const res = await fetch(`${baseUrl}/dashboard-runtime/vendor.js`);
         expect(res.headers.get("content-type") ?? "").not.toContain(
            "javascript",
         );
      });
   });

   /**
    * A dashboard is always listed, whatever the surface is. The surface decides
    * what its tiles may read: only what the package publishes, the same names
    * an agent querying index.malloy can reach. A dashboard's own file admits
    * nothing, so a tile over a hidden source answers 404 and the load warns.
    */
   describe("a package that curates its query surface", () => {
      const curatedUrl = (sub: string) =>
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${CURATED_PACKAGE}${sub}`;
      const conventionUrl = (sub: string) =>
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${CONVENTION_PACKAGE}${sub}`;
      const post = (url: string, body: object) =>
         fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
         });
      const warningsOf = async (url: string) =>
         (
            (await (await fetch(url)).json()) as {
               warnings?: {
                  model?: string;
                  message?: string;
                  severity?: string;
               }[];
            }
         ).warnings ?? [];
      const wontLoad = async (url: string) =>
         (await warningsOf(url))
            .map((w) => w.message ?? "")
            .filter((m) => m.includes("won't load"))
            .sort();

      it("lists every dashboard, including ones explores does not list", async () => {
         const res = await fetch(curatedUrl("/dashboards"));
         expect(res.status).toBe(200);
         const dashboards = (await res.json()) as DashboardItem[];
         expect(dashboards.map((d) => d.name).sort()).toEqual([
            "composite",
            "listed",
            "unlisted",
            "w1.9",
         ]);
      });

      /**
       * The query boundary is PACKAGE-wide, not per file: `orders.malloy` is
       * listed and exports `orders`, and this model resolves that name to the
       * very declaration it exported, so the tile runs.
       */
      it("runs an import-only dashboard's tile, because the source's own file is listed", async () => {
         const res = await post(
            curatedUrl("/models/dashboards/composite.malloy/query"),
            { query: "run: orders -> by_brand" },
         );
         expect(res.status).toBe(200);
      });

      it("serves and runs a dashboard explores does not list, because it reads a published source", async () => {
         const manifest = await fetch(curatedUrl("/dashboards/unlisted"));
         expect(manifest.status).toBe(200);
         expect(((await manifest.json()) as DashboardManifest).query).toBe(
            "unlisted",
         );
         const res = await post(
            curatedUrl("/models/dashboards/unlisted.malloy/query"),
            { queryName: "unlisted" },
         );
         expect(res.status).toBe(200);
      });

      it("still runs a listed dashboard through the ordinary query endpoint", async () => {
         const manifest = (await (
            await fetch(curatedUrl("/dashboards/listed"))
         ).json()) as DashboardManifest;
         const res = await post(curatedUrl(`/models/${manifest.path}/query`), {
            queryName: manifest.query,
            compactJson: true,
         });
         expect(res.status).toBe(200);
      });

      /**
       * A metadata PATCH changes the surface without changing a file, so the
       * tile findings have to be re-checked then, not only at load.
       */
      it("re-checks tile findings after a metadata PATCH changes the surface", async () => {
         const pkgUrl = curatedUrl("");
         expect(await wontLoad(pkgUrl)).toEqual([]);
         try {
            // Take orders.malloy off the surface: nothing listed exports orders.
            const patch = await fetch(pkgUrl, {
               method: "PATCH",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: CURATED_PACKAGE,
                  explores: ["dashboards/listed.malloy"],
                  queryableSources: "declared",
               }),
            });
            expect(patch.ok).toBe(true);
            const after = await wontLoad(pkgUrl);
            expect(after).toContain(
               `Dashboard listed reads orders, which no file "explores" lists ` +
                  `exports, so it won't load. Fix: delete "explores" from ` +
                  `publisher.json and add orders to the export { ... } in ` +
                  `index.malloy.`,
            );
            // Still listed: the surface limits what it reads, not whether it shows.
            const listed = (await (
               await fetch(curatedUrl("/dashboards"))
            ).json()) as DashboardItem[];
            expect(listed.map((d) => d.name)).toContain("listed");
         } finally {
            await fetch(pkgUrl, {
               method: "PATCH",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: CURATED_PACKAGE,
                  explores: [
                     "orders.malloy",
                     "dashboards/listed.malloy",
                     "dashboards/composite.malloy",
                  ],
                  queryableSources: "declared",
               }),
            });
            await fetch(curatedUrl("?reload=true"));
         }
      });

      it("reports no dead drill at a dashboard explores does not list", async () => {
         // `orders.malloy` carries `# drill { to=unlisted }`. The dashboard is
         // served, so the click lands and nothing is said.
         const messages = (await warningsOf(curatedUrl(""))).map(
            (w) => w.message ?? "",
         );
         expect(messages.filter((m) => m.includes("unlisted"))).toEqual([]);
      });

      /**
       * `queryableSources: "all"` decouples the axes: nothing is refused, so an
       * import-only dashboard's tiles run regardless of any export closure.
       */
      it("runs an import-only dashboard's tiles when the boundary is inert", async () => {
         const res = await post(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${OPEN_PACKAGE}/models/dashboards/composite.malloy/query`,
            { query: "run: orders -> by_brand", compactJson: true },
         );
         expect(res.status).toBe(200);
      });

      it("serves an unconventionally named dashboard and says only that", async () => {
         const mine = (await warningsOf(curatedUrl(""))).filter((w) =>
            (w.model ?? "").includes("w1.9"),
         );
         expect(mine).toHaveLength(1);
         expect(mine[0]?.message ?? "").toContain("is served, but");
      });

      describe("when index.malloy is the surface", () => {
         it("lists every dashboard", async () => {
            const res = await fetch(conventionUrl("/dashboards"));
            const dashboards = (await res.json()) as DashboardItem[];
            expect(dashboards.map((d) => d.name).sort()).toEqual([
               "hidden",
               "overview",
               "tiles",
            ]);
         });

         it("runs a single-query dashboard over an exported source by name", async () => {
            const res = await post(
               conventionUrl("/models/dashboards/overview.malloy/query"),
               { queryName: "overview" },
            );
            expect(res.status).toBe(200);
         });

         it("refuses a single-query dashboard over a source index.malloy does not export", async () => {
            const res = await post(
               conventionUrl("/models/dashboards/hidden.malloy/query"),
               { queryName: "hidden" },
            );
            expect(res.status).toBe(404);
         });

         it("runs tiles over exported sources and ones the dashboard derives from them, and refuses the rest", async () => {
            const tile = async (query: string) =>
               (
                  await post(
                     conventionUrl("/models/dashboards/tiles.malloy/query"),
                     {
                        query: `run: ${query}`,
                     },
                  )
               ).status;
            expect(await tile("orders -> by_status")).toBe(200);
            expect(await tile("big_orders -> by_status")).toBe(200);
            expect(await tile("orders_staging -> by_flag")).toBe(404);
            expect(await tile("staged -> by_flag")).toBe(404);
            // A named query over the dashboard's own derived source, reached by
            // name with no query text to read the derivation from.
            const named = await post(
               conventionUrl("/models/dashboards/tiles.malloy/query"),
               { queryName: "big_status" },
            );
            expect(named.status).toBe(200);
         });

         it("warns once per tile or dashboard that won't load, naming the source to export", async () => {
            expect(await wontLoad(conventionUrl(""))).toEqual([
               `Dashboard hidden reads orders_staging, which index.malloy ` +
                  `doesn't export, so it won't load. Fix: add orders_staging to ` +
                  `the export { ... } in index.malloy.`,
               `Tile orders_staging -> by_flag on dashboard tiles reads ` +
                  `orders_staging, which index.malloy doesn't export, so it ` +
                  `won't load. Fix: add orders_staging to the export { ... } in ` +
                  `index.malloy.`,
               `Tile staged -> by_flag on dashboard tiles reads orders_staging, ` +
                  `which index.malloy doesn't export, so it won't load. Fix: add ` +
                  `orders_staging to the export { ... } in index.malloy.`,
            ]);
         });
      });
   });
});

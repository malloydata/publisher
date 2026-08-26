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
               "dashboard_columns must be a positive integer",
            ),
         );
         // The same tag on the OTHER form, where it is inert rather than
         // invalid: `overview` is a single query, so its `dashboard_columns=6`
         // never reaches the manifest, and with no `# dashboard` tag the
         // renderer lays out no grid at all.
         expect(messages).toContainEqual(
            expect.stringContaining(
               "dashboard_columns does nothing on a single-query dashboard",
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
    * Discovery was the only listing path in `Package` that never consulted
    * `exploreSet()`, so a package that curates its query surface published full
    * manifests for dashboards whose every query and given name then 404s.
    * Notebooks are uncurated in BOTH directions; dashboards had taken "always
    * listed" without "always queryable".
    */
   describe("a package that curates its query surface", () => {
      const curatedUrl = (sub: string) =>
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${CURATED_PACKAGE}${sub}`;

      it("serves only the dashboards whose entry files are in explores", async () => {
         const res = await fetch(curatedUrl("/dashboards"));
         expect(res.status).toBe(200);
         const dashboards = (await res.json()) as DashboardItem[];
         expect(dashboards.map((d) => d.name).sort()).toEqual([
            "composite",
            "listed",
         ]);
      });

      /**
       * The query boundary is PACKAGE-wide, not per file. `composite.malloy` is
       * listed and re-exports nothing of its own, but `orders.malloy` is listed
       * too and exports `orders`, and this model resolves that name to the very
       * declaration it exported, so the tile runs.
       *
       * This test used to assert 404 and was correct when written: the gate then
       * consulted only the requested model's own `export {}` closure. #1008 made
       * it package-wide and this is the assertion that caught it. Kept pointing
       * at the same request rather than deleted, so the direction of the rule is
       * pinned rather than merely unasserted.
       */
      it("runs an import-only dashboard's tile, because the source's own file is listed", async () => {
         const res = await fetch(
            curatedUrl("/models/dashboards/composite.malloy/query"),
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ query: "run: orders -> by_brand" }),
            },
         );
         expect(res.status).toBe(200);
      });

      /**
       * Curation applied AFTER the package is already serving must take effect,
       * by both routes that can apply it.
       *
       * Found by security review, and both halves were real. `reloadAllModels`
       * ran discovery BEFORE installing the freshly-read `explores`, so the
       * reload that first curates a package computed its dashboard set against
       * the previous policy and kept serving the manifests curation was meant
       * to withhold, not for one request but until some later reload. And
       * `setPackageMetadata`, which the metadata PATCH goes through, re-applied
       * the query boundary without re-running discovery at all, so that route
       * never took effect.
       */
      it("applies curation added by a metadata PATCH, not just at load", async () => {
         const pkgUrl = curatedUrl("");
         const before = (await (
            await fetch(curatedUrl("/dashboards"))
         ).json()) as DashboardItem[];
         expect(before.map((d) => d.name).sort()).toEqual([
            "composite",
            "listed",
         ]);
         try {
            // Curate harder: drop the one dashboard that WAS being served.
            const patch = await fetch(pkgUrl, {
               method: "PATCH",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: CURATED_PACKAGE,
                  explores: ["orders.malloy"],
                  queryableSources: "declared",
               }),
            });
            expect(patch.ok).toBe(true);
            const after = (await (
               await fetch(curatedUrl("/dashboards"))
            ).json()) as DashboardItem[];
            expect(after.map((d) => d.name)).toEqual([]);
            const one = await fetch(curatedUrl("/dashboards/listed"));
            expect(one.status).toBe(404);
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

      it("404s the held-back dashboard rather than serving its manifest", async () => {
         // The disclosure, not just the listing: the manifest carries the query
         // name, the given names, and the suggest-query names, every one of
         // which the query endpoint would refuse. The 404 echoing the slug the
         // caller just sent is not a disclosure; its contents would be.
         const res = await fetch(curatedUrl("/dashboards/unlisted"));
         expect(res.status).toBe(404);
         const body = await res.text();
         expect(body).not.toContain("BRAND");
         expect(body).not.toContain("givens");
      });

      it("confirms the held-back dashboard's query really would be refused", async () => {
         // Pins WHY it is held back rather than assuming it: if the query
         // boundary ever stopped refusing this, holding the dashboard back
         // would become wrong and this test should fail.
         const res = await fetch(
            curatedUrl("/models/dashboards/unlisted.malloy/query"),
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ queryName: "unlisted" }),
            },
         );
         expect(res.status).toBe(404);
      });

      it("still runs the served dashboard through the ordinary query endpoint", async () => {
         const manifest = (await (
            await fetch(curatedUrl("/dashboards/listed"))
         ).json()) as DashboardManifest;
         const res = await fetch(curatedUrl(`/models/${manifest.path}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               queryName: manifest.query,
               compactJson: true,
            }),
         });
         expect(res.status).toBe(200);
      });

      /**
       * A `# drill` pointing at a WITHHELD dashboard is still pointing at a
       * dashboard this package has. Resolving the drill lint against the served
       * map made the gate turn a correct tag into "not a dashboard in this
       * package", sending the author to fix something that was right.
       * `orders.malloy` carries `# drill { to=unlisted }` for this.
       */
      it("reports a drill at a withheld dashboard as withheld, not as missing", async () => {
         const body = (await (await fetch(curatedUrl(""))).json()) as {
            warnings?: { message?: string }[];
         };
         const messages = (body.warnings ?? []).map((w) => w.message ?? "");
         // Not "missing": the dashboard is real, so that wording sends the
         // author to fix a drill tag that is correct.
         expect(
            messages.filter((m) => m.includes("is not a dashboard in this")),
         ).toEqual([]);
         // But not silent either: the click still has nowhere to land.
         const withheld = messages.filter((m) =>
            m.includes(
               'targets "unlisted", which IS a dashboard in this package but is not served',
            ),
         );
         // Exactly once. A drill is declared on a model dimension, so every
         // file importing that source carries it; reporting per importer
         // emitted this same finding four times.
         expect(withheld).toHaveLength(1);

         // And at `error`, matching `lintDrillTargets`. Both describe the same
         // broken click and differ only in why the destination is missing, so
         // they must not differ in how loudly they say it.
         const finding = (body.warnings ?? []).find((w) =>
            (w.message ?? "").includes("but is not served"),
         );
         expect(finding?.severity).toBe("error");
      });

      /**
       * `queryableSources: "all"` decouples the axes: `explores` still curates
       * DISCOVERY, but nothing is refused, so an import-only dashboard's tiles
       * run regardless of any export closure.
       */
      it("runs an import-only dashboard's tiles when the boundary is inert", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${OPEN_PACKAGE}/models/dashboards/composite.malloy/query`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  query: "run: orders -> by_brand",
                  compactJson: true,
               }),
            },
         );
         expect(res.status).toBe(200);
      });

      /**
       * Out of the documented name pattern AND withheld by curation. The
       * "is served" finding used to be pushed before the gate, so this one file
       * carried two contradictory warnings and the confident one was wrong.
       */
      it("does not claim a withheld dashboard is served, even when its name is unconventional", async () => {
         const body = (await (await fetch(curatedUrl(""))).json()) as {
            warnings?: { model?: string; message?: string }[];
         };
         const mine = (body.warnings ?? []).filter((w) =>
            (w.model ?? "").includes("w1.9"),
         );
         // Exactly one finding, and it is the withheld one.
         expect(mine).toHaveLength(1);
         expect(mine[0]?.message ?? "").toContain("It is not served.");
         expect(mine[0]?.message ?? "").not.toContain("is served, but");

         // And it really is absent from the listing.
         const listed = (await (
            await fetch(curatedUrl("/dashboards"))
         ).json()) as DashboardItem[];
         expect(listed.map((d) => d.name)).not.toContain("w1.9");
      });

      it("reports the omission instead of leaving it silent", async () => {
         const res = await fetch(curatedUrl(""));
         const body = (await res.json()) as {
            warnings?: { model?: string; message?: string }[];
         };
         const warning = (body.warnings ?? []).find((w) =>
            (w.model ?? "").includes("unlisted"),
         );
         expect(warning?.message ?? "").toContain("not listed in 'explores'");
      });
   });
});

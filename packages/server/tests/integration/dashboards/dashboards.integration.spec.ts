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

const fixtureDir = path.resolve(__dirname, "../../fixtures/dashboards-test");
const noDashboardsFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/html-data-apps-nopublic",
);
const lintFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/dashboards-lint",
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
         default: "f''",
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
      const res = await run({ MIN_AMOUNT: ">= 100" });
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
      // never imported fails closed. This is why the manifest's control row is
      // the model's surface and the per-tile lists are only for re-run scoping.
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
         expect(shipping).toMatchObject({
            title: "Carrier volumes",
            description: "Carrier volumes",
         });
      });
   });

   // ── the load-time lint ───────────────────────────────────────────

   describe("load-time lint", () => {
      const packageWarnings = async (
         packageName: string,
      ): Promise<
         {
            model?: string;
            target?: string;
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

      it("still serves the broken package's dashboards", async () => {
         // The lint is advisory: a bad tile costs you that tile, not the
         // dashboard or the package.
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${LINT_PACKAGE}/dashboards`,
         );
         expect(res.status).toBe(200);
         const dashboards = (await res.json()) as DashboardItem[];
         expect(dashboards.map((d) => d.name).sort()).toEqual([
            "broken",
            "overview",
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
                  'this package declares a given "WAREHOUSE"',
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
});

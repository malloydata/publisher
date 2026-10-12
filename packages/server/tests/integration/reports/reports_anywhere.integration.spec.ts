// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Reports anywhere in a package (`docs/reports-design.md` §1–§4, §6, §8, §9),
 * over real HTTP against `tests/fixtures/reports-anywhere`: reports at the root
 * and two levels deep, a grid report in `notebooks/` and a linear one in
 * `dashboards/`, two reports sharing a basename, drills by path and by bare
 * names that are and are not root files, a tagged model `index.malloy` imports, a tagged
 * `index.malloy`, and a tagged file under `public/`.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
   type Warning,
   warningsByCode,
} from "../../harness/reports";

const ENV_NAME = "reports-anywhere-env";
const PKG = "reports-anywhere";

interface Report {
   resource?: string;
   path: string;
   name: string;
   layout: string;
   title?: string;
   editable?: boolean;
   error?: string;
}

describe("Reports anywhere in a package (E2E)", () => {
   let env: PackageEnv;
   let baseUrl: string;
   const api = (sub = "") => env.api(sub);
   const getJson = <T>(url: string, status = 200) =>
      env.getJson<T>(url, status);
   let warnings: Warning[] = [];

   beforeAll(async () => {
      env = await startPackageEnv(ENV_NAME, { [PKG]: fixture(PKG) });
      baseUrl = env.baseUrl;
      warnings = await env.warnings();
   }, 300_000);

   afterAll(() => env?.stop());

   it("lists every report by path with its layout from its tags, wherever it lives", async () => {
      const reports = await getJson<Report[]>(api("/reports"));
      expect(
         Object.fromEntries(reports.map((r) => [r.path, r.layout])),
      ).toEqual({
         "dashboards/linear_in_dashboards.malloy": "linear",
         "deep/story.malloy": "linear",
         "finance/summary.malloy": "grid",
         "notebooks/grid_in_notebooks.malloy": "grid",
         "overview.malloy": "grid",
         "sales/2026/q3.malloy": "grid",
         "sales/report.csv.malloy": "grid",
         "sales/summary.malloy": "grid",
         "shared/reached_report.malloy": "grid",
         "tagged_model.malloy": "grid",
      });
      // Path order, and the basename is only a label.
      expect(reports.map((r) => r.path)).toEqual(
         [...reports.map((r) => r.path)].sort(),
      );
      expect(reports.find((r) => r.path === "sales/2026/q3.malloy")).toEqual({
         resource: `/api/v0/environments/${ENV_NAME}/packages/${PKG}/reports/sales/2026/q3.malloy`,
         packageName: PKG,
         path: "sales/2026/q3.malloy",
         name: "q3",
         layout: "grid",
         title: "Q3",
         editable: true,
      } as Report);
   });

   it("never makes the root index.malloy, an untagged model, or a file in public/ a report", async () => {
      const paths = (await getJson<Report[]>(api("/reports"))).map(
         (r) => r.path,
      );
      expect(paths).not.toContain("index.malloy");
      expect(paths).not.toContain("orders.malloy");
      expect(paths).not.toContain("shared/helpers.malloy");
      expect(paths).not.toContain("public/hidden_report.malloy");
   });

   it("serves both reports that share a basename", async () => {
      const a = await getJson<Report & { tiles?: unknown[] }>(
         api("/reports/sales/summary.malloy"),
      );
      const b = await getJson<Report & { tiles?: unknown[] }>(
         api("/reports/finance/summary.malloy"),
      );
      expect(a.title).toBe("Sales summary");
      expect(b.title).toBe("Finance summary");
   });

   // DEPRECATED FEATURE COVERAGE — delete together with
   // the `…/dashboards` / `…/notebooks` routes (see RELEASE_NOTES deprecations).
   describe("deprecated: the …/dashboards and …/notebooks routes", () => {
      it("says on the deprecated name route which reports share a basename", async () => {
         const ambiguous = await fetch(api("/dashboards/summary"));
         expect(ambiguous.status).toBe(409);
         const message = ((await ambiguous.json()) as { message?: string })
            .message;
         expect(message).toContain("finance/summary.malloy");
         expect(message).toContain("sales/summary.malloy");
         // A unique basename still resolves on the deprecated route.
         const q3 = await getJson<{ path: string; layout: string }>(
            api("/dashboards/q3"),
         );
         expect(q3).toMatchObject({
            path: "sales/2026/q3.malloy",
            layout: "grid",
         });
      });

      it("warns at load about the shared basename the name route answers 409 for", () => {
         const shared = warningsByCode(warnings, "report-name-ambiguous");
         expect(shared).toHaveLength(1);
         expect(shared[0]).toMatchObject({
            model: "finance/summary.malloy",
            subject: "summary",
            severity: "warn",
         });
         expect(shared[0].message).toContain(
            "finance/summary.malloy, sales/summary.malloy",
         );
         expect(shared[0].message).toContain("rename one of the files");
      });

      it("lists the deprecated aliases by layout", async () => {
         const dashboards = await getJson<{ path: string }[]>(
            api("/dashboards"),
         );
         expect(dashboards.map((d) => d.path)).toContain(
            "notebooks/grid_in_notebooks.malloy",
         );
         expect(dashboards.map((d) => d.path)).not.toContain(
            "dashboards/linear_in_dashboards.malloy",
         );
         const notebooks = await getJson<{ path: string }[]>(api("/notebooks"));
         expect(notebooks.map((n) => n.path).sort()).toEqual([
            "dashboards/linear_in_dashboards.malloy",
            "deep/story.malloy",
         ]);
      });
   });

   it("serves a grid report's manifest by path, and a run:-cell report's cells", async () => {
      const grid = await getJson<{
         layout: string;
         tiles?: { query?: string }[];
         dashboardColumns?: number;
      }>(api("/reports/overview.malloy"));
      expect(grid.layout).toBe("grid");
      expect(grid.dashboardColumns).toBe(12);
      expect(grid.tiles?.map((t) => t.query)).toEqual([
         "orders -> by_brand",
         "orders -> by_region",
      ]);

      const linear = await getJson<{
         layout: string;
         tiles?: { kind?: string }[];
         dashboardColumns?: number;
      }>(api("/reports/dashboards/linear_in_dashboards.malloy"));
      expect(linear.layout).toBe("linear");
      expect(linear.dashboardColumns).toBe(1);
      expect(linear.tiles?.map((t) => t.kind)).toEqual(["text", "query"]);

      const story = await getJson<{
         layout: string;
         editable: boolean;
         tiles?: unknown;
         notebook?: {
            notebookCells?: { type: string; kind?: string; text: string }[];
         };
      }>(api("/reports/deep/story.malloy"));
      expect(story.layout).toBe("linear");
      expect(story.editable).toBe(false);
      expect(story.tiles).toBeUndefined();
      const cells = story.notebook?.notebookCells ?? [];
      const queryCell = cells.findIndex((c) => c.kind === "query");
      expect(queryCell).toBeGreaterThanOrEqual(0);

      // A cell runs through the governed query endpoint, the report's path
      // as the model; the reports resource has no cell route.
      const ran = await env.query("deep/story.malloy", {
         query: cells[queryCell].text,
      });
      expect(ran.status).toBe(200);
      expect(
         (await fetch(api(`/reports/deep/story.malloy/cells/${queryCell}`)))
            .status,
      ).toBe(404);
      // The deprecated notebooks route still answers the same cell.
      const old = await fetch(
         api(`/notebooks/deep/story.malloy/cells/${queryCell}`),
      );
      expect(old.status).toBe(200);
   });

   it("answers 404 for a path that is not a report", async () => {
      expect((await fetch(api("/reports/orders.malloy"))).status).toBe(404);
      expect(
         (await fetch(api("/reports/public/hidden_report.malloy"))).status,
      ).toBe(404);
      expect((await fetch(api("/reports/index.malloy"))).status).toBe(404);
   });

   it("keeps reports off the model listing of a curated package", async () => {
      const models = await getJson<{ path: string }[]>(api("/models"));
      expect(models.map((m) => m.path)).toEqual(["index.malloy"]);
   });

   describe("drill targets", () => {
      const drillFindings = () =>
         warnings.filter((w) => (w.message ?? "").startsWith("# drill on"));

      it("resolves a path, at any depth and to either layout, with nothing said", () => {
         const subjects = drillFindings().map((w) => w.subject);
         expect(subjects).not.toContain("orders.brand_name");
         expect(subjects).not.toContain("orders.brand_lower");
      });

      it("resolves a bare name that is a root path with nothing said", () => {
         expect(drillFindings().map((w) => w.subject)).not.toContain(
            "orders.region_upper",
         );
      });

      it("never looks a bare name up elsewhere: one that is no root file is unresolved", () => {
         for (const subject of ["orders.region_name", "orders.brand_upper"]) {
            const finding = drillFindings().find((w) => w.subject === subject);
            expect(finding).toMatchObject({ severity: "error" });
            expect(finding?.message).toContain(
               "is not a report in this package",
            );
         }
      });
   });

   describe("curation edges", () => {
      it("reports a tagged index.malloy, whose tag is ignored", () => {
         expect(
            warningsByCode(warnings, "index-is-not-a-report")[0],
         ).toMatchObject({ model: "index.malloy", severity: "error" });
      });

      it("reports a tagged model index.malloy imports, directly or through another file", () => {
         const onSurface = warningsByCode(warnings, "report-on-surface");
         expect(onSurface).toEqual([
            expect.objectContaining({
               model: "tagged_model.malloy",
               severity: "warn",
            }),
            expect.objectContaining({
               model: "shared/reached_report.malloy",
               severity: "warn",
            }),
         ]);
         expect(onSurface[1].message).toContain(
            "surface_hop.malloy, which index.malloy reaches, imports it",
         );
      });

      it("serves no report from public/", () => {
         expect(
            warnings.some((w) => w.model === "public/hidden_report.malloy"),
         ).toBe(false);
      });

      it("still answers queries through the surface index.malloy exports", async () => {
         const res = await env.query("index.malloy", {
            query: "run: extras -> v",
         });
         expect(res.status).toBe(200);
      });
   });

   describe("SPA fallback", () => {
      it("opens a report whose path ends in an asset extension in the app", async () => {
         const res = await fetch(
            `${baseUrl}/${ENV_NAME}/${PKG}/sales/report.csv`,
            {
               redirect: "manual",
            },
         );
         // The app shell (or, where the bundle is not built, the page that
         // says so), never the redirect to a static file or the not-found page.
         expect(res.status).not.toBe(302);
         const body = await res.text();
         expect(body).not.toContain("Nothing is served at");
         expect(
            res.status === 200 || body.includes("SPA bundle isn't built"),
         ).toBe(true);
      });

      it.each(["dashboards", "notebooks"])(
         "keeps the old %s/<name> link to a report whose name ends in an asset extension in the app",
         async (segment) => {
            // `sales/report.csv.malloy` is named `report.csv`; the deprecated
            // by-name route reaches it through the Console's alias.
            const res = await fetch(
               `${baseUrl}/${ENV_NAME}/${PKG}/${segment}/report.csv`,
               { redirect: "manual" },
            );
            expect(res.status).not.toBe(302);
            const body = await res.text();
            expect(body).not.toContain("Nothing is served at");
            expect(
               res.status === 200 || body.includes("SPA bundle isn't built"),
            ).toBe(true);
         },
      );

      it("serves a package's own public/dashboards/ file again", async () => {
         const res = await fetch(
            `${baseUrl}/${ENV_NAME}/${PKG}/dashboards/export.csv`,
            { redirect: "manual" },
         );
         expect(res.status).toBe(302);
         expect(res.headers.get("location")).toBe(
            `/environments/${ENV_NAME}/packages/${PKG}/dashboards/export.csv`,
         );
      });
   });
});

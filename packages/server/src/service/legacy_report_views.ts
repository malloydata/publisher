// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the report surface as the old `…/dashboards*` and
 * `…/notebooks*` routes served it, for clients that still read those shapes
 * (Credible among them). Every function here is a projection of the report
 * API (`Package.listReports`, `Package.getReport`) into the old shape; report
 * code imports nothing from this module. Removing the deprecated routes is
 * deleting this module, `controller/legacy_report.controller.ts` and
 * `routes/deprecated.ts`, and the one line in server.ts that registers them.
 *
 * What only these routes have:
 * - `kind`, the layout in the old vocabulary (`dashboard` for grid,
 *   `notebook` for linear), synthesized here and nowhere else;
 * - addressing a grid report by its basename, which is not an identity: two
 *   reports sharing it are a 409 naming both ({@link AmbiguousReportNameError}).
 */

import { components } from "../api";
import { API_PREFIX } from "../constants";
import { recordDeprecatedUsage } from "../deprecated_usage_metrics";
import { ConflictError } from "../errors";
import type { CompiledDocument } from "./compile_document";
import type { Package } from "./package";
import type { ReportLayout } from "./report";

type ApiDashboard = components["schemas"]["Dashboard"];
type ApiDashboardManifest = components["schemas"]["DashboardManifest"];
type ApiNotebook = components["schemas"]["Notebook"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
type ApiReport = components["schemas"]["Report"];

/** The old name of a layout. */
export type LegacyKind = "dashboard" | "notebook";

export function legacyKindOf(layout: ReportLayout): LegacyKind {
   return layout === "grid" ? "dashboard" : "notebook";
}

/**
 * A basename that more than one report in the package has. A report is
 * identified by its path; the deprecated `…/dashboards/{name}` route takes a
 * basename and resolves it only when it is unique. 409, naming every match.
 */
export class AmbiguousReportNameError extends ConflictError {
   constructor(message: string) {
      super(message);
      this.name = "AmbiguousReportNameError";
   }
}

/**
 * The deprecated dashboard URL. The NAME is percent-encoded; the environment
 * and package names are not, because both are validated on the way in and
 * cannot carry a character that matters here. The name is a filename
 * basename, so it can carry anything a filesystem allows (`a#b`, `p%q`); raw,
 * a `#` or `?` would cut the URL.
 */
function dashboardResource(
   environmentName: string,
   packageName: string,
   name: string,
): string {
   return `${API_PREFIX}/environments/${environmentName}/packages/${packageName}/dashboards/${encodeURIComponent(name)}`;
}

/** The grid reports, in path order: what the dashboards routes serve. */
function gridReports(pkg: Package): ApiReport[] {
   return pkg.listReports().filter((report) => report.layout === "grid");
}

/** The grid reports, as list-dashboards lists them. */
export function listDashboards(
   pkg: Package,
   environmentName: string,
): ApiDashboard[] {
   return gridReports(pkg).map((report) => ({
      resource: dashboardResource(
         environmentName,
         pkg.getPackageName(),
         report.name,
      ),
      packageName: pkg.getPackageName(),
      name: report.name,
      path: report.path,
      title: report.title,
      description: report.description,
      error: report.error,
   }));
}

/**
 * The grid report whose basename is `name`, or undefined when there is none.
 * A basename is not an identity, so two grid reports sharing it are
 * ambiguous, and that is a 409 naming both rather than a guess.
 */
export async function getDashboard(
   pkg: Package,
   environmentName: string,
   name: string,
): Promise<ApiDashboardManifest | undefined> {
   const matches = gridReports(pkg).filter((report) => report.name === name);
   if (matches.length > 1) {
      throw new AmbiguousReportNameError(
         `More than one report in package ${pkg.getPackageName()} is named ` +
            `"${name}": ${matches.map((m) => m.path).join(", ")}. Address ` +
            `one by its path: GET …/reports/{path}.`,
      );
   }
   const report = matches[0] && (await pkg.getReport(matches[0].path));
   if (!report) return undefined;
   return {
      resource: dashboardResource(
         environmentName,
         pkg.getPackageName(),
         report.name,
      ),
      packageName: report.packageName,
      name: report.name,
      path: report.path,
      layout: report.layout,
      kind: legacyKindOf(report.layout),
      title: report.title,
      description: report.description,
      error: report.error,
      tiles: report.tiles,
      dashboardColumns: report.dashboardColumns,
      startingGivens: report.startingGivens,
      autorun: report.autorun,
      givens: report.givens,
   };
}

/**
 * The linear reports, as list-notebooks lists them: a title only when the
 * file resolves one (the report listing falls back to the name), and the
 * error a linear report that failed to compile or to read lists with.
 */
export function listNotebooks(
   pkg: Package,
   environmentName: string,
): ApiNotebook[] {
   return pkg
      .listReports()
      .filter((report) => report.layout === "linear")
      .map((report) => {
         const model = pkg.getModel(report.path);
         const listing = model?.getNotebookListing() ?? {};
         return {
            environmentName,
            packageName: pkg.getPackageName(),
            path: report.path,
            title: listing.title,
            description: listing.description,
            error: model?.getNotebookError()?.message,
         };
      });
}

/** A linear report's cells, with its tile layout (when it is written as tiles) carrying `kind`. */
export function withLegacyNotebookKind(
   notebook: ApiRawNotebook,
): ApiRawNotebook {
   if (!notebook.dashboard) return notebook;
   const { layout } = notebook.dashboard;
   return {
      ...notebook,
      dashboard: {
         ...notebook.dashboard,
         ...(layout && { kind: legacyKindOf(layout) }),
      },
   };
}

/**
 * A compiled document with the old `kind` beside `layout`, on it and on its
 * manifest: what `POST …/compile` has answered since documents were compiled.
 */
export function withLegacyDocumentKind(
   document: CompiledDocument,
): CompiledDocument & { kind: LegacyKind } {
   const kind = legacyKindOf(document.layout);
   const manifest = document.manifest && { ...document.manifest, kind };
   return { ...document, kind, ...(manifest && { manifest }) };
}

/**
 * Whether any served report, of either layout, has the basename `name`: what
 * the Console's `dashboards/<name>` and `notebooks/<name>` alias routes
 * resolve, so the SPA fallback keeps those URLs in the app.
 */
export function hasReportNamed(pkg: Package, name: string): boolean {
   return pkg.listReports().some((report) => report.name === name);
}

/**
 * Whether a Console route under the package is the deprecated
 * `dashboards/<name>` or `notebooks/<name>` alias of a report named `<name>`,
 * wherever it lives. Only when one exists, so `public/dashboards/` files stay
 * reachable.
 */
export function isLegacyReportAlias(pkg: Package, routePath: string): boolean {
   const alias = /^(?:dashboards|notebooks)\/([^/]+)$/.exec(routePath);
   const resolves = alias !== null && hasReportNamed(pkg, alias[1]);
   // Counted only when it resolves: anything else is not the alias in use.
   if (resolves) recordDeprecatedUsage("console_alias");
   return resolves;
}

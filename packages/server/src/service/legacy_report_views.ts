// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the report surface as the old `…/notebooks*` routes served it,
 * for clients that still read that shape. Every function here is a projection
 * of the report API (`Package.listReports`) into the old shape; report
 * code imports nothing from this module. What imports it, and so what a
 * removal has to touch: `controller/legacy_report.controller.ts` (the
 * notebook routes), `controller/compile.controller.ts` (`kind` on a compiled
 * document), `routes/spa.ts` (the Console's `dashboards/<name>` and
 * `notebooks/<name>` alias) and `service/package.ts` (which implements
 * {@link ReportViewPackage}).
 *
 * What only these routes have: `kind`, the layout in the old vocabulary
 * (`dashboard` for grid, `notebook` for linear), synthesized here and nowhere
 * else.
 */

import { components } from "../api";
import { recordDeprecatedUsage } from "../deprecated_usage_metrics";
import type { CompiledDocument } from "./compile_document";
import { reportName, type ReportLayout } from "./report";

type ApiNotebook = components["schemas"]["Notebook"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
type ApiReport = components["schemas"]["Report"];

/** What the deprecated projections read from a package. `Package` implements it. */
export interface ReportViewPackage {
   getPackageName(): string;
   listReports(): ApiReport[];
   isServedReport(modelPath: string): boolean;
   servedReportPaths(): IterableIterator<string>;
   getModel(modelPath: string):
      | {
           getNotebookListing(): { title?: string; description?: string };
           getNotebookError(): Error | undefined;
        }
      | undefined;
}

/** The old name of a layout. */
export type LegacyKind = "dashboard" | "notebook";

export function legacyKindOf(layout: ReportLayout): LegacyKind {
   return layout === "grid" ? "dashboard" : "notebook";
}

/**
 * The linear reports, as list-notebooks lists them: a title only when the
 * file resolves one (the report listing falls back to the name), and the
 * error a linear report that failed to compile or to read lists with.
 */
export function listNotebooks(
   pkg: ReportViewPackage,
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
 * manifest, as `POST …/compile` answers it for older clients.
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
 * resolve, so the SPA fallback keeps those URLs in the app. Read from the
 * report paths rather than the listing, which the fallback would otherwise
 * build on every candidate request.
 */
function hasReportNamed(pkg: ReportViewPackage, name: string): boolean {
   for (const path of pkg.servedReportPaths()) {
      if (reportName(path) === name) return true;
   }
   return false;
}

/**
 * Whether a Console route under the package is the deprecated
 * `dashboards/<name>` or `notebooks/<name>` alias of a report named `<name>`,
 * wherever it lives. Only when one exists, so `public/dashboards/` files stay
 * reachable; and never when a report lives at `<routePath>.malloy`, since that
 * is the report's own route. Counts nothing: {@link recordConsoleAliasUse} does.
 */
export function isLegacyReportAlias(
   pkg: ReportViewPackage,
   routePath: string,
): boolean {
   const alias = /^(?:dashboards|notebooks)\/([^/]+)$/.exec(routePath);
   return (
      alias !== null &&
      !pkg.isServedReport(`${routePath}.malloy`) &&
      hasReportNamed(pkg, alias[1])
   );
}

/**
 * Count a Console page load at `requestPath` (`/<env>/<pkg>/<route>`, still
 * percent-encoded) that is the deprecated alias of a report, once per load:
 * the SPA fallback calls this for every request it answers with the app, which
 * is where an old link or bookmark arrives. Anything else is not the alias in
 * use, so it is not counted.
 */
export function recordConsoleAliasUse(
   requestPath: string,
   packageOf: (
      environmentName: string,
      packageName: string,
   ) => ReportViewPackage | undefined,
): void {
   const segments = requestPath.split("/").filter((s) => s.length > 0);
   if (segments.length !== 4) return;
   let decoded: string[];
   try {
      decoded = segments.map((segment) => decodeURIComponent(segment));
   } catch {
      return;
   }
   const [environmentName, packageName, ...route] = decoded;
   const pkg = packageOf(environmentName, packageName);
   if (pkg && isLegacyReportAlias(pkg, route.join("/")))
      recordDeprecatedUsage("console_alias");
}

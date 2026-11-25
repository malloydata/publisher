// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the report vocabulary older clients still read. Report code
 * imports nothing from this module. What imports it, and so what a removal has
 * to touch: `controller/compile.controller.ts` (`kind` on a compiled
 * document), `routes/spa.ts` (the Console's `dashboards/<name>` and
 * `notebooks/<name>` alias) and `service/package.ts` (which implements
 * {@link ReportViewPackage}).
 *
 * What only these have: `kind`, the layout in the old vocabulary
 * (`dashboard` for grid, `notebook` for linear), synthesized here and nowhere
 * else.
 */

import { recordDeprecatedUsage } from "../deprecated_usage_metrics";
import type { CompiledDocument } from "./compile_document";
import { reportName, type ReportLayout } from "./report";

/** What the Console alias reads from a package. `Package` implements it. */
export interface ReportViewPackage {
   isServedReport(modelPath: string): boolean;
   servedReportPaths(): IterableIterator<string>;
}

/** The old name of a layout. */
export type LegacyKind = "dashboard" | "notebook";

export function legacyKindOf(layout: ReportLayout): LegacyKind {
   return layout === "grid" ? "dashboard" : "notebook";
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

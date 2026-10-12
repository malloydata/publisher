// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The DEPRECATED basename lookup, for old links only: a Console URL of the
 * form `dashboards/<name>` or `notebooks/<name>`, from before reports were
 * addressed by their path. Nothing new is resolved by name: a `# drill`
 * names a path (`resolveDrillTarget`).
 */
import {
   documentSlug,
   listedLayout,
   locatedReport,
   type ListedReport,
   type LocatedDocument,
   type ReportLayout,
} from "../Package/documentLocation";
import { layoutOfKind, type DocumentKind } from "./kindAdapter";

/** What a by-name lookup found. */
export type NameLookup =
   | { found: LocatedDocument }
   | { ambiguous: string[] }
   | { missing: true };

/**
 * The report a basename names. A dashboard and a notebook could share one
 * (`dashboards/overview.malloy` beside `notebooks/overview.malloy`), so the
 * preferred layout is tried first: exactly one report of that layout with the
 * name resolves. Failing that, exactly one report of either layout does.
 * Anything else is ambiguous, and says which paths, rather than taking the
 * first match.
 */
export function locateReportByName(
   name: string,
   reports: readonly ListedReport[],
   prefer: ReportLayout = "grid",
): NameLookup {
   const named = reports
      .filter((r): r is ListedReport & { path: string } => !!r.path)
      .filter((r) => documentSlug(r.path) === name);
   const preferred = named.filter((r) => listedLayout(r) === prefer);
   if (preferred.length === 1) return { found: locatedReport(preferred[0]) };
   if (named.length === 1) return { found: locatedReport(named[0]) };
   if (named.length === 0) return { missing: true };
   return { ambiguous: named.map((r) => r.path).sort() };
}

const ALIAS_ROUTE = /^(dashboards|notebooks)\/([^/]+)$/;

/**
 * An old by-name route under a package, `dashboards/<name>` or
 * `notebooks/<name>`: the name, and the layout its folder preferred. Undefined
 * for any other route.
 */
export function legacyAliasOf(
   routePath: string,
): { name: string; prefer: ReportLayout } | undefined {
   const alias = ALIAS_ROUTE.exec(routePath);
   if (!alias) return undefined;
   return {
      name: alias[2],
      prefer: alias[1] === "dashboards" ? "grid" : "linear",
   };
}

/**
 * @deprecated A report is addressed by its path; use `reportRoute`. The
 * Console route of `dashboards/<slug>` or `notebooks/<slug>`, which now answer
 * only as compatibility aliases.
 */
export const documentRoute = (
   environmentName: string,
   packageName: string,
   kind: DocumentKind,
   slug: string,
) => `/${environmentName}/${packageName}/${kind}s/${encodeURIComponent(slug)}`;

/**
 * @deprecated Use `locateReport` with a path. Finds a report by the basename a
 * `dashboards/<slug>` or `notebooks/<slug>` route names, preferring the
 * route's own layout, and undefined when the name is missing or ambiguous.
 */
export function locateDocument({
   kind,
   slug,
   reports,
}: {
   kind: DocumentKind;
   slug: string;
   reports: readonly ListedReport[];
}): LocatedDocument | undefined {
   const lookup = locateReportByName(slug, reports, layoutOfKind(kind));
   return "found" in lookup ? lookup.found : undefined;
}

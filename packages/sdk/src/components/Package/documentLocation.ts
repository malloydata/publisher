// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A report's layout: `grid` places tiles by columns, `linear` reads top to
 * bottom. The artifact tag decides it; where the file lives never does.
 */
export type ReportLayout = "grid" | "linear";

/**
 * The legacy name of a layout: a grid report was a dashboard, a linear one a
 * notebook. Kept because the builder, its events and a host's document store
 * still speak it.
 */
export type DocumentKind = "dashboard" | "notebook";

export const kindOfLayout = (layout: ReportLayout | undefined): DocumentKind =>
   layout === "linear" ? "notebook" : "dashboard";

export const layoutOfKind = (kind: DocumentKind): ReportLayout =>
   kind === "notebook" ? "linear" : "grid";

/** One report as the package's listing (`GET …/reports`) has it. */
export interface ListedReport {
   path?: string;
   name?: string;
   layout?: string;
   title?: string;
   editable?: boolean;
}

export interface LocatedDocument {
   kind: DocumentKind;
   layout: ReportLayout;
   /** The file within the package, wherever it lives: the report's identity. */
   path: string;
   /** Whether the builder opens it; false for a `.malloynb` or a `run:`-cell report. */
   editable: boolean;
}

const REPORT_SUFFIX = /\.malloy(nb)?$/;

/**
 * A report's display label: the basename of any path, without `.malloy` or
 * `.malloynb`. `sales/q3.malloy` is `q3`. A label only: two reports in
 * different directories may share it.
 */
export const documentSlug = (path: string | undefined): string | undefined => {
   if (!path) return undefined;
   const base = path.slice(path.lastIndexOf("/") + 1);
   return base.replace(REPORT_SUFFIX, "") || undefined;
};

/** The route segment of a report's path: the path without `.malloy`, each segment encoded. A `.malloynb` keeps its suffix, since it opens by path. */
export const reportRoutePath = (path: string) =>
   path
      .replace(/\.malloy$/, "")
      .split("/")
      .map(encodeURIComponent)
      .join("/");

/** The Console route of a report: `/<env>/<pkg>/sales/q3` for `sales/q3.malloy`. */
export const reportRoute = (
   environmentName: string,
   packageName: string,
   path: string,
) => `/${environmentName}/${packageName}/${reportRoutePath(path)}`;

/**
 * The query parameter that opens a report's route in the builder:
 * `/<env>/<pkg>/sales/q3?edit`. A parameter rather than a trailing `/edit`
 * segment, because a segment is also a path: `sales/edit.malloy` is a report
 * whose route is `/<env>/<pkg>/sales/edit`.
 */
export const REPORT_EDIT_PARAM = "edit";

/** The Console route that edits a report: its {@link reportRoute} with `?edit`. */
export const reportEditRoute = (
   environmentName: string,
   packageName: string,
   path: string,
) => `${reportRoute(environmentName, packageName, path)}?${REPORT_EDIT_PARAM}`;

/**
 * @deprecated A report is addressed by its path; use {@link reportRoute}. The
 * Console route of `dashboards/<slug>` or `notebooks/<slug>`, which now answer
 * only as compatibility aliases.
 */
export const documentRoute = (
   environmentName: string,
   packageName: string,
   kind: DocumentKind,
   slug: string,
) => `/${environmentName}/${packageName}/${kind}s/${encodeURIComponent(slug)}`;

const layoutOf = (report: ListedReport): ReportLayout =>
   report.layout === "linear" ? "linear" : "grid";

const located = (report: ListedReport & { path: string }): LocatedDocument => ({
   kind: kindOfLayout(layoutOf(report)),
   layout: layoutOf(report),
   path: report.path,
   editable: report.editable !== false,
});

/** The listed report at exactly this path, or undefined. */
export function locateReport(
   path: string,
   reports: readonly ListedReport[],
): LocatedDocument | undefined {
   const found = reports.find((r) => r.path === path);
   return found?.path ? located({ ...found, path: found.path }) : undefined;
}

/** What a deprecated by-name lookup found. */
export type NameLookup =
   | { found: LocatedDocument }
   | { ambiguous: string[] }
   | { missing: true };

/**
 * The DEPRECATED basename lookup, behind the compatibility aliases only: the
 * old `dashboards/<name>` and `notebooks/<name>` links and a bare `# drill`
 * name with no report at its root path.
 *
 * Bare names used to name a dashboard, and a dashboard and a notebook could
 * share one (`dashboards/overview.malloy` beside `notebooks/overview.malloy`),
 * so the preferred layout is tried first: exactly one report of that layout
 * with the name resolves. Failing that, exactly one report of either layout
 * does. Anything else is ambiguous, and says which paths, rather than taking
 * the first match.
 */
export function locateReportByName(
   name: string,
   reports: readonly ListedReport[],
   prefer: ReportLayout = "grid",
): NameLookup {
   const named = reports
      .filter((r): r is ListedReport & { path: string } => !!r.path)
      .filter((r) => documentSlug(r.path) === name);
   const preferred = named.filter((r) => layoutOf(r) === prefer);
   if (preferred.length === 1) return { found: located(preferred[0]) };
   if (named.length === 1) return { found: located(named[0]) };
   if (named.length === 0) return { missing: true };
   return { ambiguous: named.map((r) => r.path).sort() };
}

/** Where a `# drill { to=… }` lands. */
export type DrillTarget =
   | { kind: "path"; report: LocatedDocument }
   /** Resolved by the deprecated basename fallback. */
   | { kind: "name"; report: LocatedDocument }
   | { kind: "unresolved"; candidates: string[] };

/**
 * Whether a `# drill { to=… }` is a plain package path: non-empty segments,
 * none `.` or `..`, and no leading `/`. Anything else could name a route
 * outside the package (`../../x`), so it resolves to nothing and a host must
 * not build a route from it. The server's `isDrillTargetPath` is the same rule.
 */
export function isDrillTargetPath(to: string): boolean {
   if (to === "" || to.startsWith("/")) return false;
   return to
      .split("/")
      .every(
         (segment) => segment !== "" && segment !== "." && segment !== "..",
      );
}

/**
 * A `# drill` destination, by the server's rule (`resolveDrillDestination`):
 * read as a package path from the root (`sales/overview` is
 * `sales/overview.malloy`), and only for a bare name with no report at that
 * path, the deprecated basename fallback, preferring a grid report.
 */
export function resolveDrillTarget(
   to: string,
   reports: readonly ListedReport[],
): DrillTarget {
   if (!isDrillTargetPath(to)) return { kind: "unresolved", candidates: [] };
   const target = to;
   for (const candidate of [`${target}.malloy`, target]) {
      const report = locateReport(candidate, reports);
      if (report) return { kind: "path", report };
   }
   if (target.includes("/")) return { kind: "unresolved", candidates: [] };
   const byName = locateReportByName(target, reports, "grid");
   if ("found" in byName) return { kind: "name", report: byName.found };
   return {
      kind: "unresolved",
      candidates: "ambiguous" in byName ? byName.ambiguous : [],
   };
}

/**
 * @deprecated Use {@link locateReport} with a path, or
 * {@link locateReportByName} behind a compatibility alias. Finds a report by
 * the basename a `dashboards/<slug>` or `notebooks/<slug>` route names,
 * preferring the route's own layout, and undefined when the name is missing
 * or ambiguous.
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

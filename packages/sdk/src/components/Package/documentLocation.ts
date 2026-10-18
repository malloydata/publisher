// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A report's layout: `grid` places tiles by columns, `linear` reads top to
 * bottom. The artifact tag decides it; where the file lives never does.
 */
export type ReportLayout = "grid" | "linear";

/** One report as the package's listing (`GET …/reports`) has it. */
export interface ListedReport {
   path?: string;
   name?: string;
   layout?: string;
   title?: string;
   editable?: boolean;
   /** The givens it shows a control for: what opening it from a click can set. */
   givenNames?: string[];
}

export interface LocatedDocument {
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

/**
 * The Console route of a report: `/<env>/<pkg>/sales/q3` for
 * `sales/q3.malloy`. Every segment is encoded here, the environment and
 * package included, so no caller encodes any of them again.
 */
export const reportRoute = (
   environmentName: string,
   packageName: string,
   path: string,
) =>
   `/${encodeURIComponent(environmentName)}/${encodeURIComponent(packageName)}/${reportRoutePath(path)}`;

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

/** A listed report's layout: anything but `linear` draws as a grid. */
export const listedLayout = (report: ListedReport): ReportLayout =>
   report.layout === "linear" ? "linear" : "grid";

/** A listed report with a path, as a host opens it. */
export const locatedReport = (
   report: ListedReport & { path: string },
): LocatedDocument => ({
   layout: listedLayout(report),
   path: report.path,
   editable: report.editable !== false,
});

/** The listed report at exactly this path, or undefined. */
export function locateReport(
   path: string,
   reports: readonly ListedReport[],
): LocatedDocument | undefined {
   const found = reports.find((r) => r.path === path);
   return found?.path
      ? locatedReport({ ...found, path: found.path })
      : undefined;
}

/** Where a `# drill { to=… }` lands. */
export type DrillTarget =
   | { kind: "path"; report: LocatedDocument }
   | { kind: "unresolved" };

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
 * a package path from the root, with or without `.malloy` (`sales/overview`
 * is `sales/overview.malloy`, and a bare name is a file at the root).
 */
export function resolveDrillTarget(
   to: string,
   reports: readonly ListedReport[],
): DrillTarget {
   if (!isDrillTargetPath(to)) return { kind: "unresolved" };
   for (const candidate of [`${to}.malloy`, to]) {
      const report = locateReport(candidate, reports);
      if (report) return { kind: "path", report };
   }
   return { kind: "unresolved" };
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's Console routes carry the version as a path segment:
 * `/<env>/<pkg>/<version>/…`, with `latest` spelled out.
 *
 *  - `/<env>/<pkg>/latest`          the package page, at its latest version
 *  - `/<env>/<pkg>/1.2.0`           the package page, pinned to 1.2.0
 *  - `/<env>/<pkg>/1.2.0/sales/q3`  a report of 1.2.0
 *
 * Only a package with published versions has these routes. An unversioned
 * package keeps `/<env>/<pkg>/<path>`, so a report folder that happens to be
 * named `latest` or `1.2.0` in one still opens by its path.
 */

/**
 * `latest`, or a version as the server's `VersionIdPattern` takes it: a
 * semantic version with its pre-release and build suffixes, leading zeros
 * allowed (`01.2.0`), so every version a package can hold has a route.
 */
const VERSION_SEGMENT =
   /^(latest|[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?)$/;

export const LATEST_SEGMENT = "latest";

/** Whether a route segment reads as a version: `latest`, or a semantic version. */
export const isVersionSegment = (segment: string): boolean =>
   VERSION_SEGMENT.test(segment);

/**
 * The version segment a package route's splat starts with, and the rest of
 * the path after it; undefined when the first segment is not version-shaped.
 */
export const splitVersionSegment = (
   splat: string,
): { segment: string; rest: string } | undefined => {
   const slash = splat.indexOf("/");
   const first = slash === -1 ? splat : splat.slice(0, slash);
   if (!isVersionSegment(first)) return undefined;
   return { segment: first, rest: slash === -1 ? "" : splat.slice(slash + 1) };
};

/** The `versionId` a request sends for a segment: none for `latest`. */
export const versionIdOf = (segment: string | undefined): string | undefined =>
   segment === undefined || segment === LATEST_SEGMENT ? undefined : segment;

/** A package's Console route, with its version segment when it has one. */
export const packagePath = (
   environmentName: string,
   packageName: string,
   segment?: string,
): string =>
   `/${encodeURIComponent(environmentName)}/${encodeURIComponent(packageName)}${
      segment === undefined ? "" : `/${encodeURIComponent(segment)}`
   }`;

/**
 * A link inside a package, given the version on screen: `to` gains the
 * version segment after the package when it points into this package and
 * has none. The package page builds its links without one, so this is where
 * they learn which version they belong to.
 */
export const withVersionSegment = (
   to: string,
   environmentName: string,
   packageName: string,
   segment: string | undefined,
): string => {
   if (segment === undefined) return to;
   const prefixes = [
      `/${encodeURIComponent(environmentName)}/${encodeURIComponent(packageName)}`,
      `/${environmentName}/${packageName}`,
   ];
   for (const prefix of prefixes) {
      if (to !== prefix && !to.startsWith(`${prefix}/`)) continue;
      const rest = to.slice(prefix.length).replace(/^\//, "");
      if (splitVersionSegment(rest)) return to;
      return `${packagePath(environmentName, packageName, segment)}${
         rest === "" ? "" : `/${rest}`
      }`;
   }
   return to;
};

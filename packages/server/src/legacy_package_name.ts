// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * TRANSITION: a package version named in the package name.
 *
 * An orchestrator that predates package versions names a version by a
 * combined package name, `<package>___<version>`, and sends no `versionId`.
 * With `PUBLISHER_PACKAGE_VERSIONING=on`, such a request is read as
 * `<package>` at `<version>`, so a server that turns versioning on keeps
 * answering the orchestrator still running while the two roll one after the
 * other. Every route then validates the recovered package name and version as
 * it validates ones a caller sent.
 *
 * Remove once every caller names its version with `versionId`.
 */

import type express from "express";
import { isVersioningEnabled } from "./config";
import { API_PREFIX } from "./constants";
import { logger } from "./logger";
import { isSemver } from "./service/versions/semver";
import { LATEST_VERSION_ALIAS } from "./service/versions/version_service";

/** What separates the package name from its version in a combined name. */
export const LEGACY_VERSION_DELIMITER = "___";

/**
 * `<package>___<version>` split at the LAST delimiter (a package name may
 * itself contain one), or undefined when the name is not one: no delimiter,
 * nothing before it, or a suffix that is not a semantic version.
 */
export function splitLegacyPackageName(
   name: unknown,
): { packageName: string; versionId: string } | undefined {
   if (typeof name !== "string") return undefined;
   const at = name.lastIndexOf(LEGACY_VERSION_DELIMITER);
   if (at <= 0) return undefined;
   const versionId = name.slice(at + LEGACY_VERSION_DELIMITER.length);
   if (!isSemver(versionId)) return undefined;
   return { packageName: name.slice(0, at), versionId };
}

/**
 * The package and version a request means. With versioning on and no
 * version named (absent, null, empty or `latest`), or the same version the
 * name carries, a combined name is split into its package and version.
 * Anything else is returned as it came, for the routes to validate.
 */
export function recoverVersionFromName(
   packageName: string,
   versionId: unknown,
): { packageName: string; versionId: unknown; recovered: boolean } {
   const unchanged = { packageName, versionId, recovered: false };
   if (!isVersioningEnabled()) return unchanged;
   const split = splitLegacyPackageName(packageName);
   if (!split) return unchanged;
   const unnamed =
      versionId === undefined ||
      versionId === null ||
      versionId === "" ||
      versionId === LATEST_VERSION_ALIAS;
   if (!unnamed && versionId !== split.versionId) return unchanged;
   warnOnce(packageName, split);
   return { ...split, recovered: true };
}

// A package route: under the API prefix, its legacy `/projects/...` aliases
// included, or the data-app static routes at the root. Captured: the API
// prefix (or none), the environment segment, the package segment, the rest.
const PACKAGE_PATH = new RegExp(
   `^(${API_PREFIX})?/(?:environments|projects)/([^/]+)/packages/([^/]+)(/.*)?$`,
);
const PACKAGE_COLLECTION_PATH = new RegExp(
   `^${API_PREFIX}/(?:environments|projects)/[^/]+/packages/?$`,
);

/** The version recovered for this request, for {@link legacyPackageNameBodyMiddleware}. */
const RECOVERED_VERSION = Symbol("recoveredVersionId");
type WithRecovered = express.Request & { [RECOVERED_VERSION]?: string };

/**
 * Rewrites a combined package name in the path before routing, so every
 * route sees the package and its version. A route reading `versionId` from
 * the query string gets it there; `DELETE` of the combined name deletes that
 * one version, and its `PATCH` (a manifest rebind) sets that version's
 * manifest, the version routes' own forms. Mounted before the routes.
 */
export function legacyPackageNameMiddleware(): express.RequestHandler {
   return (req, _res, next) => {
      const queryAt = req.url.indexOf("?");
      const pathname = queryAt < 0 ? req.url : req.url.slice(0, queryAt);
      const search = queryAt < 0 ? "" : req.url.slice(queryAt);
      const match = PACKAGE_PATH.exec(pathname);
      if (!match) return next();
      let segment: string;
      try {
         segment = decodeURIComponent(match[3]);
      } catch {
         return next();
      }
      const apiPrefix = match[1] ?? "";
      // A data-app static route (no API prefix) takes no version of its own
      // from a forwarded query string: a proxy may pass a page's query on
      // unchanged, with a `versionId` that is no version of this package. The
      // combined name says which version the page is.
      const recovered = recoverVersionFromName(
         segment,
         apiPrefix === "" ? undefined : req.query.versionId,
      );
      if (!recovered.recovered) return next();

      const environment = match[2];
      const rest = match[4] ?? "";
      const pkg = encodeURIComponent(recovered.packageName);
      const version = encodeURIComponent(recovered.versionId as string);
      const packagePath = pathname.slice(
         0,
         pathname.length - match[3].length - rest.length,
      );
      let path = `${packagePath}${pkg}${rest}`;
      // The version routes exist only under `/environments/...`; a legacy
      // `/projects/...` request names the environment as its project.
      const versionsOf = `${apiPrefix}/environments/${environment}/packages/${pkg}/versions/${version}`;
      if (rest === "" && req.method === "DELETE") {
         path = versionsOf;
      } else if (rest === "" && req.method === "PATCH") {
         req.method = "PUT";
         path = `${versionsOf}/manifest`;
      }
      // The version goes into the URL itself, so a router that parses the
      // query string from `req.url` on each read (Express 5) sees it. Express
      // 4 parsed `req.query` before this ran and keeps that object, so it is
      // set there too.
      const params = new URLSearchParams(search.slice(1));
      params.set("versionId", recovered.versionId as string);
      req.url = `${path}?${params.toString()}`;
      req.query.versionId = recovered.versionId as string;
      (req as WithRecovered)[RECOVERED_VERSION] = recovered.versionId as string;
      next();
   };
}

/**
 * The body half, mounted after the JSON body parser: a load (`POST` of the
 * package collection) names the package in its body, and a route that reads
 * `versionId` only from its body (a materialization build) gets the version
 * recovered from the path.
 */
export function legacyPackageNameBodyMiddleware(): express.RequestHandler {
   return (req, _res, next) => {
      const body = req.body as Record<string, unknown> | undefined;
      if (!body || typeof body !== "object" || Array.isArray(body)) {
         return next();
      }
      if (req.method === "POST" && PACKAGE_COLLECTION_PATH.test(req.path)) {
         const recovered = recoverVersionFromName(
            body.name as string,
            undefined,
         );
         if (recovered.recovered) body.name = recovered.packageName;
         return next();
      }
      const version = (req as WithRecovered)[RECOVERED_VERSION];
      if (
         version !== undefined &&
         (body.versionId === undefined ||
            body.versionId === null ||
            body.versionId === "")
      ) {
         body.versionId = version;
      }
      next();
   };
}

// One warning per combined name: the orchestrator sends the same names on
// every heartbeat-driven call, and the warning says what to change, once.
const warned = new Set<string>();
const WARNED_CAP = 10_000;

function warnOnce(
   combined: string,
   split: { packageName: string; versionId: string },
): void {
   if (warned.has(combined)) return;
   if (warned.size >= WARNED_CAP) warned.clear();
   warned.add(combined);
   logger.warn(
      `versionId recovered from the package name ${combined}: package ` +
         `${split.packageName}, version ${split.versionId}. Pass the package ` +
         `name and versionId; reading the version from the name will be removed.`,
   );
}

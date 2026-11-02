// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Express-facing wrappers around `query_param_utils` (which must stay
 * import-free), shared so the modern and legacy route sets cannot drift.
 */

import type { Request, Response } from "express";
import {
   BadRequestError,
   internalErrorToHttpError,
   PackageVersionError,
} from "./errors";
import { invalidBooleanMessage, parseBooleanParam } from "./query_param_utils";
import { LATEST_VERSION_ALIAS } from "./service/versions/version_service";

/**
 * Read a boolean query param, keeping absent distinct from `false`, and answer
 * 400 for an unusable value. Only for params that need the distinction, such
 * as `bypass_filters`, whose Deprecation header fires on `!== undefined`;
 * otherwise use {@link booleanParamOr400}.
 */
export function optionalBooleanParamOr400(
   req: Pick<Request, "query" | "path" | "method">,
   res: Response,
   name: string,
): { ok: true; value: boolean | undefined } | { ok: false } {
   const raw = req.query[name];
   const parsed = parseBooleanParam(raw);
   if (parsed.ok) {
      return { ok: true, value: raw === undefined ? undefined : parsed.value };
   }
   const { json, status } = internalErrorToHttpError(
      new BadRequestError(
         invalidBooleanMessage(name, raw, req.method, req.path),
      ),
   );
   res.status(status).json(json);
   return { ok: false };
}

/**
 * Read a boolean query param (absent is `false`). Returns `undefined` only
 * after writing a 400, so the caller just returns:
 * `const v = booleanParamOr400(req, res, "reload"); if (v === undefined) return;`.
 */
export function booleanParamOr400(
   req: Pick<Request, "query" | "path" | "method">,
   res: Response,
   name: string,
): boolean | undefined {
   const outcome = optionalBooleanParamOr400(req, res, name);
   return outcome.ok ? (outcome.value ?? false) : undefined;
}

/**
 * Refuse `reload` (any value) on a collection route, naming the
 * single-resource route that honors it. Ignoring it would answer 200 with the
 * list, which reads as a reload that worked.
 */
export function setCollectionReloadError(
   res: Response,
   perResourceRoute: string,
): void {
   const { json, status } = internalErrorToHttpError(
      new BadRequestError(
         `Reload recompiles one named resource, and this endpoint lists them. ` +
            `Use GET ${perResourceRoute}?reload=true instead.`,
      ),
   );
   res.status(status).json(json);
}

/**
 * Refuse a version on a package listing. Listing is not package-scoped and
 * takes no version. An empty one is what a proxy forwarding a page's query
 * string may send: no version. `latest` is what the listing answers with
 * anyway.
 */
export function assertListingTakesNoVersionId(versionId: unknown): void {
   if (versionId && versionId !== LATEST_VERSION_ALIAS) {
      throw new BadRequestError(
         "A package listing takes no versionId: it lists every package, each as its latest version. Read one version with GET .../packages/{packageName}?versionId=.",
      );
   }
}

/**
 * The version a model query names: in the body, or in the URL as on every
 * other package route. Both may be given only if they agree.
 */
export function queryVersionIdOf(
   req: Pick<Request, "body" | "query">,
): unknown {
   const fromBody = req.body?.versionId;
   const fromUrl = req.query.versionId;
   const named = (value: unknown) =>
      value !== undefined && value !== null && value !== "";
   if (named(fromBody) && named(fromUrl) && fromBody !== fromUrl) {
      throw new PackageVersionError(
         "VERSION_ID_INVALID",
         "The body and the URL name different versions; name one, or the same one.",
      );
   }
   return named(fromBody) ? fromBody : fromUrl;
}

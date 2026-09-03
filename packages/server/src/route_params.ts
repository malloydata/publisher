// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Express-facing wrappers around the value contracts in `query_param_utils`.
 *
 * Separate from that file because it stays import-free on purpose (see its
 * header), and separate from `server.ts` because the modern
 * (`/environments/...`) and legacy (`/projects/...`) route sets both need these
 * and are registered from different modules. A second hand-copied version is
 * how the two surfaces drift apart, which is the bug these helpers exist to
 * close.
 */

import type { Request, Response } from "express";
import { BadRequestError, internalErrorToHttpError } from "./errors";
import { invalidBooleanMessage, parseBooleanParam } from "./query_param_utils";

/**
 * Read a boolean query param, keeping "absent" distinguishable from an explicit
 * `false`, and answering 400 when the value is unusable.
 *
 * Most routes do not need the distinction and should use
 * {@link booleanParamOr400}. `bypass_filters` does: `setFilterDeprecationHeaders`
 * fires on `bypassFilters !== undefined`, so collapsing absent to `false` would
 * stamp a Deprecation header on every notebook-cell request whether or not the
 * caller ever mentioned the parameter.
 *
 * {@link parseBooleanParam} owns which values are valid and why; this only turns
 * a refusal into the HTTP error, naming the caller's own method and path so the
 * message is actionable.
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
 * Read a boolean query param, answering 400 when the value is unusable.
 *
 * Returns the boolean on success, or `undefined` after having already written
 * the error response -- so a caller does
 * `const v = booleanParamOr400(req, res, "reload"); if (v === undefined) return;`.
 * An absent param is a successful `false`, never `undefined`, so the sentinel
 * means "refused" and nothing else. Where absence has to stay distinguishable
 * from an explicit `false`, use {@link optionalBooleanParamOr400} instead.
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
 * Refuse `reload` on a collection route, naming the single-resource route that
 * honors it.
 *
 * Reload recompiles one named resource, so a collection cannot serve the
 * request. Ignoring the parameter answers 200 with the list, which a caller
 * reads as a reload that worked: they edit a model, reload the collection, see
 * 200, and query a model the server never recompiled.
 *
 * Every value is refused, not just `true`. A collection does not model `reload`
 * at all, so `?reload=false` asserts something about a parameter that has no
 * meaning here rather than asking for no reload.
 *
 * `perResourceRoute` is the path to point at, with its `{name}` placeholder
 * already filled in as far as the caller's own params allow.
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type express from "express";
import {
   internalErrorToHttpError,
   logInternalFailure,
   WriteRollbackFailedError,
   WriteRolledBackError,
} from "../errors";
import { logger } from "../logger";

type RouteHandler = (
   req: express.Request,
   res: express.Response,
) => Promise<unknown> | unknown;

/**
 * A route answered with JSON: the handler's value is the body, sent with
 * `status` (200 by default), and a throw is mapped to its status by
 * {@link internalErrorToHttpError}. A handler that answers by itself (a 400 for
 * a malformed parameter, a 204) returns nothing and nothing more is sent.
 *
 * A 4xx is the caller's (a report that does not exist, an ambiguous name), so
 * it is logged once at `warn` with its message, never at `error`; a 5xx once
 * at `error` (see {@link sendRouteError}).
 */
export function apiRoute(
   handler: RouteHandler,
   { status = 200 }: { status?: number } = {},
): express.RequestHandler {
   return async (req, res) => {
      try {
         const result = await handler(req, res);
         if (!res.headersSent) res.status(status).json(result);
      } catch (error) {
         sendRouteError(res, error as Error, logRefusal(req, error as Error));
      }
   };
}

/** The `refused` callback every JSON route logs a 4xx through. */
export function logRefusal(
   req: express.Request,
   error: Error,
): (status: number) => void {
   return (status) =>
      logger.warn("Request refused", {
         path: req.path,
         packageName: req.params.packageName,
         status,
         error: error.message,
      });
}

/**
 * {@link apiRoute} for a read route. Its handler reads `versionId` itself
 * (omitted, empty or `latest` means the package's `latest`), so a version a
 * request names is never ignored.
 */
export function jsonRoute(handler: RouteHandler): express.RequestHandler {
   return apiRoute(handler);
}

/**
 * Answer a route's thrown error with its mapped status, and log it once: a 4xx
 * through `refused` (the caller's, so at `warn`), a 5xx at `error` — by the
 * mapping when it logs that class itself, here otherwise (a 503 or 504, say).
 * A 501 is a feature this server lacks, not a fault, so it is not logged; nor
 * is a report write's rollback, which the write logged where it happened.
 */
export function sendRouteError(
   res: express.Response,
   error: Error,
   refused: (status: number) => void,
): void {
   let logged = false;
   const { json, status } = internalErrorToHttpError(error, {
      onLogged: () => (logged = true),
   });
   if (status < 500) {
      refused(status);
   } else if (
      !logged &&
      status !== 501 &&
      !(error instanceof WriteRolledBackError) &&
      !(error instanceof WriteRollbackFailedError)
   ) {
      logInternalFailure("Request failed", error);
   }
   res.status(status).json(json);
}

/**
 * The app's last middleware: an error passed to `next` (the concurrency gate's
 * 503, a body-parser 400) answered with its mapped status and logged by the
 * same policy as a route's own errors.
 */
export function unhandledErrorMiddleware(
   err: Error,
   req: express.Request,
   res: express.Response,
   _next: express.NextFunction,
): void {
   sendRouteError(res, err, logRefusal(req, err));
}

/** Express's wildcard match, the rest of the path after the route's prefix. */
export function wildcardParam(req: express.Request): string {
   return (req.params as Record<string, string>)["0"];
}

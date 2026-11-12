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
 * at `error` (see {@link sendRouteError}). `errorBody` reshapes the mapped
 * `{ code, message }` for a route whose clients expect another shape.
 */
export function apiRoute(
   handler: RouteHandler,
   {
      status = 200,
      errorBody,
   }: { status?: number; errorBody?: ErrorBodyShape } = {},
): express.RequestHandler {
   return async (req, res) => {
      try {
         const result = await handler(req, res);
         if (!res.headersSent) res.status(status).json(result);
      } catch (error) {
         sendRouteError(
            res,
            error as Error,
            logRefusal(req, error as Error),
            errorBody,
         );
      }
   };
}

type ErrorBodyShape = (mapped: { code: number; message: string }) => object;

/** The `{ error }` body the main API's oldest routes answer a failure with. */
export const legacyErrorBody: ErrorBodyShape = ({ message }) => ({
   error: message,
});

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
   errorBody: ErrorBodyShape = (mapped) => mapped,
): void {
   let logged = false;
   const { json, status, headers } = internalErrorToHttpError(error, {
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
   if (headers) res.set(headers);
   res.status(status).json(errorBody(json));
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

/**
 * A route-level handler for a query route: records what its concurrency gate
 * refused before any controller ran, then hands the error on to be answered.
 */
export function recordGateRefusal(
   record: (req: express.Request, error: Error) => Promise<void>,
): express.ErrorRequestHandler {
   return (error, req, _res, next) => {
      // Answered whether or not the record was written, so a refusal never hangs.
      void record(req, error).then(
         () => next(error),
         () => next(error),
      );
   };
}

/** Express's wildcard match, the rest of the path after the route's prefix. */
export function wildcardParam(req: express.Request): string {
   return (req.params as Record<string, string>)["0"];
}

/**
 * Marks a response from a main-API route the admin API now serves (the storage
 * destinations' table routes, watch mode): `Deprecation: true`.
 */
export const markDeprecated: express.RequestHandler = (_req, res, next) => {
   res.set("Deprecation", "true");
   next();
};

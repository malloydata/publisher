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

/**
 * A read route answered with JSON: the handler's value is the 200 body (the
 * handler reads `versionId` itself, so a version is never ignored), and a
 * throw is mapped to its status by
 * {@link internalErrorToHttpError}. The handler may answer itself and return
 * `undefined` (a 400 for a malformed parameter), in which case nothing more is
 * sent.
 *
 * A 4xx is the caller's (a report that does not exist, an ambiguous name), so
 * it is logged once at `warn` with its message, never at `error`; a 5xx once
 * at `error` (see {@link sendRouteError}).
 */
export function jsonRoute(
   handler: (
      req: express.Request,
      res: express.Response,
   ) => Promise<unknown> | unknown,
): express.RequestHandler {
   return async (req, res) => {
      try {
         const result = await handler(req, res);
         if (!res.headersSent) res.status(200).json(result);
      } catch (error) {
         sendRouteError(res, error as Error, (status) =>
            logger.warn("Request refused", {
               path: req.path,
               packageName: req.params.packageName,
               status,
               error: (error as Error).message,
            }),
         );
      }
   };
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

/** Express's wildcard match, the rest of the path after the route's prefix. */
export function wildcardParam(req: express.Request): string {
   return (req.params as Record<string, string>)["0"];
}

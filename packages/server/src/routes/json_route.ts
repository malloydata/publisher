// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type express from "express";
import { internalErrorToHttpError, NotImplementedError } from "../errors";
import { logger } from "../logger";

/** Answer a request that names a version, which this server does not keep. */
export function setVersionIdError(res: express.Response): void {
   const { json, status } = internalErrorToHttpError(
      new NotImplementedError("Version IDs not implemented."),
   );
   res.status(status).json(json);
}

/**
 * A read route answered with JSON: a `versionId` is refused (501), the
 * handler's value is the 200 body, and a throw is mapped to its status by
 * {@link internalErrorToHttpError}. The handler may answer itself and return
 * `undefined` (a 400 for a malformed parameter), in which case nothing more is
 * sent.
 *
 * A 4xx is the caller's (a report that does not exist, an ambiguous name), so
 * it is logged once at `warn` with its message, never at `error`. A 5xx is
 * left to the mapping, which logs an internal fault once, with its stack.
 */
export function jsonRoute(
   handler: (
      req: express.Request,
      res: express.Response,
   ) => Promise<unknown> | unknown,
): express.RequestHandler {
   return async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }
      try {
         const result = await handler(req, res);
         if (!res.headersSent) res.status(200).json(result);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         if (status < 500) {
            logger.warn("Request refused", {
               path: req.path,
               packageName: req.params.packageName,
               status,
               error: (error as Error).message,
            });
         }
         res.status(status).json(json);
      }
   };
}

/** Express's wildcard match, the rest of the path after the route's prefix. */
export function wildcardParam(req: express.Request): string {
   return (req.params as Record<string, string>)["0"];
}

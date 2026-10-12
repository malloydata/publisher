// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type express from "express";
import type { ReportController } from "../controller/report.controller";
import { logger } from "../logger";
import { sendRouteError, setVersionIdError, wildcardParam } from "./json_route";

/**
 * The report save route: `PUT …/models/<path>` with `{source, expectedHash?}`.
 * 201 for a file that did not exist, the way a created materialization
 * answers; 200 for one that was replaced.
 *
 * A refused write is the endpoint working: a stale hash, a report that does
 * not compile, a path that is not a report. It is logged at `warn`, so `error`
 * keeps meaning a fault; a fault is logged once at `error`, as on every JSON
 * route.
 */
export function reportWriteRoute(
   controller: Pick<ReportController, "putReportSource">,
): express.RequestHandler {
   return async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }
      try {
         const result = await controller.putReportSource(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            req.body,
         );
         res.status(result.created ? 201 : 200).json(result);
      } catch (error) {
         sendRouteError(res, error as Error, (status) =>
            logger.warn("Report write refused", {
               packageName: req.params.packageName,
               model: wildcardParam(req),
               status,
               error: (error as Error).message,
            }),
         );
      }
   };
}

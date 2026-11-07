// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the `…/dashboards*` and `…/notebooks*` routes, registered with
 * one call from server.ts. The report routes (`…/reports*`) replace them;
 * these answer the same reports in the old shapes
 * (`service/legacy_report_views.ts`), and run a `.malloynb`'s cells, with the
 * deprecated `filter_params` and `bypass_filters`. Removing them is deleting
 * this module, its controller and its views, and that one call.
 */

import type { GivenValue } from "@malloydata/malloy";
import type express from "express";
import { LegacyReportController } from "../controller/legacy_report.controller";
import { setFilterDeprecationHeaders } from "../filter_deprecation";
import { BadRequestError, internalErrorToHttpError } from "../errors";
import { queryConcurrency } from "../query_concurrency";
import { optionalBooleanParam } from "../route_params";
import { accessIdentityOf } from "../service/access_record";
import type { EnvironmentStore } from "../service/environment_store";
import { apiRoute, recordGateRefusal, wildcardParam } from "./json_route";

/** Parse a JSON query parameter, or say why it is refused. */
function jsonParam<T>(
   req: express.Request,
   name: string,
): { ok: true; value: T | undefined } | { ok: false; message: string } {
   const raw = req.query[name];
   if (typeof raw !== "string") return { ok: true, value: undefined };
   try {
      return { ok: true, value: JSON.parse(raw) as T };
   } catch {
      return { ok: false, message: `Invalid ${name}: must be valid JSON` };
   }
}

export function registerDeprecatedRoutes(
   app: express.Express,
   apiPrefix: string,
   environmentStore: EnvironmentStore,
): void {
   const controller = new LegacyReportController(environmentStore);
   const pkg = `${apiPrefix}/environments/:environmentName/packages/:packageName`;
   const recordCellRefusal = (req: express.Request, error: Error) =>
      controller.recordCellRefusal(
         req.params.environmentName,
         req.params.packageName,
         wildcardParam(req),
         accessIdentityOf(req.headers),
         error,
      );

   app.get(
      `${pkg}/dashboards`,
      apiRoute((req) =>
         controller.listDashboards(
            req.params.environmentName,
            req.params.packageName,
            req.query.versionId,
         ),
      ),
   );

   app.get(
      `${pkg}/dashboards/:dashboardName`,
      apiRoute((req) =>
         controller.getDashboard(
            req.params.environmentName,
            req.params.packageName,
            req.params.dashboardName,
            req.query.versionId,
         ),
      ),
   );

   app.get(
      `${pkg}/notebooks`,
      apiRoute((req) =>
         controller.listNotebooks(
            req.params.environmentName,
            req.params.packageName,
            req.query.versionId,
         ),
      ),
   );

   // Run one cell of a linear report by index, kept for a `.malloynb`: a
   // `.malloy` report's cells run through `POST …/models/{path}/query`, which
   // holds them to the same surface, `#(authorize)` and row-level gates
   // (`tests/integration/reports/cell_query_parity.integration.spec.ts`).
   // Before the notebook route, so its wildcard does not swallow `/cells/{i}`.
   app.get(
      `${pkg}/notebooks/*/cells/:cellIndex`,
      queryConcurrency(),
      apiRoute(async (req, res) => {
         // Answers the 400 after recording it: no controller method has run to.
         const refuse = async (
            message: string,
            body: object = { error: message },
         ) => {
            await recordCellRefusal(req, new BadRequestError(message));
            res.status(400).json(body);
         };
         const cellIndex = parseInt(req.params.cellIndex, 10);
         if (isNaN(cellIndex)) return refuse("Invalid cell index");
         const filterParams = jsonParam<Record<string, string | string[]>>(
            req,
            "filter_params",
         );
         if (!filterParams.ok) return refuse(filterParams.message);
         // Absence must stay distinguishable from an explicit `false` here:
         // the Deprecation header below fires on `bypassFilters !== undefined`.
         let bypassFilters: boolean | undefined;
         try {
            bypassFilters = optionalBooleanParam(req, "bypass_filters");
         } catch (error) {
            return refuse(
               (error as Error).message,
               internalErrorToHttpError(error as Error).json,
            );
         }
         const givens = jsonParam<Record<string, GivenValue>>(req, "givens");
         if (!givens.ok) return refuse(givens.message);
         const result = await controller.executeNotebookCell(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            cellIndex,
            filterParams.value,
            bypassFilters,
            givens.value,
            req.query.versionId,
            accessIdentityOf(req.headers),
         );
         setFilterDeprecationHeaders(res, {
            filterParams: filterParams.value,
            bypassFilters,
         });
         return result;
      }),
      recordGateRefusal(recordCellRefusal),
   );

   app.get(
      `${pkg}/notebooks/*?`,
      apiRoute((req) =>
         controller.getNotebook(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            req.query.versionId,
         ),
      ),
   );
}

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
import type { QuerySlots } from "../query_concurrency";
import { optionalBooleanParam, versionRefOf } from "../route_params";
import { accessIdentityOf } from "../service/access_record";
import type { PackageLookup } from "../service/environment_ports";
import { apiRoute, recordGateRefusal, wildcardParam } from "./json_route";

/** A cell request refused before the controller ran, and the body it answers. */
class CellRequestRefused extends BadRequestError {
   constructor(
      message: string,
      readonly body: object = { error: message },
   ) {
      super(message);
   }
}

/** Parse a JSON query parameter, refusing one that is not JSON. */
function jsonParam<T>(req: express.Request, name: string): T | undefined {
   const raw = req.query[name];
   if (typeof raw !== "string") return undefined;
   try {
      return JSON.parse(raw) as T;
   } catch {
      throw new CellRequestRefused(`Invalid ${name}: must be valid JSON`);
   }
}

function parseCellRequest(req: express.Request, acceptGivens: boolean) {
   const cellIndex = parseInt(req.params.cellIndex, 10);
   if (isNaN(cellIndex)) throw new CellRequestRefused("Invalid cell index");
   const filterParams = jsonParam<Record<string, string | string[]>>(
      req,
      "filter_params",
   );
   // Absence must stay distinguishable from an explicit `false`: the
   // Deprecation header fires on `bypassFilters !== undefined`.
   let bypassFilters: boolean | undefined;
   try {
      bypassFilters = optionalBooleanParam(req, "bypass_filters");
   } catch (error) {
      throw new CellRequestRefused(
         (error as Error).message,
         internalErrorToHttpError(error as Error).json,
      );
   }
   const givens = acceptGivens
      ? jsonParam<Record<string, GivenValue>>(req, "givens")
      : undefined;
   return { cellIndex, filterParams, bypassFilters, givens };
}

/**
 * The handlers of the notebook-cell route, shared by the `/environments` route
 * and its legacy `/projects` alias. A request refused before the controller
 * runs is recorded, then answered 400 `{ error }` (a bad `bypass_filters`
 * keeps `{ code, message }`). The legacy alias reads its environment from
 * `projectName`, takes no `givens` and sets no filter Deprecation headers.
 */
export function notebookCellHandlers(
   controller: LegacyReportController,
   {
      environmentParam,
      legacy,
      querySlots,
   }: {
      environmentParam: "environmentName" | "projectName";
      legacy: boolean;
      querySlots: QuerySlots;
   },
): (express.RequestHandler | express.ErrorRequestHandler)[] {
   const recordCellRefusal = (req: express.Request, error: Error) =>
      controller.recordCellRefusal(
         req.params[environmentParam],
         req.params.packageName,
         wildcardParam(req),
         accessIdentityOf(req.headers),
         error,
      );
   const run = apiRoute(async (req, res) => {
      let cell: ReturnType<typeof parseCellRequest>;
      try {
         cell = parseCellRequest(req, !legacy);
      } catch (error) {
         if (!(error instanceof CellRequestRefused)) throw error;
         await recordCellRefusal(req, error);
         res.status(400).json(error.body);
         return;
      }
      const result = await controller.executeNotebookCell(
         req.params[environmentParam],
         req.params.packageName,
         wildcardParam(req),
         cell.cellIndex,
         cell.filterParams,
         cell.bypassFilters,
         cell.givens,
         versionRefOf(req),
         accessIdentityOf(req.headers),
      );
      if (!legacy) setFilterDeprecationHeaders(res, cell);
      return result;
   });
   return [querySlots.middleware, run, recordGateRefusal(recordCellRefusal)];
}

export function registerDeprecatedRoutes(
   app: express.Express,
   apiPrefix: string,
   environmentStore: PackageLookup,
   querySlots: QuerySlots,
): void {
   const controller = new LegacyReportController(environmentStore);
   const pkg = `${apiPrefix}/environments/:environmentName/packages/:packageName`;
   app.get(
      `${pkg}/dashboards`,
      apiRoute((req) =>
         controller.listDashboards(
            req.params.environmentName,
            req.params.packageName,
            versionRefOf(req),
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
            versionRefOf(req),
         ),
      ),
   );

   app.get(
      `${pkg}/notebooks`,
      apiRoute((req) =>
         controller.listNotebooks(
            req.params.environmentName,
            req.params.packageName,
            versionRefOf(req),
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
      ...notebookCellHandlers(controller, {
         environmentParam: "environmentName",
         legacy: false,
         querySlots,
      }),
   );

   app.get(
      `${pkg}/notebooks/*?`,
      apiRoute((req) =>
         controller.getNotebook(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            versionRefOf(req),
         ),
      ),
   );
}

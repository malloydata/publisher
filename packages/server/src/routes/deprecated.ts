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
import { queryConcurrency } from "../query_concurrency";
import { optionalBooleanParamOr400 } from "../route_params";
import type { EnvironmentStore } from "../service/environment_store";
import { jsonRoute, wildcardParam } from "./json_route";

/** Parse a JSON query parameter, or answer 400 naming it and return `null`. */
function jsonParam<T>(
   req: express.Request,
   res: express.Response,
   name: string,
): T | undefined | null {
   const raw = req.query[name];
   if (typeof raw !== "string") return undefined;
   try {
      return JSON.parse(raw) as T;
   } catch {
      res.status(400).json({ error: `Invalid ${name}: must be valid JSON` });
      return null;
   }
}

export function registerDeprecatedRoutes(
   app: express.Express,
   apiPrefix: string,
   environmentStore: EnvironmentStore,
): void {
   const controller = new LegacyReportController(environmentStore);
   const pkg = `${apiPrefix}/environments/:environmentName/packages/:packageName`;

   app.get(
      `${pkg}/dashboards`,
      jsonRoute((req) =>
         controller.listDashboards(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );

   app.get(
      `${pkg}/dashboards/:dashboardName`,
      jsonRoute((req) =>
         controller.getDashboard(
            req.params.environmentName,
            req.params.packageName,
            req.params.dashboardName,
         ),
      ),
   );

   app.get(
      `${pkg}/notebooks`,
      jsonRoute((req) =>
         controller.listNotebooks(
            req.params.environmentName,
            req.params.packageName,
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
      jsonRoute(async (req, res) => {
         const cellIndex = parseInt(req.params.cellIndex, 10);
         if (isNaN(cellIndex)) {
            res.status(400).json({ error: "Invalid cell index" });
            return;
         }
         const filterParams = jsonParam<Record<string, string | string[]>>(
            req,
            res,
            "filter_params",
         );
         if (filterParams === null) return;
         // Absence must stay distinguishable from an explicit `false` here:
         // the Deprecation header below fires on `bypassFilters !== undefined`.
         const bypass = optionalBooleanParamOr400(req, res, "bypass_filters");
         if (!bypass.ok) return;
         const givens = jsonParam<Record<string, GivenValue>>(
            req,
            res,
            "givens",
         );
         if (givens === null) return;
         const result = await controller.executeNotebookCell(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            cellIndex,
            filterParams,
            bypass.value,
            givens,
         );
         setFilterDeprecationHeaders(res, {
            filterParams,
            bypassFilters: bypass.value,
         });
         return result;
      }),
   );

   app.get(
      `${pkg}/notebooks/*?`,
      jsonRoute((req) =>
         controller.getNotebook(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
         ),
      ),
   );
}

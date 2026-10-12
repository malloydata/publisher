// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";
import { components } from "../api";
import { getQueryTimeoutMs } from "../config";
import { recordDeprecatedUsage } from "../deprecated_usage_metrics";
import { DashboardNotFoundError, ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import { runWithQueryTimeout } from "../query_timeout";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import {
   getDashboard,
   listDashboards,
   listNotebooks,
   withLegacyNotebookKind,
} from "../service/legacy_report_views";
import type { NotebookCellRunResult } from "../service/model";

type ApiDashboard = components["schemas"]["Dashboard"];
type ApiDashboardManifest = components["schemas"]["DashboardManifest"];
type ApiNotebook = components["schemas"]["Notebook"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];

/**
 * DEPRECATED: the `…/dashboards*` and `…/notebooks*` routes, kept for clients
 * that still read them. Each answers from the report API through the
 * projections in `service/legacy_report_views.ts`; `routes/deprecated.ts`
 * registers them, and `server-old.ts` serves the same on its legacy paths.
 *
 * Each method counts one use of its feature as it starts, whatever it then
 * answers, so every route that reaches it — on either path — counts once.
 */
export class LegacyReportController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   private async packageOf(environmentName: string, packageName: string) {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return environment.getPackage(packageName, false);
   }

   public async listDashboards(
      environmentName: string,
      packageName: string,
   ): Promise<ApiDashboard[]> {
      recordDeprecatedUsage("dashboards_list");
      return listDashboards(
         await this.packageOf(environmentName, packageName),
         environmentName,
      );
   }

   public async getDashboard(
      environmentName: string,
      packageName: string,
      dashboardName: string,
   ): Promise<ApiDashboardManifest> {
      recordDeprecatedUsage("dashboard_get");
      // Throws AmbiguousReportNameError (409) when two grid reports share it.
      const dashboard = await getDashboard(
         await this.packageOf(environmentName, packageName),
         environmentName,
         dashboardName,
      );
      if (!dashboard) {
         throw new DashboardNotFoundError(
            `Dashboard ${dashboardName} does not exist in package ${packageName}`,
         );
      }
      return dashboard;
   }

   public async listNotebooks(
      environmentName: string,
      packageName: string,
   ): Promise<ApiNotebook[]> {
      recordDeprecatedUsage("notebooks_list");
      return listNotebooks(
         await this.packageOf(environmentName, packageName),
         environmentName,
      );
   }

   public async getNotebook(
      environmentName: string,
      packageName: string,
      notebookPath: string,
   ): Promise<ApiRawNotebook> {
      recordDeprecatedUsage("notebook_get");
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (!model.isNotebook()) {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return withLegacyNotebookKind(await model.getNotebook());
   }

   public async executeNotebookCell(
      environmentName: string,
      packageName: string,
      notebookPath: string,
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
   ): Promise<NotebookCellRunResult> {
      recordDeprecatedUsage("notebook_cell");
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // Shed load before any disk / DB work; same rationale as
      // QueryController.getQuery — already-loaded packages bypass the
      // package-load admission gate.
      environment.assertCanAdmitQuery();
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (!model.isNotebook()) {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return runWithQueryTimeout(
         (abortSignal) =>
            model.executeNotebookCell(
               cellIndex,
               filterParams,
               bypassFilters,
               givens,
               abortSignal,
               {
                  environment: environmentName,
                  // The package owns its manifest, so the least-specific
                  // author-declared layer is read here; the model knows only its
                  // own file and its package's NAME.
                  packageDeclaration: p.getDeclaredQueryMetadata(),
                  // The environment owns the connection configs, so the default
                  // and enforced layers are read here rather than from the model.
                  connectionMetadata: (connectionName) => {
                     try {
                        const connection =
                           environment.getApiConnection(connectionName);
                        return {
                           default: connection.queryMetadata,
                           enforced: connection.queryMetadataEnforced,
                        };
                     } catch (error) {
                        // Fails open, and says so: what an unreadable
                        // connection costs is the enforced layer.
                        logger.debug(
                           "No query-metadata layers for connection",
                           { connectionName, error },
                        );
                        return null;
                     }
                  },
               },
            ),
         getQueryTimeoutMs(),
      );
   }
}

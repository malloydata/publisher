import { components } from "../api";
import { DashboardNotFoundError } from "../errors";
import { EnvironmentStore } from "../service/environment_store";

type ApiDashboard = components["schemas"]["Dashboard"];
type ApiDashboardManifest = components["schemas"]["DashboardManifest"];

/**
 * Read-only discovery for a package's dashboards. Both routes serve state the
 * package computed at load, so neither compiles or queries anything.
 *
 * There is deliberately no run endpoint: a dashboard's query, a composite's
 * tiles, and a control's suggest query all run through the ordinary
 * `POST …/models/{path}/query` with `givens` — the same governed path every
 * other query takes, so row caps, byte caps, authorize gates, and render-tag
 * validation apply for free.
 */
export class DashboardController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async listDashboards(
      environmentName: string,
      packageName: string,
   ): Promise<ApiDashboard[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listDashboards();
   }

   public async getDashboard(
      environmentName: string,
      packageName: string,
      dashboardName: string,
   ): Promise<ApiDashboardManifest> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const dashboard = p.getDashboard(dashboardName);
      if (!dashboard) {
         throw new DashboardNotFoundError(
            `Dashboard ${dashboardName} does not exist in package ${packageName}`,
         );
      }
      return dashboard;
   }
}

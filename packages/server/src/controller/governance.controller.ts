// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { EnvironmentStore } from "../service/environment_store";
import {
   packageGovernance,
   type ApiPackageGovernance,
} from "../service/governance";
import { versionOptions } from "./report.controller";

/**
 * `GET …/packages/{pkg}/governance`: the governance export of a loaded
 * package (`service/governance.ts`). Read-only, computed from the compile
 * the package is serving; nothing compiles or runs. Takes the package's
 * `versionId`, so a version's own document is served, never `latest`'s.
 */
export class GovernanceController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async getGovernance(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
   ): Promise<ApiPackageGovernance> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.getPackage(
         packageName,
         false,
         ...versionOptions(versionId),
      );
      return packageGovernance(pkg, environmentName, packageName);
   }
}

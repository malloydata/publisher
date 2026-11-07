// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";

/**
 * The loaded package a read route names, without reloading it: the version
 * `versionId` names, or `latest` when it is absent, empty or `latest`. 404s
 * for an environment or package this server does not hold.
 */
export async function loadedPackage(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
   versionId?: unknown,
): Promise<Package> {
   const environment = await environmentStore.getEnvironment(
      environmentName,
      false,
   );
   return environment.getPackage(packageName, false, { versionId });
}

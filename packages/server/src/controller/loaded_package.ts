// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PackageLookup } from "../service/environment_ports";
import type { Package } from "../service/package";
import {
   parseEnvironmentName,
   parsePackageName,
   VersionRef,
} from "../service/identity";

/**
 * The loaded package a read route names, without reloading it: the version
 * `versionId` names, or `latest` when it is absent, empty or `latest`. 404s
 * for an environment or package this server does not hold.
 */
export async function loadedPackage(
   environmentStore: PackageLookup,
   environmentName: string,
   packageName: string,
   versionId: VersionRef = VersionRef.LATEST,
): Promise<Package> {
   const environment = await environmentStore.getEnvironment(
      parseEnvironmentName(environmentName),
      false,
   );
   return environment.getPackage(parsePackageName(packageName), false, {
      versionId,
   });
}

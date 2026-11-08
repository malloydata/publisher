// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import type { PackageLookup } from "../service/environment_ports";
import { VersionRef } from "../service/identity";
import { loadedPackage } from "./loaded_package";

type ApiDatabase = components["schemas"]["Database"];

export class DatabaseController {
   private environmentStore: PackageLookup;

   constructor(environmentStore: PackageLookup) {
      this.environmentStore = environmentStore;
   }

   public async listDatabases(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiDatabase[]> {
      const p = await loadedPackage(
         this.environmentStore,
         environmentName,
         packageName,
         versionId,
      );
      return p.listDatabases();
   }
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EmbeddingModel } from "../../providers/types";
import { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import {
   embeddingConfigured,
   getEmbeddingProvider,
} from "../../service/embedding_provider";
import {
   enqueuePackageSync,
   getEmbeddingIndexStatus,
   type EmbeddingIndexStatus,
} from "./embedding_index";
import { PIPELINE_SETTINGS } from "./get_context_pipeline";
import {
   embeddedEntitiesOf,
   getPackageIndex,
   packageIndexOf,
} from "./package_entities";

/**
 * The semantic index state for one package; `lexical`, by design, when no
 * embedding provider is configured.
 *
 * Composed from the same cached package index retrieval uses, so
 * `totalEntities` cannot drift from what collectEntities decides is an entity.
 */
export async function getPackageEmbeddingStatus(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
): Promise<EmbeddingIndexStatus> {
   const empty = {
      embeddedRows: 0,
      totalRows: 0,
      totalEntities: 0,
      embeddedEntities: 0,
   };
   if (!embeddingConfigured()) return { status: "lexical", ...empty };
   let provider: EmbeddingModel | null;
   try {
      provider = getEmbeddingProvider();
   } catch (error) {
      return {
         status: "error",
         lastError: {
            message: error instanceof Error ? error.message : String(error),
         },
         ...empty,
      };
   }
   if (!provider) return { status: "lexical", ...empty };
   const pkgIndex = await getPackageIndex(
      environmentStore,
      environmentName,
      packageName,
   );
   return getEmbeddingIndexStatus(
      environmentStore.storageManager.getDuckDbConnection(),
      provider,
      environmentName,
      packageName,
      embeddedEntitiesOf(pkgIndex, PIPELINE_SETTINGS),
      pkgIndex.pkg,
   );
}

/**
 * Queue the semantic index for a package that has just loaded, so the vectors
 * build before the first question. Only enqueues; see {@link enqueuePackageSync}.
 * Wired to the environment store's package-loaded hook at server start.
 */
export function startPackageEmbeddingSync(
   environmentStore: EnvironmentStore,
   environmentName: string,
   pkg: Package,
): void {
   const packageName = pkg.getPackageName();
   enqueuePackageSync({
      pkg,
      environmentName,
      packageName,
      prepare: async () => {
         if (!embeddingConfigured()) return undefined;
         // Throws on a malformed embedding configuration; the queue logs it,
         // and the status endpoint reports it.
         const provider = getEmbeddingProvider();
         if (!provider) return undefined;
         // Ask whether this instance is still the one being served, without
         // loading anything. A package unloaded or deleted while this waited
         // must stay gone, and a reload queued its own sync.
         const served = environmentStore
            .peekEnvironment(environmentName)
            ?.peekPackage(packageName);
         if (served !== pkg) return undefined;
         const pkgIndex = await packageIndexOf(pkg, packageName);
         return {
            db: environmentStore.storageManager.getDuckDbConnection(),
            provider,
            entities: embeddedEntitiesOf(pkgIndex, PIPELINE_SETTINGS),
         };
      },
   });
}

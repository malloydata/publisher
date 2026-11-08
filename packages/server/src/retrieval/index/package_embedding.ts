// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EmbeddingModel } from "../llm/types";
import type {
   CatalogDatabase,
   EnvironmentPeek,
   PackageLookup,
} from "../../service/environment_ports";
import { Package } from "../../service/package";
import {
   embeddingConfigured,
   getEmbeddingProvider,
} from "../llm/embedding_provider";
import {
   fcjLastSyncedAtOf,
   fcjStatusOf,
   fcjVectorProgressOf,
   kickFcjBuild,
} from "./engine_state";
import type { EmbeddingIndexStatus } from "./embedding_index";
import { getPackageIndex, packageIndexOf } from "../model/package_entities";

/**
 * The search index state for one package: the build state of the find, list,
 * judge engine's index (see ./engine_state.ts), with how many of its
 * entity vectors are stored. `lexical`, by design, when no embedding provider
 * is configured. `loaded` is the package (or the version of it) already
 * resolved; without it, the served package's.
 */
export async function getPackageEmbeddingStatus(
   environmentStore: PackageLookup,
   environmentName: string,
   packageName: string,
   loaded?: Package,
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
   const pkgIndex = loaded
      ? await packageIndexOf(loaded, packageName)
      : await getPackageIndex(environmentStore, environmentName, packageName);
   const fcj = fcjStatusOf(pkgIndex.pkg);
   const lastSyncedAt = fcjLastSyncedAtOf(pkgIndex.pkg);
   return {
      status: fcj.status as EmbeddingIndexStatus["status"],
      ...fcjVectorProgressOf(pkgIndex.pkg),
      ...(lastSyncedAt ? { lastSyncedAt } : {}),
      fcj,
   };
}

/**
 * Start the search index build for a package that has just loaded, so the
 * index is ready before the first question. Only starts the build; see
 * {@link kickFcjBuild}. Wired to the environment store's package-loaded hook
 * at server start.
 */
export function startPackageEmbeddingSync(
   environmentStore: PackageLookup & Partial<EnvironmentPeek>,
   catalog: CatalogDatabase,
   environmentName: string,
   pkg: Package,
): void {
   kickFcjBuild({
      environmentStore,
      catalog,
      environmentName,
      packageName: pkg.getPackageName(),
      pkg,
   });
}

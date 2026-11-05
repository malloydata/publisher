// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EmbeddingModel } from "../../providers/types";
import { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import {
   embeddingConfigured,
   getEmbeddingProvider,
} from "../../service/embedding_provider";
import { fcjStatusOf, kickFcjBuild } from "../context/engine_state";
import type { EmbeddingIndexStatus } from "./embedding_index";
import { getPackageIndex } from "./package_entities";

/**
 * The search index state for one package: the build state of the find, list,
 * judge engine's index (see ../context/engine_state.ts). `lexical`, by design,
 * when no embedding provider is configured.
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
   const fcj = fcjStatusOf(pkgIndex.pkg);
   return {
      ...empty,
      status: fcj.status as EmbeddingIndexStatus["status"],
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
   environmentStore: EnvironmentStore,
   environmentName: string,
   pkg: Package,
): void {
   kickFcjBuild({
      environmentStore,
      environmentName,
      packageName: pkg.getPackageName(),
      pkg,
   });
}

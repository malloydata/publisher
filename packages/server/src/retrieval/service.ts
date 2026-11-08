// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";
import type { EmbeddingIndexStatus } from "./index/embedding_index";
import {
   deleteEnvironmentEmbeddings,
   deletePackageEmbeddings,
} from "./index/purge";
import {
   getPackageEmbeddingStatus,
   startPackageEmbeddingSync,
} from "./index/package_embedding";
import type { SearchResult } from "./model/types";
import {
   installRetrievalRegistry,
   type RetrievalRegistry,
   retrievalRegistry,
} from "./registry";
import { type FcjQueryArgs, runFcjQuery } from "./search/engine";

/** One ranked search: everything runFcjQuery takes but the store and its own signal. */
export type SearchArgs = Omit<FcjQueryArgs, "environmentStore" | "signal">;

/**
 * The one door into retrieval for the rest of the server: the get_context
 * search, a package's index status, the build a load starts, and the purge a
 * delete owes. Holds the store and the RetrievalRegistry that owns every
 * piece of state retrieval keeps (index builds, caches, held pages, the
 * embedding queue, the chat model, the value-lookup resolver and slot pool).
 *
 * The registry is also the one retrieval's modules read: given one, the
 * service installs it (the composition root builds one per process); given
 * none, it adopts the registry already in force, which is how a spec that
 * builds a service over a stub store shares the state its own seams set.
 *
 * A search answers a domain SearchResult; turning it into an MCP result is
 * the get_context tool's job, not retrieval's.
 */
export class RetrievalService {
   readonly registry: RetrievalRegistry;

   constructor(
      private readonly environmentStore: EnvironmentStore,
      registry?: RetrievalRegistry,
   ) {
      if (registry) installRetrievalRegistry(registry);
      this.registry = registry ?? retrievalRegistry();
   }

   /** A ranked get_context search over one package's index. */
   search(args: SearchArgs): Promise<SearchResult> {
      return runFcjQuery({ ...args, environmentStore: this.environmentStore });
   }

   /** How far one package's search index has got. */
   status(
      environmentName: string,
      packageName: string,
      loaded?: Package,
   ): Promise<EmbeddingIndexStatus> {
      return getPackageEmbeddingStatus(
         this.environmentStore,
         environmentName,
         packageName,
         loaded,
      );
   }

   /**
    * A package entered an environment (boot, add, install, reload): queue its
    * index build so the index is ready before the first question. Only
    * schedules work.
    */
   onPackageLoaded(environmentName: string, pkg: Package): void {
      startPackageEmbeddingSync(this.environmentStore, environmentName, pkg);
   }

   /**
    * Drop what retrieval stored for a deleted package, or for a whole
    * environment when `packageName` is omitted: embeddings, keyphrases,
    * source summaries and the engine's rows. Rejects on a storage failure;
    * the caller decides whether that matters.
    */
   async purge(environmentName: string, packageName?: string): Promise<void> {
      const db = this.environmentStore.storageManager.getDuckDbConnection();
      if (packageName === undefined) {
         await deleteEnvironmentEmbeddings(db, environmentName);
      } else {
         await deletePackageEmbeddings(db, environmentName, packageName);
      }
   }
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Everything retrieval remembers between calls, in one owner.
 *
 * Retrieval's state used to be a dozen module-level variables (index build
 * states, the entity index cache, held result pages, the embedding sync
 * queue, memo caches, the chat model, the value-lookup resolver). They now
 * live on one RetrievalRegistry. The composition root (buildApp) builds one
 * and hands it to the RetrievalService, which makes it the registry
 * retrieval's modules read ({@link retrievalRegistry}); a spec that wants a
 * clean slate installs a fresh one instead of calling a reset per cache.
 *
 * Caches keyed by a Package (or by something a Package owns) stay WeakMaps:
 * a reload builds a new Package, so a stale entry drops out on its own.
 */

import { QuerySlots } from "../query_concurrency";
import type { Package } from "../service/package";
import type { FcjState } from "./index/engine_state";
import { SerialQueue } from "./index/embedding_sync_queue";
import type { FcjIndex } from "./index/index_build";
import { SchemaIndexCaches } from "./index/schema_index_caches";
import type { SummaryHashesCache } from "./index/source_summaries";
import type { IndexValueFetcher } from "./index/values_index";
import { ChatModelSlot } from "./llm/chat_model_slot";
import type { ChatModel } from "./llm/types";
import type { SourceStartsCache } from "./model/catalog";
import type { PackageIndex } from "./model/types";
import { HeldResults } from "./search/held_results";
import type { ReverseJoinsCache } from "./search/routes";
import type { QueryModelResolver } from "./search/values";

export interface RetrievalRegistryOptions {
   /**
    * The process's query-slot pool, which value lookups take a slot from.
    * Defaults to a pool of the registry's own (a spec's).
    */
   querySlots?: QuerySlots;
   /**
    * Resolves the model a value lookup queries. The composition root passes
    * the MCP tools' resolver so a refusal is worded as execute_query words
    * it; undefined is the plain resolver (search/values.ts).
    */
   queryModelResolver?: QueryModelResolver;
}

export class RetrievalRegistry {
   /** Each package's search index build: building, ready or failed (engine_state.ts). */
   readonly engineStates = new WeakMap<Package, FcjState>();
   /** Test seam: stands in for the warehouse when the value index reads data. */
   valueFetcherOverride: IndexValueFetcher | undefined;
   /** The judge's chat model, cached on its settings so its cooldown survives (engine_state.ts). */
   judgeChat: { fingerprint: string; model: ChatModel } | null = null;
   /** The process-wide chat model (llm/active.ts). */
   readonly chat = new ChatModelSlot();
   /** Each package's entity index, built once per Package (package_entities.ts). */
   readonly packageIndexes = new WeakMap<Package, PackageIndex>();
   /** Result pages a paged get_context answer holds for its later offsets. */
   readonly heldResults = new HeldResults();
   /** The one queue every embedding sync goes through: the provider's rate limit is the process's. */
   readonly embeddingSyncQueue = new SerialQueue();
   /** Current source-summary input hashes per frozen entity list (source_summaries.ts). */
   readonly summaryHashes: SummaryHashesCache = new WeakMap();
   /** Start lines of compiled sources per model list (catalog.ts). */
   readonly sourceStarts: SourceStartsCache = new WeakMap();
   /** Reverse join walks per catalog (routes.ts). */
   readonly reverseJoins: ReverseJoinsCache = new WeakMap();
   /** Per index, whether the package uses a phrase as written (engine.ts). */
   readonly phraseCheckers = new WeakMap<
      FcjIndex,
      (phrase: string) => boolean
   >();
   /** search_database_schema's in-memory vector and lexical indexes (schema_index.ts). */
   readonly schemaIndex = new SchemaIndexCaches();
   /** Test seam: a get_context deadline shorter than the default. */
   searchDeadlineMs: number | undefined;
   readonly querySlots: QuerySlots;
   readonly queryModelResolver: QueryModelResolver | undefined;

   constructor(options: RetrievalRegistryOptions = {}) {
      this.querySlots = options.querySlots ?? new QuerySlots();
      this.queryModelResolver = options.queryModelResolver;
   }
}

// Built on first use, not at import. The classes it builds live in leaf
// modules that do not import this one (src/import_cycles.spec.ts keeps it so);
// the modules that read the registry are named here by type only.
let current: RetrievalRegistry | undefined;

/** The registry retrieval's modules read: the RetrievalService's, or a default one. */
export function retrievalRegistry(): RetrievalRegistry {
   return (current ??= new RetrievalRegistry());
}

/**
 * Make `next` the registry retrieval reads, and return the one it replaces.
 * RetrievalService calls it with the registry it is given; a spec calls it
 * with a fresh one for a clean slate, and puts the previous one back after.
 */
export function installRetrievalRegistry(
   next: RetrievalRegistry | undefined,
): RetrievalRegistry | undefined {
   const previous = current;
   current = next;
   return previous;
}

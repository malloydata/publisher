// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * When the fcj index of a package is built, and what a request sees meanwhile.
 *
 * One build per Package instance, queued at load (wired to the
 * package-loaded hook) and, failing that, by the first request. A request
 * that finds the build running answers `retrieval: "indexing"` and never
 * waits on it. A failed build is retried
 * by a request that arrives after PROVIDER_FAILURE_COOLDOWN_MS. A reload is a
 * new Package, so it gets a new build; the content caches make that cheap.
 *
 * A build whose value reads were refused for a reason that clears on its own
 * (the query cap, a timeout; see ValueBuildReport.failed) is ready but
 * missing those values. It keeps serving, and a request that arrives after
 * PROVIDER_FAILURE_COOLDOWN_MS rebuilds it in the background, so the values
 * are not left out until the next reload. At most one such rebuild runs per
 * cool-down, and only while requests arrive.
 */

import { activeLlmSettings, getChatModel } from "../../providers/active";
import { createChatModel } from "../../providers/registry";
import type { ChatModel, LlmSettings } from "../../providers/types";
import { deleteEngineRows } from "./catalog_store";
import {
   embeddingConfigured,
   getEmbeddingProvider,
} from "../../service/embedding_provider";
import type { EnvironmentStore } from "../../service/environment_store";
import type { Package } from "../../service/package";
import {
   type FcjJudgeLlm,
   fcjSettingsOf,
} from "../../service/package_retrieval";
import { logger } from "../../logger";
import { PROVIDER_FAILURE_COOLDOWN_MS } from "../tools/retrieval_shared";
import { embeddingSyncQueue } from "../tools/embedding_sync_queue";
import { SyncRequestBudget } from "../tools/get_context_llm";
import { getPackageIndex } from "../tools/package_entities";
import { resolveSourceSummarySettings } from "../tools/source_summary_settings";
import {
   currentSummaryHashes,
   loadSourceSummaries,
   resolveSourceSummaries,
} from "../tools/source_summaries";
import { buildFcjIndex, type FcjIndex } from "./index_build";
import type { VectorProgress } from "./vectors";
import type {
   IndexValueFetcher,
   ValueBuildReport,
   ValueIndex,
} from "./values_index";

export type FcjState =
   | {
        status: "building";
        startedAt: number;
        /** Set once the vectors step starts. */
        progress?: VectorProgress;
        /** When a vector this build keeps was last written. */
        lastSyncedAt?: number;
     }
   | {
        status: "ready";
        index: FcjIndex;
        /** When this index's vectors were last written; unset when it has none. */
        lastSyncedAt?: number;
        /** Set when value reads failed: when to build again for them. */
        rebuildAt?: number;
        /**
         * Builds in a row that left values unread. Each doubles the wait
         * before the next ({@link valueRetryDelayMs}).
         */
        valueFailures?: number;
        /** A rebuild for failed value reads is queued; this index serves until it lands. */
        rebuilding?: boolean;
     }
   | { status: "error"; message: string; failedAt: number };

const states = new WeakMap<Package, FcjState>();

let valueFetcherOverride: IndexValueFetcher | undefined;
/** Test seam: stand in for the warehouse when the value index reads data. */
export function _setFcjValueFetcherForTests(f: IndexValueFetcher | undefined) {
   valueFetcherOverride = f;
}

export function fcjStateOf(pkg: Package): FcjState | undefined {
   return states.get(pkg);
}

/** The chat model the index-time steps and the judge use, or null when the LLM is off. */
function fcjChat(): ChatModel | null {
   try {
      return getChatModel();
   } catch (error) {
      logger.warn("[get_context fcj] The LLM client could not be built", {
         error: error instanceof Error ? error.message : String(error),
      });
      return null;
   }
}

// Cached on the settings, as getChatModel is, so the model's cooldown state
// survives between requests.
let judgeCached: { fingerprint: string; model: ChatModel } | null = null;

/**
 * The judge's chat model under `retrieval.fcj.judgeLlm`: the operator's
 * `retrieval.llm` (provider, host, key, timeouts and limits) with the model
 * name the package gave. Null `judgeLlm` means {@link fcjChat}. Throws when
 * that client cannot be built: a judge that was asked for must not be
 * skipped quietly, so the request fails instead.
 */
export function fcjJudgeChat(judgeLlm: FcjJudgeLlm | null): ChatModel | null {
   if (!judgeLlm) return fcjChat();
   const base = activeLlmSettings();
   if (!base) return null;
   const settings: LlmSettings = { ...base, model: judgeLlm.model };
   const fingerprint = JSON.stringify(settings);
   if (!judgeCached || judgeCached.fingerprint !== fingerprint) {
      try {
         judgeCached = { fingerprint, model: createChatModel(settings) };
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         throw new Error(
            `retrieval.fcj.judgeLlm (${base.provider}/${judgeLlm.model}) could not be built: ${message}`,
         );
      }
   }
   return judgeCached.model;
}

/**
 * Queue a build of `pkg`'s index unless one is running, done, or failed
 * inside the cool-down. Never throws; the outcome lands in the state.
 */
export function kickFcjBuild(args: {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   pkg: Package;
}): void {
   const { environmentName, packageName, pkg } = args;
   const state = states.get(pkg);
   if (state?.status === "building") return;
   if (state?.status === "ready") {
      if (
         state.rebuilding ||
         state.rebuildAt === undefined ||
         Date.now() < state.rebuildAt
      ) {
         return;
      }
      rebuildForValues(args, state);
      return;
   }
   if (
      state?.status === "error" &&
      Date.now() - state.failedAt < PROVIDER_FAILURE_COOLDOWN_MS
   ) {
      return;
   }
   const building: FcjState = { status: "building", startedAt: Date.now() };
   states.set(pkg, building);
   void embeddingSyncQueue.enqueue(async () => {
      // A package unloaded, deleted or replaced while this waited must stay gone.
      if (!stillServed(args)) {
         states.delete(pkg);
         return;
      }
      try {
         const index = await buildFor(
            args,
            undefined,
            (progress, lastWrittenAt) => {
               building.progress = progress;
               building.lastSyncedAt = lastWrittenAt;
            },
         );
         if (await droppedDuringBuild(args)) return;
         states.set(
            pkg,
            readyState(
               index,
               environmentName,
               packageName,
               0,
               building.lastSyncedAt,
            ),
         );
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         logger.warn("[get_context fcj] The search index could not be built", {
            environmentName,
            packageName,
            error: message,
         });
         states.set(pkg, { status: "error", message, failedAt: Date.now() });
      }
   });
}

/**
 * Whether `pkg` is still the package served under its name. A store that
 * cannot say (a spec's stand-in, with no peekEnvironment) counts as served;
 * the real store always can, so an unloaded, deleted or replaced package
 * answers false.
 */
function stillServed(args: Parameters<typeof kickFcjBuild>[0]): boolean {
   const store = args.environmentStore;
   if (typeof store.peekEnvironment !== "function") return true;
   const environment = store.peekEnvironment(args.environmentName);
   return environment?.peekPackage?.(args.packageName) === args.pkg;
}

/**
 * After a build: when the package went away while it ran, drop its state and
 * remove the rows the build wrote, since the delete's own cleanup may have
 * run before them. The stored values are warehouse data and must not
 * outlive the package.
 */
async function droppedDuringBuild(
   args: Parameters<typeof kickFcjBuild>[0],
): Promise<boolean> {
   if (stillServed(args)) return false;
   states.delete(args.pkg);
   try {
      await deleteEngineRows(
         args.environmentStore.storageManager.getDuckDbConnection(),
         args.environmentName,
         args.packageName,
      );
   } catch (error) {
      logger.warn(
         "[get_context fcj] The rows of a package removed during its index build could not be deleted",
         {
            environmentName: args.environmentName,
            packageName: args.packageName,
            error: error instanceof Error ? error.message : String(error),
         },
      );
   }
   return true;
}

/** The longest wait between builds for unread values. */
export const VALUE_RETRY_MAX_DELAY_MS = 3_600_000;

/**
 * The wait before building again for unread values, after `failures` builds
 * in a row left some unread: the provider cool-down, doubled each time, up to
 * an hour. A dimension that always times out is then read about once an hour
 * rather than once a minute.
 */
export function valueRetryDelayMs(failures: number): number {
   return Math.min(
      VALUE_RETRY_MAX_DELAY_MS,
      PROVIDER_FAILURE_COOLDOWN_MS * 2 ** Math.max(0, failures - 1),
   );
}

/**
 * The ready state for a new index, due for a rebuild when value reads failed.
 * `failuresBefore` counts the builds in a row before this one that left
 * values unread.
 */
function readyState(
   index: FcjIndex,
   environmentName: string,
   packageName: string,
   failuresBefore = 0,
   lastWrite?: number,
): FcjState {
   // A build that never reported a write time (no vectors step) falls back to
   // when its vectors went live.
   const lastSyncedAt =
      index.vectors.size > 0 ? (lastWrite ?? Date.now()) : undefined;
   const synced = lastSyncedAt !== undefined ? { lastSyncedAt } : {};
   const values = index.report.values;
   const failed = "failed" in values ? values.failed : [];
   if (failed.length === 0) return { status: "ready", index, ...synced };
   const valueFailures = failuresBefore + 1;
   const retryAfterMs = valueRetryDelayMs(valueFailures);
   logger.warn(
      "[get_context fcj] Some #(index) values could not be read; the index serves without them and reads them again after a cool-down",
      {
         environmentName,
         packageName,
         failed,
         retryAfterMs,
      },
   );
   return {
      status: "ready",
      index,
      ...synced,
      rebuildAt: Date.now() + retryAfterMs,
      valueFailures,
   };
}

/**
 * Build a ready index again because some of its value reads failed. Only
 * the dimensions whose reads failed are read; the rest keep their values.
 * The old index serves until the new one is ready. If the rebuild fails, the
 * old one stays, due again after a longer cool-down.
 */
function rebuildForValues(
   args: Parameters<typeof kickFcjBuild>[0],
   state: Extract<FcjState, { status: "ready" }>,
): void {
   const { environmentName, packageName, pkg } = args;
   states.set(pkg, { ...state, rebuilding: true });
   void embeddingSyncQueue.enqueue(async () => {
      if (!stillServed(args)) {
         states.delete(pkg);
         return;
      }
      try {
         const previous = state.index.report.values;
         const index = await buildFor(
            args,
            "failed" in previous
               ? { index: state.index.values, report: previous }
               : undefined,
         );
         if (await droppedDuringBuild(args)) return;
         states.set(
            pkg,
            readyState(
               index,
               environmentName,
               packageName,
               state.valueFailures ?? 0,
            ),
         );
      } catch (error) {
         logger.warn(
            "[get_context fcj] The search index could not be built again for its missing values; the earlier index keeps serving",
            {
               environmentName,
               packageName,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         const valueFailures = (state.valueFailures ?? 0) + 1;
         states.set(pkg, {
            status: "ready",
            index: state.index,
            ...(state.lastSyncedAt !== undefined
               ? { lastSyncedAt: state.lastSyncedAt }
               : {}),
            rebuildAt: Date.now() + valueRetryDelayMs(valueFailures),
            valueFailures,
         });
      }
   });
}

async function buildFor(
   args: {
      environmentStore: EnvironmentStore;
      environmentName: string;
      packageName: string;
      pkg: Package;
   },
   previousValues?: { index: ValueIndex; report: ValueBuildReport },
   onVectorProgress?: (
      progress: VectorProgress,
      lastWrittenAt?: number,
   ) => void,
): Promise<FcjIndex> {
   const { environmentStore, environmentName, packageName, pkg } = args;
   const pkgIndex = await getPackageIndex(
      environmentStore,
      environmentName,
      packageName,
   );
   const db = environmentStore.storageManager.getDuckDbConnection();
   const retrieval = pkg.getRetrievalSettings?.();
   const settings = fcjSettingsOf(retrieval);
   const provider = embeddingConfigured() ? getEmbeddingProvider() : null;
   const llmSettings = activeLlmSettings();
   const chat = llmSettings ? fcjChat() : null;
   // One retrieval.llm.maxCallsPerSync for the whole build: summaries, doc
   // parts and questions all draw on it.
   const budget = new SyncRequestBudget(llmSettings?.maxCallsPerSync ?? 0);
   const shouldStop = () => !stillServed(args);

   // Source summaries: written by the summary step when the package has them
   // on, then read back. A failure to write them is reported, not fatal:
   // a summary only adds a line to a source's search text.
   const summarySettings = resolveSourceSummarySettings(pkg);
   let storedSummaries;
   if (summarySettings) {
      try {
         await resolveSourceSummaries({
            db,
            environmentName,
            packageName,
            entities: pkgIndex.directEntities,
            settings: summarySettings,
            budget,
            shouldStop,
         });
      } catch (error) {
         logger.warn(
            "[get_context fcj] Source summaries could not be written; building with what is stored",
            {
               environmentName,
               packageName,
               error: error instanceof Error ? error.message : String(error),
            },
         );
      }
      storedSummaries = await loadSourceSummaries(
         db,
         environmentName,
         packageName,
         currentSummaryHashes(pkgIndex.directEntities, summarySettings),
      );
   }
   return buildFcjIndex({
      pkgIndex,
      db,
      provider,
      environmentName,
      packageName,
      settings,
      ...(chat && llmSettings
         ? {
              llm: {
                 chat,
                 concurrency: Math.max(1, llmSettings.concurrency),
                 maxCallsPerSync: llmSettings.maxCallsPerSync,
                 ...(retrieval?.prompts.docParts
                    ? { docPartsInstructions: retrieval.prompts.docParts.text }
                    : {}),
                 ...(retrieval?.prompts.questions
                    ? {
                         questionsInstructions:
                            retrieval.prompts.questions.text,
                      }
                    : {}),
              },
           }
         : {}),
      ...(storedSummaries ? { storedSummaries } : {}),
      valueRead: {
         environmentStore,
         ...(valueFetcherOverride ? { fetcher: valueFetcherOverride } : {}),
      },
      budget,
      ...(previousValues ? { previousValues } : {}),
      // A package removed mid-build stops reading values and embedding.
      shouldStop,
      ...(onVectorProgress ? { onVectorProgress } : {}),
   });
}

/**
 * How much of `pkg`'s index holds vectors: while building, as far as the
 * vectors step has got (zero before it starts); once ready, what the index
 * serves. Zero with no build, or after a failed one.
 */
export function fcjVectorProgressOf(pkg: Package): VectorProgress {
   const state = states.get(pkg);
   if (state?.status === "building" && state.progress) return state.progress;
   if (state?.status !== "ready") {
      return {
         totalRows: 0,
         embeddedRows: 0,
         totalEntities: 0,
         embeddedEntities: 0,
      };
   }
   const { index } = state;
   let embeddedRows = 0;
   for (const facets of index.vectors.values()) embeddedRows += facets.length;
   return {
      totalRows: index.report.vectors.rows,
      embeddedRows,
      // Every searched node (one that is not a folded copy) wants a row.
      totalEntities: [...index.catalog.nodes.values()].filter((n) => !n.copyOf)
         .length,
      embeddedEntities: index.vectors.size,
   };
}

/** When the package's vectors were last written, as ISO 8601; undefined before any were. */
export function fcjLastSyncedAtOf(pkg: Package): string | undefined {
   const state = states.get(pkg);
   const at =
      state?.status === "building" || state?.status === "ready"
         ? state.lastSyncedAt
         : undefined;
   return at === undefined ? undefined : new Date(at).toISOString();
}

/** The state as the REST Package resource reports it, under `embeddingIndex.fcj`. */
export function fcjStatusOf(pkg: Package): Record<string, unknown> {
   const state = states.get(pkg);
   if (!state) return { engine: "fcj", status: "indexing" };
   if (state.status === "building") {
      return {
         engine: "fcj",
         status: "indexing",
         startedAt: new Date(state.startedAt).toISOString(),
      };
   }
   if (state.status === "error") {
      return {
         engine: "fcj",
         status: "error",
         lastError: { message: state.message },
      };
   }
   return {
      engine: "fcj",
      status: "ready",
      build: state.index.report,
      ...(state.rebuildAt !== undefined
         ? { valuesRebuildAt: new Date(state.rebuildAt).toISOString() }
         : {}),
   };
}

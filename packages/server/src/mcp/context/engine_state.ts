// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * When the fcj index of a package is built, and what a request sees meanwhile.
 *
 * One build per Package instance, queued at load (wired to the same hook as
 * the shipped embedding sync) and, failing that, by the first request. A
 * request that finds the build running answers `retrieval: "indexing"`, as
 * the shipped engine does, and never waits on it. A failed build is retried
 * by a request that arrives after PROVIDER_FAILURE_COOLDOWN_MS. A reload is a
 * new Package, so it gets a new build; the content caches make that cheap.
 */

import { activeLlmSettings, getChatModel } from "../../providers/active";
import { createChatModel } from "../../providers/registry";
import type { ChatModel, LlmSettings } from "../../providers/types";
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
import { PROVIDER_FAILURE_COOLDOWN_MS } from "../tools/embedding_index";
import { embeddingSyncQueue } from "../tools/embedding_sync_queue";
import { SyncRequestBudget } from "../tools/get_context_llm";
import { getPackageIndex } from "../tools/get_context_tool";
import { indexSettingsOf } from "../tools/index_settings";
import {
   currentSummaryHashes,
   loadSourceSummaries,
   resolveSourceSummaries,
} from "../tools/source_summaries";
import { buildFcjIndex, type FcjIndex } from "./index_build";
import type { IndexValueFetcher } from "./values_index";

export type FcjState =
   | { status: "building"; startedAt: number }
   | { status: "ready"; index: FcjIndex }
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
export function fcjChat(): ChatModel | null {
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
 * The judge's chat model under `retrieval.fcj.judgeLlm`: that provider and
 * model, with `retrieval.llm`'s timeouts and limits. Null `judgeLlm` means
 * {@link fcjChat}. Throws when `judgeLlm` is set and its client cannot be
 * built (a missing key): a judge that was asked for must not be skipped
 * quietly, so the request fails instead.
 */
export function fcjJudgeChat(judgeLlm: FcjJudgeLlm | null): ChatModel | null {
   if (!judgeLlm) return fcjChat();
   const base = activeLlmSettings();
   if (!base) return null;
   const needsKey =
      judgeLlm.provider !== "ollama" && judgeLlm.provider !== "vertex";
   const apiKey = needsKey
      ? process.env.JUDGE_LLM_API_KEY?.trim() ||
        process.env.LLM_API_KEY?.trim() ||
        undefined
      : undefined;
   const settings: LlmSettings = {
      ...base,
      provider: judgeLlm.provider,
      model: judgeLlm.model,
      baseUrl: judgeLlm.baseUrl,
      projectId: judgeLlm.projectId,
      location: judgeLlm.location,
      apiKey,
   };
   const fingerprint = JSON.stringify(settings);
   if (!judgeCached || judgeCached.fingerprint !== fingerprint) {
      try {
         judgeCached = { fingerprint, model: createChatModel(settings) };
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         throw new Error(
            `retrieval.fcj.judgeLlm (${judgeLlm.provider}/${judgeLlm.model}) could not be built: ${message.replace("LLM_API_KEY", "JUDGE_LLM_API_KEY or LLM_API_KEY")}`,
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
   const { environmentStore, environmentName, packageName, pkg } = args;
   const state = states.get(pkg);
   if (state?.status === "building" || state?.status === "ready") return;
   if (
      state?.status === "error" &&
      Date.now() - state.failedAt < PROVIDER_FAILURE_COOLDOWN_MS
   ) {
      return;
   }
   states.set(pkg, { status: "building", startedAt: Date.now() });
   void embeddingSyncQueue.enqueue(async () => {
      // A package unloaded or replaced while this waited must stay gone.
      const served = environmentStore
         .peekEnvironment?.(environmentName)
         ?.peekPackage?.(packageName);
      if (served !== undefined && served !== pkg) {
         states.delete(pkg);
         return;
      }
      try {
         const index = await buildFor(args);
         states.set(pkg, { status: "ready", index });
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

async function buildFor(args: {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   pkg: Package;
}): Promise<FcjIndex> {
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

   // Source summaries: written by the shipped step when the package has them
   // on, then read back. A failure to write them is reported, not fatal:
   // a summary only adds a line to a source's search text.
   const summarySettings = indexSettingsOf(pkg).sourceSummary;
   let storedSummaries;
   if (summarySettings) {
      try {
         await resolveSourceSummaries({
            db,
            environmentName,
            packageName,
            entities: pkgIndex.directEntities,
            settings: summarySettings,
            budget: new SyncRequestBudget(summarySettings.maxCallsPerSync),
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
   });
}

/** The state as the REST Package resource reports it, beside the shipped status. */
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
   return { engine: "fcj", status: "ready", build: state.index.report };
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The settings of the request-time LLM stages (refine, rerank and source match): the
 * package's `retrieval` block in publisher.json combined with what the
 * operator allows in publisher.config.json. Resolved once per request, before
 * any stage runs.
 */

import { DEFAULT_REFINE_INSTRUCTIONS } from "../../prompts/refine";
import { DEFAULT_RERANK_INSTRUCTIONS } from "../../prompts/rerank";
import { DEFAULT_SOURCE_MATCH_INSTRUCTIONS } from "../../prompts/source_match";
import { activeLlmSettings, getChatModel } from "../../providers/active";
import type { ChatModel } from "../../providers/types";
import type { Package } from "../../service/package";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   refineSettingsOf,
   rerankSettingsOf,
   sourceMatchSettingsOf,
   type RefineLevelName,
} from "../../service/package_retrieval";
import { StageError, type LlmMeter } from "./get_context_llm";

/** What a stage needs to run. Absent means the stage is off for this request. */
export interface LlmStageSettings {
   /** Batches in flight at once, from `retrieval.llm.concurrency`. */
   concurrency: number;
   refine?: {
      chat: ChatModel;
      minLevel: RefineLevelName;
      /** The package's prompt file, or the built-in instructions. */
      instructions: string;
   };
   rerank?: {
      chat: ChatModel;
      topSources: number;
      instructions: string;
   };
   sourceMatch?: {
      chat: ChatModel;
      instructions: string;
   };
}

/**
 * Resolve both stages. Returns undefined when no LLM is configured (every
 * stage is off, nothing else changes). Throws a StageError when an LLM is
 * configured but its client cannot be built, which the caller reports instead
 * of answering without the stage. The chat model each stage gets is counted
 * by `meter`.
 */
export function resolveLlmStages(
   pkg: Package | undefined,
   meter: LlmMeter,
): LlmStageSettings | undefined {
   const llm = activeLlmSettings();
   if (!llm) return undefined;
   const retrieval =
      (pkg as Partial<Package> | undefined)?.getRetrievalSettings?.() ??
      DEFAULT_PACKAGE_RETRIEVAL;
   const refine = refineSettingsOf(retrieval);
   const rerank = rerankSettingsOf(retrieval);
   const sourceMatch = sourceMatchSettingsOf(retrieval);
   if (
      refine.enabled === false &&
      rerank.enabled === false &&
      sourceMatch.enabled === false
   ) {
      return undefined;
   }
   let chat: ChatModel | null;
   try {
      chat = getChatModel();
   } catch (error) {
      throw new StageError(
         "llm",
         `the configured LLM client could not be built: ${error instanceof Error ? error.message : String(error)}. ` +
            `Fix: check retrieval.llm in publisher.config.json and LLM_API_KEY.`,
         error,
      );
   }
   if (!chat) return undefined;
   const metered = meter.wrap(chat);
   return {
      concurrency: Math.max(1, llm.concurrency),
      ...(refine.enabled !== false
         ? {
              refine: {
                 chat: metered,
                 minLevel: refine.minLevel,
                 instructions:
                    retrieval.prompts.refine?.text ??
                    DEFAULT_REFINE_INSTRUCTIONS,
              },
           }
         : {}),
      ...(rerank.enabled !== false
         ? {
              rerank: {
                 chat: metered,
                 topSources: rerank.topSources,
                 instructions:
                    retrieval.prompts.rerank?.text ??
                    DEFAULT_RERANK_INSTRUCTIONS,
              },
           }
         : {}),
      ...(sourceMatch.enabled !== false
         ? {
              sourceMatch: {
                 chat: metered,
                 instructions:
                    retrieval.prompts.sourceMatch?.text ??
                    DEFAULT_SOURCE_MATCH_INSTRUCTIONS,
              },
           }
         : {}),
   };
}

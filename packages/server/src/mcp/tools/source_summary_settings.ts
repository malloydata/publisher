// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What decides whether a package's sources get LLM summaries: the package's own
 * `retrieval.sourceSummary` (publisher.json) combined with what the operator
 * allows (publisher.config.json). Resolved once per call from the loaded
 * package, never derived in a loop.
 */

import { activeLlmSettings, getChatModel } from "../../providers/active";
import {
   DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS,
   sourceSummaryPromptHash,
} from "../../prompts/source_summary";
import { logger } from "../../logger";
import type { Package } from "../../service/package";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   sourceSummarySettingsOf,
} from "../../service/package_retrieval";
import {
   SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS,
   type SourceSummarySettings,
} from "./source_summaries";

/**
 * The summary settings for a package instance, or undefined when summaries
 * will not be generated: the package turns them off, or the operator has no
 * LLM configured (which is how `auto` quietly acts as off). A stand-in
 * without the accessor gets the defaults.
 */
export function resolveSourceSummarySettings(
   pkg: Package | undefined,
): SourceSummarySettings | undefined {
   const retrieval =
      (pkg as Partial<Package> | undefined)?.getRetrievalSettings?.() ??
      DEFAULT_PACKAGE_RETRIEVAL;
   if (sourceSummarySettingsOf(retrieval).enabled === false) return undefined;
   const llm = activeLlmSettings();
   if (!llm) return undefined;
   let chat;
   try {
      chat = getChatModel();
   } catch (error) {
      logger.warn(
         "[get_context] The LLM is configured but its client could not be built; source summaries are off",
         { error: error instanceof Error ? error.message : String(error) },
      );
      return undefined;
   }
   if (!chat) return undefined;
   const instructions =
      retrieval.prompts.sourceSummary?.text ??
      DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS;
   return {
      chat,
      modelId: `${llm.provider}/${llm.model}`,
      instructions,
      promptHash: sourceSummaryPromptHash(instructions),
      concurrency: llm.concurrency,
      maxCallsPerSync: llm.maxCallsPerSync,
      // A local model runs at a small default context window and cuts a longer
      // prompt without saying so; see SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS.
      ...(llm.provider === "ollama"
         ? { maxPromptChars: SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS }
         : {}),
   };
}

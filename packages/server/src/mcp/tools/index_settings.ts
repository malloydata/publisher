// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The settings that decide what a package's semantic index contains: the
 * package's own `retrieval` block (publisher.json) combined with what the
 * operator allows (publisher.config.json). Resolved once per call from the
 * loaded package, never derived in a loop.
 */

import {
   DEFAULT_KEYPHRASE_INSTRUCTIONS,
   keyphrasePromptHash,
} from "../../prompts/keyphrase";
import { activeLlmSettings, getChatModel } from "../../providers/active";
import {
   DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS,
   sourceSummaryPromptHash,
} from "../../prompts/source_summary";
import { getEgressPreset } from "../../retrieval_config";
import { logger } from "../../logger";
import type { Package } from "../../service/package";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   sourceSummarySettingsOf,
   type PackageRepresentation,
} from "../../service/package_retrieval";
import type { KeyphraseSettings } from "./keyphrases";
import {
   SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS,
   type SourceSummarySettings,
} from "./source_summaries";

export interface IndexSettings {
   representation: PackageRepresentation;
   /**
    * Present when keyphrases will be generated: the package asks for them
    * (`auto` or `always`) AND the operator has an LLM configured. Absent
    * otherwise, which is how `auto` quietly acts as `never` with no LLM.
    */
   keyphrases?: KeyphraseSettings;
   /**
    * Present when source summaries will be generated: the package does not turn
    * them off AND the operator has an LLM configured. Absent otherwise, which
    * is how `auto` quietly acts as off with no LLM.
    */
   sourceSummary?: SourceSummarySettings;
   /**
    * A string that changes whenever a setting that alters rows changes. It is
    * folded into the readiness fingerprint, so editing `publisher.json`
    * (and reloading) makes the package `indexing` until the sync has applied
    * the change, and a reload that changed nothing keeps its warm index.
    */
   key: string;
}

/**
 * The index settings for a package instance. A stand-in without the accessor
 * (a test double, a package from before the accessor existed) gets the
 * defaults.
 */
export function indexSettingsOf(pkg: Package | undefined): IndexSettings {
   const retrieval =
      (pkg as Partial<Package> | undefined)?.getRetrievalSettings?.() ??
      DEFAULT_PACKAGE_RETRIEVAL;
   const keyphrases = keyphraseSettingsFor(retrieval);
   const sourceSummary = sourceSummarySettingsFor(retrieval);
   return {
      representation: retrieval.representation,
      keyphrases,
      sourceSummary,
      key: [
         retrieval.representation,
         keyphrases
            ? [
                 "kp",
                 keyphrases.mode,
                 keyphrases.promptHash,
                 keyphrases.modelId,
                 keyphrases.egress,
              ].join(":")
            : "kp:off",
         // Added only when on, so a package without summaries keeps the key
         // it always had.
         ...(sourceSummary
            ? [
                 ["ss", sourceSummary.promptHash, sourceSummary.modelId].join(
                    ":",
                 ),
              ]
            : []),
      ].join("|"),
   };
}

function sourceSummarySettingsFor(
   retrieval: typeof DEFAULT_PACKAGE_RETRIEVAL,
): SourceSummarySettings | undefined {
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

function keyphraseSettingsFor(
   retrieval: typeof DEFAULT_PACKAGE_RETRIEVAL,
): KeyphraseSettings | undefined {
   if (retrieval.keyphrases === "never") return undefined;
   const llm = activeLlmSettings();
   if (!llm) return undefined;
   let chat;
   try {
      chat = getChatModel();
   } catch (error) {
      // Settings were validated at startup, so this is not expected. Say so
      // rather than dropping keyphrases without a word.
      logger.warn(
         "[get_context] The LLM is configured but its client could not be built; keyphrases are off",
         { error: error instanceof Error ? error.message : String(error) },
      );
      return undefined;
   }
   if (!chat) return undefined;
   const instructions =
      retrieval.prompts.keyphrase?.text ?? DEFAULT_KEYPHRASE_INSTRUCTIONS;
   return {
      mode: retrieval.keyphrases,
      chat,
      modelId: `${llm.provider}/${llm.model}`,
      instructions,
      promptHash: keyphrasePromptHash(instructions),
      egress: getEgressPreset(),
      concurrency: llm.concurrency,
      maxCallsPerSync: llm.maxCallsPerSync,
   };
}

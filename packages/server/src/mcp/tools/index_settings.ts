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
import { getEgressPreset } from "../../retrieval_config";
import { logger } from "../../logger";
import type { Package } from "../../service/package";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   type PackageRepresentation,
} from "../../service/package_retrieval";
import type { KeyphraseSettings } from "./keyphrases";

export interface IndexSettings {
   representation: PackageRepresentation;
   /**
    * Present when keyphrases will be generated: the package asks for them
    * (`auto` or `always`) AND the operator has an LLM configured. Absent
    * otherwise, which is how `auto` quietly acts as `never` with no LLM.
    */
   keyphrases?: KeyphraseSettings;
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
   return {
      representation: retrieval.representation,
      keyphrases,
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
      ].join("|"),
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

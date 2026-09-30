// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LlmConfig } from "../config";
import type { RetrievalConfig } from "./retrieval_config";

export type LlmStage =
   | "refine"
   | "rerank"
   | "keyphrase"
   | "summary"
   | "valueRefine";

/**
 * Whether the LLM stages can run at all: an endpoint is configured in the
 * environment and `retrieval.llm.enabled` has not switched it off.
 */
export function llmAvailable(
   config: RetrievalConfig,
   llm: LlmConfig | null,
): boolean {
   return llm !== null && config.llm.enabled !== false;
}

/** The model for a stage: its own, else the shared one, else the env's. */
export function resolveStageModel(
   config: RetrievalConfig,
   llm: LlmConfig | null,
   stage: LlmStage,
): string | undefined {
   return config.llm.models[stage] ?? config.llm.model ?? llm?.model;
}

/** The stages the config asks for. */
export function enabledStages(config: RetrievalConfig): LlmStage[] {
   const stages: LlmStage[] = [];
   if (config.refine.enabled) stages.push("refine");
   if (config.rerank.enabled) stages.push("rerank");
   if (config.enrichment.enabled && config.enrichment.keyphrase.mode !== "never")
      stages.push("keyphrase");
   if (config.enrichment.enabled && config.enrichment.sourceSummary.enabled)
      stages.push("summary");
   if (config.dimensionalValues.mode !== "off" && config.dimensionalValues.refine.enabled)
      stages.push("valueRefine");
   return stages;
}

export interface BootCheck {
   /** Configuration mistakes that must stop the boot. */
   errors: string[];
   /** Things worth telling the operator that do not stop it. */
   warnings: string[];
}

/**
 * Cross-checks between the tuning block and the environment, run once at boot.
 *
 * A stage asked for with no LLM endpoint configured is a warning, not an
 * error: one config file is often shared across environments, and every stage
 * is fail-soft anyway (it reports `skipped:no_llm`). A stage that WILL run
 * but has no model to call is an error, because the failure would otherwise
 * be a 400 on the first request, and the fix is one line.
 */
export function checkRetrievalAgainstEnvironment(
   config: RetrievalConfig,
   llm: LlmConfig | null,
   embeddingConfigured: boolean,
): BootCheck {
   const errors: string[] = [];
   const warnings: string[] = [];
   const stages = enabledStages(config);

   if (stages.length > 0 && !llmAvailable(config, llm)) {
      warnings.push(
         `retrieval stages ${stages.join(", ")} are enabled but no LLM is available (set LLM_API_BASE or LLM_API_KEY, and leave retrieval.llm.enabled at "auto" or true); they will be skipped.`,
      );
   }
   if (llmAvailable(config, llm)) {
      for (const stage of stages) {
         if (!resolveStageModel(config, llm, stage)) {
            errors.push(
               `Invalid retrieval.llm.models.${stage}: expected the name of a model ${llm?.baseUrl} serves, got none. Fix: set LLM_MODEL=llama3.1:8b, or retrieval.llm.model, or retrieval.llm.models.${stage}.`,
            );
         }
      }
   }
   if (config.enrichment.enabled && !embeddingConfigured) {
      warnings.push(
         "retrieval.enrichment.enabled needs embeddings (set EMBEDDING_API_BASE or EMBEDDING_API_KEY) to have any effect; enrichment will not run.",
      );
   }
   if (config.dimensionalValues.mode !== "off" && !config.dimensionalValues.lexical && !embeddingConfigured) {
      warnings.push(
         "retrieval.dimensionalValues has neither an embedding provider nor the lexical arm on; value targets will find nothing.",
      );
   }
   return { errors, warnings };
}

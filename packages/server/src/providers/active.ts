// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getLlmSettings } from "../config";
import { loadedRetrievalConfig } from "../retrieval_config";
import { createChatModel } from "./registry";
import type { ChatModel, LlmSettings } from "./types";

// Cached on a settings fingerprint, as getEmbeddingProvider is, so a change to
// the environment is seen on the next call and the model's cooldown state
// survives between calls.
let cached: { fingerprint: string; model: ChatModel } | null = null;
let testOverride: { model: ChatModel | null; settings?: LlmSettings } | null =
   null;

/**
 * The LLM settings in force, or null when every LLM feature is off: no
 * `retrieval.llm.provider`, or a provider that needs `LLM_API_KEY` and has
 * none. Nothing errors when it is off.
 */
export function activeLlmSettings(): LlmSettings | null {
   if (testOverride)
      return testOverride.model ? (testOverride.settings ?? FAKE) : null;
   return getLlmSettings(loadedRetrievalConfig()?.llm);
}

const FAKE: LlmSettings = {
   provider: "openai-compatible",
   model: "test-model",
   timeoutMs: 30_000,
   concurrency: 4,
   maxCallsPerSync: 300,
};

/** Whether an LLM is configured. Cheap; safe to call on any path. */
export function llmConfigured(): boolean {
   return activeLlmSettings() !== null;
}

/** The process-wide chat model, or null when the LLM is off. */
export function getChatModel(): ChatModel | null {
   if (testOverride) return testOverride.model;
   const settings = getLlmSettings(loadedRetrievalConfig()?.llm);
   if (!settings) {
      cached = null;
      return null;
   }
   const fingerprint = JSON.stringify(settings);
   if (!cached || cached.fingerprint !== fingerprint) {
      cached = { fingerprint, model: createChatModel(settings) };
   }
   return cached.model;
}

/** Test seam: force the chat model (or null) and the settings that go with it. */
export function _setChatModelForTests(
   model: ChatModel | null,
   settings?: Partial<LlmSettings>,
): void {
   testOverride = {
      model,
      settings: settings ? { ...FAKE, ...settings } : undefined,
   };
}

export function _clearChatModelForTests(): void {
   testOverride = null;
   cached = null;
}

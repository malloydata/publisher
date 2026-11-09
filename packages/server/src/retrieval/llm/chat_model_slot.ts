// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getLlmSettings } from "../../config";
import { loadedRetrievalConfig } from "../config";
import { createChatModel } from "./registry";
import type { ChatModel, LlmSettings } from "./types";

const FAKE: LlmSettings = {
   provider: "openai-compatible",
   model: "test-model",
   timeoutMs: 30_000,
   concurrency: 4,
   maxCallsPerSync: 300,
   maxCallsPerRequest: 20,
};

/**
 * The chat model in force, owned by the RetrievalRegistry. Cached on a
 * settings fingerprint, as getEmbeddingProvider is, so a change to the
 * environment is seen on the next call and the model's cooldown state
 * survives between calls.
 */
export class ChatModelSlot {
   private cached: { fingerprint: string; model: ChatModel } | null = null;
   private testOverride: {
      model: ChatModel | null;
      settings?: LlmSettings;
   } | null = null;

   /**
    * The LLM settings in force, or null when every LLM feature is off: no
    * `retrieval.llm.provider`, or a provider that needs `LLM_API_KEY` and has
    * none. Nothing errors when it is off.
    */
   settings(): LlmSettings | null {
      if (this.testOverride)
         return this.testOverride.model
            ? (this.testOverride.settings ?? FAKE)
            : null;
      return getLlmSettings(loadedRetrievalConfig()?.llm);
   }

   /** The process-wide chat model, or null when the LLM is off. */
   model(): ChatModel | null {
      if (this.testOverride) return this.testOverride.model;
      const settings = getLlmSettings(loadedRetrievalConfig()?.llm);
      if (!settings) {
         this.cached = null;
         return null;
      }
      const fingerprint = JSON.stringify(settings);
      if (!this.cached || this.cached.fingerprint !== fingerprint) {
         this.cached = { fingerprint, model: createChatModel(settings) };
      }
      return this.cached.model;
   }

   /** Test seam: force the chat model (or null) and the settings that go with it. */
   override(model: ChatModel | null, settings?: Partial<LlmSettings>): void {
      this.testOverride = {
         model,
         settings: settings ? { ...FAKE, ...settings } : undefined,
      };
   }

   /** Test seam: drop the override and the cached model. */
   reset(): void {
      this.testOverride = null;
      this.cached = null;
   }
}

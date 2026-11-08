// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { retrievalRegistry } from "../registry";
import type { ChatModel, LlmSettings } from "./types";

/**
 * The LLM settings in force, or null when every LLM feature is off: no
 * `retrieval.llm.provider`, or a provider that needs `LLM_API_KEY` and has
 * none. Nothing errors when it is off.
 */
export function activeLlmSettings(): LlmSettings | null {
   return retrievalRegistry().chat.settings();
}

/** Whether an LLM is configured. Cheap; safe to call on any path. */
export function llmConfigured(): boolean {
   return activeLlmSettings() !== null;
}

/** The process-wide chat model, or null when the LLM is off. */
export function getChatModel(): ChatModel | null {
   return retrievalRegistry().chat.model();
}

/** Test seam: force the chat model (or null) on the registry in force. */
export function _setChatModelForTests(
   model: ChatModel | null,
   settings?: Partial<LlmSettings>,
): void {
   retrievalRegistry().chat.override(model, settings);
}

/** Test seam: drop the override and the cached model of the registry in force. */
export function _clearChatModelForTests(): void {
   retrievalRegistry().chat.reset();
}

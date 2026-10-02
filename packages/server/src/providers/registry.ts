// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { EmbeddingProvider } from "../service/embedding_provider";
import { ChatModelImpl, type RawChat } from "./chat_model";
import {
   defaultOpenAiBaseUrl,
   OpenAiCompatibleChat,
} from "./openai_compatible";
import type {
   ChatModel,
   EmbeddingModel,
   EmbeddingSettings,
   LlmSettings,
   ProviderDeps,
} from "./types";

function trimSlash(url: string): string {
   return url.replace(/\/+$/, "");
}

function openAiBase(
   settings: { provider: LlmSettings["provider"]; baseUrl?: string },
   key: string,
): string {
   const base = settings.baseUrl ?? defaultOpenAiBaseUrl(settings.provider);
   if (!base) {
      throw new Error(
         `Invalid ${key}.baseUrl: provider "${settings.provider}" needs one. ` +
            `Fix: set ${key}.baseUrl to the server's /v1 URL.`,
      );
   }
   return trimSlash(base);
}

/** Build the chat model for resolved `retrieval.llm` settings. */
export function createChatModel(
   settings: LlmSettings,
   deps: ProviderDeps = {},
): ChatModel {
   const fetchFn = deps.fetchFn ?? fetch;
   let raw: RawChat;
   switch (settings.provider) {
      case "openai":
      case "openai-compatible":
      case "ollama":
         raw = new OpenAiCompatibleChat(
            settings.provider,
            settings.model,
            openAiBase(settings, "retrieval.llm"),
            settings.apiKey,
            fetchFn,
         );
         break;
      default:
         throw new Error(
            `Provider "${settings.provider}" has no chat adapter yet.`,
         );
   }
   return new ChatModelImpl(settings.provider, settings.model, raw, {
      timeoutMs: settings.timeoutMs,
      retry: deps.retry,
   });
}

/**
 * Build the embedding model for resolved `retrieval.embedding` settings. The
 * OpenAI-style providers are the existing {@link EmbeddingProvider}, so its
 * behaviour and test seam are unchanged.
 */
export function createEmbeddingModel(
   settings: EmbeddingSettings,
   deps: ProviderDeps = {},
): EmbeddingModel {
   switch (settings.provider) {
      case "openai":
      case "openai-compatible":
      case "ollama":
         return new EmbeddingProvider(
            {
               provider: settings.provider,
               // Ollama and some gateways ignore the key but the header is
               // still sent; a placeholder keeps the request well formed.
               apiKey: settings.apiKey ?? settings.provider,
               model: settings.model,
               baseUrl: openAiBase(settings, "retrieval.embedding"),
               dimensions: settings.dimensions,
               minSimilarity: settings.minSimilarity,
               queryPrefix: settings.queryPrefix,
               documentPrefix: settings.documentPrefix,
            },
            deps.fetchFn,
         );
      default:
         throw new Error(
            `Provider "${settings.provider}" has no embedding adapter yet.`,
         );
   }
}

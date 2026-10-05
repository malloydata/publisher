// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { EmbeddingProvider } from "../service/embedding_provider";
import { ANTHROPIC_BASE_URL, AnthropicChat } from "./anthropic";
import { ChatModelImpl, type RawChat } from "./chat_model";
import { BatchEmbeddingModel } from "./embedding_http";
import {
   GEMINI_BASE_URL,
   GEMINI_EMBED_MAX_BATCH,
   GeminiChat,
   geminiEmbedChunk,
} from "./google";
import {
   defaultOpenAiBaseUrl,
   OpenAiCompatibleChat,
} from "./openai_compatible";
import {
   EMBEDDING_PROVIDER_NAMES,
   type ChatModel,
   type EmbeddingModel,
   type EmbeddingSettings,
   type LlmSettings,
   type ProviderDeps,
} from "./types";
import {
   adcAccessToken,
   vertexEmbedMaxBatch,
   VertexChat,
   vertexEmbedChunk,
} from "./vertex";

function trimSlash(url: string): string {
   return url.replace(/\/+$/, "");
}

function requireKey(
   apiKey: string | undefined,
   provider: string,
   env: string,
): string {
   if (!apiKey) {
      throw new Error(
         `Provider "${provider}" needs an API key. Fix: set ${env} in the server's environment.`,
      );
   }
   return apiKey;
}

function requireVertex(
   s: { projectId?: string; location?: string },
   key: string,
): { projectId: string; location: string } {
   if (!s.projectId || !s.location) {
      throw new Error(
         `Invalid ${key}: provider "vertex" needs projectId and location. ` +
            `Fix: "${key}": { "provider": "vertex", "projectId": "my-project", "location": "us-central1" }`,
      );
   }
   return { projectId: s.projectId, location: s.location };
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
      case "anthropic":
         raw = new AnthropicChat(
            settings.model,
            trimSlash(settings.baseUrl ?? ANTHROPIC_BASE_URL),
            requireKey(settings.apiKey, "anthropic", "LLM_API_KEY"),
            fetchFn,
         );
         break;
      case "google":
         raw = new GeminiChat(
            settings.model,
            trimSlash(settings.baseUrl ?? GEMINI_BASE_URL),
            requireKey(settings.apiKey, "google", "LLM_API_KEY"),
            fetchFn,
         );
         break;
      case "vertex": {
         const { projectId, location } = requireVertex(
            settings,
            "retrieval.llm",
         );
         raw = new VertexChat(
            settings.model,
            projectId,
            location,
            deps.getAccessToken ?? adcAccessToken(),
            fetchFn,
         );
         break;
      }
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
      case "google":
         return new BatchEmbeddingModel(
            "google",
            settings,
            GEMINI_EMBED_MAX_BATCH,
            geminiEmbedChunk({
               model: settings.model,
               baseUrl: trimSlash(settings.baseUrl ?? GEMINI_BASE_URL),
               apiKey: requireKey(
                  settings.apiKey,
                  "google",
                  "EMBEDDING_API_KEY",
               ),
               dimensions: settings.dimensions,
               fetchFn: deps.fetchFn ?? fetch,
            }),
         );
      case "vertex": {
         const { projectId, location } = requireVertex(
            settings,
            "retrieval.embedding",
         );
         return new BatchEmbeddingModel(
            "vertex",
            settings,
            vertexEmbedMaxBatch(settings.model),
            vertexEmbedChunk({
               model: settings.model,
               projectId,
               location,
               dimensions: settings.dimensions,
               getAccessToken: deps.getAccessToken ?? adcAccessToken(),
               fetchFn: deps.fetchFn ?? fetch,
            }),
         );
      }
      case "anthropic":
         throw new Error(
            `Invalid retrieval.embedding.provider: "anthropic" has no embeddings API. ` +
               `Fix: use one of ${EMBEDDING_PROVIDER_NAMES.join(", ")}.`,
         );
   }
}

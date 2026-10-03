// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The provider layer: one interface for chat models and one for embedding
 * models, with an adapter per vendor behind each. Callers (the keyphrase
 * step today, the LLM stages later) see only these types, never a vendor.
 */

import type { RetryPolicy } from "../service/http_retry";

/** Every provider name a config file may use. */
export const PROVIDER_NAMES = [
   "openai",
   "openai-compatible",
   "ollama",
   "anthropic",
   "google",
   "vertex",
] as const;
export type ProviderName = (typeof PROVIDER_NAMES)[number];

/** Providers that can serve embeddings (Anthropic has no embeddings API). */
export const EMBEDDING_PROVIDER_NAMES = [
   "openai",
   "openai-compatible",
   "ollama",
   "google",
   "vertex",
] as const;

export interface ChatUsage {
   inputTokens?: number;
   outputTokens?: number;
}

export interface ChatRequest {
   system?: string;
   prompt: string;
   maxTokens?: number;
   signal?: AbortSignal;
}

export interface ChatResult {
   text: string;
   usage: ChatUsage;
}

export interface JsonChatRequest<T> extends ChatRequest {
   /** Throws, with a message a model can act on, when the value is wrong. */
   validate: (value: unknown) => T;
}

export interface JsonChatResult<T> {
   value: T;
   usage: ChatUsage;
}

export interface ChatModel {
   readonly provider: ProviderName;
   readonly model: string;
   complete(req: ChatRequest): Promise<ChatResult>;
   completeJson<T>(req: JsonChatRequest<T>): Promise<JsonChatResult<T>>;
}

export interface EmbedOptions {
   timeoutMs?: number;
   retry?: RetryPolicy;
}

export interface EmbeddingModel {
   readonly provider: ProviderName;
   readonly model: string;
   /** Requested vector length; undefined means the vendor's default. */
   readonly dimensions: number | undefined;
   /** Most texts one HTTP request carries. */
   readonly maxBatch: number;
   /** Cosine-similarity floor for this model's vectors. */
   readonly minSimilarity: number;
   /** Text put before a search query / before indexed text. Default ''. */
   readonly queryPrefix: string;
   readonly documentPrefix: string;
   embed(texts: string[], options?: EmbedOptions): Promise<number[][]>;
   /** Same as {@link embed} with a required timeout; the index's call shape. */
   embedBatch(
      texts: string[],
      timeoutMs: number,
      retry?: RetryPolicy,
   ): Promise<number[][]>;
}

/** What `retrieval.llm` resolves to, with the API key from `LLM_API_KEY`. */
export interface LlmSettings {
   provider: ProviderName;
   model: string;
   baseUrl?: string;
   projectId?: string;
   location?: string;
   apiKey?: string;
   timeoutMs: number;
   concurrency: number;
   maxCallsPerSync: number;
   maxCallsPerRequest: number;
}

/** What `retrieval.embedding` resolves to, with the key from `EMBEDDING_API_KEY`. */
export interface EmbeddingSettings {
   provider: ProviderName;
   model: string;
   dimensions?: number;
   baseUrl?: string;
   projectId?: string;
   location?: string;
   apiKey?: string;
   minSimilarity: number;
   queryPrefix: string;
   documentPrefix: string;
}

export type FetchFn = typeof fetch;

/** Test seams shared by every adapter. */
export interface ProviderDeps {
   fetchFn?: FetchFn;
   retry?: RetryPolicy;
   /** Vertex only: returns an OAuth access token. Defaults to Application Default Credentials. */
   getAccessToken?: () => Promise<string>;
}

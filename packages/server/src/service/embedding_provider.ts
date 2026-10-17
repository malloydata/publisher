// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { EmbeddingConfig, getEmbeddingSettings } from "../config";
import { createEmbeddingModel } from "../providers/registry";
import { loadedRetrievalConfig } from "../retrieval_config";
import { BatchEmbeddingModel } from "../providers/embedding_http";
import { openAiEmbedChunk } from "../providers/openai_compatible";
import {
   DEFAULT_RETRY,
   HttpRequestError,
   RetryPolicy,
   withRetry,
} from "./http_retry";
import type { EmbeddingModel, FetchFn } from "../providers/types";

export {
   EMBEDDING_BATCH_TIMEOUT_MS,
   MAX_EMBED_INPUT_CHARS,
   prepareEmbeddingInput,
} from "../providers/embedding_http";

/**
 * Timeout for the single per-request query embedding. Much shorter than
 * the bulk timeout: this call sits on the latency path of every semantic
 * `get_context` call, and a slow endpoint must degrade to lexical
 * quickly rather than stall the tool.
 */
export const EMBEDDING_QUERY_TIMEOUT_MS = 5_000;
/** Inputs per HTTP request; large packages embed in several requests. */
export const MAX_EMBED_BATCH_SIZE = 512;

/**
 * The retry machinery lives in ./http_retry so the chat adapters share it.
 * The embedding names stay as aliases: callers and tests import them from
 * here.
 */
export const EmbeddingRequestError = HttpRequestError;
export type EmbeddingRequestError = HttpRequestError;
export type EmbeddingRetryPolicy = RetryPolicy;
export const DEFAULT_EMBEDDING_RETRY: RetryPolicy = DEFAULT_RETRY;

/** Retry a bulk embedding call; see {@link withRetry}. */
export function withEmbeddingRetry<T>(
   attempt: () => Promise<T>,
   policy: RetryPolicy,
): Promise<T> {
   return withRetry(attempt, policy, "Embedding request");
}

/**
 * The OpenAI-compatible `/embeddings` client: {@link openAiEmbedChunk} under
 * the shared batching, retry and metrics of {@link BatchEmbeddingModel}.
 * Constructed from an {@link EmbeddingConfig}, which is what the registry and
 * the specs hold.
 */
export class EmbeddingProvider extends BatchEmbeddingModel {
   constructor(config: EmbeddingConfig, fetchFn: FetchFn = fetch) {
      super(
         config.provider ?? "openai-compatible",
         {
            provider: config.provider ?? "openai-compatible",
            model: config.model,
            dimensions: config.dimensions,
            minSimilarity: config.minSimilarity,
            queryPrefix: config.queryPrefix ?? "",
            documentPrefix: config.documentPrefix ?? "",
         },
         MAX_EMBED_BATCH_SIZE,
         openAiEmbedChunk({
            baseUrl: config.baseUrl,
            apiKey: config.apiKey,
            model: config.model,
            dimensions: config.dimensions,
            fetchFn,
         }),
      );
   }
}

// Cached on a settings fingerprint, never on null: a call after the env
// changes (tests, operator restarts with new vars are moot, but the
// integration suite runs many specs in one process) always sees the
// current configuration instead of a stale provider or a sticky "off".
let cached: { fingerprint: string; provider: EmbeddingModel } | null = null;
let testOverride: { provider: EmbeddingModel | null } | null = null;

/**
 * Whether the operator has turned the embedding feature on at all.
 * Malformed companion vars still count as configured: the operator
 * clearly intends the feature, so the retrieval marker and a warning
 * must surface rather than silently reporting plain lexical.
 */
export function embeddingConfigured(): boolean {
   if (testOverride) {
      return testOverride.provider !== null;
   }
   try {
      return getEmbeddingSettings(loadedRetrievalConfig()?.embedding) !== null;
   } catch {
      return true;
   }
}

/**
 * The process-wide embedding model for the current configuration, or null
 * when the feature is off (see getEmbeddingSettings). Throws on malformed
 * companion env vars; callers on the tool path catch and degrade.
 */
export function getEmbeddingProvider(): EmbeddingModel | null {
   if (testOverride) {
      return testOverride.provider;
   }
   const settings = getEmbeddingSettings(loadedRetrievalConfig()?.embedding);
   if (!settings) {
      cached = null;
      return null;
   }
   const fingerprint = JSON.stringify(settings);
   if (!cached || cached.fingerprint !== fingerprint) {
      cached = { fingerprint, provider: createEmbeddingModel(settings) };
   }
   return cached.provider;
}

/** Test seam: force the provider (or null). Undo with _clear...(). */
export function _setEmbeddingProviderForTests(
   provider: EmbeddingModel | null,
): void {
   testOverride = { provider };
}

export function _clearEmbeddingProviderForTests(): void {
   testOverride = null;
   cached = null;
}

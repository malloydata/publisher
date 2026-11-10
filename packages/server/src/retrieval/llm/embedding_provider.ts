// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getEmbeddingSettings } from "../../config";
import {
   DEFAULT_RETRY,
   HttpRequestError,
   RetryPolicy,
   withRetry,
} from "../../service/http_retry";
import { loadedRetrievalConfig } from "../config";
import { EMBEDDING_BATCH_TIMEOUT_MS } from "./embedding_http";
import { createEmbeddingModel } from "./registry";
import type { EmbeddingModel } from "./types";

export {
   EMBEDDING_BATCH_TIMEOUT_MS,
   MAX_EMBED_INPUT_CHARS,
   prepareEmbeddingInput,
} from "./embedding_http";
// The OpenAI-compatible client lives in a leaf, so ./registry can build it
// without importing this module (which imports ./registry).
export { EmbeddingProvider, MAX_EMBED_BATCH_SIZE } from "./openai_embedding";

/**
 * The deadline of one bulk (index-build) embedding request:
 * `retrieval.embedding.timeoutMs`, else {@link EMBEDDING_BATCH_TIMEOUT_MS}.
 */
export function embeddingBatchTimeoutMs(): number {
   return (
      loadedRetrievalConfig()?.embedding?.timeoutMs ??
      EMBEDDING_BATCH_TIMEOUT_MS
   );
}

/**
 * Timeout for the per-request query embedding. Shorter than the default bulk
 * timeout because it is on every semantic `get_context` call's latency path;
 * a slow endpoint must degrade to lexical quickly.
 */
export const EMBEDDING_QUERY_TIMEOUT_MS = 5_000;

/** Embedding-named aliases of the shared ./http_retry machinery. */
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

// Cached on a settings fingerprint, never on null, so a call after the env
// changes (many specs share one process) sees the current configuration
// rather than a stale provider or a sticky "off".
let cached: { fingerprint: string; provider: EmbeddingModel } | null = null;
let testOverride: { provider: EmbeddingModel | null } | null = null;

/**
 * Whether the operator has turned the embedding feature on. Malformed
 * companion vars still count, so the retrieval marker and a warning surface
 * rather than silently reporting plain lexical.
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

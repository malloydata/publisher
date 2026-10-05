// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   recordLlmCall,
   recordLlmRetry,
   type LlmFailureReason,
} from "../llm_metrics";
import {
   EMBEDDING_BATCH_TIMEOUT_MS,
   prepareEmbeddingInput,
} from "../service/embedding_provider";
import {
   HttpRequestError,
   malformedReply,
   type RetryPolicy,
   withRetry,
} from "../service/http_retry";
import type {
   EmbeddingModel,
   EmbedOptions,
   EmbeddingSettings,
   ProviderName,
} from "./types";

/**
 * Sends one chunk of already-prepared texts as one HTTP request and returns
 * one vector per text, in order. A single attempt: retry lives above it.
 */
export type EmbedChunkFn = (
   inputs: string[],
   signal: AbortSignal,
   timeoutMs: number,
) => Promise<number[][]>;

/** Check a vector list is one finite, non-empty numeric vector per input. */
export function checkVectors(
   vectors: unknown,
   expected: number,
   where: string,
): number[][] {
   if (!Array.isArray(vectors) || vectors.length !== expected) {
      throw malformedReply(
         "Embedding response",
         where,
         `expected ${expected} embeddings, got ${Array.isArray(vectors) ? vectors.length : "none"}`,
      );
   }
   vectors.forEach((v, i) => {
      if (
         !Array.isArray(v) ||
         v.length === 0 ||
         !v.every((n) => typeof n === "number" && Number.isFinite(n))
      ) {
         throw malformedReply("Embedding response", where, `bad item ${i}`);
      }
   });
   return vectors as number[][];
}

/**
 * The shared half of the Google and Vertex embedding models: input
 * preparation, splitting into requests of `maxBatch`, retry and metrics. The
 * vendor supplies only {@link EmbedChunkFn}.
 */
export class BatchEmbeddingModel implements EmbeddingModel {
   constructor(
      readonly provider: ProviderName,
      private readonly settings: EmbeddingSettings,
      readonly maxBatch: number,
      private readonly embedChunk: EmbedChunkFn,
   ) {}

   get model(): string {
      return this.settings.model;
   }
   get dimensions(): number | undefined {
      return this.settings.dimensions;
   }
   get minSimilarity(): number {
      return this.settings.minSimilarity;
   }
   get queryPrefix(): string {
      return this.settings.queryPrefix;
   }
   get documentPrefix(): string {
      return this.settings.documentPrefix;
   }

   embed(texts: string[], options: EmbedOptions = {}): Promise<number[][]> {
      return this.embedBatch(
         texts,
         options.timeoutMs ?? EMBEDDING_BATCH_TIMEOUT_MS,
         options.retry,
      );
   }

   async embedBatch(
      texts: string[],
      timeoutMs: number,
      retry?: RetryPolicy,
   ): Promise<number[][]> {
      const started = Date.now();
      try {
         const vectors: number[][] = [];
         for (let i = 0; i < texts.length; i += this.maxBatch) {
            const chunk = texts
               .slice(i, i + this.maxBatch)
               .map(prepareEmbeddingInput);
            const attempt = () =>
               this.embedChunk(
                  chunk,
                  AbortSignal.timeout(timeoutMs),
                  timeoutMs,
               );
            vectors.push(
               ...(retry
                  ? await withRetry(attempt, retry, "Embedding request", () =>
                       recordLlmRetry(this.provider, "embedding"),
                    )
                  : await attempt()),
            );
         }
         recordLlmCall(this.provider, "embedding", Date.now() - started);
         return vectors;
      } catch (error) {
         recordLlmCall(
            this.provider,
            "embedding",
            Date.now() - started,
            embeddingFailureReason(error),
         );
         throw error;
      }
   }
}

function embeddingFailureReason(error: unknown): LlmFailureReason {
   if (error instanceof HttpRequestError) {
      if (error.status === undefined)
         return error.retryable ? "timeout" : "other";
      if (error.status === 401 || error.status === 403) return "auth";
      if (error.status === 429) return "rate_limited";
      if (error.status >= 500) return "server_error";
      return "client_error";
   }
   return "invalid_response";
}

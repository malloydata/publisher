// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { recordLlmCall, recordLlmRetry } from "../llm_metrics";
import {
   malformedReply,
   type RetryPolicy,
   withRetry,
} from "../service/http_retry";
import { httpFailureReason } from "./http";
import type {
   EmbeddingModel,
   EmbedOptions,
   EmbeddingSettings,
   ProviderName,
} from "./types";

/** Timeout for bulk (index-build) embedding calls. */
export const EMBEDDING_BATCH_TIMEOUT_MS = 30_000;

/** Per-input character cap (matches the hosted indexing pipeline). */
export const MAX_EMBED_INPUT_CHARS = 1_024;

/**
 * Collapse whitespace (newlines confuse some embedding models) and cap the
 * length, as the hosted indexing pipeline does. Never returns an empty string:
 * providers 400 on an empty item, failing a whole package's batch. The index
 * hashes this function's output, so the hash covers exactly the text sent.
 */
export function prepareEmbeddingInput(text: string): string {
   const cleaned = text.replace(/\s+/g, " ").trim();
   const capped =
      cleaned.length > MAX_EMBED_INPUT_CHARS
         ? cleaned.slice(0, MAX_EMBED_INPUT_CHARS)
         : cleaned;
   return capped || "-";
}

/**
 * Sends one chunk of already-prepared texts as one HTTP request and returns
 * one vector per text, in order. A single attempt: retry lives above it.
 */
export type EmbedChunkFn = (
   inputs: string[],
   signal: AbortSignal,
   timeoutMs: number,
) => Promise<number[][]>;

/** Whether `v` is a non-empty vector of finite numbers. */
export function isVector(v: unknown): v is number[] {
   return (
      Array.isArray(v) &&
      v.length > 0 &&
      v.every((n) => typeof n === "number" && Number.isFinite(n))
   );
}

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
      if (!isVector(v)) {
         throw malformedReply("Embedding response", where, `bad item ${i}`);
      }
   });
   return vectors as number[][];
}

/**
 * The shared half of every embedding model: input preparation, splitting into
 * requests of `maxBatch`, retry and metrics. The vendor supplies only
 * {@link EmbedChunkFn}.
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
            httpFailureReason(error),
         );
         throw error;
      }
   }
}

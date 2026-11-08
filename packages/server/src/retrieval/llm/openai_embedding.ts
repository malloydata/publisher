// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EmbeddingConfig } from "../../config";
import { BatchEmbeddingModel } from "./embedding_http";
import { openAiEmbedChunk } from "./openai_compatible";
import type { FetchFn } from "./types";

/** Inputs per HTTP request; large packages embed in several requests. */
export const MAX_EMBED_BATCH_SIZE = 512;

/**
 * The OpenAI-compatible `/embeddings` client: {@link openAiEmbedChunk} under
 * the shared batching, retry and metrics of {@link BatchEmbeddingModel}.
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

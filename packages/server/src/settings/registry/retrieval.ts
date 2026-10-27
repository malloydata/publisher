// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../constants";
import { setting } from "../setting";
import { boolean, integer, number, string, structured } from "../types";

/**
 * The configuration design's `retrieval` table: the block itself, which
 * retrieval_config.ts parses and validates, and the leaves that have a
 * variable today. Where `provider` decides a leaf's default, the code that
 * assembles the block applies it.
 */
export const retrieval = [
   setting({
      key: "retrieval",
      type: structured(),
      kind: "structured",
      description:
         "Semantic retrieval: indexing limits, the LLM, the embedding provider and egress.",
      aliases: { file: ["retrieval"] },
   }),
   setting({
      key: "retrieval.embedding.apiKey",
      type: string(),
      secret: true,
      description:
         "Key of the embedding provider; unset turns semantic search off, except for providers that need none.",
      aliases: { env: ["EMBEDDING_API_KEY"] },
   }),
   setting({
      key: "retrieval.embedding.model",
      type: string(),
      description: "Embedding model; text-embedding-3-small for OpenAI.",
      aliases: {
         env: ["EMBEDDING_MODEL"],
         file: ["retrieval.embedding.model"],
         today: { fileFirst: true },
      },
   }),
   setting({
      key: "retrieval.embedding.baseUrl",
      type: string(),
      description:
         "Base URL of the embedding API; default the provider's, else https://api.openai.com/v1.",
      aliases: {
         env: ["EMBEDDING_API_BASE"],
         file: ["retrieval.embedding.baseUrl"],
         today: { fileFirst: true },
      },
   }),
   setting({
      key: "retrieval.embedding.dimensions",
      type: integer({ min: 1 }),
      description:
         "dimensions request parameter; unset takes the provider's vector length.",
      aliases: {
         env: ["EMBEDDING_DIMENSIONS"],
         file: ["retrieval.embedding.dimensions"],
         today: { fileFirst: true },
      },
   }),
   setting({
      key: "retrieval.embedding.minSimilarity",
      type: number({ min: 0, below: 1 }),
      default: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      description: "Cosine-similarity floor a semantic match must clear.",
      aliases: { env: ["EMBEDDING_MIN_SIMILARITY"] },
   }),
   setting({
      key: "retrieval.embedding.connectionSchemaIndexingEnabled",
      type: boolean(),
      default: false,
      description:
         "Let search_database_schema send a connection's table and column names to the embedding provider.",
      aliases: { env: ["EMBEDDING_INDEX_CONNECTION_SCHEMA"] },
   }),
   setting({
      key: "retrieval.llm.apiKey",
      type: string(),
      secret: true,
      description:
         "Key of the retrieval LLM; unset turns every LLM feature off, except for providers that need none.",
      aliases: { env: ["LLM_API_KEY"] },
   }),
];

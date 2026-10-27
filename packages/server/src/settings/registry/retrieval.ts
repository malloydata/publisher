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
      notes: 'Enables semantic (embedding-based) ranking for `get_context` question retrieval. Sent as a bearer token to the embedding endpoint. Unset: retrieval stays lexical (lunr/BM25), unchanged. Must be set explicitly; an ambient `OPENAI_API_KEY` is deliberately not read. See "Semantic retrieval for get_context" below.',
      aliases: { env: ["EMBEDDING_API_KEY"] },
   }),
   setting({
      key: "retrieval.embedding.model",
      type: string(),
      description: "Embedding model; text-embedding-3-small for OpenAI.",
      notes: "Embedding model name sent to the endpoint. A `retrieval.embedding.model` in `publisher.config.json` wins over it.",
      defaultText: "`text-embedding-3-small` for OpenAI",
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
      notes: "Base URL of an OpenAI-compatible embeddings API (`POST <base>/embeddings`). Point at any compatible endpoint (e.g. a local Ollama or vLLM server). A `retrieval.embedding.baseUrl` wins over it.",
      defaultText: "the provider's, else `https://api.openai.com/v1`",
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
      notes: "Optional `dimensions` request parameter (e.g. `512` to shrink `text-embedding-3-small` vectors). When unset the parameter is omitted, which suits providers that do not support it. A `retrieval.embedding.dimensions` wins over it.",
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
      notes: 'Cosine-similarity floor a match must clear to be returned at all, for both `get_context` and `search_database_schema`. Below it an entity is dropped rather than returned as a weak hit, which is what makes an empty result mean "not modelled here". Tunable because cosine similarity is not calibrated across embedding models, so the right floor belongs to the endpoint you point at, not to Publisher. Must be in `[0, 1)`; `0` disables the floor, and an out-of-range or non-numeric value is a startup error rather than a silent clamp. See "Tuning the floor" below.',
      aliases: { env: ["EMBEDDING_MIN_SIMILARITY"] },
   }),
   setting({
      key: "retrieval.embedding.connectionSchemaIndexingEnabled",
      type: boolean(),
      default: false,
      description:
         "Let search_database_schema send a connection's table and column names to the embedding provider.",
      notes: "Allows `search_database_schema` to send a connection's schema name, table names, column names and column types, plus the agent's search text, to the embedding endpoint for semantic ranking. Never row values. A second switch on top of `EMBEDDING_API_KEY`, which alone covers only your own model text; unset, schema search still works and ranks lexically. Accepts `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`. It is read when the tool is called, not at startup, so an unrecognised value does not stop the server: the tool logs a warning and ranks lexically for that call. See \"Semantic ranking for search_database_schema\" below.",
      aliases: { env: ["EMBEDDING_INDEX_CONNECTION_SCHEMA"] },
   }),
   setting({
      key: "retrieval.llm.apiKey",
      type: string(),
      secret: true,
      description:
         "Key of the retrieval LLM; unset turns every LLM feature off, except for providers that need none.",
      notes: 'API key for the chat model named by `retrieval.llm` in `publisher.config.json` (OpenAI, an OpenAI-compatible server, Anthropic or Google; not needed for `ollama` or `vertex`). Used for index-time keyphrases and source summaries. Unset with a keyed provider: every LLM feature is off and nothing errors. Never read from a file, never logged. See "Providers and the retrieval settings" below.',
      aliases: { env: ["LLM_API_KEY"] },
   }),
];

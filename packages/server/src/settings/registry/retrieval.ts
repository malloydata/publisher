// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../constants";
import {
   EMBEDDING_PROVIDER_NAMES,
   PROVIDER_NAMES,
} from "../../providers/types";
import {
   DEFAULT_LLM_CONCURRENCY,
   DEFAULT_LLM_MAX_CALLS_PER_REQUEST,
   DEFAULT_LLM_MAX_CALLS_PER_SYNC,
   DEFAULT_LLM_TIMEOUT_MS,
   DIMENSION_VALUE_DEFAULTS,
   EGRESS_PRESETS,
} from "../../retrieval_config";
import { setting } from "../setting";
import { boolean, integer, number, oneOf, string, structured } from "../types";

/**
 * The configuration design's `retrieval` table: the block itself, and every
 * scalar leaf of it. config.ts assembles the block from the leaves' flags,
 * variables and file keys and retrieval_config.ts validates it as one object,
 * since `provider` decides which other fields apply. So a leaf has no default
 * here where the block's validation supplies one (an `llm` block needs a
 * provider before its limits mean anything); its `defaultText` says what it is.
 * `indexing.maxEntities` does nothing since #1469 and gets no variable.
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
      key: "retrieval.llm.provider",
      type: oneOf(PROVIDER_NAMES),
      description:
         "Chat model provider for index-time keyphrases and source summaries; unset turns every LLM feature off.",
      notes: 'Which provider serves the chat model behind index-time keyphrases and source summaries. Unset: every LLM feature is off. It decides which other `retrieval.llm` fields apply: `openai-compatible` needs `baseUrl`, `vertex` needs `projectId` and `location`, and every keyed provider needs `retrieval.llm.apiKey`. The block is assembled from every source and then checked as one, so a variable that changes the provider is checked against the file\'s other fields. See "Providers and the retrieval settings" below.',
      defaultText: "_unset_ (LLM features off)",
      aliases: { file: ["retrieval.llm.provider"] },
   }),
   setting({
      key: "retrieval.llm.model",
      type: string(),
      description: "Chat model name; required with a provider.",
      aliases: { file: ["retrieval.llm.model"] },
   }),
   setting({
      key: "retrieval.llm.baseUrl",
      type: string(),
      description:
         "Base URL of the chat API; required for openai-compatible, else the provider's.",
      defaultText: "the provider's",
      aliases: { file: ["retrieval.llm.baseUrl"] },
   }),
   setting({
      key: "retrieval.llm.projectId",
      type: string(),
      description: "Google Cloud project of a vertex chat model.",
      aliases: { file: ["retrieval.llm.projectId"] },
   }),
   setting({
      key: "retrieval.llm.location",
      type: string(),
      description: "Google Cloud region of a vertex chat model.",
      aliases: { file: ["retrieval.llm.location"] },
   }),
   setting({
      key: "retrieval.llm.timeoutMs",
      type: integer({ min: 1 }),
      description: "Milliseconds one chat call may take.",
      defaultText: `\`${DEFAULT_LLM_TIMEOUT_MS}\` with a provider`,
      aliases: { file: ["retrieval.llm.timeoutMs"] },
   }),
   setting({
      key: "retrieval.llm.concurrency",
      type: integer({ min: 1 }),
      description: "Chat calls in flight at once.",
      defaultText: `\`${DEFAULT_LLM_CONCURRENCY}\` with a provider`,
      aliases: { file: ["retrieval.llm.concurrency"] },
   }),
   setting({
      key: "retrieval.llm.maxCallsPerSync",
      type: integer({ min: 1 }),
      description:
         "Most chat calls one index build may make; the spend ceiling.",
      defaultText: `\`${DEFAULT_LLM_MAX_CALLS_PER_SYNC}\` with a provider`,
      aliases: { file: ["retrieval.llm.maxCallsPerSync"] },
   }),
   setting({
      key: "retrieval.llm.maxCallsPerRequest",
      type: integer({ min: 1 }),
      description:
         "Most chat calls one get_context request may make; a request past it fails, naming this setting.",
      defaultText: `\`${DEFAULT_LLM_MAX_CALLS_PER_REQUEST}\` with a provider`,
      aliases: { file: ["retrieval.llm.maxCallsPerRequest"] },
   }),
   setting({
      key: "retrieval.embedding.provider",
      type: oneOf(EMBEDDING_PROVIDER_NAMES),
      description:
         "Embedding provider; unset reads the OpenAI-compatible settings.",
      notes: "Which provider serves embeddings. Unset: an OpenAI-compatible endpoint, set by `retrieval.embedding.baseUrl` and the rest. It decides which other `retrieval.embedding` fields apply: every provider but `openai` needs `model`, `openai-compatible` needs `baseUrl`, and `vertex` needs `projectId` and `location`. A `google` or `vertex` provider never reads `EMBEDDING_MODEL`, `EMBEDDING_API_BASE` or `EMBEDDING_DIMENSIONS`, which were only ever the OpenAI-compatible provider's.",
      defaultText: "_unset_ (OpenAI-compatible)",
      aliases: { file: ["retrieval.embedding.provider"] },
   }),
   setting({
      key: "retrieval.embedding.projectId",
      type: string(),
      description: "Google Cloud project of a vertex embedding model.",
      aliases: { file: ["retrieval.embedding.projectId"] },
   }),
   setting({
      key: "retrieval.embedding.location",
      type: string(),
      description: "Google Cloud region of a vertex embedding model.",
      aliases: { file: ["retrieval.embedding.location"] },
   }),
   setting({
      key: "retrieval.embedding.queryPrefix",
      type: string({ trim: false }),
      description:
         "Text put before a search query, as written (spaces included).",
      defaultText: "empty",
      aliases: { file: ["retrieval.embedding.queryPrefix"] },
   }),
   setting({
      key: "retrieval.embedding.documentPrefix",
      type: string({ trim: false }),
      description:
         "Text put before indexed text, as written; a change re-embeds every package.",
      defaultText: "empty",
      aliases: { file: ["retrieval.embedding.documentPrefix"] },
   }),
   setting({
      key: "retrieval.egress.preset",
      type: oneOf(EGRESS_PRESETS),
      default: "default",
      description:
         "What may be sent to the LLM: default (names, doc text, schema context) or full (also code and dimension values).",
      notes: "`default` sends entity names, doc text (`#(doc)`, `#\"`, and the database's descriptions on a connection with `dbmeta` on) and schema context. `full` also sends code and dimension values. Access predicates (`#(access_filter)`, `#(authorize)`) never leave, whatever this says.",
      aliases: { file: ["retrieval.egress.preset"] },
   }),
   setting({
      key: "retrieval.dimensionValues.enabled",
      type: boolean(),
      default: DIMENSION_VALUE_DEFAULTS.enabled,
      description:
         'Value search: off reads no #(index) value and answers a dimensional_value target with "No index".',
      aliases: {
         file: ["retrieval.dimensionValues.enabled"],
         // The block's parser took a JSON boolean only.
         today: { file: "json-boolean" },
      },
   }),
   setting({
      key: "retrieval.embedding.apiKey",
      type: string(),
      secret: true,
      description:
         "Key of the embedding provider; unset turns semantic search off, except for providers that need none.",
      notes: 'Turns on embedding closeness in `get_context`\'s search, and embedding ranking in `search_database_schema` (see below). Sent as a bearer token to the embedding endpoint. Unset: `get_context` ranks by keyword and stored values only. Must be set explicitly; an ambient `OPENAI_API_KEY` is deliberately not read. See "How get_context searches" below.',
      aliases: { env: ["EMBEDDING_API_KEY"] },
   }),
   setting({
      key: "retrieval.embedding.model",
      type: string(),
      description: "Embedding model; text-embedding-3-small for OpenAI.",
      notes: "Embedding model name sent to the endpoint. Precedence is the same as for every setting: a flag, then a variable, then `retrieval.embedding.model` in `publisher.config.json`.",
      defaultText: "`text-embedding-3-small` for OpenAI",
      aliases: {
         env: ["EMBEDDING_MODEL"],
         file: ["retrieval.embedding.model"],
      },
   }),
   setting({
      key: "retrieval.embedding.baseUrl",
      type: string(),
      description:
         "Base URL of the embedding API; default the provider's, else https://api.openai.com/v1.",
      notes: "Base URL of an OpenAI-compatible embeddings API (`POST <base>/embeddings`). Point at any compatible endpoint (e.g. a local Ollama or vLLM server). Precedence is the same as for every setting: a flag, then a variable, then `retrieval.embedding.baseUrl` in `publisher.config.json`.",
      defaultText: "the provider's, else `https://api.openai.com/v1`",
      aliases: {
         env: ["EMBEDDING_API_BASE"],
         file: ["retrieval.embedding.baseUrl"],
      },
   }),
   setting({
      key: "retrieval.embedding.dimensions",
      type: integer({ min: 1 }),
      description:
         "dimensions request parameter; unset takes the provider's vector length.",
      notes: "Optional `dimensions` request parameter (e.g. `512` to shrink `text-embedding-3-small` vectors). When unset the parameter is omitted, which suits providers that do not support it. Precedence is the same as for every setting: a flag, then a variable, then `retrieval.embedding.dimensions` in `publisher.config.json`.",
      aliases: {
         env: ["EMBEDDING_DIMENSIONS"],
         file: ["retrieval.embedding.dimensions"],
      },
   }),
   setting({
      key: "retrieval.embedding.minSimilarity",
      type: number({ min: 0, below: 1 }),
      default: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      description:
         "Cosine-similarity floor a table must clear to be returned by search_database_schema.",
      notes: "Cosine-similarity floor a table must clear to be returned by `search_database_schema`. `get_context` has no floor. Tunable because cosine similarity is not calibrated across embedding models, so the right floor belongs to the endpoint you point at, not to Publisher. Must be in `[0, 1)`; `0` disables the floor, and an out-of-range or non-numeric value is a startup error rather than a silent clamp.",
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
      notes: 'API key for the chat model named by `retrieval.llm` in `publisher.config.json` (OpenAI, an OpenAI-compatible server, Anthropic or Google; not needed for `ollama` or `vertex`). Used for index-time keyphrases and source summaries. Unset with a keyed provider: every LLM feature is off and nothing errors. In `publisher.config.json` only as one `${VAR}` reference; never logged. See "Providers and the retrieval settings" below.',
      aliases: { env: ["LLM_API_KEY"] },
   }),
];

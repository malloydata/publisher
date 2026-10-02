// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `retrieval` block of publisher.config.json: the server operator's
 * settings for `get_context` (credentials live in environment variables, not
 * here). The package author's side is `publisher.json`; see
 * ./service/package_retrieval.ts.
 */

import {
   EMBEDDING_PROVIDER_NAMES,
   PROVIDER_NAMES,
   type ProviderName,
} from "./providers/types";

/** What may leave the machine for an LLM. See {@link RetrievalConfig.egress}. */
export type EgressPreset = "default" | "full";
export const EGRESS_PRESETS: readonly EgressPreset[] = ["default", "full"];

export const DEFAULT_LLM_TIMEOUT_MS = 30_000;
export const DEFAULT_LLM_CONCURRENCY = 4;
export const DEFAULT_LLM_MAX_CALLS_PER_SYNC = 300;
export const DEFAULT_LLM_MAX_CALLS_PER_REQUEST = 20;

export type RetrievalLlmConfig = {
   provider: ProviderName;
   model: string;
   baseUrl?: string;
   /** Vertex only. */
   projectId?: string;
   location?: string;
   timeoutMs: number;
   concurrency: number;
   /** Most chat calls one index sync may make; the spend ceiling. */
   maxCallsPerSync: number;
   /** Most chat calls one `get_context` request may make. */
   maxCallsPerRequest: number;
};

export type RetrievalEmbeddingConfig = {
   /** Absent means the `EMBEDDING_*` variables decide (OpenAI-compatible). */
   provider?: ProviderName;
   model?: string;
   dimensions?: number;
   baseUrl?: string;
   projectId?: string;
   location?: string;
   /** Text put before a search query. Default ''. */
   queryPrefix: string;
   /** Text put before indexed text; a change re-embeds. Default ''. */
   documentPrefix: string;
};

/**
 * The `retrieval` block of publisher.config.json. Keys not listed here are
 * ignored at this level so the block can grow; inside `llm`, `embedding` and
 * `egress` an unknown key is an error naming the valid ones.
 */
export type RetrievalConfig = {
   indexing?: {
      /**
       * Most entities a package may have and still be embedded. A package
       * over this is not embedded, and `get_context` answers it with an error
       * that names this setting. See {@link DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES}.
       */
      maxEntities?: number;
   };
   /** Present only when `provider` is set; otherwise every LLM feature is off. */
   llm?: RetrievalLlmConfig;
   embedding?: RetrievalEmbeddingConfig;
   /**
    * What may be sent to the LLM. `default`: entity names, `#(doc)` text and
    * schema context. `full`: also code and dimension values. Access
    * predicates (`#(access_filter)`, `#(authorize)`) never leave, whatever
    * this says; there is no switch for that.
    */
   egress?: { preset: EgressPreset };
};

/**
 * Default for `retrieval.indexing.maxEntities`. A package past this is not
 * embedded: its first index would take minutes of provider calls and rate
 * limit. The bundled examples sit around a few hundred entities.
 *
 * Counted in ENTITIES, not rows. Faceting means a documented entity costs
 * more than one embedding (a name row plus its doc rows), so the ceiling on
 * first-sync provider calls is a small multiple of this number. It is still
 * expressed in entities because the check runs before facets are computed and
 * it is the figure an operator can reason about from their model.
 */
export const DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES = 5_000;

function describe(value: unknown): string {
   return value === undefined ? "nothing" : JSON.stringify(value);
}

/**
 * A positive integer, or digit-only text (what `${VAR}` substitution in the
 * config file always produces). Throws naming the key and a fix.
 */
function positiveInt(
   value: unknown,
   key: string,
   fix: string,
): number | undefined {
   if (value === undefined || value === null) return undefined;
   const parsed =
      typeof value === "string" && /^\d+$/.test(value.trim())
         ? Number(value.trim())
         : value;
   if (
      typeof parsed !== "number" ||
      !Number.isSafeInteger(parsed) ||
      parsed <= 0
   ) {
      throw new Error(
         `Invalid ${key}: expected a positive integer, got ${describe(value)}. Fix: ${fix}`,
      );
   }
   return parsed;
}

function nonEmptyString(
   value: unknown,
   key: string,
   fix: string,
): string | undefined {
   if (value === undefined || value === null) return undefined;
   if (typeof value !== "string" || value.trim() === "") {
      throw new Error(
         `Invalid ${key}: expected a non-empty string, got ${describe(value)}. Fix: ${fix}`,
      );
   }
   return value.trim();
}

function plainString(
   value: unknown,
   key: string,
   fix: string,
): string | undefined {
   if (value === undefined || value === null) return undefined;
   if (typeof value !== "string") {
      throw new Error(
         `Invalid ${key}: expected a string, got ${describe(value)}. Fix: ${fix}`,
      );
   }
   return value;
}

function url(value: unknown, key: string, fix: string): string | undefined {
   const text = nonEmptyString(value, key, fix);
   if (text === undefined) return undefined;
   try {
      new URL(text);
   } catch {
      throw new Error(
         `Invalid ${key}: expected a URL, got ${describe(value)}. Fix: ${fix}`,
      );
   }
   return text.replace(/\/+$/, "");
}

function block(
   raw: unknown,
   key: string,
   valid: readonly string[],
   fix: string,
): Record<string, unknown> {
   if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      throw new Error(
         `Invalid ${key}: expected an object, got ${describe(raw)}. Fix: ${fix}`,
      );
   }
   const obj = raw as Record<string, unknown>;
   const unknown = Object.keys(obj).filter((k) => !valid.includes(k));
   if (unknown.length > 0) {
      throw new Error(
         `Invalid ${key}: unknown key ${unknown.map((k) => `'${k}'`).join(", ")}. ` +
            `Valid keys: ${valid.join(", ")}.`,
      );
   }
   return obj;
}

function providerName(
   value: unknown,
   key: string,
   allowed: readonly string[],
   fix: string,
): ProviderName | undefined {
   if (value === undefined || value === null) return undefined;
   if (typeof value !== "string" || !allowed.includes(value)) {
      throw new Error(
         `Invalid ${key}: expected one of ${allowed.join(", ")}, got ${describe(value)}. Fix: ${fix}`,
      );
   }
   return value as ProviderName;
}

function parseLlm(raw: unknown): RetrievalLlmConfig | undefined {
   const keys = [
      "provider",
      "model",
      "baseUrl",
      "projectId",
      "location",
      "timeoutMs",
      "concurrency",
      "maxCallsPerSync",
      "maxCallsPerRequest",
   ];
   const fix = `"llm": { "provider": "anthropic", "model": "<model name>" }`;
   const obj = block(raw, "retrieval.llm", keys, fix);
   if (Object.keys(obj).length === 0) return undefined;
   const provider = providerName(
      obj.provider,
      "retrieval.llm.provider",
      PROVIDER_NAMES,
      `set "provider" to one of ${PROVIDER_NAMES.join(", ")}`,
   );
   if (provider === undefined) {
      throw new Error(
         `Invalid retrieval.llm.provider: expected one of ${PROVIDER_NAMES.join(", ")}, got nothing. ` +
            `Fix: ${fix}`,
      );
   }
   const model = nonEmptyString(
      obj.model,
      "retrieval.llm.model",
      `set "model" to the model name your provider serves`,
   );
   if (model === undefined) {
      throw new Error(
         `Invalid retrieval.llm.model: expected a non-empty string, got nothing. ` +
            `Fix: set "model" to the model name your provider serves.`,
      );
   }
   const baseUrl = url(
      obj.baseUrl,
      "retrieval.llm.baseUrl",
      `"baseUrl": "http://localhost:11434/v1"`,
   );
   if (provider === "openai-compatible" && baseUrl === undefined) {
      throw new Error(
         `Invalid retrieval.llm.baseUrl: provider "openai-compatible" needs one, got nothing. ` +
            `Fix: "baseUrl": "https://your-server.example.com/v1"`,
      );
   }
   const projectId = nonEmptyString(
      obj.projectId,
      "retrieval.llm.projectId",
      `"projectId": "my-gcp-project"`,
   );
   const location = nonEmptyString(
      obj.location,
      "retrieval.llm.location",
      `"location": "us-central1"`,
   );
   if (provider === "vertex" && (!projectId || !location)) {
      throw new Error(
         `Invalid retrieval.llm: provider "vertex" needs projectId and location. ` +
            `Fix: "llm": { "provider": "vertex", "model": "<model>", "projectId": "my-project", "location": "us-central1" }`,
      );
   }
   return {
      provider,
      model,
      ...(baseUrl ? { baseUrl } : {}),
      ...(projectId ? { projectId } : {}),
      ...(location ? { location } : {}),
      timeoutMs:
         positiveInt(
            obj.timeoutMs,
            "retrieval.llm.timeoutMs",
            "set it to e.g. 30000",
         ) ?? DEFAULT_LLM_TIMEOUT_MS,
      concurrency:
         positiveInt(
            obj.concurrency,
            "retrieval.llm.concurrency",
            "set it to e.g. 4",
         ) ?? DEFAULT_LLM_CONCURRENCY,
      maxCallsPerSync:
         positiveInt(
            obj.maxCallsPerSync,
            "retrieval.llm.maxCallsPerSync",
            "set it to e.g. 300",
         ) ?? DEFAULT_LLM_MAX_CALLS_PER_SYNC,
      maxCallsPerRequest:
         positiveInt(
            obj.maxCallsPerRequest,
            "retrieval.llm.maxCallsPerRequest",
            "set it to e.g. 20",
         ) ?? DEFAULT_LLM_MAX_CALLS_PER_REQUEST,
   };
}

function parseEmbedding(raw: unknown): RetrievalEmbeddingConfig | undefined {
   const keys = [
      "provider",
      "model",
      "dimensions",
      "baseUrl",
      "projectId",
      "location",
      "queryPrefix",
      "documentPrefix",
   ];
   const fix = `"embedding": { "provider": "openai", "model": "text-embedding-3-small" }`;
   const obj = block(raw, "retrieval.embedding", keys, fix);
   if (Object.keys(obj).length === 0) return undefined;
   if (obj.provider === "anthropic") {
      throw new Error(
         `Invalid retrieval.embedding.provider: "anthropic" has no embeddings API. ` +
            `Fix: use one of ${EMBEDDING_PROVIDER_NAMES.join(", ")}.`,
      );
   }
   const provider = providerName(
      obj.provider,
      "retrieval.embedding.provider",
      EMBEDDING_PROVIDER_NAMES,
      `set "provider" to one of ${EMBEDDING_PROVIDER_NAMES.join(", ")}`,
   );
   const model = nonEmptyString(
      obj.model,
      "retrieval.embedding.model",
      `"model": "text-embedding-3-small"`,
   );
   // OpenAI has a default model (text-embedding-3-small); every other named
   // provider serves different models, so a guess would fail at the vendor.
   if (provider !== undefined && provider !== "openai" && model === undefined) {
      throw new Error(
         `Invalid retrieval.embedding.model: provider "${provider}" needs one, got nothing. ` +
            `Fix: set "model" to the embedding model your provider serves.`,
      );
   }
   const baseUrl = url(
      obj.baseUrl,
      "retrieval.embedding.baseUrl",
      `"baseUrl": "http://localhost:11434/v1"`,
   );
   if (provider === "openai-compatible" && baseUrl === undefined) {
      throw new Error(
         `Invalid retrieval.embedding.baseUrl: provider "openai-compatible" needs one, got nothing. ` +
            `Fix: "baseUrl": "https://your-server.example.com/v1"`,
      );
   }
   const projectId = nonEmptyString(
      obj.projectId,
      "retrieval.embedding.projectId",
      `"projectId": "my-gcp-project"`,
   );
   const location = nonEmptyString(
      obj.location,
      "retrieval.embedding.location",
      `"location": "us-central1"`,
   );
   if (provider === "vertex" && (!projectId || !location)) {
      throw new Error(
         `Invalid retrieval.embedding: provider "vertex" needs projectId and location. ` +
            `Fix: "embedding": { "provider": "vertex", "model": "<model>", "projectId": "my-project", "location": "us-central1" }`,
      );
   }
   const dimensions = positiveInt(
      obj.dimensions,
      "retrieval.embedding.dimensions",
      "set it to e.g. 512",
   );
   return {
      ...(provider ? { provider } : {}),
      ...(model ? { model } : {}),
      ...(dimensions !== undefined ? { dimensions } : {}),
      ...(baseUrl ? { baseUrl } : {}),
      ...(projectId ? { projectId } : {}),
      ...(location ? { location } : {}),
      queryPrefix:
         plainString(
            obj.queryPrefix,
            "retrieval.embedding.queryPrefix",
            `"queryPrefix": "search_query: "`,
         ) ?? "",
      documentPrefix:
         plainString(
            obj.documentPrefix,
            "retrieval.embedding.documentPrefix",
            `"documentPrefix": "search_document: "`,
         ) ?? "",
   };
}

function parseEgress(raw: unknown): { preset: EgressPreset } | undefined {
   const obj = block(
      raw,
      "retrieval.egress",
      ["preset"],
      `"egress": { "preset": "default" }`,
   );
   if (obj.preset === undefined || obj.preset === null) return undefined;
   if (
      typeof obj.preset !== "string" ||
      !EGRESS_PRESETS.includes(obj.preset as EgressPreset)
   ) {
      throw new Error(
         `Invalid retrieval.egress.preset: expected one of ${EGRESS_PRESETS.join(", ")}, got ${describe(obj.preset)}. ` +
            `Fix: "preset": "default" (names, #(doc) text and schema context) or "full" (also code and dimension values).`,
      );
   }
   return { preset: obj.preset as EgressPreset };
}

/**
 * Validate the `retrieval` block. Throws, naming the key and the fix, on a
 * value that cannot be used: a bad value must stop the server at startup
 * rather than silently fall back to a setting the operator did not choose.
 *
 * A digit-only string is accepted for numbers because `${VAR}` substitution
 * in the config file always produces a string.
 */
export function parseRetrievalConfig(
   raw: unknown,
): RetrievalConfig | undefined {
   if (raw === undefined || raw === null) return undefined;
   if (typeof raw !== "object" || Array.isArray(raw)) {
      throw new Error(
         `Invalid retrieval: expected an object, got ${JSON.stringify(raw)}. ` +
            `Fix: "retrieval": { "indexing": { "maxEntities": 20000 } }`,
      );
   }
   const input = raw as Record<string, unknown>;
   const out: RetrievalConfig = {};

   const indexing = input.indexing;
   if (indexing !== undefined && indexing !== null) {
      if (typeof indexing !== "object" || Array.isArray(indexing)) {
         throw new Error(
            `Invalid retrieval.indexing: expected an object, got ${JSON.stringify(indexing)}. ` +
               `Fix: "indexing": { "maxEntities": 20000 }`,
         );
      }
      const maxEntities = positiveInt(
         (indexing as { maxEntities?: unknown }).maxEntities,
         "retrieval.indexing.maxEntities",
         "set it to e.g. 20000",
      );
      out.indexing = maxEntities === undefined ? {} : { maxEntities };
   }

   if (input.llm !== undefined && input.llm !== null) {
      const llm = parseLlm(input.llm);
      if (llm) out.llm = llm;
   }
   if (input.embedding !== undefined && input.embedding !== null) {
      const embedding = parseEmbedding(input.embedding);
      if (embedding) out.embedding = embedding;
   }
   if (input.egress !== undefined && input.egress !== null) {
      const egress = parseEgress(input.egress);
      if (egress) out.egress = egress;
   }
   return out;
}

// The block as the running server loaded it, set once at startup by
// setRetrievalConfig and read by the provider getters. Module state, like the
// entity cap in embedding_index: nothing re-reads the file on a request.
let loaded: RetrievalConfig | undefined;

export function setRetrievalConfig(config: RetrievalConfig | undefined): void {
   loaded = config;
}

export function loadedRetrievalConfig(): RetrievalConfig | undefined {
   return loaded;
}

/** The egress preset in force: `default` unless the operator chose `full`. */
export function getEgressPreset(): EgressPreset {
   return loaded?.egress?.preset ?? "default";
}

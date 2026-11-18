// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `retrieval` block of publisher.config.json: the server operator's
 * settings for `get_context` (credentials live in environment variables, not
 * here). The package author's side is `publisher.json`; see
 * ../service/package_retrieval_settings.ts.
 */

import { logger } from "../logger";
import {
   EMBEDDING_PROVIDER_NAMES,
   PROVIDER_NAMES,
   type ProviderName,
} from "./llm/types";

import {
   DEFAULT_LLM_CONCURRENCY,
   DEFAULT_LLM_MAX_CALLS_PER_REQUEST,
   DEFAULT_LLM_MAX_CALLS_PER_SYNC,
   DEFAULT_LLM_TIMEOUT_MS,
   DIMENSION_VALUE_DEFAULTS,
} from "./defaults";

/**
 * Logged when a config still carries `retrieval.egress`. The block chose
 * whether `#(index)` values could reach the embedding provider and the LLM;
 * now the `#(index)` tag decides, as it does on a hosted index, so the block
 * loads and does nothing.
 */
export const RETIRED_EGRESS_WARNING =
   "retrieval.egress is no longer read and is ignored. The values of #(index) dimensions on ungated sources are now sent to the configured embedding provider and LLM whenever one is configured. To keep them on this machine, leave EMBEDDING_API_KEY unset, set retrieval.fcj.valueEmbeddings to false in the package's publisher.json, or set retrieval.dimensionValues.enabled to false. Remove the egress block to silence this warning.";

export {
   DEFAULT_LLM_CONCURRENCY,
   DEFAULT_LLM_MAX_CALLS_PER_REQUEST,
   DEFAULT_LLM_MAX_CALLS_PER_SYNC,
   DEFAULT_LLM_TIMEOUT_MS,
   DIMENSION_VALUE_DEFAULTS,
};

export type RetrievalLlmConfig = {
   provider: ProviderName;
   model: string;
   baseUrl?: string;
   /** Vertex only. */
   projectId?: string;
   location?: string;
   timeoutMs: number;
   concurrency: number;
   /** Most chat calls one index build may make; the spend ceiling. */
   maxCallsPerSync: number;
   /**
    * Most chat calls one `get_context` request may make (the judge's); the
    * request-time spend ceiling. A request that would pass it
    * fails with an error that names this setting.
    */
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
 * The `retrieval` block of publisher.config.json. An unknown key, here or in
 * a nested block, is an error naming the valid ones. `egress` is the one
 * exception: it is accepted and ignored with {@link RETIRED_EGRESS_WARNING}.
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
   /** The file still carries the retired `egress` block: {@link setRetrievalConfig} warns. */
   egressIgnored?: true;
   /**
    * Value search: how `get_context` answers a `dimensional_value` target.
    * Every key is optional; see {@link DIMENSION_VALUE_DEFAULTS}.
    */
   dimensionValues?: DimensionValuesConfig;
};

/** The `retrieval.dimensionValues` block, as the operator wrote it. */
export type DimensionValuesConfig = {
   /**
    * `false` turns value search off: no `#(index)` value is read when a
    * package's search index is built, and a `dimensional_value` target gets
    * the "No index" warning.
    */
   enabled?: boolean;
   /**
    * For local development against a large model: per package, the givens
    * the value index reads with, each one value or a list. A source gated by
    * `column = $GIVEN` is then read only for the listed values, up to the cap
    * for each, instead of once across every value; any other source that
    * reads a listed given is read with it. Lives in the local server's config
    * so publishing a package never carries it.
    */
   givens?: Readonly<
      Record<string, Readonly<Record<string, readonly GivenValue[]>>>
   >;
};

/** A value `retrieval.dimensionValues.givens` may name. */
export type GivenValue = string | number | boolean;

/**
 * Most reads one dimension may take under `retrieval.dimensionValues.givens`:
 * each combination of the listed values is one read.
 */
export const MAX_GIVEN_COMBINATIONS = 50;

/**
 * Default for `retrieval.indexing.maxEntities`. A package past this is not
 * embedded: its first index would take minutes of provider calls and rate
 * limit.
 *
 * Counted in ENTITIES, not embedding rows (a documented entity embeds a name
 * row plus doc rows), because the check runs before faceting and it is the
 * figure an operator can reason about from their model.
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
      // Read by the settings registry (retrieval.llm.apiKey), not here.
      "apiKey",
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
      // Read by the settings registry (retrieval.embedding.*), not here.
      "apiKey",
      "minSimilarity",
      "connectionSchemaIndexingEnabled",
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

const DIMENSION_VALUE_KEYS = ["enabled", "givens"] as const;

const GIVENS_FIX = `"givens": { "<package>": { "REPORT_ID": [123, 456] } }`;

function parseValueGivens(
   raw: unknown,
): Record<string, Record<string, GivenValue[]>> {
   const isObject = (v: unknown): v is Record<string, unknown> =>
      typeof v === "object" && v !== null && !Array.isArray(v);
   const isValue = (v: unknown): v is GivenValue =>
      typeof v === "string" ||
      typeof v === "boolean" ||
      (typeof v === "number" && Number.isFinite(v));
   if (!isObject(raw)) {
      throw new Error(
         `Invalid retrieval.dimensionValues.givens: expected an object of packages, got ${describe(raw)}. Fix: ${GIVENS_FIX}`,
      );
   }
   const out: Record<string, Record<string, GivenValue[]>> = {};
   for (const [pkg, givens] of Object.entries(raw)) {
      const at = `retrieval.dimensionValues.givens.${pkg}`;
      if (!isObject(givens)) {
         throw new Error(
            `Invalid ${at}: expected an object of givens, got ${describe(givens)}. Fix: ${GIVENS_FIX}`,
         );
      }
      const parsed: Record<string, GivenValue[]> = {};
      let combinations = 1;
      for (const [name, value] of Object.entries(givens)) {
         const list = Array.isArray(value) ? value : [value];
         if (list.length === 0 || !list.every(isValue)) {
            throw new Error(
               `Invalid ${at}.${name}: expected a string, number or boolean, or a non-empty list of them, got ${describe(value)}. Fix: ${GIVENS_FIX}`,
            );
         }
         parsed[name] = [...new Set(list)];
         combinations *= parsed[name].length;
      }
      if (combinations > MAX_GIVEN_COMBINATIONS) {
         throw new Error(
            `Invalid ${at}: its lists make ${combinations} combinations, and each is one read per indexed dimension; at most ${MAX_GIVEN_COMBINATIONS}. Fix: list fewer values`,
         );
      }
      out[pkg] = parsed;
   }
   return out;
}

function parseDimensionValues(raw: unknown): DimensionValuesConfig {
   const fields = block(
      raw,
      "retrieval.dimensionValues",
      DIMENSION_VALUE_KEYS,
      `"dimensionValues": { "enabled": false }`,
   );
   const out: DimensionValuesConfig = {};
   if (fields.enabled !== undefined && fields.enabled !== null) {
      if (typeof fields.enabled !== "boolean") {
         throw new Error(
            `Invalid retrieval.dimensionValues.enabled: expected a boolean, got ${describe(fields.enabled)}. Fix: "enabled": false`,
         );
      }
      out.enabled = fields.enabled;
   }
   if (fields.givens !== undefined && fields.givens !== null) {
      out.givens = parseValueGivens(fields.givens);
   }
   return out;
}

/**
 * Validate the `retrieval` block. Throws, naming the key and the fix, on a
 * value that cannot be used: a bad value must stop the server at startup
 * rather than silently fall back to a setting the operator did not choose.
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
   // Unknown keys are refused here and in every nested block, so a typo
   // ("embeding") fails loudly instead of leaving a setting at its default.
   const input = block(
      raw,
      "retrieval",
      ["indexing", "llm", "embedding", "egress", "dimensionValues"],
      `"retrieval": { "indexing": { "maxEntities": 20000 } }`,
   );
   const out: RetrievalConfig = {};

   const indexing = input.indexing;
   if (indexing !== undefined && indexing !== null) {
      const fields = block(
         indexing,
         "retrieval.indexing",
         ["maxEntities"],
         `"indexing": { "maxEntities": 20000 }`,
      );
      const maxEntities = positiveInt(
         fields.maxEntities,
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
   if (input.egress !== undefined) out.egressIgnored = true;
   if (input.dimensionValues !== undefined && input.dimensionValues !== null) {
      out.dimensionValues = parseDimensionValues(input.dimensionValues);
   }
   return out;
}

// Set once at startup; nothing re-reads the file on a request.
let loaded: RetrievalConfig | undefined;

export function setRetrievalConfig(config: RetrievalConfig | undefined): void {
   // Installed once at boot, though the file is parsed more than once.
   if (config?.egressIgnored) logger.warn(RETIRED_EGRESS_WARNING);
   loaded = config;
}

export function loadedRetrievalConfig(): RetrievalConfig | undefined {
   return loaded;
}

/** The `retrieval.dimensionValues` settings in force, defaults filled in. */
export function getDimensionValueSettings(): Required<DimensionValuesConfig> {
   return { ...DIMENSION_VALUE_DEFAULTS, ...loaded?.dimensionValues };
}

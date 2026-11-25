// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The provider names, in a module that imports nothing, so the settings
 * registry can type `retrieval.llm.provider` and `retrieval.embedding.provider`
 * with them without importing the retrieval code (see import_cycles.spec.ts).
 * ./types.ts re-exports them.
 */

/** Every provider name a config file may use. */
export const PROVIDER_NAMES = [
   "openai",
   "openai-compatible",
   "ollama",
   "anthropic",
   "google",
   "vertex",
] as const;
export type ProviderName = (typeof PROVIDER_NAMES)[number];

/** Providers that can serve embeddings (Anthropic has no embeddings API). */
export const EMBEDDING_PROVIDER_NAMES = [
   "openai",
   "openai-compatible",
   "ollama",
   "google",
   "vertex",
] as const;

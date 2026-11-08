// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LexicalCacheEntry, VectorCacheEntry } from "./schema_index";

/**
 * search_database_schema's caches (./schema_index.ts), held by the
 * RetrievalRegistry: in memory only, keyed by schema and bounded by
 * MAX_CACHED_SCHEMAS. Its own module so the registry can build it without
 * loading schema_index.ts, which reads the registry.
 */
export class SchemaIndexCaches {
   readonly vectors = new Map<string, VectorCacheEntry>();
   readonly lexical = new Map<string, LexicalCacheEntry>();
   readonly cooldownUntilMs = new Map<string, number>();
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Small helpers the retrieval tools (get_context, search_database_schema,
 * search_malloy_docs) share, kept in one place so their copies cannot drift.
 */

import type { EmbeddingModel } from "../../providers/types";

/**
 * After a provider failure the semantic path is off for this long, so a down
 * or misconfigured endpoint costs one timeout per window, not one per call.
 */
export const PROVIDER_FAILURE_COOLDOWN_MS = 60_000;

/**
 * The model/dims request-config vectors were embedded under. Rows embedded
 * under one config are not interchangeable with another's. Text put before
 * indexed text changes every vector, so the document prefix is part of it.
 * Persisted with the package index, so its format must not change.
 */
export function providerKeyFor(
   provider: Pick<EmbeddingModel, "model" | "dimensions" | "documentPrefix">,
): string {
   return `${provider.model}\x00${provider.dimensions ?? ""}\x00${provider.documentPrefix ?? ""}`;
}

/**
 * lunr treats several characters as query operators; strip them so a
 * plain-English query never throws and is matched as an OR of its terms.
 */
export function sanitizeLunrQuery(query: string): string {
   return query.replace(/[~^:*+\-"]/g, " ").trim();
}

/** Round to the four decimal places a published score carries. */
export function round4(x: number): number {
   return Math.round(x * 10_000) / 10_000;
}

/**
 * Drop everything an access predicate could ride on. Doc text reaches here
 * already limited to `#(doc)` lines; this removes any other annotation that
 * got into it (a `#(access_filter)` or `#(authorize)` predicate, a persist
 * line) from its marker to the end of that line.
 */
export function scrubForEgress(text: string): string {
   return text
      .split(/\r?\n/)
      .map((line) => {
         const marker = /(^|\s)(#\((?!doc\))[^)]*\)|#@)/.exec(line);
         return marker ? line.slice(0, marker.index) : line;
      })
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();
}

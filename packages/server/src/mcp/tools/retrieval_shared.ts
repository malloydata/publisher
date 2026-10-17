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

/**
 * Stable order for two strings, by UTF-16 code unit.
 *
 * Deliberately not `localeCompare`: called with no locale it uses the
 * runtime's, so the same bytes sort differently under a different LANG. Every
 * caller here wants "the same order everywhere", never a human-friendly one.
 */
export function compareText(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}

/** Round to the four decimal places a published score carries. */
export function round4(x: number): number {
   return Math.round(x * 10_000) / 10_000;
}

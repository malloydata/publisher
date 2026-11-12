// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview The compile-derived inputs to a stable id for
 * ``#(index)``-tagged dimension entities.
 *
 * This server computes only the two inputs that require compiling the
 * model: the canonical fingerprint query ({@link buildFingerprintQuery})
 * and the hash of its compiled SQL ({@link computeSqlFingerprint}). It
 * reports those (plus the required-filter set) on the entity as its
 * ``dim_index_fingerprint``. A host that indexes the package can derive a
 * stable id from the fingerprint together with its own identifiers (and
 * anything it knows about the connection), so the id survives re-indexing
 * as long as the dimension's definition does not change.
 *
 * Every other entity type — measures, views, joins, columns, and
 * non-``#(index)`` dimensions — keeps the per-run ``randomUUID()`` id and
 * reports no fingerprint.
 */

import { createHash } from "crypto";
import { quotedIdentifier, quotedPath } from "../malloy_text";

/**
 * The definitional inputs reported for a single ``#(index)``-tagged
 * dimension entity (the entity's ``dim_index_fingerprint``), from which a
 * host can derive a stable id.
 */
export interface DimIndexFingerprintInputs {
   sqlFingerprint: string;
   requiredFilterDimensions: string[];
}

/**
 * Hex-lowercase SHA-256 of the publisher-compiled SQL.
 *
 * Lowercased hex matches the common ``sha256(...).hexdigest()`` form, so a
 * host recomputing the fingerprint in another language gets the same value.
 */
export function computeSqlFingerprint(compiledSql: string): string {
   return createHash("sha256").update(compiledSql, "utf8").digest("hex");
}

const leafOf = (path: string): string => path.split(".").at(-1) ?? path;

/**
 * Build the canonical Malloy fingerprint fragment. The compiled SQL of
 * this fragment is what feeds {@link computeSqlFingerprint}.
 *
 * Shape:
 *
 * ```malloy
 * run: <source> -> {
 *   group_by: `<sortedFilter1>`, `<sortedFilter2>`, ..., value is `<dimensionPath>`
 *   aggregate: weight is count()
 *   order_by: value
 * }
 * ```
 *
 * - ``required_filter_dimensions`` are sorted ascending so two
 *   semantically-equivalent declarations don't produce different
 *   fingerprints.
 * - ``gateDimensions`` follow the filters, sorted among themselves and always
 *   aliased ``__g<i>``, so the key layout (filters, then gates) is part of the id
 *   and a column changing role changes it. Filters-only ids are unaffected.
 * - Bounds (``where:``, ``limit:``) are deliberately omitted: those
 *   are pagination concerns and would flip the fingerprint with every
 *   page size change. We want the fingerprint to identify the
 *   *projection* a dimension carves out of its source.
 */
export function buildFingerprintQuery(
   sourceName: string,
   dimensionPath: string,
   requiredFilterDimensions: string[],
   gateDimensions: string[] = [],
): string {
   const sortedFilters = [...requiredFilterDimensions].sort();
   const leaves = [...sortedFilters.map(leafOf), "value", "weight"];
   // Malloy names an output after its leaf, so `account.id` + `id` would collide.
   // Only colliding leaves are aliased (`__pk<i>`, i in the sorted filter list);
   // aliasing every key would change the id of every existing filtered dimension.
   const groupByColumns = [
      ...sortedFilters.map((column, i) =>
         leaves.indexOf(leafOf(column)) !== leaves.lastIndexOf(leafOf(column))
            ? `\`__pk${i}\` is ${quotedPath(column)}`
            : quotedPath(column),
      ),
      ...[...gateDimensions]
         .sort()
         .map((column, i) => `\`__g${i}\` is ${quotedPath(column)}`),
      `value is ${quotedPath(dimensionPath)}`,
   ];
   return (
      `run: ${quotedIdentifier(sourceName)} -> { ` +
      `group_by: ${groupByColumns.join(", ")} ` +
      `aggregate: weight is count() ` +
      `order_by: value ` +
      `}`
   );
}

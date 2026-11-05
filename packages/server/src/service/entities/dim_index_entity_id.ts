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

/**
 * The two SQL forms a compiled fingerprint fragment can expose.
 *
 * - ``preparedSql`` is ``preparedResult.sql`` — the SQL Malloy actually
 *   runs. It is *post-manifest-rewrite*: for a table-backed index it would
 *   reference the materialized physical table names (which churn), so it is
 *   NOT a stable definitional fingerprint for those.
 * - ``preRewriteSql`` is the *manifest-free / logical* SQL — the query as it
 *   would compile with no materialization manifest applied. This is the
 *   fingerprint we want, because it identifies the logical projection a
 *   dimension carves out of its source independent of where the bytes
 *   currently live.
 */
export interface FingerprintSqlSources {
   preparedSql: string;
   /**
    * Populated only when the compiler can produce manifest-free SQL.
    * ``undefined`` for every source today: forked-Malloy does not yet expose a
    * manifest-free compile, and it is only *needed* by table-backed indexes.
    * Until the compiler exposes one, {@link resolveFingerprintSql} falls back
    * to ``preparedSql``.
    */
   preRewriteSql?: string;
}

/**
 * Choose the SQL to fingerprint.
 *
 * Prefer the manifest-free ``preRewriteSql`` when the compiler can produce
 * it; otherwise fall back to ``preparedSql``. The fallback is *correct today*
 * because every shipped ``#(index)`` dimension is connection-backed and has no
 * materialization manifest — its ``preparedResult.sql`` is already
 * manifest-free, so the two forms are byte-identical. Table-backed indexes
 * are the only case where the two diverge, and they cannot be supported
 * until the compiler can populate ``preRewriteSql``.
 *
 * Centralizing the decision here means the id derivation contract (what SQL
 * feeds the fingerprint) has exactly one definition, and flipping table-backed
 * indexes on later is a one-line change at the call site (populate
 * ``preRewriteSql``), not a semantics change here.
 */
export function resolveFingerprintSql(sources: FingerprintSqlSources): string {
   return sources.preRewriteSql ?? sources.preparedSql;
}

/**
 * Backtick-quote a (possibly dotted) Malloy field reference per segment.
 * Malloy traverses joins with ``.`` but quotes *identifiers* with
 * backticks, so a join-qualified path must be quoted segment-wise
 * (``\`order_items\`.\`oi_label\``) — wrapping the whole dotted string in
 * one pair would be read as a single bogus identifier and fail the
 * compile. An indexer that scans the dimension's values should quote the
 * same way, so its scan query and the fingerprint fragment agree. A
 * dot-free name is unchanged, so derived ids for top-level dims are
 * unaffected.
 */
function quoteMalloyPath(path: string): string {
   return path
      .split(".")
      .map((segment) => `\`${segment}\``)
      .join(".");
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
            ? `\`__pk${i}\` is ${quoteMalloyPath(column)}`
            : quoteMalloyPath(column),
      ),
      ...[...gateDimensions]
         .sort()
         .map((column, i) => `\`__g${i}\` is ${quoteMalloyPath(column)}`),
      `value is ${quoteMalloyPath(dimensionPath)}`,
   ];
   return (
      `run: ${sourceName} -> { ` +
      `group_by: ${groupByColumns.join(", ")} ` +
      `aggregate: weight is count() ` +
      `order_by: value ` +
      `}`
   );
}

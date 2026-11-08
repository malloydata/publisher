// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which rollup namespaces can be spliced into a generated table name. A leaf:
 * the annotation reader (which refuses a bad `namespace=`) and the synthesizer
 * (which derives one from a `#@ persist name=`) both apply it.
 */

/**
 * One segment of a namespace: a plain identifier, plus the hyphen.
 *
 * {@link assertSafeSqlIdentifier}'s `[A-Za-z_][A-Za-z0-9_$]*` plus the hyphen:
 * that rule excludes hyphens because BigQuery never reaches it, but a rollup
 * namespace does reach BigQuery, where `my-project.analytics` is ordinary.
 *
 * Everything else is out: a space or a quote cannot be spliced bare, and a name
 * needing quotes cannot be joined to a generated table name (see
 * `persistNamespace` in ./preaggregation_synthesis.ts).
 */
const NAMESPACE_SEGMENT = /^[A-Za-z_][A-Za-z0-9_$-]*$/;

/**
 * Whether every dot-separated segment can be spliced into a generated name.
 *
 * Dots are the one separator that survives: BigQuery addresses a dataset as
 * `project.dataset`, so a namespace is a path, not a single identifier.
 *
 * An empty string is refused by the per-segment test, which `""` fails.
 */
export function isSpliceableNamespace(namespace: string): boolean {
   return namespace.split(".").every((s) => NAMESPACE_SEGMENT.test(s));
}

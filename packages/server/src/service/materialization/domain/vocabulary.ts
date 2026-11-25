// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The materialization vocabulary, defined once: what a stored table holds
 * (its kind), where it lives (its tier), and which leg answers a query from
 * it. Every other union of these words (a binding's kind, a gate row, a
 * metric label, a router candidate) is this list or a subset of it, so a
 * word added here is seen everywhere it must be handled.
 */

/**
 * What a stored table holds: a `#@ persist` source's rows, a
 * `#(materialize)` source's, a pair table (an inlined join_many), a rollup,
 * a persisted view's or a persisted named query's result.
 */
export const TABLE_KINDS = [
   "persist",
   "materialize",
   "pair",
   "rollup",
   "view",
   "query",
] as const;
export type TableKind = (typeof TABLE_KINDS)[number];

/** Where a stored table lives: the source's own warehouse, or a `storage=` destination. */
export const TIERS = ["database", "storage"] as const;
export type Tier = (typeof TIERS)[number];

/** The legs a query is routed over, in the router's order. */
export const LEGS = ["view", "query", "rollup", "served"] as const;
export type Leg = (typeof LEGS)[number];

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The two annotation routes an access-control gate is written under, and the
 * mapping from a written spelling to the route everything downstream keys on.
 *
 * The name declares the scope; nothing is inferred from the body. `#(authorize)`
 * always asks about the SOURCE and `#(access_filter)` always asks about a ROW,
 * and a body of the wrong shape is refused at load naming the other annotation
 * (see `authorize_grammar.ts`). There is no alias, and no spelling lets one
 * route carry the other's question.
 *
 * Its own module, with ZERO imports, for two reasons. It is read by both
 * `authorize.ts` and `authorize_grammar.ts`, which previously each declared
 * their own copy of the same literals — a latent fail-open, because the grammar
 * module decides a scope refusal against its copy while `authorize.ts`
 * classifies notes against its own, so a value changed in one file alone would
 * move enforcement with every test still green. And `authorize.ts` bundles into
 * the package-load worker, which constrains what it may import (see its module
 * doc) — a constant module that imports nothing cannot violate that.
 *
 * `access_filter` is snake_case because Malloy's own multi-word tag names are
 * (`bar_chart`, `shape_map`, `url_template`, every `##!` flag), and because a
 * hyphen is safe only in the bracketed ROUTE position: in the bare-tag position
 * `-` is not in the identifier class and at statement position it is the DELETE
 * operator, so `#access-filter` parses as `define ["access"]` plus a deleted
 * `filter` — with no error. On an annotation whose failure mode is "no gate, no
 * error", a name that is silently wrong one keystroke away is not worth the
 * aesthetics.
 */

/**
 * The lock — *may this caller reach this source at all?* Its body is a
 * `'literal' <op> $GIVEN` term or a whole-body `true`/`false` sentinel, and a
 * caller it does not admit is refused rather than served zero rows.
 */
export const AUTHORIZE_ROUTE = "authorize";

/**
 * The row filter — *which rows may this caller see?* Its body is a
 * `field_path <op> $GIVEN` term grafted onto the source the query enters
 * through; a caller it admits nowhere gets their (empty) rows, not a refusal.
 */
export const ACCESS_FILTER_ROUTE = "access_filter";

/**
 * Every spelling a note may be WRITTEN in. Equal to
 * {@link CANONICAL_AUTHORIZE_ROUTES} because there is no alias; the two exports
 * stay separate because their call sites mean different things, and a future
 * recognized-but-not-canonical spelling would divide them again.
 *
 * The near-miss sweep iterates this. It previously leaned on a deprecated bare
 * `authorize` alias sitting here to refuse `#(AUTHORIZE)` and `# (authorize)`;
 * that refusal is now stated outright in `authorize.ts`'s `nearMissRouteNames`
 * rather than falling out of this list's contents.
 */
export const RECOGNIZED_AUTHORIZE_SPELLINGS: readonly string[] = [
   ACCESS_FILTER_ROUTE,
   AUTHORIZE_ROUTE,
];

/**
 * Every route anything downstream may group, walk or key on.
 *
 * Order is load-bearing and the filter comes first: own-annotation collection
 * returns early per route (`gate_classification.ts`'s
 * `gateExprsForOwnAnnotations`) and the ancestor walk runs once per route, so
 * anything memoizing across the walk sees the filter route first.
 */
export const CANONICAL_AUTHORIZE_ROUTES: readonly string[] = [
   ACCESS_FILTER_ROUTE,
   AUTHORIZE_ROUTE,
];

/** The canonical route for a written spelling, or `undefined` if it is not an
 *  access-control route at all. */
export function canonicalAuthorizeRoute(
   route: string | undefined,
): string | undefined {
   switch (route) {
      case ACCESS_FILTER_ROUTE:
         return ACCESS_FILTER_ROUTE;
      case AUTHORIZE_ROUTE:
         return AUTHORIZE_ROUTE;
      default:
         return undefined;
   }
}

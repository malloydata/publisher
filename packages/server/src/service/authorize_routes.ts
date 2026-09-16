// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The annotation route names an authorize gate is collected under.
 *
 * Its own module, with ZERO imports, for two reasons. It is read by both
 * `authorize.ts` and `authorize_grammar.ts`, which previously each declared
 * their own copy of the same two literals — a latent fail-open, because
 * `parseAuthorizeGrammarBody` decides `row_level_term_in_source_authorize`
 * against the grammar module's copy while `authorize.ts` classifies notes
 * against its own, so a value changed in one file alone would move enforcement
 * with every test still green. And `authorize.ts` bundles into the
 * package-load worker, which constrains what it may import (see its module
 * doc) — a constant module that imports nothing cannot violate that.
 */

/** The row-level route: *which rows may this caller see?* */
export const AUTHORIZE_ROUTE = "authorize";

/**
 * The source-level route: *may this caller reach this source at all?* Its body
 * is restricted to `'literal' <op> $GIVEN` terms — see
 * `authorize_grammar.ts`'s `row_level_term_in_source_authorize`.
 */
export const SOURCE_AUTHORIZE_ROUTE = "source-authorize";

/** Every route an authorize gate is recognized under, for a caller that needs
 *  to enumerate both. */
export const AUTHORIZE_ROUTES: readonly string[] = [
   AUTHORIZE_ROUTE,
   SOURCE_AUTHORIZE_ROUTE,
];

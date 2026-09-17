// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The annotation route names an authorize gate can be WRITTEN under, and the
 * mapping from a written spelling to the ONE route everything downstream keys
 * on.
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
 *
 * Both names are snake_case because Malloy's own multi-word tag names are
 * (`bar_chart`, `shape_map`, `url_template`, every `##!` flag), and because a
 * hyphen is safe only in the bracketed ROUTE position: in the bare-tag
 * position `-` is not in the identifier class and at statement position it is
 * the DELETE operator, so `#source-authorize` parses as `define ["source"]`
 * plus a deleted `authorize` — with no error. On an annotation whose failure
 * mode is "no gate, no error", a name that is silently wrong one keystroke
 * away is not worth the aesthetics.
 */

/** The route a row-level gate — *which rows may this caller see?* — is
 *  collected, grouped, walked and keyed under. */
export const ROW_AUTHORIZE_ROUTE = "row_authorize";

/**
 * The route a source-level gate — *may this caller reach this source at
 * all?* — is collected under. Its body is restricted to `'literal' <op>
 * $GIVEN` terms; see `authorize_grammar.ts`'s
 * `row_level_term_in_source_authorize`.
 */
export const SOURCE_AUTHORIZE_ROUTE = "source_authorize";

/**
 * DEPRECATED alias for {@link ROW_AUTHORIZE_ROUTE}. Recognized as a spelling,
 * never canonical: {@link canonicalAuthorizeRoute} folds it into the row route
 * before anything groups, walks or keys on it. It keeps loading and keeps
 * behaving exactly as it does today — published posts and customer models
 * carry live `#(authorize)` examples that cannot be recalled, so a load
 * refusal is off the table.
 */
export const DEPRECATED_AUTHORIZE_ROUTE = "authorize";

/**
 * Every spelling a note may be WRITTEN in. The near-miss sweep iterates this
 * rather than {@link CANONICAL_AUTHORIZE_ROUTES}: pointing that sweep at the
 * canonical two would stop refusing `# (authorize)` and `#( authorize )`,
 * which are refused today — a fail-open introduced by the rename itself.
 */
export const RECOGNIZED_AUTHORIZE_SPELLINGS: readonly string[] = [
   ROW_AUTHORIZE_ROUTE,
   DEPRECATED_AUTHORIZE_ROUTE,
   SOURCE_AUTHORIZE_ROUTE,
];

/**
 * Every route anything downstream may group, walk or key on. Exactly TWO, and
 * that is the whole point of canonicalizing rather than adding the alias as a
 * third route. Own-annotation collection returns early per route
 * (`gate_classification.ts`'s `gateExprsForOwnAnnotations`) and the ancestor
 * walk runs once per route, so an alias reaching here as its own route would
 * AND against its canonical twin instead of joining it: an extension's
 * deliberate `#(row_authorize) true` over a `#(authorize)` base would see no
 * own note ON THE ALIAS ROUTE, inherit the base's lock, and stay locked — with
 * no load error. Fail-closed, but the author's re-open became dead text.
 */
export const CANONICAL_AUTHORIZE_ROUTES: readonly string[] = [
   ROW_AUTHORIZE_ROUTE,
   SOURCE_AUTHORIZE_ROUTE,
];

/** The canonical route for a written spelling, or `undefined` if it is not an
 *  authorize route at all. */
export function canonicalAuthorizeRoute(
   route: string | undefined,
): string | undefined {
   switch (route) {
      case ROW_AUTHORIZE_ROUTE:
      case DEPRECATED_AUTHORIZE_ROUTE:
         return ROW_AUTHORIZE_ROUTE;
      case SOURCE_AUTHORIZE_ROUTE:
         return SOURCE_AUTHORIZE_ROUTE;
      default:
         return undefined;
   }
}

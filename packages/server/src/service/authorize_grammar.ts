// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(authorize)` / `#(source-authorize)` body grammar.
 *
 * `#(authorize)` no longer accepts an arbitrary Malloy boolean handed
 * verbatim to the compiler. Its body is a narrow grammar publisher parses
 * itself: one or more terms joined by `and`, each either a ROW-LEVEL term
 * (`field_path <op> $GIVEN`, field path on the left) or a SOURCE-LEVEL term
 * (`'<literal>' in $GIVEN`), whose literal and given may be written in either
 * order since neither side is a column. The operator is fixed by
 * the given's declared arity — `in` for a list-typed given, `=` for a
 * scalar one. Nothing else parses: no `or`, `not`, `!=`, `<`/`>`/`<=`/`>=`,
 * `like`, `is not null`, no function calls, no literal on the right of a
 * row-level term.
 *
 * `#(source-authorize)` is a second annotation route ({@link
 * SOURCE_AUTHORIZE_ROUTE}) declared on a `source:` line exactly like
 * `#(authorize)`, and parsed by this same grammar — but every term its body
 * declares must be SOURCE-LEVEL (the whole-body `false`/`true` sentinels
 * below are the carve-outs): it is a rule about the CALLER, not the row, and
 * ANDs with the row-level `#(authorize)` gate rather than replacing or
 * bypassing it.
 * There is deliberately no spelling anywhere in this grammar for "admit and
 * skip the row filter". See `gate_classification.ts`'s `collectEntryPointGates`
 * for how the two routes' gates are collected (independently, so an own
 * declaration on one route never sheds the other's inherited gate) and
 * combined (AND, identically to two `#(authorize)` notes).
 *
 * Two exceptions, deliberately narrow: a body that is EXACTLY (trimmed,
 * case-insensitive) `false` or `true` — never a term inside an `and` —
 * parses as an unconditional deny or an unconditional admit. Every other
 * term references a caller-suppliable given, so without `false` there is no
 * gate a caller cannot eventually satisfy.
 *
 * `true` is legal because of INHERITANCE, not because of how the annotation
 * reads on its own. `gate_classification.ts`'s `collectEntryPointGates`
 * short-circuits on a source's OWN notes, so a source declaring none
 * inherits its ancestor's gate: over a `#(authorize) false` base, omitting
 * the annotation inherits the lock rather than opening it. Every other legal
 * body references a given, so `true` is the ONLY spelling for "this
 * extension is deliberately open", and without it the
 * locked-base-plus-curated-extensions pattern `docs/authorize.md` teaches can
 * lock a base and narrow it but can never re-open any part of it. The
 * argument that an admit-everyone gate is not access control and should just
 * be omitted holds ONLY for a source with no ancestor gate, where the
 * annotation is a harmless marker that the source is open by decision rather
 * than by omission; do not let it take the capability away from the
 * inherited case, which is the case this grammar was built for.
 *
 * `true` is also the only token in this grammar that turns a gate OFF, and
 * own-wins-over-ancestor means one line on an extension opens a locked base,
 * so it carries its own guards: it is refused alongside any sibling note
 * ({@link AuthorizeGrammarRejectionCause}'s `admit_all_with_sibling`, on its
 * own route), a
 * caller-minted one is still a 400 on the caller-annotation guard
 * (`assertNoCallerAuthorizeAnnotation`), and every own declaration of it is
 * counted at load (`publisher_authorize_admit_all_total`) so "how many
 * sources are gated open" is answerable without reading every model.
 *
 * This module answers "does this body fit the grammar", nothing more. It
 * does not resolve a field path against a struct (a fan-out join check needs
 * the compiled IR — see `gate_classification.ts`'s
 * `assertNoFanoutFieldPath`) and it does not feed the graft path: the
 * `collectAuthorizeExprs` -> `gateFilterText` -> `liftGateCondition`
 * pipeline still hands the author's own text to the compiler unmodified —
 * Malloy already compiles a bare `false` or `true` as a boolean literal on
 * its own, which is what the retired string form relied on too, so accepting
 * either here needs no graft-side change (`true` grafts as `where: (true)`,
 * a live filter that keeps every row).
 * This module only decides whether that text is legal to hand over at all.
 */

import { routeOf } from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";
import { AUTHORIZE_ROUTE, SOURCE_AUTHORIZE_ROUTE } from "./authorize_routes";

/** Malloy's own routing for ONE note — see `authorize.ts`'s identical helper. */
function noteRoute(text: string): string | undefined {
   return routeOf({ value: text.trimStart() } as Parameters<typeof routeOf>[0]);
}

/** Every way an `#(authorize)` body is refused by the grammar, named
 *  distinctly so a caller (and a test) can tell which rule fired rather than
 *  pattern-matching a message string. `fanout_path` is raised by
 *  `gate_classification.ts`'s `assertNoFanoutFieldPath`, not by this module —
 *  it needs the compiled struct, which this module never sees — but shares
 *  this error shape rather than inventing a second one. */
export type AuthorizeGrammarRejectionCause =
   | "empty_body"
   | "compound_boolean"
   | "negated_operator"
   | "comparison_operator"
   | "left_not_field_path"
   | "missing_given_reference"
   | "malformed_body"
   | "duplicate_given"
   | "duplicate_field_path"
   | "mixed_scope_body"
   | "deny_all_with_sibling"
   // The admit-all `true` sentinel alongside another note ON THE SAME ROUTE.
   // A route's notes AND into one body, so `true and x` reduces to `x` and
   // the sentinel is dead text there. Route-scoped, unlike
   // `deny_all_with_sibling`: `true` sheds only its own route's inherited
   // gate, so it is live beside a note on the OTHER route. Raised by
   // {@link assertAuthorizeGrammarTermsCoherent}, after
   // `deny_all_with_sibling`, which is the fail-closed reading when one
   // route carries both sentinels.
   | "admit_all_with_sibling"
   | "operator_arity_mismatch"
   | "fanout_path"
   // A row-level term (field on the left) inside a `#(source-authorize)`
   // body — that route is a rule about the CALLER, not the row, so every
   // term must be `source_level` (the `deny_all` sentinel is the one carved
   // out, since it names no row at all). Raised by
   // `parseAuthorizeGrammarBody` when `route` is the source-authorize route.
   | "row_level_term_in_source_authorize"
   // `$GIVEN in 'literal'`: unlike `=`, `in` is not reversible — the graft
   // compiles the author's ORIGINAL text unchanged, and Malloy rejects array-
   // in-string membership, so this would pass validation and then fail at
   // model compilation. Raised by `parseTerm`.
   | "reversed_in_operands";

/**
 * An `#(authorize)` annotation that fails this module's grammar. Extends
 * {@link ModelCompilationError} so it maps to the same 424 an author already
 * gets from any other malformed gate. `rejectionCause` is the
 * machine-readable half of the message, for a caller that wants to branch
 * on WHY rather than parse prose.
 */
export class AuthorizeGrammarError extends ModelCompilationError {
   constructor(
      public readonly rejectionCause: AuthorizeGrammarRejectionCause,
      message: string,
   ) {
      super({ message });
   }
}

const IDENT = "[A-Za-z_][A-Za-z0-9_]*";
/** A backtick-quoted identifier — Malloy's escape hatch for a name that needs
 *  quoting (a space, a reserved word, a leading digit), e.g. `` `cost center` ``. */
const QUOTED_IDENT = "`[^`]+`";
const SEGMENT = `(?:${IDENT}|${QUOTED_IDENT})`;
/** A single column or a dotted join path — never a call, operator, or literal. */
const FIELD_PATH_RE = new RegExp(`^${SEGMENT}(?:\\.${SEGMENT})*$`);
/** `$NAME` and nothing else — no trailing text, no missing sigil. */
const GIVEN_REF_RE = new RegExp(`^\\$(${IDENT})$`);
/** A single- or double-quoted string literal consuming the whole (trimmed) side. */
const STRING_LITERAL_RE = /^(?:'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*")$/;

const NEGATED_OPERATOR_RE = /!=/gu;
const COMPARISON_OPERATOR_RE = /(?:>=|<=|>|<)/gu;

/** One parsed TERM of an `#(authorize)` body — never the whole-body
 *  `deny_all`/`admit_all` exceptions, which are not terms and cannot appear
 *  alongside one; see {@link AuthorizeGrammarTerm}. */
export type AuthorizeGrammarParsedTerm =
   | {
        scope: "row_level";
        fieldPath: string;
        /** `fieldPath` split on its top-level dots, each segment already
         *  unquoted (backticks stripped) — the shape a struct walk needs to
         *  compare against a field's real name, computed once here rather
         *  than re-split downstream with a naive `.split(".")` that would
         *  both break on a literal dot inside a backtick-quoted segment and
         *  leave the backticks in place, so a quoted segment can never match
         *  the field it actually names. See `gate_classification.ts`'s
         *  `assertNoFanoutFieldPath`. */
        fieldPathSegments: readonly string[];
        given: string;
        operator: "=" | "in";
     }
   | { scope: "source_level"; literal: string; given: string };

/** What {@link parseAuthorizeGrammarBody} returns: either the parsed terms
 *  of an ordinary `and`-joined body, or a single whole-body sentinel —
 *  `deny_all` for a bare `false`, `admit_all` for a bare `true` — never a
 *  mix, since either sentinel can only ever be the sole element of the
 *  returned array. */
export type AuthorizeGrammarTerm =
   | AuthorizeGrammarParsedTerm
   | { scope: "deny_all" }
   | { scope: "admit_all" };

/**
 * Split an already-`FIELD_PATH_RE`-validated field path on its top-level
 * dots, unquoting each backtick-quoted segment. A dot inside a backtick
 * segment (`` `cost.center` ``) is part of that segment's name, not a
 * separator — this walks character by character rather than `.split(".")`
 * for exactly that reason.
 */
function splitFieldPathSegments(fieldPath: string): string[] {
   const segments: string[] = [];
   let i = 0;
   while (i < fieldPath.length) {
      if (fieldPath[i] === "`") {
         const end = fieldPath.indexOf("`", i + 1);
         // FIELD_PATH_RE guarantees a matching closing backtick.
         segments.push(fieldPath.slice(i + 1, end));
         i = end + 1;
      } else {
         const dot = fieldPath.indexOf(".", i);
         const end = dot === -1 ? fieldPath.length : dot;
         segments.push(fieldPath.slice(i, end));
         i = end;
      }
      if (fieldPath[i] === ".") i++;
   }
   return segments;
}

/** Shared tail for every grammar rejection message — the reference grammar
 *  recap, independent of whether the violation was found within one body's
 *  own text ({@link reject}) or across more than one note's terms
 *  ({@link rejectCoherence}). */
const GRAMMAR_SUMMARY =
   "#(authorize) only accepts one or more `and`-joined terms, each " +
   "either `<column> <op> $GIVEN` (a single field or a dotted join " +
   "path on the left) or `'<literal>' in $GIVEN` (a literal on the " +
   "left), where <op> is fixed by the given's own arity: `in` for a " +
   "list-typed given, `=` for a scalar one.";

function rejectionMessage(
   sourceName: string,
   body: string,
   detail: string,
): string {
   return (
      `Source "${sourceName}" declares \`#(authorize) ${body}\`: ${detail} ` +
      GRAMMAR_SUMMARY
   );
}

function reject(
   sourceName: string,
   body: string,
   cause: AuthorizeGrammarRejectionCause,
   detail: string,
): never {
   throw new AuthorizeGrammarError(
      cause,
      rejectionMessage(sourceName, body, detail),
   );
}

/** Same shape as {@link reject}, for a violation found across more than one
 *  note's terms rather than within a single body's own text — there is no
 *  single `body` string to echo, so the message names the source only. */
function rejectCoherence(
   sourceName: string,
   cause: AuthorizeGrammarRejectionCause,
   detail: string,
): never {
   throw new AuthorizeGrammarError(
      cause,
      `Source "${sourceName}" declares #(authorize) notes whose terms ` +
         `conflict: ${detail} ${GRAMMAR_SUMMARY}`,
   );
}

/**
 * Split `body` on a word-bounded, depth-0, outside-any-quote `and` — literal
 * aware, unlike a bare regex over the whole body, which would cut
 * `'research and development' in $GROUPS` in half. Tracks single- and
 * double-quote state and parenthesis depth as it scans.
 *
 * Also collects any depth-0, outside-quote `or`/`not` token it sees along the
 * way (`compoundTokens`), so the caller can refuse a compound boolean while
 * still accepting one buried inside a string literal (`'rock or roll' in
 * $GROUPS` is a legal body).
 *
 * A backtick-quoted identifier is quoted for this purpose too: `` `and` `` is
 * a column name, not a conjunction.
 */
function splitTerms(body: string): {
   terms: string[];
   compoundTokens: string[];
} {
   const terms: string[] = [];
   const compoundTokens: string[] = [];
   let depth = 0;
   let quote: Quote | undefined;
   let start = 0;
   let i = 0;
   while (i < body.length) {
      const ch = body[i];
      if (quote) {
         if (ch === "\\" && quote !== "`") {
            i += 2;
            continue;
         }
         if (ch === quote) quote = undefined;
         i++;
         continue;
      }
      if (isQuote(ch)) {
         quote = ch;
         i++;
         continue;
      }
      if (ch === "(") {
         depth++;
         i++;
         continue;
      }
      if (ch === ")") {
         depth = Math.max(0, depth - 1);
         i++;
         continue;
      }
      if (depth === 0) {
         const wordMatch = /^(and|or|not)\b/i.exec(body.slice(i));
         if (wordMatch && (i === 0 || !/[A-Za-z0-9_]/.test(body[i - 1]))) {
            const word = wordMatch[1].toLowerCase();
            if (word === "and") {
               terms.push(body.slice(start, i));
               i += wordMatch[0].length;
               start = i;
               continue;
            }
            compoundTokens.push(word);
         }
      }
      i++;
   }
   terms.push(body.slice(start));
   return { terms, compoundTokens };
}

/** A string literal's delimiter, or a backtick-quoted identifier's. Both hide
 *  their contents from every token scan in this module. */
type Quote = "'" | '"' | "`";

function isQuote(ch: string): ch is Quote {
   return ch === "'" || ch === '"' || ch === "`";
}

/** Find an operator token outside any quoted substring. Returns its index in
 *  `s`, or -1. */
function findOperatorOutsideQuotes(s: string, re: RegExp): number {
   let quote: Quote | undefined;
   for (let i = 0; i < s.length; i++) {
      const ch = s[i];
      if (quote) {
         if (ch === "\\" && quote !== "`") {
            i++;
            continue;
         }
         if (ch === quote) quote = undefined;
         continue;
      }
      if (isQuote(ch)) {
         quote = ch;
         continue;
      }
      re.lastIndex = i;
      const m = re.exec(s);
      if (m && m.index === i) return i;
   }
   return -1;
}

const IN_OPERATOR_RE = /\bin\b/giu;
const EQ_OPERATOR_RE = /=/gu;

/**
 * Parse and validate one term (`field_path <op> $GIVEN` or `'<literal>' in
 * $GIVEN`), returning its classified shape. Throws on any grammar violation
 * — including an arity mismatch against `givenDeclaredTypes`, when the
 * given's declared type is known.
 */
function parseTerm(
   sourceName: string,
   body: string,
   term: string,
   givenDeclaredTypes: ReadonlyMap<string, string>,
): AuthorizeGrammarParsedTerm {
   const trimmed = term.trim();
   if (findOperatorOutsideQuotes(trimmed, NEGATED_OPERATOR_RE) !== -1) {
      reject(sourceName, body, "negated_operator", "`!=` is not allowed.");
   }
   if (findOperatorOutsideQuotes(trimmed, COMPARISON_OPERATOR_RE) !== -1) {
      reject(
         sourceName,
         body,
         "comparison_operator",
         "only `=` and `in` are allowed, not `<`/`>`/`<=`/`>=`.",
      );
   }

   const inIdx = findOperatorOutsideQuotes(trimmed, IN_OPERATOR_RE);
   const eqIdx = findOperatorOutsideQuotes(trimmed, EQ_OPERATOR_RE);
   let operator: "=" | "in";
   let opIdx: number;
   let opLen: number;
   if (inIdx !== -1 && (eqIdx === -1 || inIdx < eqIdx)) {
      operator = "in";
      opIdx = inIdx;
      opLen = 2;
   } else if (eqIdx !== -1) {
      operator = "=";
      opIdx = eqIdx;
      opLen = 1;
   } else {
      reject(
         sourceName,
         body,
         "malformed_body",
         "no `=` or `in` operator was found.",
      );
   }

   let left = trimmed.slice(0, opIdx).trim();
   let right = trimmed.slice(opIdx + opLen).trim();

   // A SOURCE-LEVEL `=` term compares a literal to a given, and reads equally
   // well either way round (`$ROLE = 'admin'` is the spelling publisher's own
   // fixtures and docs used first). Normalize to literal-on-the-left so one
   // shape reaches the checks below. A ROW-LEVEL term is not reversible: the
   // field path is what the build scan groups by and what the graft filters
   // on, so it stays on the left.
   //
   // `in` is not reversible the way `=` is: `'literal' in $GIVEN` is array
   // membership, but `$GIVEN in 'literal'` is Malloy's (invalid) array-in-
   // string form. The graft compiles the author's ORIGINAL text unchanged (no
   // normalization step), so silently swapping here would validate a body
   // that then fails at model compilation. Refuse it instead, naming the
   // literal-first spelling.
   if (GIVEN_REF_RE.test(left) && STRING_LITERAL_RE.test(right)) {
      if (operator === "in") {
         reject(
            sourceName,
            body,
            "reversed_in_operands",
            `\`${left} in ${right}\` reads $GIVEN in 'literal', which Malloy ` +
               `rejects as array-in-string membership. Write the literal ` +
               `first: \`${right} in ${left}\`.`,
         );
      }
      [left, right] = [right, left];
   }

   const leftIsFieldPath = FIELD_PATH_RE.test(left);
   const leftIsLiteral = STRING_LITERAL_RE.test(left);

   if (!leftIsFieldPath && !leftIsLiteral) {
      reject(
         sourceName,
         body,
         "left_not_field_path",
         `\`${left}\` is neither a field path nor a string literal — one ` +
            "side must be a single column, a dotted join path, or a quoted " +
            "literal, and the other a `$GIVEN`.",
      );
   }

   const givenMatch = GIVEN_REF_RE.exec(right);
   if (!givenMatch) {
      reject(
         sourceName,
         body,
         "missing_given_reference",
         `\`${right}\` is not a given reference — a term compares against ` +
            "`$NAME`.",
      );
   }
   const given = givenMatch[1];

   // A given's declared type renders as the bare Malloy type name
   // (`malloyGivenToApi`) — `"array"` for a list-typed given (Malloy has no
   // element-type-qualified rendering), any scalar type name otherwise. A
   // given absent from the map (unresolvable at this call site) skips the
   // check rather than guessing.
   const declaredType = givenDeclaredTypes.get(given);
   if (declaredType !== undefined) {
      const isListType = declaredType === "array";
      if (operator === "=" && isListType) {
         reject(
            sourceName,
            body,
            "operator_arity_mismatch",
            `\`$${given}\` is a list-typed given — use \`in\`, not \`=\`.`,
         );
      }
      if (operator === "in" && !isListType) {
         reject(
            sourceName,
            body,
            "operator_arity_mismatch",
            `\`$${given}\` is a scalar given — use \`=\`, not \`in\`.`,
         );
      }
   }

   if (leftIsLiteral) {
      return { scope: "source_level", literal: left, given };
   }
   return {
      scope: "row_level",
      fieldPath: left,
      fieldPathSegments: splitFieldPathSegments(left),
      given,
      operator,
   };
}

/** One term paired with the route its declaring note was parsed under —
 *  what {@link assertAuthorizeGrammarTermsCoherent} needs to tell "two
 *  terms from the same route" (must be mutually coherent) from "two terms
 *  from different routes" (meant to AND, not required to agree on scope or
 *  given). Kept as a wrapper rather than a field on {@link AuthorizeGrammarTerm}
 *  itself so a route stays a caller-side bookkeeping detail — every existing
 *  reader of a parsed term (the fan-out check, the graft pipeline) has no
 *  reason to know it. */
export type AuthorizeGrammarRoutedTerm = {
   term: AuthorizeGrammarTerm;
   route: string;
};

/**
 * Cross-term coherence for an `#(authorize)` gate assembled from MORE THAN
 * ONE note — `duplicate_given`, `duplicate_field_path`, `mixed_scope_body`,
 * and the `false` deny-all sentinel all used to be safe checking WITHIN one
 * body, because a source could declare at most one `#(authorize)`. Now that
 * repeats are legal (and AND together — see this module's doc), the same
 * four mistakes can be spread across notes instead of terms in one body, so
 * this checks the SET of terms a declaring source contributes rather than
 * one note's own list. {@link parseAuthorizeGrammarBody} calls this on its
 * own single-body terms (trivially a no-op for one note); a caller
 * assembling more than one note for the same declaring source (today,
 * `gate_classification.ts`'s `assertAuthorizeGrammarValid`, over the OWN
 * groups an `AuthorizeMap` entry carries across BOTH routes) calls it again
 * over the concatenation.
 *
 * `duplicate_given`, `duplicate_field_path`, and `mixed_scope_body` are
 * scoped PER ROUTE (`route` on each entry), never across routes: a term
 * declared under `#(authorize)` and one declared under `#(source-authorize)`
 * on the SAME source are meant to AND, not agree on scope or given — e.g.
 * `#(authorize) org_id in $GROUPS` alongside
 * `#(source-authorize) 'finance' in $GROUPS` is the intended design, and
 * must stay legal even though it reuses `$GROUPS` and mixes scope. Two
 * routes exist today (`AUTHORIZE_ROUTE`, `SOURCE_AUTHORIZE_ROUTE`); a further
 * route would slot in the same way, by tagging its own terms with its own
 * route string and calling this same function — nothing here needs to
 * change.
 *
 * `deny_all_with_sibling` is the one check that is NOT route-scoped: an
 * unconditional `#(authorize) false` alongside ANY sibling note — same route
 * or not — is the same authoring mistake regardless of which route the
 * companion used, because a deny-all admits nothing and no sibling on any
 * route can change what is served. It is checked over every entry passed in,
 * before the per-route split below.
 *
 * `admit_all_with_sibling` is route-scoped, and the asymmetry is the point.
 * `true` sheds only its OWN route's inherited gate, so `#(authorize) true`
 * beside `#(source-authorize) 'finance' in $GROUPS` is a live combination —
 * open every row of a base that locked them, still gate the caller — not
 * dead text. WITHIN one route the notes AND into a single body, where `true
 * and x` really does reduce to `x` and the sentinel is dead, which is what
 * that cause names.
 *
 * Deny is checked first, so a source carrying both sentinels on one route is
 * reported as `deny_all_with_sibling` — the fail-closed reading.
 *
 * CRITICAL: never call this over a flattened list spanning more than one
 * DECLARING SOURCE (`AuthorizeMap`'s `groups.flat()`) — a query-source
 * base's own gate and its composite member's own gate are two different
 * sources' gates that AND by design and must never be cross-checked against
 * each other; see `AuthorizeMap`'s doc and `assertAuthorizeGrammarValid`.
 */
export function assertAuthorizeGrammarTermsCoherent(
   sourceName: string,
   terms: readonly AuthorizeGrammarRoutedTerm[],
): void {
   if (
      terms.length > 1 &&
      terms.some(({ term }) => term.scope === "deny_all")
   ) {
      rejectCoherence(
         sourceName,
         "deny_all_with_sibling",
         "an unconditional `false` deny-all cannot be combined with any " +
            "other `#(authorize)` note — a deny-all admits nothing, so a " +
            "sibling note can never change what is served and its " +
            "presence is very likely a mistake.",
      );
   }

   const byRoute = new Map<string, AuthorizeGrammarTerm[]>();
   for (const { term, route } of terms) {
      if (term.scope === "deny_all") continue;
      const list = byRoute.get(route);
      if (list) list.push(term);
      else byRoute.set(route, [term]);
   }

   for (const allRouteTerms of byRoute.values()) {
      // Route-scoped, unlike the deny-all above — see this function's doc.
      // Checked after it, so both sentinels on one route report the deny.
      if (
         allRouteTerms.length > 1 &&
         allRouteTerms.some((t) => t.scope === "admit_all")
      ) {
         rejectCoherence(
            sourceName,
            "admit_all_with_sibling",
            "an unconditional `true` admit-all cannot be combined with " +
               "another note on the same route — that route's notes AND " +
               "together, so `true and x` reduces to `x` and the admit-all " +
               "is dead text. To open one route while another still gates, " +
               "declare the `true` on its own route only.",
         );
      }
      // Both sentinels named, not just `admit_all`: `deny_all` is already
      // gone by the `continue` above, but a predicate that relies on that
      // would silently mistype one if the `continue` ever moved.
      const routeTerms = allRouteTerms.filter(
         (t): t is AuthorizeGrammarParsedTerm =>
            t.scope !== "admit_all" && t.scope !== "deny_all",
      );
      const rowLevel = routeTerms.filter((t) => t.scope === "row_level");
      const sourceLevel = routeTerms.filter((t) => t.scope === "source_level");
      if (rowLevel.length > 0 && sourceLevel.length > 0) {
         rejectCoherence(
            sourceName,
            "mixed_scope_body",
            "a row-level term (field on the left) may not be combined with " +
               "a source-level term (`'literal' in/= $GIVEN`) on the same " +
               "route.",
         );
      }

      const seenGivens = new Set<string>();
      const seenFieldPaths = new Set<string>();
      for (const t of routeTerms) {
         if (seenGivens.has(t.given)) {
            rejectCoherence(
               sourceName,
               "duplicate_given",
               `\`$${t.given}\` is used by more than one term — a given ` +
                  "can back at most one term per route.",
            );
         }
         seenGivens.add(t.given);
         if (t.scope === "row_level") {
            // Canonical on `fieldPathSegments`, not the authored spelling: a
            // backtick-quoted segment and a bare one (`` `region` `` vs
            // `region`) name the same column but differ as strings.
            // `JSON.stringify` is collision-free even if a segment itself
            // contains whatever plain separator this could otherwise join on.
            const canonicalPath = JSON.stringify(t.fieldPathSegments);
            if (seenFieldPaths.has(canonicalPath)) {
               rejectCoherence(
                  sourceName,
                  "duplicate_field_path",
                  `\`${t.fieldPath}\` is used by more than one term.`,
               );
            }
            seenFieldPaths.add(canonicalPath);
         }
      }
   }
}

/**
 * Parse and validate one `#(authorize)` body — the note's payload, already
 * extracted by {@link ../service/authorize}'s `collectAuthorizeExprs` —
 * against the grammar. Throws an {@link AuthorizeGrammarError} on any
 * violation; returns the parsed terms otherwise, so a caller
 * (`gate_classification.ts`) can run the fan-out check that needs the
 * compiled struct this module never sees.
 *
 * `givenDeclaredTypes` is the model's given surface (name -> declared Malloy
 * type); a given absent from it (unresolvable at this point) skips the
 * arity check rather than failing it — a load path that has no given
 * surface handy would otherwise be forced to guess.
 *
 * Handles SYNTAX only — term splitting, `parseTerm`, `compound_boolean`, the
 * whole-body `deny_all`/`admit_all` exceptions. The four checks that need to see more
 * than this one body's own terms live in
 * {@link assertAuthorizeGrammarTermsCoherent}, called here on this body's
 * own terms so a single-note source is refused exactly as before.
 *
 * `route` defaults to {@link AUTHORIZE_ROUTE} so every existing caller keeps
 * its exact prior behavior. Passed {@link SOURCE_AUTHORIZE_ROUTE}, every
 * parsed term must be `scope: "source_level"` — the two whole-body
 * sentinels are the carve-outs, since neither `false` nor `true` names a row
 * at all and both are accepted identically on both routes (each returns
 * before the route check below; see this module's doc). A `row_level` term
 * reaching here under that route is refused as
 * `row_level_term_in_source_authorize`, the mirror of `mixed_scope_body`.
 */
export function parseAuthorizeGrammarBody(
   sourceName: string,
   body: string,
   givenDeclaredTypes: ReadonlyMap<string, string>,
   route: string = AUTHORIZE_ROUTE,
): AuthorizeGrammarTerm[] {
   const trimmedBody = body.trim();
   if (trimmedBody.length === 0) {
      reject(
         sourceName,
         trimmedBody,
         "empty_body",
         "the expression body is empty.",
      );
   }

   // The two exceptions to "every term references a given" — see this
   // module's doc. Checked against the WHOLE body, before splitting on
   // `and`, so `false and org_id in $GROUPS` does NOT take this path: it is
   // not a deny-all, it is a two-term body whose first term is malformed,
   // and falls through to the ordinary per-term errors below. `true and
   // org_id = $A` is read the same way.
   if (trimmedBody.toLowerCase() === "false") {
      return [{ scope: "deny_all" }];
   }
   if (trimmedBody.toLowerCase() === "true") {
      return [{ scope: "admit_all" }];
   }

   const { terms: rawTerms, compoundTokens } = splitTerms(trimmedBody);
   if (compoundTokens.length > 0) {
      reject(
         sourceName,
         trimmedBody,
         "compound_boolean",
         `a compound boolean (\`${compoundTokens[0]}\`) is not allowed — ` +
            "join terms with `and` only.",
      );
   }

   const parsed = rawTerms.map((term) =>
      parseTerm(sourceName, trimmedBody, term, givenDeclaredTypes),
   );

   if (route === SOURCE_AUTHORIZE_ROUTE) {
      const rowLevelTerm = parsed.find((t) => t.scope === "row_level");
      if (rowLevelTerm) {
         reject(
            sourceName,
            trimmedBody,
            "row_level_term_in_source_authorize",
            "a row-level term (field on the left) is not allowed in " +
               "`#(source-authorize)` — move the term to `#(authorize)`.",
         );
      }
   }

   assertAuthorizeGrammarTermsCoherent(
      sourceName,
      parsed.map((term) => ({ term, route })),
   );

   return parsed;
}

/**
 * Every retired annotation route this module still watches for, so a
 * leftover marker from a removed feature fails load rather than fails open —
 * see {@link containsRetiredRouteTag}/{@link reachesRetiredRouteTagBelow},
 * and `gate_classification.ts`'s `assertNoRetiredRouteMarkers`, which this
 * list backs. `"partition"` is `#(partition)`'s own retired route: once
 * nothing inspects it, Malloy still routes and parses it happily (its
 * bracket routing is generic, there is no registry), and a source carrying a
 * leftover marker would otherwise load clean and serve every row.
 */
export const RETIRED_ROUTES = ["partition"] as const;

/**
 * Whether any of `texts` routes (by Malloy's own routing, never a text
 * match) to one of `RETIRED_ROUTES` — same convention as
 * `authorize.ts`'s `containsAuthorizeAnnotationTag`. Returns the matched
 * route name, or `undefined`.
 */
export function containsRetiredRouteTag(texts: string[]): string | undefined {
   for (const text of texts) {
      const route = noteRoute(text);
      if (
         route !== undefined &&
         (RETIRED_ROUTES as readonly string[]).includes(route)
      ) {
         return route;
      }
   }
   return undefined;
}

/** Depth cap for {@link reachesRetiredRouteTagBelow}, matching the other IR walks. */
const MAX_RETIRED_ROUTE_WALK_DEPTH = 200;

/**
 * Whether a retired-route marker (see {@link RETIRED_ROUTES}) sits ANYWHERE
 * inside `struct`'s own IR below the struct level — on a field, a nested
 * pipeline, an inline `compose(...)`, or any other node a targeted resolver
 * does not visit.
 *
 * This is `#(partition)`'s own former `reachesPartitionTagBelow`,
 * repurposed rather than deleted: the same fail-open hazard applies to any
 * route this module retires, not only `partition` — a marker one level too
 * low is not a no-op, it is a source that publishes clean and then serves
 * every row, because nothing rejected the declaration and nothing grafted a
 * filter.
 *
 * Joins are descended into. No marker on a retired route is legitimate
 * anywhere, so a join carrying one is a finding rather than a false positive,
 * and an inline join is the one place a marker would otherwise sit unread.
 *
 * Throws past the depth cap rather than returning `undefined` — an unread
 * chain is "unknown", and every caller treats a throw as "assume a marker is
 * there".
 */
export function reachesRetiredRouteTagBelow(
   node: unknown,
   seen: WeakSet<object> = new WeakSet(),
   depth = 0,
): string | undefined {
   if (depth > MAX_RETIRED_ROUTE_WALK_DEPTH) {
      throw new Error("retired-route IR walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return undefined;
   if (seen.has(node as object)) return undefined;
   seen.add(node as object);

   if (Array.isArray(node)) {
      for (const item of node) {
         const found = reachesRetiredRouteTagBelow(item, seen, depth + 1);
         if (found) return found;
      }
      return undefined;
   }

   const record = node as Record<string, unknown>;

   if (depth > 0) {
      for (const key of ["blockNotes", "notes"]) {
         const arr = record[key];
         if (!Array.isArray(arr)) continue;
         const texts = arr
            .map((n) =>
               typeof n === "string"
                  ? n
                  : n &&
                      typeof n === "object" &&
                      typeof (n as { text?: unknown }).text === "string"
                    ? (n as { text: string }).text
                    : undefined,
            )
            .filter((text): text is string => text !== undefined);
         const found = containsRetiredRouteTag(texts);
         if (found) return found;
      }
   }

   for (const [key, value] of Object.entries(record)) {
      // The root's OWN `annotations` (and the `inherits` chain under it) is
      // exactly what a caller checking own-level notes reads separately, so
      // skipping it is what makes this "below the struct level". Deeper
      // `annotations` are field/view notes, which is the case being hunted.
      if (depth === 0 && key === "annotations") continue;
      const found = reachesRetiredRouteTagBelow(value, seen, depth + 1);
      if (found) return found;
   }
   return undefined;
}

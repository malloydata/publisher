// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(authorize)` / `#(access_filter)` body grammar.
 *
 * Neither accepts an arbitrary Malloy boolean handed verbatim to the
 * compiler. A body is a narrow grammar publisher parses itself: one or more
 * terms joined by `and`, with the operator fixed by the given's declared
 * arity — `in` for a list-typed given, `=` for a scalar one. Nothing else
 * parses: no `or`, `not`, `!=`, `<`/`>`/`<=`/`>=`, `like`, `is not null`, no
 * function calls, no literal on the right of a row-level term.
 *
 * **The annotation declares the scope, and nothing is inferred from the
 * body.** {@link AUTHORIZE_ROUTE} takes SOURCE-LEVEL terms only
 * (`'<literal>' <op> $GIVEN`, either order since neither side is a column) —
 * it is a rule about the CALLER — plus the whole-body sentinels below.
 * {@link ACCESS_FILTER_ROUTE} takes ROW-LEVEL terms only (`field_path <op>
 * $GIVEN`, field path on the left). A term on the wrong route is refused,
 * both ways, naming the other annotation. So the scope classification in
 * {@link parseTerm} is a VALIDATION rule, never a routing one.
 *
 * The mirror matters as much as the original: a source-level term on the
 * filter route grafts as a constant predicate, which answers a refused
 * caller 200 with zero rows instead of a refusal. There is likewise no
 * spelling for "admit and skip the row filter".
 *
 * Two exceptions, both on the LOCK only: a body that is EXACTLY (trimmed,
 * case-insensitive) `false` or `true` — never a term inside an `and` — is an
 * unconditional deny or admit. Every other term references a
 * caller-suppliable given, so without `false` no gate is unsatisfiable.
 * Neither is legal on the filter route: `false` there would serve zero rows
 * instead of refusing, and `true` there is a no-op.
 *
 * `true` exists because of INHERITANCE: a source with no own notes inherits
 * its ancestor's gate, so `true` is the ONLY way to re-open part of a
 * `#(authorize) false` base (the locked-base-plus-curated-extensions pattern
 * in docs/authorize.md). Because it is the only token that turns a gate OFF,
 * it is guarded: refused beside a sibling lock note
 * (`admit_all_with_sibling`), a 400 when caller-minted
 * (`assertNoCallerAuthorizeAnnotation`), and counted at load
 * (`publisher_authorize_admit_all_total`).
 *
 * This module only decides whether a body is legal. It does not resolve field
 * paths against a struct (`gate_classification.ts`'s
 * `assertNoFanoutFieldPath` does) and does not feed the graft: the author's
 * own text reaches the compiler unmodified (`true` grafts as
 * `where: (true)`, keeping every row).
 */

import { routeOf } from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";

/** Malloy's own routing for ONE note — see `authorize.ts`'s identical helper. */
function noteRoute(text: string): string | undefined {
   return routeOf({ value: text.trimStart() } as Parameters<typeof routeOf>[0]);
}

/** Every way an `#(authorize)` body is refused by the grammar, named so a
 *  caller can branch on which rule fired. `fanout_path` is raised by
 *  `gate_classification.ts`'s `assertNoFanoutFieldPath` (it needs the
 *  compiled struct) but shares this error shape. */
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
   | "deny_all_with_sibling"
   // `true` beside another lock note: the lock's notes AND, so `true and x`
   // reduces to `x` and the sentinel is dead text.
   | "admit_all_with_sibling"
   // A bare `true`/`false` under `#(access_filter)`: both are the lock's
   // question, and `false` there would serve zero rows instead of refusing.
   | "sentinel_in_access_filter"
   | "operator_arity_mismatch"
   | "fanout_path"
   // A row-level term (field on the left) inside `#(authorize)`.
   | "row_level_term_in_authorize"
   // A source-level term (literal on the left) inside `#(access_filter)`: it
   // grafts as a constant predicate, answering an excluded caller with zero
   // rows rather than a refusal.
   | "source_level_term_in_access_filter"
   // `$GIVEN in 'literal'`: the graft compiles the author's original text,
   // which Malloy rejects as array-in-string membership.
   | "reversed_in_operands";

/**
 * An `#(authorize)` annotation that fails this module's grammar. Extends
 * {@link ModelCompilationError} so it maps to the same 424 as any other
 * malformed gate; `rejectionCause` is the machine-readable reason.
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
        /** `fieldPath` split on its top-level dots, backticks stripped — what a
         *  struct walk compares against real field names. Use this, never
         *  `fieldPath.split(".")`, which breaks on a dot inside a quoted
         *  segment and keeps the backticks. */
        fieldPathSegments: readonly string[];
        given: string;
        operator: "=" | "in";
     }
   | { scope: "source_level"; literal: string; given: string };

/** What {@link parseAuthorizeGrammarBody} returns: the parsed terms of an
 *  `and`-joined body, or a single whole-body sentinel (`deny_all` for `false`,
 *  `admit_all` for `true`) — never a mix. */
export type AuthorizeGrammarTerm =
   | AuthorizeGrammarParsedTerm
   | { scope: "deny_all" }
   | { scope: "admit_all" };

/**
 * Split an already-`FIELD_PATH_RE`-validated field path on its top-level
 * dots, unquoting each backtick-quoted segment. A dot inside a backtick
 * segment (`` `cost.center` ``) is part of the name, not a separator.
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

/** Grammar recap appended to every rejection message. */
const GRAMMAR_SUMMARY =
   "An authorize gate only accepts one or more `and`-joined terms, each " +
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
      `Source "${sourceName}" declares an authorize gate \`${body}\`: ${detail} ` +
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

/** {@link reject} for a conflict across notes, where there is no single body
 *  to echo. */
function rejectCoherence(
   sourceName: string,
   cause: AuthorizeGrammarRejectionCause,
   detail: string,
): never {
   throw new AuthorizeGrammarError(
      cause,
      `Source "${sourceName}" declares authorize notes whose terms ` +
         `conflict: ${detail} ${GRAMMAR_SUMMARY}`,
   );
}

/**
 * Split `body` on a word-bounded, depth-0, outside-any-quote `and`, so
 * `'research and development' in $GROUPS` stays one term (backtick
 * identifiers count as quoted too). Also collects depth-0 `or`/`not` tokens
 * (`compoundTokens`) so the caller can refuse a compound boolean while
 * accepting `'rock or roll' in $GROUPS`.
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

   // A source-level `=` term reads either way round (`$ROLE = 'admin'`), so
   // normalize to literal-on-the-left. A row-level term is not reversible: the
   // field path is what the graft filters on. Nor is `in`: the graft compiles
   // the author's ORIGINAL text, and `$GIVEN in 'literal'` is Malloy's invalid
   // array-in-string form, so swapping would validate a body that then fails
   // to compile.
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

   // Declared types render as in `malloyGivenToApi`: `number[]`/`string[]`, or
   // bare `array` when the element type could not be rendered. Both list
   // spellings must count, or every `#(secure)` set-valued given refuses `in`
   // and fails the load. A given absent from the map skips the check rather
   // than guessing.
   const declaredType = givenDeclaredTypes.get(given);
   if (declaredType !== undefined) {
      const isListType =
         declaredType === "array" || declaredType.endsWith("[]");
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

/** One term paired with its note's route, so
 *  {@link assertAuthorizeGrammarTermsCoherent} can scope its checks per
 *  route. A wrapper rather than a field so other term readers need not know
 *  the route. */
export type AuthorizeGrammarRoutedTerm = {
   term: AuthorizeGrammarTerm;
   route: string;
};

/**
 * Cross-term coherence over the SET of notes one declaring source
 * contributes (notes repeat and AND, so the same mistakes can be spread
 * across notes). {@link parseAuthorizeGrammarBody} calls it per body;
 * `assertAuthorizeGrammarValid` calls it again over a source's own notes on
 * both routes.
 *
 * `duplicate_given` / `duplicate_field_path` are scoped PER ROUTE:
 * `#(access_filter) org_id in $GROUPS` beside `#(authorize) 'finance' in
 * $GROUPS` is intended and must stay legal.
 *
 * `deny_all_with_sibling` is NOT route-scoped: a deny-all admits nothing, so
 * any sibling on any route is dead. `admit_all_with_sibling` IS route-scoped:
 * `true` sheds only the lock's inherited gate, so beside an
 * `#(access_filter)` it is live (unlock, still filter rows). Deny is checked
 * first, so both sentinels together report the fail-closed reading.
 *
 * CRITICAL: never call this over terms from more than one DECLARING SOURCE
 * (`AuthorizeMap`'s `groups.flat()`) — a query-source base's gate and a
 * composite member's gate AND by design and must not be cross-checked.
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
            "other authorize note — a deny-all admits nothing, so a " +
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
      if (
         allRouteTerms.length > 1 &&
         allRouteTerms.some((t) => t.scope === "admit_all")
      ) {
         rejectCoherence(
            sourceName,
            "admit_all_with_sibling",
            "an unconditional `true` admit-all cannot be combined with " +
               "another `#(authorize)` note — the lock's notes AND " +
               "together, so `true and x` reduces to `x` and the admit-all " +
               "is dead text. To unlock a base while its rows stay " +
               "filtered, pair the `true` with `#(access_filter)` instead.",
         );
      }
      // Names both sentinels so the narrowing does not depend on the
      // `continue` above.
      const routeTerms = allRouteTerms.filter(
         (t): t is AuthorizeGrammarParsedTerm =>
            t.scope !== "admit_all" && t.scope !== "deny_all",
      );
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
            // Keyed on the unquoted segments (`` `region` `` and `region` are
            // one column); `JSON.stringify` cannot collide on a separator.
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
 * Parse and validate one note body against the grammar, throwing
 * {@link AuthorizeGrammarError} on any violation and returning the parsed
 * terms for the caller's struct-aware fan-out check.
 *
 * `givenDeclaredTypes` maps given name -> declared Malloy type; a given
 * absent from it skips the arity check.
 *
 * `route` is required, not defaulted, so a lock body can never be validated
 * under the filter's rules: {@link AUTHORIZE_ROUTE} admits `source_level`
 * terms and the sentinels, {@link ACCESS_FILTER_ROUTE} only `row_level`
 * terms.
 */
export function parseAuthorizeGrammarBody(
   sourceName: string,
   body: string,
   givenDeclaredTypes: ReadonlyMap<string, string>,
   route: string,
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

   // Sentinels match the WHOLE body only: `false and x in $G` is a two-term
   // body with a malformed first term. The route is checked BEFORE returning
   // the sentinel, or `#(access_filter) false` would pass as a deny-all that
   // serves zero rows instead of refusing.
   const sentinel = trimmedBody.toLowerCase();
   if (sentinel === "false" || sentinel === "true") {
      if (route !== AUTHORIZE_ROUTE) {
         reject(
            sourceName,
            trimmedBody,
            "sentinel_in_access_filter",
            `a bare \`${sentinel}\` is not allowed in \`#(access_filter)\` — ` +
               (sentinel === "false"
                  ? "write `#(authorize) false`, which refuses the caller " +
                    "rather than serving them zero rows."
                  : "it filters nothing; delete the annotation, or write " +
                    "`#(authorize) true` to re-open a locked base."),
         );
      }
      return [{ scope: sentinel === "false" ? "deny_all" : "admit_all" }];
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

   // ONE table over (route, scope) rather than a check per route, so a new
   // route cannot be added with only half the pair — the missing half is the
   // one that loads inert and serves everything.
   const ADMITTED_SCOPE: Record<string, AuthorizeGrammarParsedTerm["scope"]> = {
      [AUTHORIZE_ROUTE]: "source_level",
      [ACCESS_FILTER_ROUTE]: "row_level",
   };
   const admitted = ADMITTED_SCOPE[route];
   const offending = parsed.find((t) => t.scope !== admitted);
   if (offending) {
      const inLock = route === AUTHORIZE_ROUTE;
      reject(
         sourceName,
         trimmedBody,
         inLock
            ? "row_level_term_in_authorize"
            : "source_level_term_in_access_filter",
         inLock
            ? "a row-level term (field on the left) is not allowed in " +
                 "`#(authorize)`, which decides whether the caller may reach " +
                 "the source at all — move the term to `#(access_filter)`."
            : "a source-level term (literal on the left) is not allowed in " +
                 "`#(access_filter)`, which decides which rows a caller may " +
                 "see — move the term to `#(authorize)`, which refuses a " +
                 "caller it does not admit rather than serving them zero rows.",
      );
   }

   assertAuthorizeGrammarTermsCoherent(
      sourceName,
      parsed.map((term) => ({ term, route })),
   );

   return parsed;
}

/**
 * Retired annotation routes, watched so a leftover marker fails load rather
 * than fails open (backs `gate_classification.ts`'s
 * `assertNoRetiredRouteMarkers`). Malloy's bracket routing is generic, so a
 * leftover `#(partition)` would otherwise parse, load clean and serve every
 * row.
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
 * does not visit. A marker one level too low is not a no-op: the source
 * would publish clean and serve every row.
 *
 * Joins are descended into: no retired marker is legitimate anywhere.
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

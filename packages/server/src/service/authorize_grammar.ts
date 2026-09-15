// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(authorize)` body grammar.
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
 * One exception, deliberately narrow: a body that is EXACTLY (trimmed,
 * case-insensitive) `false` — never a term inside an `and` — parses as an
 * unconditional deny. Every other term references a caller-suppliable
 * given, so without this there is no gate a caller cannot eventually
 * satisfy, and the locked-base-plus-curated-extensions pattern this page's
 * own docs teach has no legal spelling. `true` is deliberately NOT given
 * the same treatment: an admit-everyone gate is not a gate at all — omit
 * the annotation instead.
 *
 * This module answers "does this body fit the grammar", nothing more. It
 * does not resolve a field path against a struct (a fan-out join check needs
 * the compiled IR — see `gate_classification.ts`'s
 * `assertNoFanoutFieldPath`) and it does not feed the graft path: the
 * `collectAuthorizeExprs` -> `gateFilterText` -> `liftGateCondition`
 * pipeline still hands the author's own text to the compiler unmodified —
 * Malloy already compiles a bare `false` as a boolean literal on its own,
 * which is what the retired string form relied on too, so accepting it
 * here needs no graft-side change.
 * This module only decides whether that text is legal to hand over at all.
 */

import { routeOf } from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";

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
   | "operator_arity_mismatch"
   | "fanout_path";

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
 *  `deny_all` exception, which is not a term and cannot appear alongside
 *  one; see {@link AuthorizeGrammarTerm}. */
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
 *  of an ordinary `and`-joined body, or the single `deny_all` sentinel for
 *  the one whole-body exception (a bare `false`) — never a mix, since
 *  `deny_all` can only ever be the sole element of the returned array. */
export type AuthorizeGrammarTerm =
   | AuthorizeGrammarParsedTerm
   | { scope: "deny_all" };

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

function rejectionMessage(
   sourceName: string,
   body: string,
   detail: string,
): string {
   return (
      `Source "${sourceName}" declares \`#(authorize) ${body}\`: ${detail} ` +
      `#(authorize) only accepts one or more \`and\`-joined terms, each ` +
      `either \`<column> <op> $GIVEN\` (a single field or a dotted join ` +
      `path on the left) or \`'<literal>' in $GIVEN\` (a literal on the ` +
      `left), where <op> is fixed by the given's own arity: \`in\` for a ` +
      `list-typed given, \`=\` for a scalar one.`
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

   // A SOURCE-LEVEL term compares a literal to a given, and reads equally well
   // either way round (`$ROLE = 'admin'` is the spelling publisher's own
   // fixtures and docs used first). Normalize to literal-on-the-left so one
   // shape reaches the checks below. A ROW-LEVEL term is not reversible: the
   // field path is what the build scan groups by and what the graft filters
   // on, so it stays on the left.
   if (GIVEN_REF_RE.test(left) && STRING_LITERAL_RE.test(right)) {
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
 */
export function parseAuthorizeGrammarBody(
   sourceName: string,
   body: string,
   givenDeclaredTypes: ReadonlyMap<string, string>,
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

   // The one exception to "every term references a given" — see this
   // module's doc. Checked against the WHOLE body, before splitting on
   // `and`, so `false and org_id in $GROUPS` does NOT take this path: it is
   // not a deny-all, it is a two-term body whose first term is malformed,
   // and falls through to the ordinary per-term errors below.
   if (trimmedBody.toLowerCase() === "false") {
      return [{ scope: "deny_all" }];
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

   const rowLevel = parsed.filter((t) => t.scope === "row_level");
   const sourceLevel = parsed.filter((t) => t.scope === "source_level");
   if (rowLevel.length > 0 && sourceLevel.length > 0) {
      reject(
         sourceName,
         trimmedBody,
         "mixed_scope_body",
         "a body may not mix a row-level term (field on the left) with a " +
            "source-level term (`'literal' in $GIVEN`).",
      );
   }

   const seenGivens = new Set<string>();
   const seenFieldPaths = new Set<string>();
   for (const t of parsed) {
      if (seenGivens.has(t.given)) {
         reject(
            sourceName,
            trimmedBody,
            "duplicate_given",
            `\`$${t.given}\` is used by more than one term — a given can ` +
               "back at most one term per body.",
         );
      }
      seenGivens.add(t.given);
      if (t.scope === "row_level") {
         if (seenFieldPaths.has(t.fieldPath)) {
            reject(
               sourceName,
               trimmedBody,
               "duplicate_field_path",
               `\`${t.fieldPath}\` is used by more than one term.`,
            );
         }
         seenFieldPaths.add(t.fieldPath);
      }
   }

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

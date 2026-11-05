// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Access-gate resolution: which `#(authorize)` / `#(access_filter)` gates apply to a
 * source as an entry point, and what an `#(access_filter)` body says.
 *
 * Reports what the query path enforces and never decides access. Kept free of
 * the extraction and the Malloy SDK so both `source_extraction` and
 * `dim_index_discovery` can import it without a cycle.
 */

export type GateRoute = "authorize" | "access_filter";

/** Fixed order: the lock first, then the row filter. */
export const GATE_ROUTES: readonly GateRoute[] = ["authorize", "access_filter"];

export interface GateExpr {
   route: GateRoute;
   expr: string;
}

/** One `field_path <op> $GIVEN` conjunct of an `#(access_filter)` body. */
export interface AccessFilterTerm {
   field_path: string;
   given: string;
   op: "=" | "in";
}

// Malloy routes an annotation by NAME inside any of four bracket pairs, after
// a `#` or `##` sigil and an optional block `|` — `#(x)`, `#[x]`, `#{x}`,
// `#<x>`, `##|(x)` all reach the same route, pinned in
// ../malloy_annotation_invariants.spec.ts. Matching only the parenthesis form
// fails in the dangerous direction: the query path enforces `#[access_filter]
// org in $GROUPS` while this file reports the source ungated.
//
// Padding is tolerated on both sides of the bracket (`# (authorize)`,
// `#( authorize )`). Malloy routes neither, but the server refuses BOTH at load
// (`assertNoAuthorizeNearMisses`), so over-matching a padded form can only mark
// a source that will never load — the safe direction.
//
// The closing bracket is required, which keeps this off an unrelated annotation
// whose name merely starts with the route (`#(authorized-users)`); hyphenated
// spellings stay excluded because the server refuses them at load.
const GATE_ROUTE_ALTERNATION = "authorize|access_filter";
const GATE_TAGLINE_RE = new RegExp(
   "^##?\\|?\\s*(?:" +
      [
         ["\\(", "\\)"],
         ["\\[", "\\]"],
         ["\\{", "\\}"],
         ["<", ">"],
      ]
         .map(
            ([open, close]) =>
               `${open}\\s*(?:${GATE_ROUTE_ALTERNATION})\\s*${close}`,
         )
         .join("|") +
      ")",
);

/** Extract the route and Malloy expression from an `#(authorize)` or
 *  `#(access_filter)` annotation. Two payload shapes are read:
 *
 *   - A bare expression (`#(authorize) $ROLE = 'analyst'`), returned verbatim.
 *   - A double-quoted string, from which exactly one layer of wrapping quotes
 *     is stripped; inner single quotes pass through, and `\"` / `\\` are unescaped.
 *
 *  The quoted form is refused at model load (../authorize.ts), so a package
 *  carrying it never reaches this code; reading it costs nothing because the
 *  payload is metadata here, never enforcement.
 *
 *  Returns `null` if the string is not a gate annotation. Throws on an empty
 *  payload, or on a quoted payload with missing/mismatched quotes. Callers differ
 *  on purpose: the file-level reader skip-and-warns, while a source's own gate
 *  resolution fails closed (deny).
 *
 *  Deliberately looser than the query path: it reads a file-level `##(...)` gate
 *  (refused at load) and matches by pattern rather than asking
 *  Malloy's note routing, so it can only over-report a gate. */
export const parseGateAnnotation = (annotation: string): GateExpr | null => {
   const trimmed = annotation.trim();
   const match = GATE_TAGLINE_RE.exec(trimmed);
   if (!match) return null;
   const route = (/access_filter|authorize/.exec(match[0]) ?? [])[0] as
      | GateRoute
      | undefined;
   if (!route) return null;
   const body = trimmed.slice(match[0].length).trim();
   if (body.length === 0) {
      throw new Error("authorize annotation has an empty expression body");
   }
   if (body[0] !== '"') return { route, expr: body };
   let expr = "";
   let i = 1;
   let closed = false;
   for (; i < body.length; i++) {
      const ch = body[i];
      if (ch === "\\" && i + 1 < body.length) {
         const next = body[i + 1];
         if (next === '"' || next === "\\") {
            expr += next;
            i++;
            continue;
         }
      }
      if (ch === '"') {
         closed = true;
         i++;
         break;
      }
      expr += ch;
   }
   if (!closed) {
      throw new Error(`authorize annotation has mismatched quotes: ${body}`);
   }
   const rest = body.slice(i).trim();
   if (rest.length > 0) {
      throw new Error(
         `authorize annotation has unexpected content after the expression: ${rest}`,
      );
   }
   if (expr.trim().length === 0) {
      throw new Error("authorize annotation has an empty expression body");
   }
   return { route, expr };
};

const ANCESTOR_WALK_MAX_DEPTH = 32;
// A separate budget from the annotation chain above: this one bounds
// `query_source` structRef resolution by NAME, which has no object identity for
// the `seen` set to compare.
const STRING_REF_RESOLUTION_MAX_DEPTH = 32;

/** Unlike the join path, a malformed gate propagates so the caller can deny.
 *  Every tagline is parsed whatever its route, so a malformed note of either
 *  route denies on both walks. */
const collectGateExprsOrThrow = (
   taglines: string[],
   route: GateRoute,
): string[] => {
   const out: string[] = [];
   for (const tagline of taglines) {
      const gate = parseGateAnnotation(tagline);
      if (gate && gate.route === route) out.push(gate.expr);
   }
   return out;
};

export interface InheritableAnnotations {
   inherits?: InheritableAnnotations;
   blockNotes?: { text?: string }[];
   notes?: { text?: string }[];
}

// Malloy files the multi-definition `source:` block form's gate under `notes`
// and the line-above form's under `blockNotes`, so both must be read.
const notesOf = (a: InheritableAnnotations | undefined): string[] =>
   [...(a?.blockNotes ?? []), ...(a?.notes ?? [])]
      .map((n) => n.text)
      .filter((t): t is string => typeof t === "string");

/** Walks `annotations.inherits` for the nearest ancestor's gate on `route`
 *  (declaring its own on that route replaces an ancestor's, so this only runs
 *  when the struct has none of its own). Throws (caller fails closed) if the
 *  chain still has more ancestors past `ANCESTOR_WALK_MAX_DEPTH`. */
const ancestorGateExprs = (
   annotation: InheritableAnnotations | undefined,
   route: GateRoute,
): string[] => {
   let inherited = annotation?.inherits;
   for (let depth = 0; inherited && depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const exprs = collectGateExprsOrThrow(notesOf(inherited), route);
      if (exprs.length > 0) return exprs;
      inherited = inherited.inherits;
   }
   if (inherited) {
      throw new Error(
         `annotations.inherits chain exceeds ${ANCESTOR_WALK_MAX_DEPTH} levels`,
      );
   }
   return [];
};

/** The subset of a raw `structDef` this module resolves a gate over: own
 *  annotations; a query-derived struct's base and resolved composite branch;
 *  a bare composite's members. Shape-checked JSON, not `Explore`. */
export interface GateStructDef {
   type?: string;
   annotations?: InheritableAnnotations;
   query?: {
      structRef?: GateStructDef | string;
      compositeResolvedSourceDef?: GateStructDef;
   };
   sources?: GateStructDef[];
   selectSegments?: unknown[];
}

/** Structural stand-in for `@malloydata/malloy`'s `isSegmentSQL`, which isn't
 *  importable; mirrors its own check (`'sql' in f`). */
const isSQLStringSegment = (segment: unknown): boolean =>
   typeof segment === "object" && segment !== null && "sql" in segment;

interface WalkContext {
   /** Set when any composite is met: which member branch a query takes is not
    *  known at index time. */
   composite: boolean;
}

/** One route's walk, mirroring `collectEntryPointGatesForRoute`
 *  (../gate_classification.ts): own notes on THIS route, else the nearest
 *  `inherits` ancestor's, PLUS — for a query-derived struct — the base it derives
 *  from and its resolved composite branch (`source: mine is locked -> {...}` must
 *  not launder `locked`'s gate away, and a base's `#(access_filter)` ANDs with
 *  the derivation's own). A bare composite with no query wrapper reports its first
 *  gated member (deliberate over-report; the query path resolves one branch per query).
 *  `seen` (struct identity) stops a repeat struct rather than denying it; only
 *  string-name resolution, which has no identity to compare, is depth-capped.
 *  `walkCompositeMembers` is false only for a query_source's own structRef base:
 *  Malloy already resolved that derivation to one branch
 *  (`compositeResolvedSourceDef`, walked separately), so the any-member rule would
 *  over-gate it.
 *
 *  `ownTaglines` overrides the own-notes step for the initial call only: the join
 *  call site has already location-filtered its own notes per tag-kind. */
const routeGateExprs = (
   struct: GateStructDef,
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
   route: GateRoute,
   ctx: WalkContext,
   seen: Set<GateStructDef>,
   stringResolutionDepth: number,
   walkCompositeMembers: boolean,
   ownTaglines?: string[],
): string[] => {
   // A restricted visit of a composite recurses nowhere, so it must NOT claim the
   // struct in `seen`: doing so would suppress a later unrestricted visit, which
   // WOULD walk its members, and launder a gated member away.
   const claimsSeen = walkCompositeMembers || struct.type !== "composite";
   if (claimsSeen) {
      if (seen.has(struct)) return [];
      seen.add(struct);
   }
   const own = collectGateExprsOrThrow(
      ownTaglines ?? notesOf(struct.annotations),
      route,
   );
   const out =
      own.length > 0 ? own : ancestorGateExprs(struct.annotations, route);

   if (struct.type === "composite") {
      ctx.composite = true;
      if (out.length > 0 || !walkCompositeMembers) return out;
      for (const member of struct.sources ?? []) {
         const memberGate = routeGateExprs(
            member,
            sourceDefsByName,
            route,
            ctx,
            seen,
            stringResolutionDepth,
            true,
         );
         if (memberGate.length > 0) return memberGate;
      }
      return [];
   }
   // Terminal shapes: none can reference another named source, so a gate could
   // only be on the own/ancestor annotations already read above.
   if (
      struct.type === "table" ||
      struct.type === "record" ||
      struct.type === "array"
   ) {
      return out;
   }
   if (struct.type === "sql_select") {
      // A plain `duckdb.sql(...)` compiles with no query-bearing segment, so it
      // can't embed a gated source. A segment that is not raw SQL text is an
      // embedded query/source this walk doesn't descend into — deny rather than
      // silently drop its gate.
      const embedsSource = (struct.selectSegments ?? []).some(
         (segment) => !isSQLStringSegment(segment),
      );
      if (!embedsSource) return out;
      throw new Error(
         "resolveGates does not walk an embedded query/source in sql_select.selectSegments",
      );
   }
   if (struct.type !== "query_source") {
      throw new Error(
         `resolveGates does not support struct.type '${struct.type}'`,
      );
   }
   const ref = struct.query?.structRef;
   const isStringRef = typeof ref === "string";
   if (
      isStringRef &&
      stringResolutionDepth >= STRING_REF_RESOLUTION_MAX_DEPTH
   ) {
      throw new Error(
         `query_source structRef name chain exceeds ${STRING_REF_RESOLUTION_MAX_DEPTH} levels`,
      );
   }
   const base = isStringRef ? sourceDefsByName.get(ref) : ref;
   // Throws even when `out` is non-empty: skipping an unresolved base would drop its gate and fail open.
   if (!base) {
      throw new Error(
         isStringRef
            ? `query_source structRef '${ref}' did not resolve against the model's own sources`
            : "query_source has an unresolvable structRef",
      );
   }
   const results = [
      ...out,
      ...routeGateExprs(
         base,
         sourceDefsByName,
         route,
         ctx,
         seen,
         isStringRef ? stringResolutionDepth + 1 : stringResolutionDepth,
         /* walkCompositeMembers */ false,
      ),
   ];
   const resolvedComposite = struct.query?.compositeResolvedSourceDef;
   if (resolvedComposite) {
      ctx.composite = true;
      results.push(
         ...routeGateExprs(
            resolvedComposite,
            sourceDefsByName,
            route,
            ctx,
            seen,
            stringResolutionDepth,
            true,
         ),
      );
   }
   return results;
};

export interface ResolvedGates {
   /** Authorize-route gates first, then access_filter, each in walk order. */
   gates: GateExpr[];
   /** The walk met a composite, so which branch gates a query is unknown. */
   composite: boolean;
}

/** The gates that apply to `struct` as an entry point, one independent walk per
 *  route (own `seen` set, own own-wins-over-ancestor decision) — the shape of
 *  `Model.collectEntryPointGates` (../model.ts). A single route-blind list would miss a locked
 *  base under a child declaring its own `#(access_filter)`. Throws on a
 *  resolution failure; the caller denies. */
export const resolveGates = (
   struct: GateStructDef,
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
   ownTaglines?: string[],
): ResolvedGates => {
   const ctx: WalkContext = { composite: false };
   const gates: GateExpr[] = [];
   for (const route of GATE_ROUTES) {
      const seenExprs = new Set<string>();
      for (const expr of routeGateExprs(
         struct,
         sourceDefsByName,
         route,
         ctx,
         new Set(),
         0,
         true,
         ownTaglines,
      )) {
         // Malloy copies a base's note objects onto a derivation, so the same
         // predicate can be read twice; ANDing it with itself changes nothing.
         if (seenExprs.has(expr)) continue;
         seenExprs.add(expr);
         gates.push({ route, expr });
      }
   }
   return { gates, composite: ctx.composite };
};

// --- `#(access_filter)` body grammar ---------------------------------------
// Must agree with ../authorize_grammar.ts (`parseAuthorizeGrammarBody` on the
// access_filter route), narrowed to the row-level shape an index needs:
// `field_path <op> $GIVEN` terms joined by `and`, `=` for a scalar given and
// `in` for a list one. The conformance test replays vectors recorded from
// `parseAuthorizeGrammarBody`.

export type AccessFilterRejection =
   | "empty_body"
   | "compound_boolean"
   | "negated_operator"
   | "comparison_operator"
   | "left_not_field_path"
   | "missing_given_reference"
   | "malformed_body"
   | "duplicate_given"
   | "duplicate_field_path"
   | "operator_arity_mismatch"
   | "sentinel_in_access_filter"
   | "source_level_term_in_access_filter"
   | "reversed_in_operands"
   | "unsupported_field_path";

export type AccessFilterParse =
   | { ok: true; terms: AccessFilterTerm[] }
   | { ok: false; cause: AccessFilterRejection };

const IDENT = "[A-Za-z_][A-Za-z0-9_]*";
const QUOTED_IDENT = "`[^`]+`";
const SEGMENT = `(?:${IDENT}|${QUOTED_IDENT})`;
const FIELD_PATH_RE = new RegExp(`^${SEGMENT}(?:\\.${SEGMENT})*$`);
const GIVEN_REF_RE = new RegExp(`^\\$(${IDENT})$`);
const STRING_LITERAL_RE = /^(?:'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*")$/;
const NEGATED_OPERATOR_RE = /!=/gu;
const COMPARISON_OPERATOR_RE = /(?:>=|<=|>|<)/gu;
const IN_OPERATOR_RE = /\bin\b/giu;
const EQ_OPERATOR_RE = /=/gu;

type Quote = "'" | '"' | "`";
const isQuote = (ch: string): ch is Quote =>
   ch === "'" || ch === '"' || ch === "`";

/** Split a validated field path on its top-level dots, unquoting each backtick
 *  segment (a dot inside backticks belongs to the segment's name). */
const splitFieldPathSegments = (fieldPath: string): string[] => {
   const segments: string[] = [];
   let i = 0;
   while (i < fieldPath.length) {
      if (fieldPath[i] === "`") {
         const end = fieldPath.indexOf("`", i + 1);
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
};

/** Split on a word-bounded, depth-0, outside-any-quote `and`, collecting any
 *  depth-0 `or`/`not` token so a compound boolean is refused while one buried in
 *  a string literal is not. */
const splitTerms = (
   body: string,
): { terms: string[]; compoundTokens: string[] } => {
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
};

/** Index of an operator token outside any quoted substring, or -1. */
const findOperatorOutsideQuotes = (s: string, re: RegExp): number => {
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
};

interface RowTerm {
   segments: string[];
   given: string;
   op: "=" | "in";
}

const parseRowTerm = (
   term: string,
   givenTypes: ReadonlyMap<string, string>,
): RowTerm | "source_level" | AccessFilterRejection => {
   const trimmed = term.trim();
   if (findOperatorOutsideQuotes(trimmed, NEGATED_OPERATOR_RE) !== -1) {
      return "negated_operator";
   }
   if (findOperatorOutsideQuotes(trimmed, COMPARISON_OPERATOR_RE) !== -1) {
      return "comparison_operator";
   }
   const inIdx = findOperatorOutsideQuotes(trimmed, IN_OPERATOR_RE);
   const eqIdx = findOperatorOutsideQuotes(trimmed, EQ_OPERATOR_RE);
   let op: "=" | "in";
   let opIdx: number;
   let opLen: number;
   if (inIdx !== -1 && (eqIdx === -1 || inIdx < eqIdx)) {
      op = "in";
      opIdx = inIdx;
      opLen = 2;
   } else if (eqIdx !== -1) {
      op = "=";
      opIdx = eqIdx;
      opLen = 1;
   } else {
      return "malformed_body";
   }

   let left = trimmed.slice(0, opIdx).trim();
   let right = trimmed.slice(opIdx + opLen).trim();
   // `$GIVEN = 'literal'` reads either way round for a source-level term; `in`
   // does not reverse (array-in-string), so it is refused instead of swapped.
   if (GIVEN_REF_RE.test(left) && STRING_LITERAL_RE.test(right)) {
      if (op === "in") return "reversed_in_operands";
      [left, right] = [right, left];
   }

   const leftIsFieldPath = FIELD_PATH_RE.test(left);
   if (!leftIsFieldPath && !STRING_LITERAL_RE.test(left)) {
      return "left_not_field_path";
   }
   const givenMatch = GIVEN_REF_RE.exec(right);
   if (!givenMatch) return "missing_given_reference";
   const given = givenMatch[1];

   // A bare `array` or `<elem>[]` rendering is a list; a given absent from the map
   // skips the check rather than guessing.
   const declaredType = givenTypes.get(given);
   if (declaredType !== undefined) {
      const isListType =
         declaredType === "array" || declaredType.endsWith("[]");
      if (isListType !== (op === "in")) return "operator_arity_mismatch";
   }
   if (!leftIsFieldPath) return "source_level";
   return { segments: splitFieldPathSegments(left), given, op };
};

/** Parse an `#(access_filter)` body into its row-level terms, or say which
 *  grammar rule it breaks. Anything that does not parse is opaque to callers. */
export const parseAccessFilterBody = (
   body: string,
   givenTypes: ReadonlyMap<string, string> = new Map(),
): AccessFilterParse => {
   const trimmedBody = body.trim();
   if (trimmedBody.length === 0) return { ok: false, cause: "empty_body" };
   const sentinel = trimmedBody.toLowerCase();
   if (sentinel === "false" || sentinel === "true") {
      return { ok: false, cause: "sentinel_in_access_filter" };
   }
   const { terms: rawTerms, compoundTokens } = splitTerms(trimmedBody);
   if (compoundTokens.length > 0)
      return { ok: false, cause: "compound_boolean" };

   // Scope is checked only once every term has parsed, so a later malformed term
   // outranks an earlier source-level one, as in `parseAuthorizeGrammarBody`.
   const rowTerms: RowTerm[] = [];
   let sawSourceLevel = false;
   for (const raw of rawTerms) {
      const parsed = parseRowTerm(raw, givenTypes);
      if (parsed === "source_level") {
         sawSourceLevel = true;
      } else if (typeof parsed === "string") {
         return { ok: false, cause: parsed };
      } else {
         rowTerms.push(parsed);
      }
   }
   if (sawSourceLevel) {
      return { ok: false, cause: "source_level_term_in_access_filter" };
   }

   const terms: AccessFilterTerm[] = [];
   const seenGivens = new Set<string>();
   const seenPaths = new Set<string>();
   for (const parsed of rowTerms) {
      // A dot inside a segment cannot round-trip through a dotted `field_path`.
      if (parsed.segments.some((s) => s.includes(".") || s.includes("`"))) {
         return { ok: false, cause: "unsupported_field_path" };
      }
      if (seenGivens.has(parsed.given)) {
         return { ok: false, cause: "duplicate_given" };
      }
      seenGivens.add(parsed.given);
      // Canonical on the segments: `` `region` `` and `region` name one column.
      const canonicalPath = JSON.stringify(parsed.segments);
      if (seenPaths.has(canonicalPath)) {
         return { ok: false, cause: "duplicate_field_path" };
      }
      seenPaths.add(canonicalPath);
      terms.push({
         field_path: parsed.segments.join("."),
         given: parsed.given,
         op: parsed.op,
      });
   }
   return { ok: true, terms };
};

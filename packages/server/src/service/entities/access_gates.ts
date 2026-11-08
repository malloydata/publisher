// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Access-gate resolution: which `#(authorize)` / `#(access_filter)` gates apply to a
 * source as an entry point, and what an `#(access_filter)` body says.
 *
 * Reports what the query path enforces and never decides access. Kept free of
 * the extraction so both `source_extraction` and `dim_index_discovery` can
 * import it without a cycle. What counts as a gate note, and what a gate body
 * may say, are the query path's own answers (`../authorize.ts`'s Malloy note
 * routing, `../authorize_grammar.ts`), never a second grammar of this file's.
 */

import {
   collectAuthorizeNearMissesAllRoutes,
   parseAuthorizeAnnotation,
} from "../authorize";
import {
   AuthorizeGrammarError,
   parseAuthorizeGrammarBody,
   type AuthorizeGrammarTerm,
} from "../authorize_grammar";
import { ACCESS_FILTER_ROUTE } from "../authorize_routes";

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

/** Extract the route and Malloy expression from an `#(authorize)` or
 *  `#(access_filter)` annotation.
 *
 *  Whether a note IS a gate is Malloy's note routing, the same test the query
 *  path enforces with (`parseAuthorizeAnnotation`): every bracket pair, the
 *  `##` and block `#|` sigils, and nothing Malloy routes elsewhere. A near miss
 *  Malloy does not route as a gate (`# (authorize)`, `#( authorize )`,
 *  `#(AUTHORIZE)`) is refused at load (`assertNoAuthorizeNearMisses`), so a
 *  package carrying one never serves; it throws here, which every caller
 *  treats as fail-closed (deny, or skip-and-warn for a file-level note).
 *
 *  Two payload shapes are read:
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
 *  payload, a near miss, or a quoted payload with missing/mismatched quotes.
 *  Callers differ on purpose: the file-level reader skip-and-warns, while a
 *  source's own gate resolution fails closed (deny).
 *
 *  Deliberately looser than the query path in one way: it reads a file-level
 *  `##(...)` gate (refused at load), so it can only over-report a gate. */
export const parseGateAnnotation = (annotation: string): GateExpr | null => {
   const parsed = parseAuthorizeAnnotation(annotation);
   if (parsed === null) {
      if (collectAuthorizeNearMissesAllRoutes([annotation]).length > 0) {
         throw new Error(
            `authorize annotation is not routed as a gate by Malloy (a near miss, refused at load): ${annotation.trim().split(/[\r\n]/, 1)[0]}`,
         );
      }
      return null;
   }
   const route = parsed.route as GateRoute;
   const body = parsed.expr.trim();
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
// A projection of ../authorize_grammar.ts (`parseAuthorizeGrammarBody` on the
// access_filter route), narrowed to the row-level shape an index needs:
// `field_path <op> $GIVEN` terms joined by `and`, `=` for a scalar given and
// `in` for a list one. There is one grammar; this only reshapes its answer.
// The conformance test replays vectors recorded from `parseAuthorizeGrammarBody`.

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

const ACCESS_FILTER_REJECTIONS: ReadonlySet<string> =
   new Set<AccessFilterRejection>([
      "empty_body",
      "compound_boolean",
      "negated_operator",
      "comparison_operator",
      "left_not_field_path",
      "missing_given_reference",
      "malformed_body",
      "duplicate_given",
      "duplicate_field_path",
      "operator_arity_mismatch",
      "sentinel_in_access_filter",
      "source_level_term_in_access_filter",
      "reversed_in_operands",
      "unsupported_field_path",
   ]);

/** Parse an `#(access_filter)` body into its row-level terms, or say which
 *  grammar rule it breaks. Anything that does not parse is opaque to callers.
 *  Never throws: a failure of any kind is a refusal (opaque, the safe side). */
export const parseAccessFilterBody = (
   body: string,
   givenTypes: ReadonlyMap<string, string> = new Map(),
): AccessFilterParse => {
   let parsed: AuthorizeGrammarTerm[];
   try {
      parsed = parseAuthorizeGrammarBody(
         "",
         body,
         givenTypes,
         ACCESS_FILTER_ROUTE,
      );
   } catch (err) {
      const cause =
         err instanceof AuthorizeGrammarError &&
         ACCESS_FILTER_REJECTIONS.has(err.rejectionCause)
            ? (err.rejectionCause as AccessFilterRejection)
            : "malformed_body";
      return { ok: false, cause };
   }
   const terms: AccessFilterTerm[] = [];
   for (const term of parsed) {
      // The grammar admits only row-level terms on this route; anything else
      // is not a shape this projection can carry.
      if (term.scope !== "row_level") {
         return { ok: false, cause: "malformed_body" };
      }
      // A dot inside a segment cannot round-trip through a dotted `field_path`.
      if (
         term.fieldPathSegments.some((s) => s.includes(".") || s.includes("`"))
      ) {
         return { ok: false, cause: "unsupported_field_path" };
      }
      terms.push({
         field_path: term.fieldPathSegments.join("."),
         given: term.given,
         op: term.operator,
      });
   }
   return { ok: true, terms };
};

/**
 * Whether `apiSource` is denied unconditionally: a bare `false` conjunct on
 * EITHER route (the routes AND together) denies every caller whatever givens
 * are supplied. The filter route can carry one as the fail-closed placeholder
 * for an unattributable gate, so this keys on the deny, not the route.
 * Case-insensitive, because the wire payload can carry `FALSE` uppercase.
 */
export function isUnconditionalDenyAuthorize(apiSource: {
   accessFilter?: string[];
   authorize?: string[];
}): boolean {
   const isDeny = (expr: string) => expr.trim().toLowerCase() === "false";
   return (
      (apiSource.accessFilter ?? []).some(isDeny) ||
      (apiSource.authorize ?? []).some(isDeny)
   );
}

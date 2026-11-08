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

import type { SourceDef } from "@malloydata/malloy";
import {
   collectAuthorizeNearMissesAllRoutes,
   parseAuthorizeAnnotation,
} from "../authorize";
import {
   parseAuthorizeGrammarBody,
   type AuthorizeGrammarTerm,
} from "../authorize_grammar";
import { ACCESS_FILTER_ROUTE } from "../authorize_routes";
import { gatePlan, type GateGraph } from "../governance/gate_plan";
import type { AuthorizeTerm } from "./internal_types";

export type GateRoute = "authorize" | "access_filter";

/** Fixed order: the lock first, then the row filter. */
export const GATE_ROUTES: readonly GateRoute[] = ["authorize", "access_filter"];

export interface GateExpr {
   route: GateRoute;
   expr: string;
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

// A budget for `query_source` structRef resolution by NAME, separate from the
// ancestor chain's: a name lookup has no object identity for `seen` to compare.
const STRING_REF_RESOLUTION_MAX_DEPTH = 32;

/** Unlike the join path, a malformed gate propagates so the caller can deny.
 *  Every tagline is parsed whatever its route, so a malformed note of either
 *  route denies on both walks. */
const collectGateExprsOrThrow = (
   taglines: string[],
   route: string,
): string[] => {
   const out: string[] = [];
   for (const tagline of taglines) {
      if (typeof tagline !== "string") continue;
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

/** The index's view of the source graph: raw `structDef` JSON, a name ref
 *  looked up among the model's own explores, and no `sourceRegistry` (an entry
 *  point's ancestors are all on its `annotations.inherits` chain). */
const exploreGateGraph = (
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
): GateGraph => ({
   declaredSource: () => ({ kind: "none" }),
   queryBase: (struct) => {
      const ref = (struct as unknown as GateStructDef).query?.structRef;
      const byName = typeof ref === "string";
      const base = byName ? sourceDefsByName.get(ref) : ref;
      return base
         ? { kind: "resolved", base: base as unknown as SourceDef, byName }
         : {
              kind: "unresolvable",
              byName,
              message: byName
                 ? `query_source structRef '${ref}' did not resolve against the model's own sources`
                 : "query_source has an unresolvable structRef",
           };
   },
   compositeResolved: (struct) =>
      (struct as unknown as GateStructDef).query
         ?.compositeResolvedSourceDef as unknown as SourceDef | undefined,
});

export interface ResolvedGates {
   /** Authorize-route gates first, then access_filter, each in walk order. */
   gates: GateExpr[];
   /** The walk met a composite, so which branch gates a query is unknown. */
   composite: boolean;
}

/** The gates that apply to `struct` as an entry point: a thin adapter over
 *  `gatePlan` (../governance/gate_plan.ts), the one gate-inheritance walk the
 *  query path enforces with, under the `anyMember` composite rule: an index
 *  cannot know which branch a query takes, so a bare composite with no gate of
 *  its own reports its first gated member (a deliberate over-report).
 *
 *  `ownTaglines` replaces the root's own notes: the join call site has already
 *  location-filtered them per tag-kind.
 *
 *  Throws on anything the walk could not read, or a shape it does not descend
 *  into (an embedded query in `sql_select`, an unknown struct type); the
 *  caller denies. */
export const resolveGates = (
   struct: GateStructDef,
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
   ownTaglines?: string[],
): ResolvedGates => {
   const plan = gatePlan(
      struct as unknown as SourceDef,
      exploreGateGraph(sourceDefsByName),
      {
         composite: "anyMember",
         routes: GATE_ROUTES,
         readRoute: collectGateExprsOrThrow,
         maxNameHops: STRING_REF_RESOLUTION_MAX_DEPTH,
         ownTexts: ownTaglines,
      },
   );
   const gates: GateExpr[] = [];
   const seenExprs = new Map<string, Set<string>>();
   for (const step of plan.steps) {
      if (step.kind === "unwalked") throw new Error(step.message);
      if (step.failure) {
         if (step.failure.kind === "unreadable") throw step.failure.error;
         throw new Error(step.failure.message);
      }
      const route = step.route as GateRoute;
      const seen = seenExprs.get(route) ?? new Set<string>();
      seenExprs.set(route, seen);
      for (const expr of step.exprs) {
         // Malloy copies a base's note objects onto a derivation, so the same
         // predicate can be read twice; ANDing it with itself changes nothing.
         if (seen.has(expr)) continue;
         seen.add(expr);
         gates.push({ route, expr });
      }
   }
   return { gates, composite: plan.metComposite };
};

/**
 * The row-level terms of an `#(access_filter)` body, in the `/entities` wire
 * shape (`field_path <op> $GIVEN`), or undefined when the body is opaque to an
 * index. The server's grammar (`parseAuthorizeGrammarBody` on the
 * access_filter route) reads the body; this only reshapes its answer:
 *  - a body the grammar refuses is opaque (the load refuses it too, so a
 *    served package never carries one);
 *  - a term whose segment holds a dot (`` reps.`cost.center` ``) is opaque,
 *    because a dotted `field_path` cannot carry it. Opaque is the safe side:
 *    the source's row filter stays withheld from the index.
 * Never throws.
 */
export const accessFilterTerms = (
   body: string,
   givenTypes: ReadonlyMap<string, string> = new Map(),
): AuthorizeTerm[] | undefined => {
   let parsed: AuthorizeGrammarTerm[];
   try {
      parsed = parseAuthorizeGrammarBody(
         "",
         body,
         givenTypes,
         ACCESS_FILTER_ROUTE,
      );
   } catch {
      return undefined;
   }
   const terms: AuthorizeTerm[] = [];
   for (const term of parsed) {
      // The grammar admits only row-level terms on this route.
      if (term.scope !== "row_level") return undefined;
      if (term.fieldPathSegments.some((s) => s.includes("."))) return undefined;
      terms.push({
         field_path: term.fieldPathSegments.join("."),
         given: term.given,
         op: term.operator,
      });
   }
   return terms;
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

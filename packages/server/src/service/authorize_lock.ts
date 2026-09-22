// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";

/**
 * Decide a `#(authorize)` lock: may this caller reach this source at all.
 *
 * Read from the COMPILED condition Malloy built, never from the annotation
 * text. The text route would be a second parser and a second string-literal
 * decoder for something Malloy has already decoded (`stringLiteral.literal`
 * arrives unquoted and unescaped) and already type-checked — a `string` given
 * compared with `in`, or a `string[]` compared with `=`, is a 424 at load via
 * `validateAuthorizeProbes`, so nothing of that shape reaches here.
 *
 * A POSITIVE allowlist over node kinds, the same posture `isBareFalseLiteral`
 * (`./gate_classification`) and `containsNegatedMembership`
 * (`./gate_dimension`) already take toward this IR: an unrecognized node kind,
 * an unresolvable given, an unset given, a value of the wrong arity and a
 * throw all refuse. That is what makes the coupling to Malloy's node names
 * (pinned `@malloydata/malloy`) safe to carry — a renamed node stops admitting,
 * it never starts admitting.
 *
 * Those refusals return `"unresolvable"` rather than `"deny"`. Both are a 403
 * to the caller; the split exists so an operator can tell a gate refusing
 * someone from a gate that could not be decided at all (see `LockContext`).
 *
 * `=` is accepted with the given on either side: the graft compiles the
 * author's original text, so both `$ROLE = 'admin'` and `'admin' = $ROLE`
 * arrive, with `kids.left`/`kids.right` in the order they were written.
 *
 * Comparison is exact and case-sensitive — deliberately not the warehouse's
 * collation. A source-shaped gate on the pre-flip row route was compared by
 * the warehouse, so a MySQL tenant's case-insensitive default admitted
 * `'Finance'` against `["finance"]`; here it denies.
 */
export type LockOutcome = "admit" | "deny" | "unresolvable";

export function decideLock(
   conditionExpr: unknown,
   givenNameOf: (givenId: string) => string | undefined,
   givens: Readonly<Record<string, GivenValue>>,
): LockOutcome {
   const ctx: LockContext = { givenNameOf, givens, unresolvable: false };
   try {
      if (admits(conditionExpr, ctx, 0)) return "admit";
   } catch {
      // A malformed node reaching a property access (`null.includes`) is a
      // TypeError, which would surface as a 500 rather than the 403 every
      // other unadmitted shape gets.
      ctx.unresolvable = true;
   }
   return ctx.unresolvable ? "unresolvable" : "deny";
}

/**
 * Evaluation state threaded through the walk.
 *
 * `unresolvable` separates "this caller is not admitted" from "this gate could
 * not be decided" — both 403, but only the second means something is wrong,
 * and it is the one an operator alerts on. It is set for a given that does not
 * resolve, a value of the wrong arity, a node kind this allowlist does not
 * know, and a throw: each of those is a drift between the load-time grammar
 * and this walk rather than a rule refusing a caller. A `false` sentinel and a
 * literal that simply does not match are ordinary denials and leave it clear.
 */
interface LockContext {
   givenNameOf: (givenId: string) => string | undefined;
   givens: Readonly<Record<string, GivenValue>>;
   unresolvable: boolean;
}

interface GivenNode {
   node: "given";
   id?: unknown;
}

function admits(node: unknown, ctx: LockContext, depth: number): boolean {
   if (depth > 64 || node === null || typeof node !== "object") {
      ctx.unresolvable = true;
      return false;
   }
   const n = node as {
      node?: unknown;
      e?: unknown;
      not?: unknown;
      givenRef?: unknown;
      kids?: { left?: unknown; right?: unknown };
   };
   switch (n.node) {
      case "()":
         return admits(n.e, ctx, depth + 1);
      case "true":
         return true;
      case "false":
         // `#(authorize) false` is the deliberate deny-all sentinel, not a
         // gate that failed to decide.
         return false;
      case "and": {
         // Both sides are walked rather than short-circuited: a left-hand
         // denial must not hide a right-hand term this cannot read.
         const left = admits(n.kids?.left, ctx, depth + 1);
         const right = admits(n.kids?.right, ctx, depth + 1);
         return left && right;
      }
      case "=": {
         const { left, right } = n.kids ?? {};
         const pair =
            givenAndLiteral(left, right) ?? givenAndLiteral(right, left);
         if (!pair) {
            ctx.unresolvable = true;
            return false;
         }
         const value = resolve(pair.given, ctx);
         // `=` is the SCALAR operator: a list-typed given is `in`, refused at
         // load, so a list arriving here is a shape this must not guess at.
         if (typeof value !== "string") {
            ctx.unresolvable = true;
            return false;
         }
         return value === pair.literal;
      }
      case "inGiven": {
         // `not (x in $Y)` is W2-warned at load and left servable, so it is an
         // ordinary denial rather than a gate that could not be decided.
         if (n.not === true) return false;
         const given = asGiven(n.givenRef);
         const literal = asStringLiteral(n.e);
         if (!given || literal === undefined) {
            ctx.unresolvable = true;
            return false;
         }
         const value = resolve(given, ctx);
         if (!Array.isArray(value)) {
            ctx.unresolvable = true;
            return false;
         }
         return value.some((item) => item === literal);
      }
      default:
         ctx.unresolvable = true;
         return false;
   }
}

/** `{given, literal}` when `a` is a given reference and `b` a string literal. */
function givenAndLiteral(
   a: unknown,
   b: unknown,
): { given: GivenNode; literal: string } | undefined {
   const given = asGiven(a);
   const literal = asStringLiteral(b);
   if (!given || literal === undefined) return undefined;
   return { given, literal };
}

function asGiven(node: unknown): GivenNode | undefined {
   if (node === null || typeof node !== "object") return undefined;
   const n = node as { node?: unknown; id?: unknown };
   return n.node === "given" ? (n as GivenNode) : undefined;
}

function asStringLiteral(node: unknown): string | undefined {
   if (node === null || typeof node !== "object") return undefined;
   const n = node as { node?: unknown; literal?: unknown };
   if (n.node !== "stringLiteral") return undefined;
   return typeof n.literal === "string" ? n.literal : undefined;
}

/**
 * The caller's value for a given node, or `undefined` when the id does not
 * resolve to a name on this model or the caller supplied nothing for it.
 *
 * An unbound given denies. Its guarantor is G4 (`./gate_dimension`'s
 * `validateSourceLineGateGivenUsage`), which refuses a DEFAULTED given on any
 * lifted gate condition at load — without it a `given: GROUPS :: string[] =
 * ['finance']` would be silently ignored here.
 */
function resolve(given: GivenNode, ctx: LockContext): GivenValue | undefined {
   if (typeof given.id !== "string") {
      ctx.unresolvable = true;
      return undefined;
   }
   const name = ctx.givenNameOf(given.id);
   if (name === undefined) {
      ctx.unresolvable = true;
      return undefined;
   }
   if (!Object.prototype.hasOwnProperty.call(ctx.givens, name)) {
      ctx.unresolvable = true;
      return undefined;
   }
   return ctx.givens[name];
}

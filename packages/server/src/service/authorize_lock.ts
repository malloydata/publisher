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
 * throw all DENY. That is what makes the coupling to Malloy's node names
 * (pinned `@malloydata/malloy`) safe to carry — a renamed node stops admitting,
 * it never starts admitting.
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
export function decideLock(
   conditionExpr: unknown,
   givenNameOf: (givenId: string) => string | undefined,
   givens: Readonly<Record<string, GivenValue>>,
): "admit" | "deny" {
   try {
      return admits(conditionExpr, givenNameOf, givens, 0) ? "admit" : "deny";
   } catch {
      // A malformed node reaching a property access (`null.includes`) is a
      // TypeError, which would surface as a 500 rather than the 403 every
      // other unadmitted shape gets.
      return "deny";
   }
}

interface GivenNode {
   node: "given";
   id?: unknown;
}

function admits(
   node: unknown,
   givenNameOf: (givenId: string) => string | undefined,
   givens: Readonly<Record<string, GivenValue>>,
   depth: number,
): boolean {
   if (depth > 64 || node === null || typeof node !== "object") return false;
   const n = node as {
      node?: unknown;
      e?: unknown;
      not?: unknown;
      givenRef?: unknown;
      kids?: { left?: unknown; right?: unknown };
   };
   switch (n.node) {
      case "()":
         return admits(n.e, givenNameOf, givens, depth + 1);
      case "true":
         return true;
      case "false":
         return false;
      case "and":
         return (
            admits(n.kids?.left, givenNameOf, givens, depth + 1) &&
            admits(n.kids?.right, givenNameOf, givens, depth + 1)
         );
      case "=": {
         const { left, right } = n.kids ?? {};
         const pair =
            givenAndLiteral(left, right) ?? givenAndLiteral(right, left);
         if (!pair) return false;
         const value = resolve(pair.given, givenNameOf, givens);
         // `=` is the SCALAR operator: a list-typed given is `in`, refused at
         // load, so a list arriving here is a shape this must not guess at.
         return typeof value === "string" && value === pair.literal;
      }
      case "inGiven": {
         // `not (x in $Y)` is W2-warned at load and is not an admission rule.
         if (n.not === true) return false;
         const given = asGiven(n.givenRef);
         const literal = asStringLiteral(n.e);
         if (!given || literal === undefined) return false;
         const value = resolve(given, givenNameOf, givens);
         return Array.isArray(value) && value.some((item) => item === literal);
      }
      default:
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
function resolve(
   given: GivenNode,
   givenNameOf: (givenId: string) => string | undefined,
   givens: Readonly<Record<string, GivenValue>>,
): GivenValue | undefined {
   if (typeof given.id !== "string") return undefined;
   const name = givenNameOf(given.id);
   if (name === undefined) return undefined;
   return Object.prototype.hasOwnProperty.call(givens, name)
      ? givens[name]
      : undefined;
}

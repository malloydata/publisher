// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `decideLock`'s contract, against the IR shapes Malloy actually emits.
 *
 * The node literals below were captured from a real `getPreparedQuery()` on a
 * `#(authorize)` probe (`@malloydata/malloy` 0.0.432) — `=` carries
 * `kids.left`/`kids.right` in the order the author wrote them, `inGiven`
 * carries `givenRef` plus a `stringLiteral` operand, and `stringLiteral.literal`
 * arrives already unquoted and unescaped. `authorize_syntax_conformance.spec.ts`
 * exercises the same rules end to end through DuckDB; this file pins the
 * fail-closed edges a served query cannot easily reach.
 */

import { describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { decideLock } from "./authorize_lock";

const ROLE_ID = "given/4:ROLE";
const GROUPS_ID = "given/6:GROUPS";

const NAMES = new Map([
   [ROLE_ID, "ROLE"],
   [GROUPS_ID, "GROUPS"],
]);
const nameOf = (id: string) => NAMES.get(id);

const given = (id: string) => ({ node: "given", id, refName: id });
const str = (literal: string) => ({ node: "stringLiteral", literal });
const paren = (e: unknown) => ({ node: "()", e });
const eq = (left: unknown, right: unknown) => ({
   node: "=",
   kids: { left, right },
});
const inGiven = (id: string, literal: string, not = false) => ({
   node: "inGiven",
   not,
   givenRef: given(id),
   e: str(literal),
});
const and = (left: unknown, right: unknown) => ({
   node: "and",
   kids: { left, right },
});

const decide = (expr: unknown, givens: Record<string, GivenValue>) =>
   decideLock(expr, nameOf, givens);

describe("decideLock — the accepted shapes", () => {
   it("`'admin' = $ROLE` and `$ROLE = 'admin'` decide identically", () => {
      // The graft compiles the author's original text, so both operand orders
      // reach here. A rule that read only `kids.left` would admit one author
      // and deny the other for the same intent.
      const literalFirst = paren(eq(str("admin"), given(ROLE_ID)));
      const givenFirst = paren(eq(given(ROLE_ID), str("admin")));
      expect(decide(literalFirst, { ROLE: "admin" })).toBe("admit");
      expect(decide(givenFirst, { ROLE: "admin" })).toBe("admit");
      expect(decide(literalFirst, { ROLE: "intern" })).toBe("deny");
      expect(decide(givenFirst, { ROLE: "intern" })).toBe("deny");
   });

   it("`'finance' in $GROUPS` admits a member and denies a non-member", () => {
      const expr = paren(inGiven(GROUPS_ID, "finance"));
      expect(decide(expr, { GROUPS: ["sales", "finance"] })).toBe("admit");
      expect(decide(expr, { GROUPS: ["sales"] })).toBe("deny");
      expect(decide(expr, { GROUPS: [] })).toBe("deny");
   });

   it("`and` admits only when both terms admit", () => {
      const expr = paren(
         and(eq(str("admin"), given(ROLE_ID)), inGiven(GROUPS_ID, "finance")),
      );
      expect(decide(expr, { ROLE: "admin", GROUPS: ["finance"] })).toBe(
         "admit",
      );
      expect(decide(expr, { ROLE: "admin", GROUPS: ["sales"] })).toBe("deny");
      expect(decide(expr, { ROLE: "intern", GROUPS: ["finance"] })).toBe(
         "deny",
      );
   });

   it("the bare sentinels decide without reading any given", () => {
      expect(decide(paren({ node: "true" }), {})).toBe("admit");
      expect(decide(paren({ node: "false" }), {})).toBe("deny");
   });
});

describe("decideLock — everything else refuses", () => {
   // Nothing below admits. The label separates the two refusals: `deny` is the
   // gate's own rule turning a caller away, `unresolvable` is the gate failing
   // to decide at all — a drift between the load-time grammar and this walk.
   // Both are the same 403; only `unresolvable` means something is wrong, and
   // it is the one an operator alerts on. Every case asserts the exact label,
   // so a regression that relabels a broken gate as routine fails here.
   const refuses = (outcome: string) => expect(outcome).not.toBe("admit");

   it("an unset, absent or null given is unresolvable, not a throw", () => {
      // `null` is the one that mattered: reaching `(null).some(...)` is a
      // TypeError, which surfaces as a 500 instead of the 403 every other
      // unadmitted shape gets.
      const expr = paren(inGiven(GROUPS_ID, "finance"));
      refuses(decide(expr, {}));
      expect(decide(expr, {})).toBe("unresolvable");
      expect(decide(expr, { GROUPS: null as unknown as GivenValue })).toBe(
         "unresolvable",
      );
      expect(decide(expr, { GROUPS: undefined as unknown as GivenValue })).toBe(
         "unresolvable",
      );
   });

   it("a given id that resolves to no name on this model is unresolvable", () => {
      // The cross-import-hop case: the lift reports the gate unexpressible at
      // this entry point, and the decision agrees rather than admitting on a
      // value it happened to find under a same-spelled name.
      const expr = paren(eq(str("admin"), given("given/9:ELSEWHERE")));
      refuses(decide(expr, { ELSEWHERE: "admin" }));
      expect(decide(expr, { ELSEWHERE: "admin" })).toBe("unresolvable");
   });

   it("an unknown node kind is unresolvable — the allowlist is positive", () => {
      // A node kind the grammar should never have let through is drift, not a
      // caller being refused, so it must not book as routine.
      const or = paren({
         node: "or",
         kids: { left: { node: "true" }, right: { node: "true" } },
      });
      refuses(decide(or, {}));
      expect(decide(or, {})).toBe("unresolvable");
      expect(decide(paren({ node: "not", e: { node: "false" } }), {})).toBe(
         "unresolvable",
      );
      expect(decide(paren({ node: "someFutureNode" }), {})).toBe(
         "unresolvable",
      );
      expect(decide(undefined, {})).toBe("unresolvable");
      expect(decide(null, {})).toBe("unresolvable");
   });

   it("a negated membership is an ordinary deny, not a broken gate", () => {
      // W2 warns at load and leaves the model servable, so this shape is an
      // author's choice refusing a caller — routine, not something to alert on.
      expect(
         decide(paren(inGiven(GROUPS_ID, "finance", true)), {
            GROUPS: ["sales"],
         }),
      ).toBe("deny");
   });

   it("arity mismatches are unresolvable — `=` is scalar, `in` is list", () => {
      // Load refuses these as a 424, so one arriving here is drift.
      expect(
         decide(paren(eq(str("admin"), given(ROLE_ID))), {
            ROLE: ["admin"] as unknown as GivenValue,
         }),
      ).toBe("unresolvable");
      expect(
         decide(paren(inGiven(GROUPS_ID, "finance")), {
            GROUPS: "finance" as unknown as GivenValue,
         }),
      ).toBe("unresolvable");
   });

   it("comparison is exact and case-sensitive, not the warehouse's collation", () => {
      // A source-shaped gate on the pre-flip row route was compared by the
      // warehouse, so a MySQL tenant's case-insensitive default admitted this.
      expect(
         decide(paren(inGiven(GROUPS_ID, "finance")), { GROUPS: ["Finance"] }),
      ).toBe("deny");
      expect(
         decide(paren(eq(str("admin"), given(ROLE_ID))), { ROLE: "Admin" }),
      ).toBe("deny");
   });

   it("a cycle in the node graph is unresolvable rather than hanging", () => {
      const cyclic: { node: string; e?: unknown } = { node: "()" };
      cyclic.e = cyclic;
      refuses(decide(cyclic, {}));
      expect(decide(cyclic, {})).toBe("unresolvable");
   });
});

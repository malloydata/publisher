// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of "which physical table" as each of today's keys answers
 * it, on case, quoting and schema-qualification inputs, so the approved
 * unification onto one domain key (exact, leaf, may-be-same) shows its change
 * as a diff of the tables below rather than as a silent move.
 *
 * - exact: `physicalTargetKey` (a run's duplicate-target skip and failed-target
 *   reuse) and `tableKeyOf` (the ledger key): the name as written;
 * - identity: `tableIdentity` (claims, superseding, the owned-name lock),
 *   `tableIdentityKey` (the uniqueness ledger; the run's
 *   `writtenTableIdentity` is this key over the instruction's place) and
 *   `tableLeafKey` / `mayBeOneTableAnywhere` (reclaim): the last segment,
 *   unquoted, case-folded;
 * - may-be-one: `mayBeOneTable` (a rename's reclaim): every segment unquoted
 *   and case-folded, containers matched by suffix.
 */

import { describe, expect, it } from "bun:test";
import { makeInstruction } from "../../test_helpers/materialization_test_fixtures";
import { tableIdentity } from "../versions/materialization_scope";
import { tableIdentityKey } from "./run/inline_uniqueness";
import {
   physicalTargetKey,
   tableKeyOf,
} from "./run/materialization_run_support";
import {
   mayBeOneTable,
   mayBeOneTableAnywhere,
   tableLeafKey,
} from "./run/run_instructions";

/** Each key's answer to "are `a` and `b` one table", in one place. */
function sameUnder(a: string, b: string) {
   const instruction = (name: string) =>
      makeInstruction({ physicalTableName: name });
   return {
      exact:
         physicalTargetKey(instruction(a), "duckdb") ===
         physicalTargetKey(instruction(b), "duckdb"),
      ledger:
         tableKeyOf("duckdb", undefined, a) ===
         tableKeyOf("duckdb", undefined, b),
      identity: tableIdentity(a) === tableIdentity(b),
      uniqueness:
         tableIdentityKey("connection", "duckdb", a) ===
         tableIdentityKey("connection", "duckdb", b),
      leaf: tableLeafKey(a) === tableLeafKey(b),
      anywhere: mayBeOneTableAnywhere(a, b),
      mayBeOne: mayBeOneTable(a, b),
   };
}

type Answers = ReturnType<typeof sameUnder>;
const all = (v: boolean): Answers => ({
   exact: v,
   ledger: v,
   identity: v,
   uniqueness: v,
   leaf: v,
   anywhere: v,
   mayBeOne: v,
});
/** Exact keys say two tables; every folding key says one. */
const FOLDED: Answers = { ...all(true), exact: false, ledger: false };

const PAIRS: [string, string, Answers][] = [
   ["orders", "orders", all(true)],
   ["orders", "Orders", FOLDED],
   ["orders", "ORDERS", FOLDED],
   ["orders", '"orders"', FOLDED],
   ["orders", "`orders`", FOLDED],
   ["orders", "[orders]", FOLDED],
   ['"Orders"', '"orders"', FOLDED],
   // Schema qualification: one table in the connection's defaults, maybe.
   ["orders", "main.orders", FOLDED],
   ["main.orders", "memory.main.orders", FOLDED],
   ['"Main"."Orders"', "main.orders", FOLDED],
   // Two schemas: the leaf keys fold them together; may-be-one does not.
   ["a.orders", "b.orders", { ...FOLDED, mayBeOne: false }],
   ["x.a.orders", "y.a.orders", { ...FOLDED, mayBeOne: false }],
   // A quoted name holding a dot is split on it like any other dot.
   ['"a.orders"', "orders", { ...FOLDED, mayBeOne: true }],
   ['"a.orders"', '"a"."orders"', FOLDED],
   // Spaces are kept; case is folded around them.
   ['"Order Lines"', "order lines", FOLDED],
   ["orders", "orders_v2", all(false)],
   ["orders", "orders ", all(false)],
];

describe("which physical table, per key", () => {
   for (const [a, b, expected] of PAIRS) {
      it(`${a} | ${b}`, () => {
         expect(sameUnder(a, b)).toEqual(expected);
      });
   }

   it("reads a name's identity as its last segment, unquoted, case-folded", () => {
      const identities = Object.fromEntries(
         [
            "orders",
            '"Main"."Orders"',
            "memory.main.ORDERS",
            "`ds`.`Orders`",
            "[dbo].[Orders]",
            '"a.orders"',
            '"Order Lines"',
            '"we""ird"',
         ].map((n) => [n, tableIdentity(n)]),
      );
      expect(identities).toEqual({
         orders: "orders",
         '"Main"."Orders"': "orders",
         "memory.main.ORDERS": "orders",
         "`ds`.`Orders`": "orders",
         "[dbo].[Orders]": "orders",
         '"a.orders"': "orders",
         '"Order Lines"': "order lines",
         // A doubled quote is stripped like any other.
         '"we""ird"': "weird",
      });
      for (const name of Object.keys(identities)) {
         expect(tableLeafKey(name)).toBe(tableIdentity(name));
      }
   });
});

describe("where a table lives, per key", () => {
   it("keeps a connection and a destination of one name apart in every keyed form", () => {
      const viaConnection = makeInstruction({ physicalTableName: "t" });
      const viaDestination = makeInstruction({
         physicalTableName: "t",
         destination: "lake",
      });
      expect(physicalTargetKey(viaConnection, "lake")).not.toBe(
         physicalTargetKey(viaDestination, "lake"),
      );
      expect(tableKeyOf("lake", undefined, "t")).not.toBe(
         tableKeyOf("lake", "lake", "t"),
      );
      expect(tableIdentityKey("connection", "lake", "t")).not.toBe(
         tableIdentityKey("destination", "lake", "t"),
      );
   });

   it("ignores the connection for an instruction into a destination (exact key)", () => {
      const into = makeInstruction({
         physicalTableName: "t",
         destination: "lake",
      });
      expect(physicalTargetKey(into, "duckdb")).toBe(
         physicalTargetKey(into, "pg"),
      );
      expect(physicalTargetKey(into, "duckdb")).toBe(
         '["destination","lake","t"]',
      );
      expect(
         physicalTargetKey(makeInstruction({ physicalTableName: "t" }), "pg"),
      ).toBe('["connection","pg","t"]');
   });

   it("keeps the place in the ledger and uniqueness keys, but not in the leaf keys", () => {
      expect(tableKeyOf("a", undefined, "t")).not.toBe(
         tableKeyOf("b", undefined, "t"),
      );
      expect(tableIdentityKey("connection", "a", "T")).toBe(
         tableIdentityKey("connection", "a", "t"),
      );
      expect(tableIdentityKey("connection", "a", "t")).not.toBe(
         tableIdentityKey("connection", "b", "t"),
      );
      expect(tableKeyOf("duckdb", "lake", "t")).toBe("duckdb\u0000lake\u0000t");
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of "which physical table" as each of today's keys answers
 * it, on case, quoting and schema-qualification inputs, so the approved
 * unification onto one domain key (exact, leaf, may-be-same) shows its change
 * as a diff of the tables below rather than as a silent move.
 *
 * - written: `tableKey` (a run's duplicate-target skip and failed-target
 *   reuse) in a place that folds case (a destination, a folding dialect):
 *   every segment unquoted and case-folded, the whole path compared;
 *   `keeping`: the same key in a place that keeps case (Postgres): the name
 *   exactly as written;
 * - ledger: `recordedTableKey` (a ledger row, matched as the store matches
 *   it, and the reclaim's renamed and kept tables): the name as written;
 * - identity: `tableIdentity` (claims, superseding, the owned-name lock),
 *   `tableIdentityKey` (the uniqueness ledger; the run's
 *   `writtenTableIdentity` is this key over the instruction's place) and
 *   `leafKey` (reclaim, compared anywhere): the last segment, unquoted,
 *   case-folded;
 * - may-be-one: `mayBeSameTable` (a rename's reclaim): every segment
 *   unquoted and case-folded, containers matched by suffix.
 *
 * The keys live in `domain/physical_table.ts`; `tableIdentity` and
 * `tableIdentityKey` delegate to it. The written key folds case and quotes
 * in the run refactor where the place folds them (approved): `Foo` and `foo`
 * were two targets before everywhere; they still are on Postgres.
 */

import { describe, expect, it } from "bun:test";
import { makeInstruction } from "../../test_helpers/materialization_test_fixtures";
import { tableIdentity } from "../versions/materialization_scope";
import { tableIdentityKey } from "./run/inline_uniqueness";
import {
   instructedTable,
   leafKey,
   recordedTableKey,
   mayBeSameTable,
   tableKey,
} from "./domain/physical_table";
import type { BuildInstruction } from "./domain/types";

/** The run's written-table key for an instruction on `connectionName`. */
function physicalTargetKey(
   instruction: BuildInstruction,
   connectionName: string,
   foldsCase = true,
): string {
   return tableKey(instructedTable(instruction, connectionName), foldsCase);
}

/** A ledger row's key on a connection, in a destination when given. */
function tableKeyOf(
   connectionName: string,
   storageDestinationName: string | undefined,
   physicalTableName: string,
): string {
   return recordedTableKey({
      connectionName,
      storageDestinationName,
      physicalTableName,
   });
}

/** Each key's answer to "are `a` and `b` one table", in one place. */
function sameUnder(a: string, b: string) {
   const instruction = (name: string) =>
      makeInstruction({ physicalTableName: name });
   return {
      written:
         physicalTargetKey(instruction(a), "duckdb") ===
         physicalTargetKey(instruction(b), "duckdb"),
      keeping:
         physicalTargetKey(instruction(a), "pg", false) ===
         physicalTargetKey(instruction(b), "pg", false),
      ledger:
         tableKeyOf("duckdb", undefined, a) ===
         tableKeyOf("duckdb", undefined, b),
      identity: tableIdentity(a) === tableIdentity(b),
      uniqueness:
         tableIdentityKey("connection", "duckdb", a) ===
         tableIdentityKey("connection", "duckdb", b),
      leaf: leafKey(a) === leafKey(b),
      anywhere: leafKey(a) === leafKey(b),
      mayBeOne: mayBeSameTable(a, b),
   };
}

type Answers = ReturnType<typeof sameUnder>;
const all = (v: boolean): Answers => ({
   written: v,
   keeping: v,
   ledger: v,
   identity: v,
   uniqueness: v,
   leaf: v,
   anywhere: v,
   mayBeOne: v,
});
/** The ledger's exact key says two tables; every folding key says one. */
const FOLDED: Answers = { ...all(true), keeping: false, ledger: false };
/** Spelled with other containers: the written key compares the whole path. */
const QUALIFIED: Answers = { ...FOLDED, written: false };

const PAIRS: [string, string, Answers][] = [
   ["orders", "orders", all(true)],
   ["orders", "Orders", FOLDED],
   ["orders", "ORDERS", FOLDED],
   ["orders", '"orders"', FOLDED],
   ["orders", "`orders`", FOLDED],
   ["orders", "[orders]", FOLDED],
   ['"Orders"', '"orders"', FOLDED],
   // Schema qualification: one table in the connection's defaults, maybe.
   ["orders", "main.orders", QUALIFIED],
   ["main.orders", "memory.main.orders", QUALIFIED],
   ['"Main"."Orders"', "main.orders", FOLDED],
   // Two schemas: the leaf keys fold them together; may-be-one does not.
   ["a.orders", "b.orders", { ...QUALIFIED, mayBeOne: false }],
   ["x.a.orders", "y.a.orders", { ...QUALIFIED, mayBeOne: false }],
   // A quoted name holding a dot is split on it like any other dot.
   ['"a.orders"', "orders", { ...QUALIFIED, mayBeOne: true }],
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
         physicalTargetKey(
            makeInstruction({
               physicalTableName: '"Main"."T"',
               destination: "lake",
            }),
            "pg",
         ),
      ).toBe('["destination","lake","main.t"]');
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
      expect(tableKeyOf("duckdb", "lake", "t")).toBe('["duckdb","lake","t"]');
   });
});

describe("a recorded table, as the reclaim keys it", () => {
   it("decides each recorded table on its own: case, and the connection that wrote a destination table, both count", () => {
      const key = (connectionName: string, name: string, dest?: string) =>
         recordedTableKey({
            connectionName,
            storageDestinationName: dest,
            physicalTableName: name,
         });
      expect(key("pg", "s.Orders")).not.toBe(key("pg", "s.orders"));
      expect(key("pg", "t", "lake")).not.toBe(key("bq", "t", "lake"));
      expect(key("pg", "t", "lake")).toBe(key("pg", "t", "lake"));
   });
});

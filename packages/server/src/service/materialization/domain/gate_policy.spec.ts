// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   answerKeepsGates,
   legMayPassGate,
   tableKeepsGates,
   type GatedTable,
   type GateSwitches,
} from "./gate_policy";

const ON: GateSwitches = {
   materialize: true,
   gatedSources: true,
};
const OWN_GATE = {
   rowLevel: true,
   attributed: true,
   inheritedRowFilter: false,
};
const table = (t: Partial<GatedTable>): GatedTable => ({
   kind: "materialize",
   tier: "storage",
   v1: false,
   gate: OWN_GATE,
   ...t,
});

describe("the gate policy's rows", () => {
   it("keeps a materialized or pair table's own row-level gate on either tier, with v2's switch and with or without the gated-sources one (G3)", () => {
      for (const kind of ["materialize", "pair"] as const) {
         for (const tier of ["database", "storage"] as const) {
            expect(tableKeepsGates(table({ kind, tier }), ON)).toBe(true);
            expect(
               tableKeepsGates(table({ kind, tier }), {
                  ...ON,
                  gatedSources: false,
               }),
            ).toBe(true);
         }
         expect(
            tableKeepsGates(table({ kind }), { ...ON, materialize: false }),
         ).toBe(false);
         for (const gate of [
            { ...OWN_GATE, rowLevel: false },
            { ...OWN_GATE, attributed: false },
            { ...OWN_GATE, inheritedRowFilter: true },
            undefined,
         ]) {
            expect(tableKeepsGates(table({ kind, gate }), ON)).toBe(false);
         }
      }
   });

   it("keeps a v2 rollup's proven base gate on either tier under the switch", () => {
      const rollup = (t: Partial<GatedTable>) =>
         table({ kind: "rollup", gate: undefined, baseGateProven: true, ...t });
      expect(tableKeepsGates(rollup({ tier: "database" }), ON)).toBe(true);
      expect(tableKeepsGates(rollup({ tier: "storage" }), ON)).toBe(true);
      expect(
         tableKeepsGates(rollup({ tier: "database" }), {
            ...ON,
            gatedSources: false,
         }),
      ).toBe(false);
      // The rollup row does not read the materialize switch.
      expect(
         tableKeepsGates(rollup({ tier: "database" }), {
            ...ON,
            materialize: false,
         }),
      ).toBe(true);
      expect(tableKeepsGates(rollup({ v1: true, tier: "database" }), ON)).toBe(
         false,
      );
      expect(tableKeepsGates(rollup({ baseGateProven: false }), ON)).toBe(
         false,
      );
   });

   it("keeps a rollup's own proven base gate with the switch off (G3), on either tier, and no other", () => {
      const OFF_ALL: GateSwitches = {
         materialize: false,
         gatedSources: false,
      };
      const own = {
         rowLevel: true,
         attributed: true,
         inheritedRowFilter: false,
      };
      const rollup = (t: Partial<GatedTable>) =>
         table({ kind: "rollup", gate: own, baseGateProven: true, ...t });
      for (const sw of [OFF_ALL, { ...OFF_ALL, materialize: true }]) {
         expect(tableKeepsGates(rollup({ tier: "database" }), sw)).toBe(true);
         expect(tableKeepsGates(rollup({ tier: "storage" }), sw)).toBe(true);
         expect(
            tableKeepsGates(
               rollup({
                  tier: "storage",
                  gate: { ...own, attributed: false, inheritedRowFilter: true },
               }),
               sw,
            ),
         ).toBe(false);
         // An inherited gate, or one not proven on the grain.
         expect(
            tableKeepsGates(
               rollup({
                  tier: "database",
                  gate: { ...own, attributed: false, inheritedRowFilter: true },
               }),
               sw,
            ),
         ).toBe(false);
         expect(
            tableKeepsGates(
               rollup({ tier: "database", baseGateProven: false }),
               sw,
            ),
         ).toBe(false);
         expect(
            tableKeepsGates(rollup({ tier: "database", v1: true }), sw),
         ).toBe(false);
      }
   });

   it("keeps a v1 table's own row-level gate under G3 on either tier, whatever the switches, and nothing else of v1", () => {
      const persist = (t: Partial<GatedTable>) =>
         table({ kind: "persist", v1: true, ...t });
      const NONE: GateSwitches = {
         materialize: false,
         gatedSources: false,
      };
      for (const s of [ON, NONE]) {
         expect(tableKeepsGates(persist({ tier: "storage" }), s)).toBe(true);
         expect(tableKeepsGates(persist({ tier: "database" }), s)).toBe(true);
         for (const gate of [
            { ...OWN_GATE, rowLevel: false },
            { ...OWN_GATE, attributed: false },
            { ...OWN_GATE, inheritedRowFilter: true },
            undefined,
         ]) {
            expect(tableKeepsGates(persist({ gate }), s)).toBe(false);
         }
      }
   });

   it("keeps no view table's gates, and fails closed on a kind with no row", () => {
      expect(tableKeepsGates(table({ kind: "view" }), ON)).toBe(false);
      expect(
         tableKeepsGates(
            table({ kind: "toString" as unknown as GatedTable["kind"] }),
            ON,
         ),
      ).toBe(false);
   });
});

describe("a rollup over locks reading no row field (g-inh-lock)", () => {
   const LOCKS = { ...OWN_GATE, attributed: false, inheritedRowFilter: true };
   const OFF = { ...ON, gatedSources: false };
   it("is kept on either tier and switch, own or inherited", () => {
      for (const tier of ["database", "storage"] as const)
         for (const sw of [ON, OFF])
            for (const gate of [OWN_GATE, LOCKS]) {
               expect(
                  tableKeepsGates(
                     table({
                        kind: "rollup",
                        tier,
                        baseGateProven: true,
                        gate: { ...gate, lockOnly: true },
                     }),
                     sw,
                  ),
               ).toBe(true);
            }
   });
   it("is not kept without the proof, for a v1 line, or for an inherited gate that is not lock-only", () => {
      const r = (t: Partial<GatedTable>) =>
         table({ kind: "rollup", baseGateProven: true, ...t });
      const lock = { ...LOCKS, lockOnly: true };
      expect(
         tableKeepsGates(r({ gate: lock, baseGateProven: false }), OFF),
      ).toBe(false);
      expect(tableKeepsGates(r({ gate: lock, v1: true }), OFF)).toBe(false);
      expect(
         tableKeepsGates(r({ gate: { ...LOCKS, lockOnly: false } }), OFF),
      ).toBe(false);
      expect(tableKeepsGates(r({ gate: LOCKS, tier: "storage" }), OFF)).toBe(
         false,
      );
   });
});

describe("the gate policy at the router", () => {
   const G3 = { databaseReadsKeepGates: true };
   const NO_G3 = { databaseReadsKeepGates: false };
   const OFF = { ...ON, gatedSources: false };
   const database = [{ tier: "database" as const }];
   // A `storage=` read carries its gate columns' stored types, as the router
   // lists them.
   const exact = { gateColumnTypes: ["BIGINT"] };
   const mixed = [
      { tier: "database" as const },
      { tier: "storage" as const, ...exact },
   ];

   it("lets every leg past a gate under the gated-sources switch", () => {
      for (const leg of ["view", "rollup", "served"] as const) {
         expect(legMayPassGate(leg, ON, NO_G3)).toBe(true);
         expect(answerKeepsGates(leg, mixed, ON, NO_G3)).toBe(true);
         expect(answerKeepsGates(leg, [], ON, NO_G3)).toBe(true);
      }
   });

   it("keeps a rollup answer under G3 only from a database rollup, a storage= one admitted past its base's own gates, or for an entry point of locks alone", () => {
      const own = [{ tier: "storage" as const, ownGateKept: true, ...exact }];
      const other = [{ tier: "storage" as const, ...exact }];
      expect(answerKeepsGates("rollup", own, OFF, G3)).toBe(true);
      expect(answerKeepsGates("rollup", other, OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", [...own, ...other], OFF, G3)).toBe(
         false,
      );
      expect(answerKeepsGates("rollup", other, ON, NO_G3)).toBe(true);
      // An entry point whose gates are locks reading no column.
      const locks = [
         { tier: "storage" as const, gatesReadNoColumn: true, ...exact },
      ];
      expect(answerKeepsGates("rollup", locks, OFF, G3)).toBe(true);
      expect(answerKeepsGates("rollup", locks, OFF, NO_G3)).toBe(false);
      // Never past an inexact or unlisted gate column, under any switch.
      for (const sw of [ON, OFF]) {
         expect(
            answerKeepsGates(
               "rollup",
               [{ ...own[0], gateColumnTypes: ["DOUBLE"] }],
               sw,
               G3,
            ),
         ).toBe(false);
         expect(
            answerKeepsGates(
               "rollup",
               [{ tier: "storage" as const, ownGateKept: true }],
               sw,
               G3,
            ),
         ).toBe(false);
      }
   });

   it("lets the served copy and the rollup leg past without it, for an answer reading only database tables (G3)", () => {
      expect(legMayPassGate("view", OFF, G3)).toBe(false);
      expect(legMayPassGate("rollup", OFF, G3)).toBe(true);
      expect(legMayPassGate("served", OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", database, OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", mixed, OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [], OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", database, OFF, G3)).toBe(true);
      expect(answerKeepsGates("rollup", mixed, OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", [], OFF, G3)).toBe(false);
      expect(legMayPassGate("rollup", OFF, NO_G3)).toBe(false);
      expect(legMayPassGate("served", OFF, NO_G3)).toBe(false);
      expect(answerKeepsGates("served", database, OFF, NO_G3)).toBe(false);
   });

   it("counts a storage= table admitted past its own gate as a G3 table, and no other storage= table, on the served copy and the rollup leg", () => {
      const own = { tier: "storage" as const, ownGateKept: true, ...exact };
      const other = { tier: "storage" as const, ...exact };
      expect(answerKeepsGates("served", [own], OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", [own, ...database], OFF, G3)).toBe(
         true,
      );
      // A mixed answer, an own-gate table beside one plan did not admit.
      expect(answerKeepsGates("served", [own, other], OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [other], OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [own], OFF, NO_G3)).toBe(false);
      expect(answerKeepsGates("view", [own], OFF, G3)).toBe(false);
      // A `storage=` rollup admitted past its base's own gates (P3/P4).
      expect(answerKeepsGates("rollup", [own], OFF, G3)).toBe(true);
      // g-mixed for a `storage=` rollup: a join branch reads the query
      // source's own table there beside a rollup that holds only ungated
      // rows; beside a table proven neither way, it is not kept.
      const ungatedRollup = {
         tier: "storage" as const,
         holdsOnlyUngatedRows: true,
         ...exact,
      };
      expect(answerKeepsGates("rollup", [own, ungatedRollup], OFF, G3)).toBe(
         true,
      );
      expect(answerKeepsGates("rollup", [ungatedRollup], OFF, NO_G3)).toBe(
         false,
      );
      expect(answerKeepsGates("rollup", [other, ungatedRollup], OFF, G3)).toBe(
         false,
      );
      expect(answerKeepsGates("rollup", [other], OFF, G3)).toBe(false);
   });

   it("L2: keeps no answer whose gate column is stored inexactly, under any switch", () => {
      const own = (gateColumnTypes?: string[]) => ({
         tier: "storage" as const,
         ownGateKept: true,
         ...(gateColumnTypes ? { gateColumnTypes } : {}),
      });
      for (const inexact of [
         ["DOUBLE"],
         ["BIGINT", "FLOAT"],
         ["REAL"],
         ["HUGEINT"],
         ["UHUGEINT"],
         ["STRUCT(acct DOUBLE, region VARCHAR)"],
      ]) {
         for (const sw of [ON, OFF]) {
            expect(answerKeepsGates("served", [own(inexact)], sw, G3)).toBe(
               false,
            );
         }
         expect(answerKeepsGates("rollup", [own(inexact)], ON, G3)).toBe(false);
         expect(answerKeepsGates("view", [own(inexact)], ON, G3)).toBe(false);
         expect(
            answerKeepsGates(
               "served",
               [{ tier: "database", gateColumnTypes: inexact }],
               ON,
               G3,
            ),
         ).toBe(false);
      }
      // Types that could not be listed: a storage= table keeps nothing, a
      // database table (built in the source's own types) keeps its gate.
      expect(answerKeepsGates("served", [own()], OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [own()], ON, G3)).toBe(false);
      expect(answerKeepsGates("served", database, OFF, G3)).toBe(true);
      // Exact types keep it, as before.
      for (const exactTypes of [
         ["BIGINT"],
         ["DECIMAL(38,0)"],
         ["VARCHAR", "DATE", "TIMESTAMP"],
         [],
      ]) {
         expect(answerKeepsGates("served", [own(exactTypes)], OFF, G3)).toBe(
            true,
         );
      }
   });

   it("R-10: keeps a gate over a stored DOUBLE that copies a warehouse double compared as DuckDB compares it, and no other float", () => {
      const read = (
         stored: string,
         upstream: { dialect: string; nativeType: string } | undefined,
      ) => [
         {
            tier: "storage" as const,
            ownGateKept: true,
            gateColumnTypes: ["BIGINT", stored],
            gateColumnUpstreamTypes: [undefined, upstream],
         },
      ];
      const keeps = (
         stored: string,
         upstream: { dialect: string; nativeType: string } | undefined,
      ) => answerKeepsGates("served", read(stored, upstream), OFF, G3);
      // Exact: the same 8-byte double, NaN compared alike.
      expect(
         keeps("DOUBLE", { dialect: "snowflake", nativeType: "FLOAT" }),
      ).toBe(true);
      expect(
         keeps("DOUBLE", {
            dialect: "snowflake",
            nativeType: "double precision",
         }),
      ).toBe(true);
      expect(
         keeps("DOUBLE", {
            dialect: "postgres",
            nativeType: "double precision",
         }),
      ).toBe(true);
      expect(keeps("DOUBLE", { dialect: "duckdb", nativeType: "DOUBLE" })).toBe(
         true,
      );
      // Not: BigQuery's FLOAT64 compares NaN false; a 4-byte float rounds a
      // literal live; an exact number or an integer rounds past 2^53; a
      // column the build computed, or whose table was not read, is unknown.
      for (const upstream of [
         { dialect: "standardsql", nativeType: "FLOAT64" },
         { dialect: "postgres", nativeType: "real" },
         { dialect: "duckdb", nativeType: "FLOAT" },
         { dialect: "snowflake", nativeType: "NUMBER(38,2)" },
         { dialect: "snowflake", nativeType: "NUMBER(38,0)" },
         { dialect: "postgres", nativeType: "bigint" },
         { dialect: "postgres", nativeType: "numeric(12,4)" },
         { dialect: "mysql", nativeType: "double" },
         undefined,
      ]) {
         expect({ upstream, kept: keeps("DOUBLE", upstream) }).toEqual({
            upstream,
            kept: false,
         });
      }
      // Stored narrower than the double it copies, or as a 128-bit integer.
      expect(
         keeps("FLOAT", { dialect: "snowflake", nativeType: "FLOAT" }),
      ).toBe(false);
      expect(
         keeps("HUGEINT", { dialect: "snowflake", nativeType: "FLOAT" }),
      ).toBe(false);
      // A struct holding a double is not a copy of one.
      expect(
         keeps("STRUCT(x DOUBLE)", {
            dialect: "snowflake",
            nativeType: "FLOAT",
         }),
      ).toBe(false);
   });

   it("fails closed on a leg with no row", () => {
      const leg = "storage" as unknown as "served";
      expect(legMayPassGate(leg, ON, G3)).toBe(false);
      expect(answerKeepsGates(leg, database, ON, G3)).toBe(false);
   });
});

describe("the gate policy's rows at their edges", () => {
   it("fails closed on a rollup whose base gate was never proven", () => {
      expect(
         tableKeepsGates(
            table({ kind: "rollup", tier: "database", gate: undefined }),
            ON,
         ),
      ).toBe(false);
   });

   it("keeps a materialized table's gate in storage", () => {
      expect(
         tableKeepsGates(table({ kind: "materialize", tier: "storage" }), ON),
      ).toBe(true);
   });

   it("keeps a persist table's gate on either tier, and only its own row-level gate (G3), under every switch", () => {
      const all: GateSwitches = {
         materialize: true,
         gatedSources: true,
      };
      for (const v1 of [true, false]) {
         expect(
            tableKeepsGates(
               table({ kind: "persist", v1, tier: "database" }),
               all,
            ),
         ).toBe(true);
         expect(
            tableKeepsGates(
               table({ kind: "persist", v1, tier: "storage" }),
               all,
            ),
         ).toBe(true);
         for (const gate of [
            { ...OWN_GATE, rowLevel: false },
            { ...OWN_GATE, attributed: false },
            { ...OWN_GATE, inheritedRowFilter: true },
         ]) {
            expect(
               tableKeepsGates(
                  table({ kind: "persist", v1, tier: "storage", gate }),
                  all,
               ),
            ).toBe(false);
         }
      }
      expect(tableKeepsGates(table({ kind: "view" }), all)).toBe(false);
   });

   it("keeps no gate for a served answer that read only storage tables under G3 alone", () => {
      const OFF = { ...ON, gatedSources: false };
      expect(
         answerKeepsGates("served", [{ tier: "storage" }], OFF, {
            databaseReadsKeepGates: true,
         }),
      ).toBe(false);
   });
});

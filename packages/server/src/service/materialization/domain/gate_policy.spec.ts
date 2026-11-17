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
   gatedStorageRollupsReviewed: false,
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
         for (const tier of ["colocated", "storage"] as const) {
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

   it("keeps a v2 rollup's proven base gate colocated, and in storage only once reviewed", () => {
      const rollup = (t: Partial<GatedTable>) =>
         table({ kind: "rollup", gate: undefined, baseGateProven: true, ...t });
      expect(tableKeepsGates(rollup({ tier: "colocated" }), ON)).toBe(true);
      expect(tableKeepsGates(rollup({ tier: "storage" }), ON)).toBe(false);
      expect(
         tableKeepsGates(rollup({ tier: "storage" }), {
            ...ON,
            gatedStorageRollupsReviewed: true,
         }),
      ).toBe(true);
      expect(
         tableKeepsGates(rollup({ tier: "colocated" }), {
            ...ON,
            gatedSources: false,
         }),
      ).toBe(false);
      // The rollup row does not read the materialize switch.
      expect(
         tableKeepsGates(rollup({ tier: "colocated" }), {
            ...ON,
            materialize: false,
         }),
      ).toBe(true);
      expect(tableKeepsGates(rollup({ v1: true, tier: "colocated" }), ON)).toBe(
         false,
      );
      expect(tableKeepsGates(rollup({ baseGateProven: false }), ON)).toBe(
         false,
      );
   });

   it("keeps a colocated rollup's own proven base gate with the switch off (G3), and no other", () => {
      const OFF_ALL: GateSwitches = {
         materialize: false,
         gatedSources: false,
         gatedStorageRollupsReviewed: false,
      };
      const own = {
         rowLevel: true,
         attributed: true,
         inheritedRowFilter: false,
      };
      const rollup = (t: Partial<GatedTable>) =>
         table({ kind: "rollup", gate: own, baseGateProven: true, ...t });
      for (const sw of [OFF_ALL, { ...OFF_ALL, materialize: true }]) {
         expect(tableKeepsGates(rollup({ tier: "colocated" }), sw)).toBe(true);
         // In a storage destination: awaits its review.
         expect(tableKeepsGates(rollup({ tier: "storage" }), sw)).toBe(false);
         // An inherited gate, or one not proven on the grain.
         expect(
            tableKeepsGates(
               rollup({
                  tier: "colocated",
                  gate: { ...own, attributed: false, inheritedRowFilter: true },
               }),
               sw,
            ),
         ).toBe(false);
         expect(
            tableKeepsGates(
               rollup({ tier: "colocated", baseGateProven: false }),
               sw,
            ),
         ).toBe(false);
         expect(
            tableKeepsGates(rollup({ tier: "colocated", v1: true }), sw),
         ).toBe(false);
      }
   });

   it("keeps a v1 table's own row-level gate under G3 on either tier, whatever the switches, and nothing else of v1", () => {
      const persist = (t: Partial<GatedTable>) =>
         table({ kind: "persist", v1: true, ...t });
      const NONE: GateSwitches = {
         materialize: false,
         gatedSources: false,
         gatedStorageRollupsReviewed: false,
      };
      for (const s of [ON, NONE]) {
         expect(tableKeepsGates(persist({ tier: "storage" }), s)).toBe(true);
         expect(tableKeepsGates(persist({ tier: "colocated" }), s)).toBe(true);
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
      for (const tier of ["colocated", "storage"] as const)
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
      expect(tableKeepsGates(r({ gate: LOCKS, tier: "storage" }), ON)).toBe(
         false,
      );
   });
});

describe("the gate policy at the router", () => {
   const G3 = { colocatedReadsKeepGates: true };
   const NO_G3 = { colocatedReadsKeepGates: false };
   const OFF = { ...ON, gatedSources: false };
   const ON_UNREVIEWED = ON;
   const REVIEWED = { ...ON, gatedStorageRollupsReviewed: true };
   const colocated = [{ tier: "colocated" as const }];
   // A `storage=` read carries its gate columns' stored types, as the router
   // lists them.
   const exact = { gateColumnTypes: ["BIGINT"] };
   const mixed = [
      { tier: "colocated" as const },
      { tier: "storage" as const, ...exact },
   ];

   it("lets every leg past a gate under the gated-sources switch", () => {
      for (const leg of ["view", "rollup", "served"] as const) {
         expect(legMayPassGate(leg, ON, NO_G3)).toBe(true);
         expect(answerKeepsGates(leg, mixed, REVIEWED, NO_G3)).toBe(true);
         expect(answerKeepsGates(leg, [], ON, NO_G3)).toBe(true);
      }
   });

   it("keeps no rollup answer that read a storage= table past a gate reading a column until that tier's review, under either switch", () => {
      expect(answerKeepsGates("rollup", mixed, ON_UNREVIEWED, NO_G3)).toBe(
         false,
      );
      expect(answerKeepsGates("rollup", colocated, ON_UNREVIEWED, G3)).toBe(
         true,
      );
      // An entry point whose gates are locks reading no column.
      const locks = [
         { tier: "storage" as const, gatesReadNoColumn: true, ...exact },
      ];
      expect(answerKeepsGates("rollup", locks, ON_UNREVIEWED, NO_G3)).toBe(
         true,
      );
      expect(answerKeepsGates("rollup", locks, OFF, G3)).toBe(true);
      expect(answerKeepsGates("rollup", locks, OFF, NO_G3)).toBe(false);
      expect(
         answerKeepsGates(
            "rollup",
            [{ ...locks[0], gateColumnTypes: ["DOUBLE"] }],
            OFF,
            G3,
         ),
      ).toBe(false);
   });

   it("lets the served copy and the rollup leg past without it, for an answer reading only colocated tables (G3)", () => {
      expect(legMayPassGate("view", OFF, G3)).toBe(false);
      expect(legMayPassGate("rollup", OFF, G3)).toBe(true);
      expect(legMayPassGate("served", OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", colocated, OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", mixed, OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [], OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", colocated, OFF, G3)).toBe(true);
      expect(answerKeepsGates("rollup", mixed, OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", [], OFF, G3)).toBe(false);
      expect(legMayPassGate("rollup", OFF, NO_G3)).toBe(false);
      expect(legMayPassGate("served", OFF, NO_G3)).toBe(false);
      expect(answerKeepsGates("served", colocated, OFF, NO_G3)).toBe(false);
   });

   it("counts a storage= table admitted past its own gate as a G3 table, and no other storage= table", () => {
      const own = { tier: "storage" as const, ownGateKept: true, ...exact };
      const other = { tier: "storage" as const, ...exact };
      expect(answerKeepsGates("served", [own], OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", [own, ...colocated], OFF, G3)).toBe(
         true,
      );
      // A mixed answer, an own-gate table beside one plan did not admit.
      expect(answerKeepsGates("served", [own, other], OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [other], OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [own], OFF, NO_G3)).toBe(false);
      expect(answerKeepsGates("view", [own], OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", [own], OFF, G3)).toBe(false);
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
               [{ tier: "colocated", gateColumnTypes: inexact }],
               ON,
               G3,
            ),
         ).toBe(false);
      }
      // Types that could not be listed: a storage= table keeps nothing, a
      // database table (built in the source's own types) keeps its gate.
      expect(answerKeepsGates("served", [own()], OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [own()], ON, G3)).toBe(false);
      expect(answerKeepsGates("served", colocated, OFF, G3)).toBe(true);
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

   it("fails closed on a leg with no row", () => {
      const leg = "storage" as unknown as "served";
      expect(legMayPassGate(leg, ON, G3)).toBe(false);
      expect(answerKeepsGates(leg, colocated, ON, G3)).toBe(false);
   });
});

describe("the gate policy's rows at their edges", () => {
   it("fails closed on a rollup whose base gate was never proven", () => {
      expect(
         tableKeepsGates(
            table({ kind: "rollup", tier: "colocated", gate: undefined }),
            ON,
         ),
      ).toBe(false);
   });

   it("keeps a materialized table's gate in storage without the rollup review", () => {
      for (const reviewed of [true, false]) {
         expect(
            tableKeepsGates(table({ kind: "materialize", tier: "storage" }), {
               ...ON,
               gatedStorageRollupsReviewed: reviewed,
            }),
         ).toBe(true);
      }
   });

   it("keeps a persist table's gate on either tier, and only its own row-level gate (G3), under every switch", () => {
      const all: GateSwitches = {
         materialize: true,
         gatedSources: true,
         gatedStorageRollupsReviewed: true,
      };
      for (const v1 of [true, false]) {
         expect(
            tableKeepsGates(
               table({ kind: "persist", v1, tier: "colocated" }),
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
            colocatedReadsKeepGates: true,
         }),
      ).toBe(false);
   });
});

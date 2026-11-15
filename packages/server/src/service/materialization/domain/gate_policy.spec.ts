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
   it("keeps a materialized or pair table's own row-level gate, with both switches", () => {
      for (const kind of ["materialize", "pair"] as const) {
         for (const tier of ["colocated", "storage"] as const) {
            expect(tableKeepsGates(table({ kind, tier }), ON)).toBe(true);
         }
         expect(
            tableKeepsGates(table({ kind }), { ...ON, materialize: false }),
         ).toBe(false);
         expect(
            tableKeepsGates(table({ kind }), { ...ON, gatedSources: false }),
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

   it("keeps no persist or view table's gates, and fails closed on a kind with no row", () => {
      expect(tableKeepsGates(table({ kind: "persist", v1: true }), ON)).toBe(
         false,
      );
      expect(tableKeepsGates(table({ kind: "view" }), ON)).toBe(false);
      expect(
         tableKeepsGates(
            table({ kind: "toString" as unknown as GatedTable["kind"] }),
            ON,
         ),
      ).toBe(false);
   });
});

describe("the gate policy at the router", () => {
   const G3 = { colocatedReadsKeepGates: true };
   const NO_G3 = { colocatedReadsKeepGates: false };
   const OFF = { ...ON, gatedSources: false };
   const colocated = [{ tier: "colocated" as const }];
   const mixed = [{ tier: "colocated" as const }, { tier: "storage" as const }];

   it("lets every leg past a gate under the gated-sources switch", () => {
      for (const leg of ["view", "rollup", "served"] as const) {
         expect(legMayPassGate(leg, ON, NO_G3)).toBe(true);
         expect(answerKeepsGates(leg, mixed, ON, NO_G3)).toBe(true);
         expect(answerKeepsGates(leg, [], ON, NO_G3)).toBe(true);
      }
   });

   it("lets only the served copy past without it, for an answer reading only colocated tables (G3)", () => {
      expect(legMayPassGate("view", OFF, G3)).toBe(false);
      expect(legMayPassGate("rollup", OFF, G3)).toBe(false);
      expect(legMayPassGate("served", OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", colocated, OFF, G3)).toBe(true);
      expect(answerKeepsGates("served", mixed, OFF, G3)).toBe(false);
      expect(answerKeepsGates("served", [], OFF, G3)).toBe(false);
      expect(answerKeepsGates("rollup", colocated, OFF, G3)).toBe(false);
      expect(legMayPassGate("served", OFF, NO_G3)).toBe(false);
      expect(answerKeepsGates("served", colocated, OFF, NO_G3)).toBe(false);
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

   it("keeps no persist table's gate, v1 or not, under every switch", () => {
      const all: GateSwitches = {
         materialize: true,
         gatedSources: true,
         gatedStorageRollupsReviewed: true,
      };
      for (const v1 of [true, false]) {
         expect(tableKeepsGates(table({ kind: "persist", v1 }), all)).toBe(
            false,
         );
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

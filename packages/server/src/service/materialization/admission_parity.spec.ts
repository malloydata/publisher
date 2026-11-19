// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The router and the serve verdict, each held to an oracle neither computes:
 * the gate policy's frozen leg rows (`domain/gate_policy_frozen_rows.ts`),
 * which only the security review changes. For every leg, switch setting and
 * read the rows name, read with exact facts (every probe answers that the
 * table compares as its source does, no composite row filter):
 *
 * - `admitRead` admits exactly the reads a frozen row keeps, unless a
 *   `storage=` gate column is stored with a zone (its zone rule);
 * - the serve verdict's keep-gate path (`verdictKeepsGate`) says a source's
 *   own table keeps its gates exactly where the frozen `served` rows do for
 *   that one table, and a rollup's exactly where the frozen rows let the
 *   rollup leg past a gate and its plan proved the variant.
 *
 * A change that loosens or tightens the router and the verdict together, or
 * moves the verdict onto another leg, disagrees with the rows and fails here.
 */

import { describe, expect, it } from "bun:test";
import { admitRead, type ReadFacts } from "./domain/admission";
import type { TableRead } from "./domain/gate_policy";
import {
   FROZEN_LEG_ROWS,
   GATE_READS,
   switches,
   switchesLabel,
} from "./domain/gate_policy_frozen_rows";
import { LEGS } from "./domain/vocabulary";
import { verdictKeepsGate } from "./serve_verdicts";

/** A read past an entry point's gate whose every probe finds it exact. */
const EXACT: ReadFacts = {
   pastEntryPointGate: true,
   compositeRowFilter: false,
   reads: { scoped: false, gated: true },
   columnsListed: true,
   probes: {
      scope: {
         source: { refusal: undefined },
         stored: { refusal: undefined },
      },
   },
};

/** Each frozen read again with its `storage=` gate columns stored zoned. */
const ZONED: Record<string, readonly TableRead[]> = Object.fromEntries(
   Object.entries(GATE_READS)
      .filter(([, tables]) => tables.some((t) => t.tier === "storage"))
      .map(([name, tables]) => [
         `${name}+zoned`,
         tables.map((t) =>
            t.tier === "storage"
               ? {
                    ...t,
                    gateColumnTypes: [
                       ...(t.gateColumnTypes ?? []),
                       "TIMESTAMP WITH TIME ZONE",
                    ],
                 }
               : t,
         ),
      ]),
);

// The serving context routes with own-gate keeping on (`g3=1`).
const keeps = (leg: string, read: string, label: string) =>
   FROZEN_LEG_ROWS.includes(`keeps ${leg} read=${read} ${label} g3=1`);
const mayPass = (leg: string, label: string) =>
   FROZEN_LEG_ROWS.includes(`may ${leg} ${label} g3=1`);

describe("admission and the serve verdict agree with the frozen gate policy", () => {
   it("admitRead admits exactly the reads a frozen row keeps", () => {
      const wrong: string[] = [];
      let admitted = 0;
      for (const leg of LEGS) {
         for (const sw of switches()) {
            const label = switchesLabel(sw);
            for (const [read, tables] of Object.entries({
               ...GATE_READS,
               ...ZONED,
            })) {
               const verdict = admitRead(leg, tables, EXACT, sw);
               expect(verdict.admit).not.toBe("needs_probe");
               const expected =
                  !read.endsWith("+zoned") && keeps(leg, read, label);
               if (verdict.admit === true) admitted++;
               if ((verdict.admit === true) !== expected) {
                  wrong.push(`${leg} read=${read} ${label}: ${verdict.admit}`);
               }
            }
         }
      }
      expect(wrong).toEqual([]);
      expect(admitted).toBeGreaterThan(0);
   });

   it("the verdict's keep-gate path says what the frozen served rows say of a source's own table", () => {
      const wrong: string[] = [];
      const outcomes = new Set<boolean>();
      for (const sw of switches()) {
         const label = switchesLabel(sw);
         for (const [read, tables] of Object.entries({
            ...GATE_READS,
            ...ZONED,
         })) {
            // The verdict asks of one table: the source's own.
            if (tables.length !== 1) continue;
            const said = verdictKeepsGate(
               { rollup: false, table: tables[0] },
               sw,
            );
            outcomes.add(said);
            const expected =
               !read.endsWith("+zoned") && keeps("served", read, label);
            if (said !== expected) wrong.push(`read=${read} ${label}: ${said}`);
         }
      }
      expect(wrong).toEqual([]);
      expect([...outcomes].sort()).toEqual([false, true]);
   });

   it("a rollup keeps its gates where the rollup leg may pass one and its plan proved the variant", () => {
      const wrong: string[] = [];
      const outcomes = new Set<boolean>();
      for (const sw of switches()) {
         const label = switchesLabel(sw);
         for (const keptAtPlan of [false, true]) {
            const said = verdictKeepsGate({ rollup: true, keptAtPlan }, sw);
            outcomes.add(said);
            if (said !== (keptAtPlan && mayPass("rollup", label))) {
               wrong.push(`keptAtPlan=${keptAtPlan} ${label}: ${said}`);
            }
         }
      }
      expect(wrong).toEqual([]);
      expect([...outcomes].sort()).toEqual([false, true]);
   });
});

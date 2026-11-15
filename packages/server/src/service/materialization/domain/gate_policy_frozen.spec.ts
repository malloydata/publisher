// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The gate policy, frozen. Gated sources stay behind
 * `PUBLISHER_MATERIALIZE_GATED_SOURCES` until the security review (plan
 * question 14): no new leg, tier or kind may keep its gates without it.
 *
 * Every input the policy reads is enumerated, and the inputs it answers
 * `true` for are compared with the literal lists below. Adding a row, or
 * loosening one, changes a list and fails here; the fix is that review's
 * sign-off, not an edit to the list. A tightened row fails too, so a list
 * edited down is visible in review.
 */

import { describe, expect, it } from "bun:test";
import {
   answerKeepsGates,
   type GatedTable,
   type GateSwitches,
   legMayPassGate,
   tableKeepsGates,
} from "./gate_policy";

const REVIEW =
   "The gate policy is frozen until the security review (plan question 14): " +
   "a leg, tier or kind that keeps its gates, or a row loosened, needs that " +
   "review's sign-off before this list changes.";

const KINDS = ["persist", "materialize", "pair", "rollup", "view"] as const;
const TIERS = ["colocated", "storage"] as const;
const LEGS = ["view", "rollup", "served"] as const;
const BOOLS = [false, true] as const;

function* switches(): Generator<GateSwitches> {
   for (const materialize of BOOLS)
      for (const gatedSources of BOOLS)
         for (const gatedStorageRollupsReviewed of BOOLS)
            yield { materialize, gatedSources, gatedStorageRollupsReviewed };
}

const s = (x: GateSwitches) =>
   `mz=${+x.materialize} gs=${+x.gatedSources} rr=${+x.gatedStorageRollupsReviewed}`;

/** Every table and switch setting the policy keeps gates for. */
function tableRows(): string[] {
   const out: string[] = [];
   for (const kind of KINDS)
      for (const tier of TIERS)
         for (const v1 of BOOLS)
            for (const gate of [
               undefined,
               ...BOOLS.flatMap((rowLevel) =>
                  BOOLS.flatMap((attributed) =>
                     BOOLS.map((inheritedRowFilter) => ({
                        rowLevel,
                        attributed,
                        inheritedRowFilter,
                     })),
                  ),
               ),
            ])
               for (const baseGateProven of [undefined, ...BOOLS])
                  for (const sw of switches()) {
                     const t = {
                        kind,
                        tier,
                        v1,
                        ...(gate ? { gate } : {}),
                        ...(baseGateProven !== undefined
                           ? { baseGateProven }
                           : {}),
                     } as GatedTable;
                     if (!tableKeepsGates(t, sw)) continue;
                     const g = gate
                        ? `gate=${+gate.rowLevel}${+gate.attributed}${+gate.inheritedRowFilter}`
                        : "gate=-";
                     out.push(
                        `${kind} ${tier} v1=${+v1} ${g} proven=${baseGateProven === undefined ? "-" : +baseGateProven} ${s(sw)}`,
                     );
                  }
   return out;
}

/** Every leg, switch setting and tables read the router lets past a gate. */
function legRows(): string[] {
   const out: string[] = [];
   const reads = {
      none: [],
      colocated: [{ tier: "colocated" as const }],
      storage: [{ tier: "storage" as const }],
      mixed: [{ tier: "colocated" as const }, { tier: "storage" as const }],
   };
   for (const leg of LEGS)
      for (const sw of switches())
         for (const g3 of BOOLS) {
            const ctx = { colocatedReadsKeepGates: g3 };
            if (legMayPassGate(leg, sw, ctx))
               out.push(`may ${leg} ${s(sw)} g3=${+g3}`);
            for (const [read, tables] of Object.entries(reads))
               if (answerKeepsGates(leg, tables, sw, ctx))
                  out.push(`keeps ${leg} read=${read} ${s(sw)} g3=${+g3}`);
         }
   return out;
}

/** Fails naming the review, with what was added or dropped. */
function expectFrozen(actual: string[], frozen: readonly string[]): void {
   const added = actual.filter((r) => !frozen.includes(r));
   const dropped = frozen.filter((r) => !actual.includes(r));
   if (added.length > 0 || dropped.length > 0) {
      throw new Error(
         `${REVIEW}\nAdded: ${JSON.stringify(added, null, 1)}\nDropped: ${JSON.stringify(dropped, null, 1)}`,
      );
   }
   expect(actual).toEqual([...frozen]);
}

const DUMP = process.env.GATE_POLICY_FROZEN_DUMP;

describe("the gate policy, frozen until the security review (plan question 14)", () => {
   it("keeps gates for exactly these tables", () => {
      if (DUMP)
         return void Bun.write(
            `${DUMP}.tables.json`,
            JSON.stringify(tableRows(), null, 3),
         );
      expectFrozen(tableRows(), FROZEN_TABLE_ROWS);
   });

   it("lets exactly these legs past a gate, and keeps it for exactly these answers", () => {
      if (DUMP)
         return void Bun.write(
            `${DUMP}.legs.json`,
            JSON.stringify(legRows(), null, 3),
         );
      expectFrozen(legRows(), FROZEN_LEG_ROWS);
   });
});

// prettier-ignore
const FROZEN_TABLE_ROWS: readonly string[] = [
   "materialize colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=0",
   "materialize colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=1",
   "materialize colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=0",
   "materialize colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=1",
   "materialize colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "materialize colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "materialize colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=0",
   "materialize colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=1",
   "materialize colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=0",
   "materialize colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=1",
   "materialize colocated v1=1 gate=110 proven=1 mz=1 gs=1 rr=0",
   "materialize colocated v1=1 gate=110 proven=1 mz=1 gs=1 rr=1",
   "materialize storage v1=0 gate=110 proven=- mz=1 gs=1 rr=0",
   "materialize storage v1=0 gate=110 proven=- mz=1 gs=1 rr=1",
   "materialize storage v1=0 gate=110 proven=0 mz=1 gs=1 rr=0",
   "materialize storage v1=0 gate=110 proven=0 mz=1 gs=1 rr=1",
   "materialize storage v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "materialize storage v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "materialize storage v1=1 gate=110 proven=- mz=1 gs=1 rr=0",
   "materialize storage v1=1 gate=110 proven=- mz=1 gs=1 rr=1",
   "materialize storage v1=1 gate=110 proven=0 mz=1 gs=1 rr=0",
   "materialize storage v1=1 gate=110 proven=0 mz=1 gs=1 rr=1",
   "materialize storage v1=1 gate=110 proven=1 mz=1 gs=1 rr=0",
   "materialize storage v1=1 gate=110 proven=1 mz=1 gs=1 rr=1",
   "pair colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=0",
   "pair colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=1",
   "pair colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=0",
   "pair colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=1",
   "pair colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "pair colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "pair colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=0",
   "pair colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=1",
   "pair colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=0",
   "pair colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=1",
   "pair colocated v1=1 gate=110 proven=1 mz=1 gs=1 rr=0",
   "pair colocated v1=1 gate=110 proven=1 mz=1 gs=1 rr=1",
   "pair storage v1=0 gate=110 proven=- mz=1 gs=1 rr=0",
   "pair storage v1=0 gate=110 proven=- mz=1 gs=1 rr=1",
   "pair storage v1=0 gate=110 proven=0 mz=1 gs=1 rr=0",
   "pair storage v1=0 gate=110 proven=0 mz=1 gs=1 rr=1",
   "pair storage v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "pair storage v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "pair storage v1=1 gate=110 proven=- mz=1 gs=1 rr=0",
   "pair storage v1=1 gate=110 proven=- mz=1 gs=1 rr=1",
   "pair storage v1=1 gate=110 proven=0 mz=1 gs=1 rr=0",
   "pair storage v1=1 gate=110 proven=0 mz=1 gs=1 rr=1",
   "pair storage v1=1 gate=110 proven=1 mz=1 gs=1 rr=0",
   "pair storage v1=1 gate=110 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=- proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=- proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=- proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=- proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=000 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=000 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=000 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=000 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=001 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=001 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=001 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=001 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=010 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=010 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=010 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=010 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=011 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=011 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=011 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=011 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=100 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=100 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=100 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=100 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=101 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=101 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=101 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=101 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=110 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=110 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "rollup colocated v1=0 gate=111 proven=1 mz=0 gs=1 rr=0",
   "rollup colocated v1=0 gate=111 proven=1 mz=0 gs=1 rr=1",
   "rollup colocated v1=0 gate=111 proven=1 mz=1 gs=1 rr=0",
   "rollup colocated v1=0 gate=111 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=- proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=- proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=000 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=000 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=001 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=001 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=010 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=010 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=011 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=011 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=100 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=100 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=101 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=101 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=110 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "rollup storage v1=0 gate=111 proven=1 mz=0 gs=1 rr=1",
   "rollup storage v1=0 gate=111 proven=1 mz=1 gs=1 rr=1",
];
// prettier-ignore
const FROZEN_LEG_ROWS: readonly string[] = [
   "may view mz=0 gs=1 rr=0 g3=0",
   "keeps view read=none mz=0 gs=1 rr=0 g3=0",
   "keeps view read=colocated mz=0 gs=1 rr=0 g3=0",
   "keeps view read=storage mz=0 gs=1 rr=0 g3=0",
   "keeps view read=mixed mz=0 gs=1 rr=0 g3=0",
   "may view mz=0 gs=1 rr=0 g3=1",
   "keeps view read=none mz=0 gs=1 rr=0 g3=1",
   "keeps view read=colocated mz=0 gs=1 rr=0 g3=1",
   "keeps view read=storage mz=0 gs=1 rr=0 g3=1",
   "keeps view read=mixed mz=0 gs=1 rr=0 g3=1",
   "may view mz=0 gs=1 rr=1 g3=0",
   "keeps view read=none mz=0 gs=1 rr=1 g3=0",
   "keeps view read=colocated mz=0 gs=1 rr=1 g3=0",
   "keeps view read=storage mz=0 gs=1 rr=1 g3=0",
   "keeps view read=mixed mz=0 gs=1 rr=1 g3=0",
   "may view mz=0 gs=1 rr=1 g3=1",
   "keeps view read=none mz=0 gs=1 rr=1 g3=1",
   "keeps view read=colocated mz=0 gs=1 rr=1 g3=1",
   "keeps view read=storage mz=0 gs=1 rr=1 g3=1",
   "keeps view read=mixed mz=0 gs=1 rr=1 g3=1",
   "may view mz=1 gs=1 rr=0 g3=0",
   "keeps view read=none mz=1 gs=1 rr=0 g3=0",
   "keeps view read=colocated mz=1 gs=1 rr=0 g3=0",
   "keeps view read=storage mz=1 gs=1 rr=0 g3=0",
   "keeps view read=mixed mz=1 gs=1 rr=0 g3=0",
   "may view mz=1 gs=1 rr=0 g3=1",
   "keeps view read=none mz=1 gs=1 rr=0 g3=1",
   "keeps view read=colocated mz=1 gs=1 rr=0 g3=1",
   "keeps view read=storage mz=1 gs=1 rr=0 g3=1",
   "keeps view read=mixed mz=1 gs=1 rr=0 g3=1",
   "may view mz=1 gs=1 rr=1 g3=0",
   "keeps view read=none mz=1 gs=1 rr=1 g3=0",
   "keeps view read=colocated mz=1 gs=1 rr=1 g3=0",
   "keeps view read=storage mz=1 gs=1 rr=1 g3=0",
   "keeps view read=mixed mz=1 gs=1 rr=1 g3=0",
   "may view mz=1 gs=1 rr=1 g3=1",
   "keeps view read=none mz=1 gs=1 rr=1 g3=1",
   "keeps view read=colocated mz=1 gs=1 rr=1 g3=1",
   "keeps view read=storage mz=1 gs=1 rr=1 g3=1",
   "keeps view read=mixed mz=1 gs=1 rr=1 g3=1",
   "may rollup mz=0 gs=1 rr=0 g3=0",
   "keeps rollup read=none mz=0 gs=1 rr=0 g3=0",
   "keeps rollup read=colocated mz=0 gs=1 rr=0 g3=0",
   "keeps rollup read=storage mz=0 gs=1 rr=0 g3=0",
   "keeps rollup read=mixed mz=0 gs=1 rr=0 g3=0",
   "may rollup mz=0 gs=1 rr=0 g3=1",
   "keeps rollup read=none mz=0 gs=1 rr=0 g3=1",
   "keeps rollup read=colocated mz=0 gs=1 rr=0 g3=1",
   "keeps rollup read=storage mz=0 gs=1 rr=0 g3=1",
   "keeps rollup read=mixed mz=0 gs=1 rr=0 g3=1",
   "may rollup mz=0 gs=1 rr=1 g3=0",
   "keeps rollup read=none mz=0 gs=1 rr=1 g3=0",
   "keeps rollup read=colocated mz=0 gs=1 rr=1 g3=0",
   "keeps rollup read=storage mz=0 gs=1 rr=1 g3=0",
   "keeps rollup read=mixed mz=0 gs=1 rr=1 g3=0",
   "may rollup mz=0 gs=1 rr=1 g3=1",
   "keeps rollup read=none mz=0 gs=1 rr=1 g3=1",
   "keeps rollup read=colocated mz=0 gs=1 rr=1 g3=1",
   "keeps rollup read=storage mz=0 gs=1 rr=1 g3=1",
   "keeps rollup read=mixed mz=0 gs=1 rr=1 g3=1",
   "may rollup mz=1 gs=1 rr=0 g3=0",
   "keeps rollup read=none mz=1 gs=1 rr=0 g3=0",
   "keeps rollup read=colocated mz=1 gs=1 rr=0 g3=0",
   "keeps rollup read=storage mz=1 gs=1 rr=0 g3=0",
   "keeps rollup read=mixed mz=1 gs=1 rr=0 g3=0",
   "may rollup mz=1 gs=1 rr=0 g3=1",
   "keeps rollup read=none mz=1 gs=1 rr=0 g3=1",
   "keeps rollup read=colocated mz=1 gs=1 rr=0 g3=1",
   "keeps rollup read=storage mz=1 gs=1 rr=0 g3=1",
   "keeps rollup read=mixed mz=1 gs=1 rr=0 g3=1",
   "may rollup mz=1 gs=1 rr=1 g3=0",
   "keeps rollup read=none mz=1 gs=1 rr=1 g3=0",
   "keeps rollup read=colocated mz=1 gs=1 rr=1 g3=0",
   "keeps rollup read=storage mz=1 gs=1 rr=1 g3=0",
   "keeps rollup read=mixed mz=1 gs=1 rr=1 g3=0",
   "may rollup mz=1 gs=1 rr=1 g3=1",
   "keeps rollup read=none mz=1 gs=1 rr=1 g3=1",
   "keeps rollup read=colocated mz=1 gs=1 rr=1 g3=1",
   "keeps rollup read=storage mz=1 gs=1 rr=1 g3=1",
   "keeps rollup read=mixed mz=1 gs=1 rr=1 g3=1",
   "may served mz=0 gs=0 rr=0 g3=1",
   "keeps served read=colocated mz=0 gs=0 rr=0 g3=1",
   "may served mz=0 gs=0 rr=1 g3=1",
   "keeps served read=colocated mz=0 gs=0 rr=1 g3=1",
   "may served mz=0 gs=1 rr=0 g3=0",
   "keeps served read=none mz=0 gs=1 rr=0 g3=0",
   "keeps served read=colocated mz=0 gs=1 rr=0 g3=0",
   "keeps served read=storage mz=0 gs=1 rr=0 g3=0",
   "keeps served read=mixed mz=0 gs=1 rr=0 g3=0",
   "may served mz=0 gs=1 rr=0 g3=1",
   "keeps served read=none mz=0 gs=1 rr=0 g3=1",
   "keeps served read=colocated mz=0 gs=1 rr=0 g3=1",
   "keeps served read=storage mz=0 gs=1 rr=0 g3=1",
   "keeps served read=mixed mz=0 gs=1 rr=0 g3=1",
   "may served mz=0 gs=1 rr=1 g3=0",
   "keeps served read=none mz=0 gs=1 rr=1 g3=0",
   "keeps served read=colocated mz=0 gs=1 rr=1 g3=0",
   "keeps served read=storage mz=0 gs=1 rr=1 g3=0",
   "keeps served read=mixed mz=0 gs=1 rr=1 g3=0",
   "may served mz=0 gs=1 rr=1 g3=1",
   "keeps served read=none mz=0 gs=1 rr=1 g3=1",
   "keeps served read=colocated mz=0 gs=1 rr=1 g3=1",
   "keeps served read=storage mz=0 gs=1 rr=1 g3=1",
   "keeps served read=mixed mz=0 gs=1 rr=1 g3=1",
   "may served mz=1 gs=0 rr=0 g3=1",
   "keeps served read=colocated mz=1 gs=0 rr=0 g3=1",
   "may served mz=1 gs=0 rr=1 g3=1",
   "keeps served read=colocated mz=1 gs=0 rr=1 g3=1",
   "may served mz=1 gs=1 rr=0 g3=0",
   "keeps served read=none mz=1 gs=1 rr=0 g3=0",
   "keeps served read=colocated mz=1 gs=1 rr=0 g3=0",
   "keeps served read=storage mz=1 gs=1 rr=0 g3=0",
   "keeps served read=mixed mz=1 gs=1 rr=0 g3=0",
   "may served mz=1 gs=1 rr=0 g3=1",
   "keeps served read=none mz=1 gs=1 rr=0 g3=1",
   "keeps served read=colocated mz=1 gs=1 rr=0 g3=1",
   "keeps served read=storage mz=1 gs=1 rr=0 g3=1",
   "keeps served read=mixed mz=1 gs=1 rr=0 g3=1",
   "may served mz=1 gs=1 rr=1 g3=0",
   "keeps served read=none mz=1 gs=1 rr=1 g3=0",
   "keeps served read=colocated mz=1 gs=1 rr=1 g3=0",
   "keeps served read=storage mz=1 gs=1 rr=1 g3=0",
   "keeps served read=mixed mz=1 gs=1 rr=1 g3=0",
   "may served mz=1 gs=1 rr=1 g3=1",
   "keeps served read=none mz=1 gs=1 rr=1 g3=1",
   "keeps served read=colocated mz=1 gs=1 rr=1 g3=1",
   "keeps served read=storage mz=1 gs=1 rr=1 g3=1",
   "keeps served read=mixed mz=1 gs=1 rr=1 g3=1",
];

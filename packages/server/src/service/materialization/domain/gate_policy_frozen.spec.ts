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

import { writeFileSync } from "node:fs";
import { describe, expect, it } from "bun:test";
import {
   answerKeepsGates,
   GATE_KINDS,
   GATE_LEGS,
   GATE_SWITCH_KEYS,
   GATE_TIERS,
   type GatedTable,
   type GateSwitches,
   legMayPassGate,
   tableKeepsGates,
} from "./gate_policy";

const REVIEW =
   "The gate policy is frozen until the security review (plan question 14): " +
   "a leg, tier or kind that keeps its gates, or a row loosened, needs that " +
   "review's sign-off before this list changes.";

const KINDS = GATE_KINDS;
const TIERS = GATE_TIERS;
const LEGS = GATE_LEGS;
const BOOLS = [false, true] as const;

// Every input the policy can read is one this spec enumerates: a new kind,
// tier or leg is a new array value (iterated below), and a new switch or
// table field fails to compile here until the enumeration covers it.
type Exactly<A, B> = [A] extends [B] ? ([B] extends [A] ? true : never) : never;
const TABLE_FIELDS: Exactly<
   keyof GatedTable,
   "kind" | "tier" | "v1" | "gate" | "baseGateProven"
> = true;
const GATE_FIELDS: Exactly<
   keyof NonNullable<GatedTable["gate"]>,
   "rowLevel" | "attributed" | "inheritedRowFilter"
> = true;
const KIND_UNION: Exactly<GatedTable["kind"], (typeof GATE_KINDS)[number]> =
   true;
const TIER_UNION: Exactly<GatedTable["tier"], (typeof GATE_TIERS)[number]> =
   true;
const LEG_UNION: Exactly<
   Parameters<typeof legMayPassGate>[0],
   (typeof GATE_LEGS)[number]
> = true;
const SWITCH_KEYS: Exactly<keyof GateSwitches, keyof typeof GATE_SWITCH_KEYS> =
   true;
void [
   TABLE_FIELDS,
   GATE_FIELDS,
   KIND_UNION,
   TIER_UNION,
   LEG_UNION,
   SWITCH_KEYS,
];

/** Every setting of every switch, from the switch keys. */
function* switches(): Generator<GateSwitches> {
   const keys = Object.keys(GATE_SWITCH_KEYS) as (keyof GateSwitches)[];
   for (let bits = 0; bits < 1 << keys.length; bits++) {
      yield Object.fromEntries(
         keys.map((k, i) => [k, ((bits >> i) & 1) === 1]),
      ) as unknown as GateSwitches;
   }
}

const s = (x: GateSwitches) =>
   (Object.keys(GATE_SWITCH_KEYS) as (keyof GateSwitches)[])
      .map((k) => `${SHORT[k] ?? k}=${+x[k]}`)
      .join(" ");
const SHORT: Partial<Record<keyof GateSwitches, string>> = {
   materialize: "mz",
   gatedSources: "gs",
   gatedStorageRollupsReviewed: "rr",
};

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
   expect([...actual].sort()).toEqual([...frozen].sort());
}

const DUMP = process.env.GATE_POLICY_FROZEN_DUMP;

/** Writes the rows for a reviewed update, and fails: a dump is never a pass. */
function dumpAndFail(file: string, rows: string[]): never {
   writeFileSync(file, JSON.stringify(rows, null, 3));
   throw new Error(
      `Wrote ${rows.length} rows to ${file}. ${REVIEW} This run fails so a dump never passes.`,
   );
}

describe("the gate policy, frozen until the security review (plan question 14)", () => {
   it("keeps gates for exactly these tables", () => {
      if (DUMP) dumpAndFail(`${DUMP}.tables.json`, tableRows());
      expectFrozen(tableRows(), FROZEN_TABLE_ROWS);
   });

   it("lets exactly these legs past a gate, and keeps it for exactly these answers", () => {
      if (DUMP) dumpAndFail(`${DUMP}.legs.json`, legRows());
      expectFrozen(legRows(), FROZEN_LEG_ROWS);
   });
});

// The `materialize colocated` and `pair colocated` rows with `gs=0` are the
// one exception made outside the review, by maintainer decision: G3, which
// serves a v1 colocated table past its entry point's own row-level gate,
// applies to a colocated `#(materialize)` table too.
// prettier-ignore
const FROZEN_TABLE_ROWS: readonly string[] = [
   "materialize colocated v1=0 gate=110 proven=- mz=1 gs=0 rr=0",
   "materialize colocated v1=0 gate=110 proven=- mz=1 gs=0 rr=1",
   "materialize colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=0",
   "materialize colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=1",
   "materialize colocated v1=0 gate=110 proven=0 mz=1 gs=0 rr=0",
   "materialize colocated v1=0 gate=110 proven=0 mz=1 gs=0 rr=1",
   "materialize colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=0",
   "materialize colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=1",
   "materialize colocated v1=0 gate=110 proven=1 mz=1 gs=0 rr=0",
   "materialize colocated v1=0 gate=110 proven=1 mz=1 gs=0 rr=1",
   "materialize colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "materialize colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "materialize colocated v1=1 gate=110 proven=- mz=1 gs=0 rr=0",
   "materialize colocated v1=1 gate=110 proven=- mz=1 gs=0 rr=1",
   "materialize colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=0",
   "materialize colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=1",
   "materialize colocated v1=1 gate=110 proven=0 mz=1 gs=0 rr=0",
   "materialize colocated v1=1 gate=110 proven=0 mz=1 gs=0 rr=1",
   "materialize colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=0",
   "materialize colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=1",
   "materialize colocated v1=1 gate=110 proven=1 mz=1 gs=0 rr=0",
   "materialize colocated v1=1 gate=110 proven=1 mz=1 gs=0 rr=1",
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
   "pair colocated v1=0 gate=110 proven=- mz=1 gs=0 rr=0",
   "pair colocated v1=0 gate=110 proven=- mz=1 gs=0 rr=1",
   "pair colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=0",
   "pair colocated v1=0 gate=110 proven=- mz=1 gs=1 rr=1",
   "pair colocated v1=0 gate=110 proven=0 mz=1 gs=0 rr=0",
   "pair colocated v1=0 gate=110 proven=0 mz=1 gs=0 rr=1",
   "pair colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=0",
   "pair colocated v1=0 gate=110 proven=0 mz=1 gs=1 rr=1",
   "pair colocated v1=0 gate=110 proven=1 mz=1 gs=0 rr=0",
   "pair colocated v1=0 gate=110 proven=1 mz=1 gs=0 rr=1",
   "pair colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=0",
   "pair colocated v1=0 gate=110 proven=1 mz=1 gs=1 rr=1",
   "pair colocated v1=1 gate=110 proven=- mz=1 gs=0 rr=0",
   "pair colocated v1=1 gate=110 proven=- mz=1 gs=0 rr=1",
   "pair colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=0",
   "pair colocated v1=1 gate=110 proven=- mz=1 gs=1 rr=1",
   "pair colocated v1=1 gate=110 proven=0 mz=1 gs=0 rr=0",
   "pair colocated v1=1 gate=110 proven=0 mz=1 gs=0 rr=1",
   "pair colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=0",
   "pair colocated v1=1 gate=110 proven=0 mz=1 gs=1 rr=1",
   "pair colocated v1=1 gate=110 proven=1 mz=1 gs=0 rr=0",
   "pair colocated v1=1 gate=110 proven=1 mz=1 gs=0 rr=1",
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

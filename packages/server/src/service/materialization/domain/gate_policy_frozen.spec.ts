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
   type TableRead,
   tableKeepsGates,
} from "./gate_policy";
import {
   FROZEN_LEG_ROWS,
   GATE_READS,
   switchesLabel as s,
   switches,
} from "./gate_policy_frozen_rows";

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
   "rowLevel" | "attributed" | "inheritedRowFilter" | "lockOnly"
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
// A table an answer read: a new field fails to compile here until the reads
// below enumerate it.
const READ_FIELDS: Exactly<
   keyof TableRead,
   | "tier"
   | "ownGateKept"
   | "holdsOnlyUngatedRows"
   | "gateColumnTypes"
   | "gatesReadNoColumn"
> = true;
void [
   TABLE_FIELDS,
   GATE_FIELDS,
   KIND_UNION,
   TIER_UNION,
   LEG_UNION,
   SWITCH_KEYS,
   READ_FIELDS,
];

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
                     BOOLS.flatMap((inheritedRowFilter) =>
                        [undefined, ...BOOLS].map((lockOnly) => ({
                           rowLevel,
                           attributed,
                           inheritedRowFilter,
                           ...(lockOnly !== undefined ? { lockOnly } : {}),
                        })),
                     ),
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
                        ? `gate=${+gate.rowLevel}${+gate.attributed}${+gate.inheritedRowFilter}${gate.lockOnly === undefined ? "" : ` lock=${+gate.lockOnly}`}`
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
   const reads = GATE_READS;
   for (const leg of LEGS)
      for (const sw of switches())
         for (const g3 of BOOLS) {
            const ctx = { databaseReadsKeepGates: g3 };
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

   // F-14 (P4-03): a rollup with no `storage=` that a host instructed into a
   // destination is read from its storage binding (storageLookupOf), and the
   // rollup leg reports that binding as the table it read, so the answer is
   // judged as a `storage=` rollup's: under G3 alone it keeps no gate, and
   // the gated query is answered live. Not a row of its own: the
   // binding's tier puts it in the `read=storage` rows above.
   it("F-14: refuses, under G3, a rollup with no storage= read from a host's storage binding", () => {
      const hostChosen = { tier: "storage" as const };
      const g3Only: GateSwitches = {
         materialize: true,
         gatedSources: false,
      };
      expect(
         answerKeepsGates("rollup", [hostChosen], g3Only, {
            databaseReadsKeepGates: true,
         }),
      ).toBe(false);
      // The same rollup read database keeps its gates under G3.
      expect(
         answerKeepsGates("rollup", [{ tier: "database" }], g3Only, {
            databaseReadsKeepGates: true,
         }),
      ).toBe(true);
   });
});

// Rows added outside the review, by maintainer decision, for consistency
// ahead of it: G3, which serves a v1 database table past its entry point's
// own row-level gate, applies to a database `#(materialize)` table (the
// `materialize database` and `pair database` rows with `gs=0`), and on the
// same conditions to a `storage=` table: the `materialize storage` and
// `pair storage` rows with `gs=0`, and every `persist storage` row (v1, which
// reads no switch, as v1 database does not). The `persist database` rows are
// plan's database `#@ persist` rule since the v1 cutover, decided here rather
// than restated in plan; they add no table plan did not already admit. The legs' `keeps served
// read=own*` rows with `gs=0` are the router's half: an answer reading only
// G3 tables, database or `storage=` admitted past its own gate. A `storage=`
// table plan did not admit still keeps an answer off G3 (`read=own_storage`).
// prettier-ignore
const FROZEN_TABLE_ROWS: readonly string[] = [
   "persist database v1=0 gate=110 proven=- mz=0 gs=0",
   "persist database v1=0 gate=110 proven=- mz=1 gs=0",
   "persist database v1=0 gate=110 proven=- mz=0 gs=1",
   "persist database v1=0 gate=110 proven=- mz=1 gs=1",
   "persist database v1=0 gate=110 proven=0 mz=0 gs=0",
   "persist database v1=0 gate=110 proven=0 mz=1 gs=0",
   "persist database v1=0 gate=110 proven=0 mz=0 gs=1",
   "persist database v1=0 gate=110 proven=0 mz=1 gs=1",
   "persist database v1=0 gate=110 proven=1 mz=0 gs=0",
   "persist database v1=0 gate=110 proven=1 mz=1 gs=0",
   "persist database v1=0 gate=110 proven=1 mz=0 gs=1",
   "persist database v1=0 gate=110 proven=1 mz=1 gs=1",
   "persist database v1=1 gate=110 proven=- mz=0 gs=0",
   "persist database v1=1 gate=110 proven=- mz=1 gs=0",
   "persist database v1=1 gate=110 proven=- mz=0 gs=1",
   "persist database v1=1 gate=110 proven=- mz=1 gs=1",
   "persist database v1=1 gate=110 proven=0 mz=0 gs=0",
   "persist database v1=1 gate=110 proven=0 mz=1 gs=0",
   "persist database v1=1 gate=110 proven=0 mz=0 gs=1",
   "persist database v1=1 gate=110 proven=0 mz=1 gs=1",
   "persist database v1=1 gate=110 proven=1 mz=0 gs=0",
   "persist database v1=1 gate=110 proven=1 mz=1 gs=0",
   "persist database v1=1 gate=110 proven=1 mz=0 gs=1",
   "persist database v1=1 gate=110 proven=1 mz=1 gs=1",
   "persist storage v1=0 gate=110 proven=- mz=0 gs=0",
   "persist storage v1=0 gate=110 proven=- mz=1 gs=0",
   "persist storage v1=0 gate=110 proven=- mz=0 gs=1",
   "persist storage v1=0 gate=110 proven=- mz=1 gs=1",
   "persist storage v1=0 gate=110 proven=0 mz=0 gs=0",
   "persist storage v1=0 gate=110 proven=0 mz=1 gs=0",
   "persist storage v1=0 gate=110 proven=0 mz=0 gs=1",
   "persist storage v1=0 gate=110 proven=0 mz=1 gs=1",
   "persist storage v1=0 gate=110 proven=1 mz=0 gs=0",
   "persist storage v1=0 gate=110 proven=1 mz=1 gs=0",
   "persist storage v1=0 gate=110 proven=1 mz=0 gs=1",
   "persist storage v1=0 gate=110 proven=1 mz=1 gs=1",
   "persist storage v1=1 gate=110 proven=- mz=0 gs=0",
   "persist storage v1=1 gate=110 proven=- mz=1 gs=0",
   "persist storage v1=1 gate=110 proven=- mz=0 gs=1",
   "persist storage v1=1 gate=110 proven=- mz=1 gs=1",
   "persist storage v1=1 gate=110 proven=0 mz=0 gs=0",
   "persist storage v1=1 gate=110 proven=0 mz=1 gs=0",
   "persist storage v1=1 gate=110 proven=0 mz=0 gs=1",
   "persist storage v1=1 gate=110 proven=0 mz=1 gs=1",
   "persist storage v1=1 gate=110 proven=1 mz=0 gs=0",
   "persist storage v1=1 gate=110 proven=1 mz=1 gs=0",
   "persist storage v1=1 gate=110 proven=1 mz=0 gs=1",
   "persist storage v1=1 gate=110 proven=1 mz=1 gs=1",
   "materialize database v1=0 gate=110 proven=- mz=1 gs=0",
   "materialize database v1=0 gate=110 proven=- mz=1 gs=1",
   "materialize database v1=0 gate=110 proven=0 mz=1 gs=0",
   "materialize database v1=0 gate=110 proven=0 mz=1 gs=1",
   "materialize database v1=0 gate=110 proven=1 mz=1 gs=0",
   "materialize database v1=0 gate=110 proven=1 mz=1 gs=1",
   "materialize database v1=1 gate=110 proven=- mz=1 gs=0",
   "materialize database v1=1 gate=110 proven=- mz=1 gs=1",
   "materialize database v1=1 gate=110 proven=0 mz=1 gs=0",
   "materialize database v1=1 gate=110 proven=0 mz=1 gs=1",
   "materialize database v1=1 gate=110 proven=1 mz=1 gs=0",
   "materialize database v1=1 gate=110 proven=1 mz=1 gs=1",
   "materialize storage v1=0 gate=110 proven=- mz=1 gs=0",
   "materialize storage v1=0 gate=110 proven=- mz=1 gs=1",
   "materialize storage v1=0 gate=110 proven=0 mz=1 gs=0",
   "materialize storage v1=0 gate=110 proven=0 mz=1 gs=1",
   "materialize storage v1=0 gate=110 proven=1 mz=1 gs=0",
   "materialize storage v1=0 gate=110 proven=1 mz=1 gs=1",
   "materialize storage v1=1 gate=110 proven=- mz=1 gs=0",
   "materialize storage v1=1 gate=110 proven=- mz=1 gs=1",
   "materialize storage v1=1 gate=110 proven=0 mz=1 gs=0",
   "materialize storage v1=1 gate=110 proven=0 mz=1 gs=1",
   "materialize storage v1=1 gate=110 proven=1 mz=1 gs=0",
   "materialize storage v1=1 gate=110 proven=1 mz=1 gs=1",
   "pair database v1=0 gate=110 proven=- mz=1 gs=0",
   "pair database v1=0 gate=110 proven=- mz=1 gs=1",
   "pair database v1=0 gate=110 proven=0 mz=1 gs=0",
   "pair database v1=0 gate=110 proven=0 mz=1 gs=1",
   "pair database v1=0 gate=110 proven=1 mz=1 gs=0",
   "pair database v1=0 gate=110 proven=1 mz=1 gs=1",
   "pair database v1=1 gate=110 proven=- mz=1 gs=0",
   "pair database v1=1 gate=110 proven=- mz=1 gs=1",
   "pair database v1=1 gate=110 proven=0 mz=1 gs=0",
   "pair database v1=1 gate=110 proven=0 mz=1 gs=1",
   "pair database v1=1 gate=110 proven=1 mz=1 gs=0",
   "pair database v1=1 gate=110 proven=1 mz=1 gs=1",
   "pair storage v1=0 gate=110 proven=- mz=1 gs=0",
   "pair storage v1=0 gate=110 proven=- mz=1 gs=1",
   "pair storage v1=0 gate=110 proven=0 mz=1 gs=0",
   "pair storage v1=0 gate=110 proven=0 mz=1 gs=1",
   "pair storage v1=0 gate=110 proven=1 mz=1 gs=0",
   "pair storage v1=0 gate=110 proven=1 mz=1 gs=1",
   "pair storage v1=1 gate=110 proven=- mz=1 gs=0",
   "pair storage v1=1 gate=110 proven=- mz=1 gs=1",
   "pair storage v1=1 gate=110 proven=0 mz=1 gs=0",
   "pair storage v1=1 gate=110 proven=0 mz=1 gs=1",
   "pair storage v1=1 gate=110 proven=1 mz=1 gs=0",
   "pair storage v1=1 gate=110 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=- proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=- proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=000 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=000 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=001 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=001 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=010 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=010 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=011 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=011 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=100 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=100 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=101 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=101 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=110 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=110 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=111 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=111 proven=1 mz=1 gs=1",
   // G3 for a database rollup (maintainer decision, for consistency, ahead
   // of the review, which covers it): with the switch off, a database
   // rollup over a base whose gates are all its own (gate=110), each proven
   // exact on the rollup (proven=1). No switch is read, as for v1's G3.
   "rollup database v1=0 gate=110 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=110 proven=1 mz=1 gs=0",
   // The lock-only input (`lock=`), enumerated: where a row was already
   // kept without it, it is kept with either value.
   "persist database v1=0 gate=110 lock=0 proven=- mz=0 gs=0",
   "persist database v1=0 gate=110 lock=0 proven=- mz=1 gs=0",
   "persist database v1=0 gate=110 lock=0 proven=- mz=0 gs=1",
   "persist database v1=0 gate=110 lock=0 proven=- mz=1 gs=1",
   "persist database v1=0 gate=110 lock=0 proven=0 mz=0 gs=0",
   "persist database v1=0 gate=110 lock=0 proven=0 mz=1 gs=0",
   "persist database v1=0 gate=110 lock=0 proven=0 mz=0 gs=1",
   "persist database v1=0 gate=110 lock=0 proven=0 mz=1 gs=1",
   "persist database v1=0 gate=110 lock=0 proven=1 mz=0 gs=0",
   "persist database v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "persist database v1=0 gate=110 lock=0 proven=1 mz=0 gs=1",
   "persist database v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "persist database v1=0 gate=110 lock=1 proven=- mz=0 gs=0",
   "persist database v1=0 gate=110 lock=1 proven=- mz=1 gs=0",
   "persist database v1=0 gate=110 lock=1 proven=- mz=0 gs=1",
   "persist database v1=0 gate=110 lock=1 proven=- mz=1 gs=1",
   "persist database v1=0 gate=110 lock=1 proven=0 mz=0 gs=0",
   "persist database v1=0 gate=110 lock=1 proven=0 mz=1 gs=0",
   "persist database v1=0 gate=110 lock=1 proven=0 mz=0 gs=1",
   "persist database v1=0 gate=110 lock=1 proven=0 mz=1 gs=1",
   "persist database v1=0 gate=110 lock=1 proven=1 mz=0 gs=0",
   "persist database v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "persist database v1=0 gate=110 lock=1 proven=1 mz=0 gs=1",
   "persist database v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "persist database v1=1 gate=110 lock=0 proven=- mz=0 gs=0",
   "persist database v1=1 gate=110 lock=0 proven=- mz=1 gs=0",
   "persist database v1=1 gate=110 lock=0 proven=- mz=0 gs=1",
   "persist database v1=1 gate=110 lock=0 proven=- mz=1 gs=1",
   "persist database v1=1 gate=110 lock=0 proven=0 mz=0 gs=0",
   "persist database v1=1 gate=110 lock=0 proven=0 mz=1 gs=0",
   "persist database v1=1 gate=110 lock=0 proven=0 mz=0 gs=1",
   "persist database v1=1 gate=110 lock=0 proven=0 mz=1 gs=1",
   "persist database v1=1 gate=110 lock=0 proven=1 mz=0 gs=0",
   "persist database v1=1 gate=110 lock=0 proven=1 mz=1 gs=0",
   "persist database v1=1 gate=110 lock=0 proven=1 mz=0 gs=1",
   "persist database v1=1 gate=110 lock=0 proven=1 mz=1 gs=1",
   "persist database v1=1 gate=110 lock=1 proven=- mz=0 gs=0",
   "persist database v1=1 gate=110 lock=1 proven=- mz=1 gs=0",
   "persist database v1=1 gate=110 lock=1 proven=- mz=0 gs=1",
   "persist database v1=1 gate=110 lock=1 proven=- mz=1 gs=1",
   "persist database v1=1 gate=110 lock=1 proven=0 mz=0 gs=0",
   "persist database v1=1 gate=110 lock=1 proven=0 mz=1 gs=0",
   "persist database v1=1 gate=110 lock=1 proven=0 mz=0 gs=1",
   "persist database v1=1 gate=110 lock=1 proven=0 mz=1 gs=1",
   "persist database v1=1 gate=110 lock=1 proven=1 mz=0 gs=0",
   "persist database v1=1 gate=110 lock=1 proven=1 mz=1 gs=0",
   "persist database v1=1 gate=110 lock=1 proven=1 mz=0 gs=1",
   "persist database v1=1 gate=110 lock=1 proven=1 mz=1 gs=1",
   "persist storage v1=0 gate=110 lock=0 proven=- mz=0 gs=0",
   "persist storage v1=0 gate=110 lock=0 proven=- mz=1 gs=0",
   "persist storage v1=0 gate=110 lock=0 proven=- mz=0 gs=1",
   "persist storage v1=0 gate=110 lock=0 proven=- mz=1 gs=1",
   "persist storage v1=0 gate=110 lock=0 proven=0 mz=0 gs=0",
   "persist storage v1=0 gate=110 lock=0 proven=0 mz=1 gs=0",
   "persist storage v1=0 gate=110 lock=0 proven=0 mz=0 gs=1",
   "persist storage v1=0 gate=110 lock=0 proven=0 mz=1 gs=1",
   "persist storage v1=0 gate=110 lock=0 proven=1 mz=0 gs=0",
   "persist storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "persist storage v1=0 gate=110 lock=0 proven=1 mz=0 gs=1",
   "persist storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "persist storage v1=0 gate=110 lock=1 proven=- mz=0 gs=0",
   "persist storage v1=0 gate=110 lock=1 proven=- mz=1 gs=0",
   "persist storage v1=0 gate=110 lock=1 proven=- mz=0 gs=1",
   "persist storage v1=0 gate=110 lock=1 proven=- mz=1 gs=1",
   "persist storage v1=0 gate=110 lock=1 proven=0 mz=0 gs=0",
   "persist storage v1=0 gate=110 lock=1 proven=0 mz=1 gs=0",
   "persist storage v1=0 gate=110 lock=1 proven=0 mz=0 gs=1",
   "persist storage v1=0 gate=110 lock=1 proven=0 mz=1 gs=1",
   "persist storage v1=0 gate=110 lock=1 proven=1 mz=0 gs=0",
   "persist storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "persist storage v1=0 gate=110 lock=1 proven=1 mz=0 gs=1",
   "persist storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "persist storage v1=1 gate=110 lock=0 proven=- mz=0 gs=0",
   "persist storage v1=1 gate=110 lock=0 proven=- mz=1 gs=0",
   "persist storage v1=1 gate=110 lock=0 proven=- mz=0 gs=1",
   "persist storage v1=1 gate=110 lock=0 proven=- mz=1 gs=1",
   "persist storage v1=1 gate=110 lock=0 proven=0 mz=0 gs=0",
   "persist storage v1=1 gate=110 lock=0 proven=0 mz=1 gs=0",
   "persist storage v1=1 gate=110 lock=0 proven=0 mz=0 gs=1",
   "persist storage v1=1 gate=110 lock=0 proven=0 mz=1 gs=1",
   "persist storage v1=1 gate=110 lock=0 proven=1 mz=0 gs=0",
   "persist storage v1=1 gate=110 lock=0 proven=1 mz=1 gs=0",
   "persist storage v1=1 gate=110 lock=0 proven=1 mz=0 gs=1",
   "persist storage v1=1 gate=110 lock=0 proven=1 mz=1 gs=1",
   "persist storage v1=1 gate=110 lock=1 proven=- mz=0 gs=0",
   "persist storage v1=1 gate=110 lock=1 proven=- mz=1 gs=0",
   "persist storage v1=1 gate=110 lock=1 proven=- mz=0 gs=1",
   "persist storage v1=1 gate=110 lock=1 proven=- mz=1 gs=1",
   "persist storage v1=1 gate=110 lock=1 proven=0 mz=0 gs=0",
   "persist storage v1=1 gate=110 lock=1 proven=0 mz=1 gs=0",
   "persist storage v1=1 gate=110 lock=1 proven=0 mz=0 gs=1",
   "persist storage v1=1 gate=110 lock=1 proven=0 mz=1 gs=1",
   "persist storage v1=1 gate=110 lock=1 proven=1 mz=0 gs=0",
   "persist storage v1=1 gate=110 lock=1 proven=1 mz=1 gs=0",
   "persist storage v1=1 gate=110 lock=1 proven=1 mz=0 gs=1",
   "persist storage v1=1 gate=110 lock=1 proven=1 mz=1 gs=1",
   "materialize database v1=0 gate=110 lock=0 proven=- mz=1 gs=0",
   "materialize database v1=0 gate=110 lock=0 proven=- mz=1 gs=1",
   "materialize database v1=0 gate=110 lock=0 proven=0 mz=1 gs=0",
   "materialize database v1=0 gate=110 lock=0 proven=0 mz=1 gs=1",
   "materialize database v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "materialize database v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "materialize database v1=0 gate=110 lock=1 proven=- mz=1 gs=0",
   "materialize database v1=0 gate=110 lock=1 proven=- mz=1 gs=1",
   "materialize database v1=0 gate=110 lock=1 proven=0 mz=1 gs=0",
   "materialize database v1=0 gate=110 lock=1 proven=0 mz=1 gs=1",
   "materialize database v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "materialize database v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "materialize database v1=1 gate=110 lock=0 proven=- mz=1 gs=0",
   "materialize database v1=1 gate=110 lock=0 proven=- mz=1 gs=1",
   "materialize database v1=1 gate=110 lock=0 proven=0 mz=1 gs=0",
   "materialize database v1=1 gate=110 lock=0 proven=0 mz=1 gs=1",
   "materialize database v1=1 gate=110 lock=0 proven=1 mz=1 gs=0",
   "materialize database v1=1 gate=110 lock=0 proven=1 mz=1 gs=1",
   "materialize database v1=1 gate=110 lock=1 proven=- mz=1 gs=0",
   "materialize database v1=1 gate=110 lock=1 proven=- mz=1 gs=1",
   "materialize database v1=1 gate=110 lock=1 proven=0 mz=1 gs=0",
   "materialize database v1=1 gate=110 lock=1 proven=0 mz=1 gs=1",
   "materialize database v1=1 gate=110 lock=1 proven=1 mz=1 gs=0",
   "materialize database v1=1 gate=110 lock=1 proven=1 mz=1 gs=1",
   "materialize storage v1=0 gate=110 lock=0 proven=- mz=1 gs=0",
   "materialize storage v1=0 gate=110 lock=0 proven=- mz=1 gs=1",
   "materialize storage v1=0 gate=110 lock=0 proven=0 mz=1 gs=0",
   "materialize storage v1=0 gate=110 lock=0 proven=0 mz=1 gs=1",
   "materialize storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "materialize storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "materialize storage v1=0 gate=110 lock=1 proven=- mz=1 gs=0",
   "materialize storage v1=0 gate=110 lock=1 proven=- mz=1 gs=1",
   "materialize storage v1=0 gate=110 lock=1 proven=0 mz=1 gs=0",
   "materialize storage v1=0 gate=110 lock=1 proven=0 mz=1 gs=1",
   "materialize storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "materialize storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "materialize storage v1=1 gate=110 lock=0 proven=- mz=1 gs=0",
   "materialize storage v1=1 gate=110 lock=0 proven=- mz=1 gs=1",
   "materialize storage v1=1 gate=110 lock=0 proven=0 mz=1 gs=0",
   "materialize storage v1=1 gate=110 lock=0 proven=0 mz=1 gs=1",
   "materialize storage v1=1 gate=110 lock=0 proven=1 mz=1 gs=0",
   "materialize storage v1=1 gate=110 lock=0 proven=1 mz=1 gs=1",
   "materialize storage v1=1 gate=110 lock=1 proven=- mz=1 gs=0",
   "materialize storage v1=1 gate=110 lock=1 proven=- mz=1 gs=1",
   "materialize storage v1=1 gate=110 lock=1 proven=0 mz=1 gs=0",
   "materialize storage v1=1 gate=110 lock=1 proven=0 mz=1 gs=1",
   "materialize storage v1=1 gate=110 lock=1 proven=1 mz=1 gs=0",
   "materialize storage v1=1 gate=110 lock=1 proven=1 mz=1 gs=1",
   "pair database v1=0 gate=110 lock=0 proven=- mz=1 gs=0",
   "pair database v1=0 gate=110 lock=0 proven=- mz=1 gs=1",
   "pair database v1=0 gate=110 lock=0 proven=0 mz=1 gs=0",
   "pair database v1=0 gate=110 lock=0 proven=0 mz=1 gs=1",
   "pair database v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "pair database v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "pair database v1=0 gate=110 lock=1 proven=- mz=1 gs=0",
   "pair database v1=0 gate=110 lock=1 proven=- mz=1 gs=1",
   "pair database v1=0 gate=110 lock=1 proven=0 mz=1 gs=0",
   "pair database v1=0 gate=110 lock=1 proven=0 mz=1 gs=1",
   "pair database v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "pair database v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "pair database v1=1 gate=110 lock=0 proven=- mz=1 gs=0",
   "pair database v1=1 gate=110 lock=0 proven=- mz=1 gs=1",
   "pair database v1=1 gate=110 lock=0 proven=0 mz=1 gs=0",
   "pair database v1=1 gate=110 lock=0 proven=0 mz=1 gs=1",
   "pair database v1=1 gate=110 lock=0 proven=1 mz=1 gs=0",
   "pair database v1=1 gate=110 lock=0 proven=1 mz=1 gs=1",
   "pair database v1=1 gate=110 lock=1 proven=- mz=1 gs=0",
   "pair database v1=1 gate=110 lock=1 proven=- mz=1 gs=1",
   "pair database v1=1 gate=110 lock=1 proven=0 mz=1 gs=0",
   "pair database v1=1 gate=110 lock=1 proven=0 mz=1 gs=1",
   "pair database v1=1 gate=110 lock=1 proven=1 mz=1 gs=0",
   "pair database v1=1 gate=110 lock=1 proven=1 mz=1 gs=1",
   "pair storage v1=0 gate=110 lock=0 proven=- mz=1 gs=0",
   "pair storage v1=0 gate=110 lock=0 proven=- mz=1 gs=1",
   "pair storage v1=0 gate=110 lock=0 proven=0 mz=1 gs=0",
   "pair storage v1=0 gate=110 lock=0 proven=0 mz=1 gs=1",
   "pair storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "pair storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "pair storage v1=0 gate=110 lock=1 proven=- mz=1 gs=0",
   "pair storage v1=0 gate=110 lock=1 proven=- mz=1 gs=1",
   "pair storage v1=0 gate=110 lock=1 proven=0 mz=1 gs=0",
   "pair storage v1=0 gate=110 lock=1 proven=0 mz=1 gs=1",
   "pair storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "pair storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "pair storage v1=1 gate=110 lock=0 proven=- mz=1 gs=0",
   "pair storage v1=1 gate=110 lock=0 proven=- mz=1 gs=1",
   "pair storage v1=1 gate=110 lock=0 proven=0 mz=1 gs=0",
   "pair storage v1=1 gate=110 lock=0 proven=0 mz=1 gs=1",
   "pair storage v1=1 gate=110 lock=0 proven=1 mz=1 gs=0",
   "pair storage v1=1 gate=110 lock=0 proven=1 mz=1 gs=1",
   "pair storage v1=1 gate=110 lock=1 proven=- mz=1 gs=0",
   "pair storage v1=1 gate=110 lock=1 proven=- mz=1 gs=1",
   "pair storage v1=1 gate=110 lock=1 proven=0 mz=1 gs=0",
   "pair storage v1=1 gate=110 lock=1 proven=0 mz=1 gs=1",
   "pair storage v1=1 gate=110 lock=1 proven=1 mz=1 gs=0",
   "pair storage v1=1 gate=110 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=000 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=000 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=000 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=000 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=001 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=001 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=001 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=001 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=010 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=010 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=010 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=010 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=011 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=011 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=011 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=011 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=100 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=100 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=100 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=100 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=101 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=101 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=101 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=101 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=110 lock=0 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=110 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=110 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=110 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=111 lock=0 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=111 lock=0 proven=1 mz=1 gs=1",
   "rollup database v1=0 gate=111 lock=1 proven=1 mz=0 gs=1",
   "rollup database v1=0 gate=111 lock=1 proven=1 mz=1 gs=1",
   // Pending the question-14 review's sign-off (g-inh-lock): a rollup
   // whose base gates are all locks reading no row field (lock=1), own
   // or inherited, on either tier, under every switch.
   "rollup database v1=0 gate=000 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=000 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=001 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=001 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=010 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=010 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=011 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=011 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=100 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=100 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=101 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=101 lock=1 proven=1 mz=1 gs=0",
   "rollup database v1=0 gate=111 lock=1 proven=1 mz=0 gs=0",
   "rollup database v1=0 gate=111 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=000 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=000 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=000 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=000 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=001 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=001 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=001 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=001 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=010 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=010 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=010 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=010 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=011 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=011 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=011 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=011 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=100 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=100 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=100 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=100 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=101 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=101 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=101 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=101 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=110 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=110 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=110 lock=1 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=111 lock=1 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=111 lock=1 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=111 lock=1 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=111 lock=1 proven=1 mz=1 gs=1",
   // Pending the question-14 review's sign-off (P3/P4): a `storage=` rollup
   // keeps its base's gates on the database terms, its own under G3 (gs=0,
   // gate=110) and any proven one under the switch (gs=1). The review seam
   // (`rr`) is removed.
   "rollup storage v1=0 gate=- proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=- proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=000 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=000 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=000 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=000 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=001 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=001 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=001 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=001 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=010 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=010 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=010 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=010 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=011 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=011 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=011 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=011 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=100 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=100 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=100 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=100 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=101 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=101 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=101 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=101 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=110 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=110 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=110 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=110 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=110 lock=0 proven=1 mz=0 gs=0",
   "rollup storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=0",
   "rollup storage v1=0 gate=110 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=110 lock=0 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=111 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=111 proven=1 mz=1 gs=1",
   "rollup storage v1=0 gate=111 lock=0 proven=1 mz=0 gs=1",
   "rollup storage v1=0 gate=111 lock=0 proven=1 mz=1 gs=1",
];

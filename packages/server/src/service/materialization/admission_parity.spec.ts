// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The serve verdict and the router judge a stored table by one rule. The
 * verdict asks `keepsGatesWhenReadExact` of a source's own table before any
 * query; the router asks `admitRead` of each answer. Over every leg, every
 * switch setting and a table of stored-table shapes, read with exact facts
 * (every probe answers that the table compares as its source does, no
 * composite row filter), the two agree: the verdict never says a table keeps
 * its gates where `admitRead` declines it, nor the reverse.
 */

import { describe, expect, it } from "bun:test";
import {
   admitRead,
   keepsGatesWhenReadExact,
   type ReadFacts,
} from "./domain/admission";
import type { GateSwitches, TableRead } from "./domain/gate_policy";
import { LEGS, TIERS } from "./domain/vocabulary";

const SWITCHES: GateSwitches[] = [false, true].flatMap((materialize) =>
   [false, true].map((gatedSources) => ({ materialize, gatedSources })),
);

const COLUMN_TYPES: (readonly string[] | undefined)[] = [
   undefined,
   [],
   ["VARCHAR"],
   ["BIGINT"],
   ["DOUBLE"],
   ["HUGEINT"],
   ["TIMESTAMP WITH TIME ZONE"],
   ["DATE", "VARCHAR"],
];

/** Every stored-table shape the gate policy and the zone rule read. */
const TABLES: TableRead[] = TIERS.flatMap((tier) =>
   [false, true].flatMap((ownGateKept) =>
      [false, true].flatMap((holdsOnlyUngatedRows) =>
         [false, true].flatMap((gatesReadNoColumn) =>
            COLUMN_TYPES.map((gateColumnTypes) => ({
               tier,
               ownGateKept,
               holdsOnlyUngatedRows,
               gatesReadNoColumn,
               ...(gateColumnTypes ? { gateColumnTypes } : {}),
            })),
         ),
      ),
   ),
);

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

describe("the serve verdict and admitRead agree on a gated stored read", () => {
   it("over every leg, switch setting and table shape", () => {
      const disagreements: string[] = [];
      let cases = 0;
      for (const leg of LEGS) {
         for (const s of SWITCHES) {
            for (const table of TABLES) {
               cases++;
               const keeps = keepsGatesWhenReadExact(leg, [table], s);
               const verdict = admitRead(leg, [table], EXACT, s);
               // An exact read asks for no probe.
               expect(verdict.admit).not.toBe("needs_probe");
               if (keeps !== (verdict.admit === true)) {
                  disagreements.push(
                     JSON.stringify({ leg, s, table, keeps, verdict }),
                  );
               }
            }
         }
      }
      expect(cases).toBeGreaterThan(500);
      expect(disagreements).toEqual([]);
   });

   it("both admit some shape and decline another, so the agreement is not vacuous", () => {
      const s = { materialize: true, gatedSources: true };
      const outcomes = new Set(
         TABLES.map((t) => keepsGatesWhenReadExact("served", [t], s)),
      );
      expect([...outcomes].sort()).toEqual([false, true]);
   });
});

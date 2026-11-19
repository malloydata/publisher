// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The serve-time backstop over a table stored as of its build: an answer
 * that reads one is declined, whatever else holds, when the query is an
 * access decision (past an entry-point gate, or reading a given), and when
 * that is not known (fail closed). Any other query reads it as before.
 */

import { describe, expect, it } from "bun:test";
import { admitRead, type ReadFacts } from "./domain/admission";
import { gateSwitches } from "../../config";

const OPEN: ReadFacts = {
   pastEntryPointGate: false,
   compositeRowFilter: false,
   reads: undefined,
   columnsListed: false,
};
const DATABASE = [{ tier: "database" as const }];

describe("admitRead over a table stored as of its build", () => {
   for (const leg of ["served", "rollup", "view", "query"] as const) {
      it(`declines an access decision's ${leg} answer, and admits any other`, () => {
         const s = gateSwitches();
         for (const facts of [
            { ...OPEN, readsAsOfBuild: true, readsAccess: true },
            { ...OPEN, readsAsOfBuild: true },
            { ...OPEN, readsAsOfBuild: true, pastEntryPointGate: true },
         ]) {
            expect(admitRead(leg, DATABASE, facts, s)).toMatchObject({
               admit: false,
               reason: "as_of_build_access",
            });
         }
         expect(
            admitRead(
               leg,
               DATABASE,
               { ...OPEN, readsAsOfBuild: true, readsAccess: false },
               s,
            ),
         ).toEqual({ admit: true, pastEntryPointGate: false });
         expect(
            admitRead(leg, DATABASE, { ...OPEN, readsAccess: true }, s),
         ).toEqual({ admit: true, pastEntryPointGate: false });
      });
   }
});

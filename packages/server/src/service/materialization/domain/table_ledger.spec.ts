// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   dropVerdict,
   isTransientGuard,
   keptReasonOf,
   type RenameFacts,
   type RenameGuard,
   renameVerdict,
} from "./table_ledger";

/** Every fact in the direction that lets the old table go. */
const DROPPABLE: RenameFacts = {
   replacementCommitted: true,
   replacementServed: true,
   builtByPackage: true,
   servedThere: false,
   servedByLeaf: false,
   referencedElsewhere: false,
   claimedByActiveRun: false,
   publisherNamed: true,
};

describe("renameVerdict", () => {
   it("drops a replaced table only when every fact lets it go", () => {
      expect(renameVerdict(DROPPABLE)).toBeUndefined();
   });

   const rows: [Partial<RenameFacts>, RenameGuard][] = [
      [{ replacementCommitted: false }, "replacement-not-committed"],
      [{ replacementServed: false }, "not-serving-replacement"],
      [{ builtByPackage: false }, "not-in-package-records"],
      [{ servedThere: true, servedByLeaf: true }, "still-served"],
      [{ servedByLeaf: true }, "may_be_served"],
      [{ referencedElsewhere: true }, "still-referenced"],
      [{ claimedByActiveRun: true }, "still-referenced"],
      [{ publisherNamed: false }, "author_name"],
   ];
   for (const [facts, guard] of rows) {
      it(`keeps it: ${guard} (${Object.keys(facts).join(", ")})`, () => {
         expect(renameVerdict({ ...DROPPABLE, ...facts })).toBe(guard);
      });
   }

   it("asks the rules in order: the first that keeps names the guard", () => {
      const keepAll: RenameFacts = {
         replacementCommitted: false,
         replacementServed: false,
         builtByPackage: false,
         servedThere: true,
         servedByLeaf: true,
         referencedElsewhere: true,
         claimedByActiveRun: true,
         publisherNamed: false,
      };
      const order: RenameGuard[] = [];
      let facts = keepAll;
      const lift: (keyof RenameFacts)[][] = [
         ["replacementCommitted"],
         ["replacementServed"],
         ["builtByPackage"],
         ["servedThere"],
         ["servedByLeaf"],
         ["referencedElsewhere", "claimedByActiveRun"],
         ["publisherNamed"],
      ];
      for (const keys of lift) {
         order.push(renameVerdict(facts)!);
         facts = {
            ...facts,
            ...Object.fromEntries(keys.map((k) => [k, DROPPABLE[k]])),
         };
      }
      expect(order).toEqual([
         "replacement-not-committed",
         "not-serving-replacement",
         "not-in-package-records",
         "still-served",
         "may_be_served",
         "still-referenced",
         "author_name",
      ]);
      expect(renameVerdict(facts)).toBeUndefined();
   });

   it("retries only the guards a later run may find passing, and lists only two", () => {
      const guards: RenameGuard[] = [
         "replacement-not-committed",
         "not-serving-replacement",
         "not-in-package-records",
         "still-served",
         "may_be_served",
         "still-referenced",
         "author_name",
      ];
      expect(
         Object.fromEntries(
            guards.map((g) => [g, [isTransientGuard(g), keptReasonOf(g)]]),
         ),
      ).toEqual({
         "replacement-not-committed": [true, undefined],
         "not-serving-replacement": [true, undefined],
         "not-in-package-records": [false, undefined],
         "still-served": [false, undefined],
         may_be_served: [false, "may_be_served"],
         "still-referenced": [true, undefined],
         author_name: [false, "author_name"],
      });
   });
});

describe("dropVerdict", () => {
   it("keeps a table a committed run names there, then one it names anywhere", () => {
      expect(
         dropVerdict({ referencedThere: true, referencedByLeaf: true }),
      ).toBe("still_referenced");
      expect(
         dropVerdict({ referencedThere: false, referencedByLeaf: true }),
      ).toBe("may_be_served");
      expect(
         dropVerdict({ referencedThere: false, referencedByLeaf: false }),
      ).toBe("drop");
   });
});

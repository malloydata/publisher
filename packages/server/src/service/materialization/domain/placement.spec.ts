// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   readTier,
   servedCopyReadsTier,
   storageServes,
   storageWritable,
} from "./placement";

describe("the storage tier under each PERSIST_STORAGE_MODE", () => {
   it("is written under write-only and on, and read only under on", () => {
      expect(
         (["off", "write-only", "on"] as const).map((mode) => [
            mode,
            storageWritable(mode),
            storageServes(mode),
         ]),
      ).toEqual([
         ["off", false, false],
         ["write-only", true, false],
         ["on", true, true],
      ]);
   });
});

describe("the tier a query reads a source bound in both tiers in", () => {
   it("the served copy re-roots on the storage table over the database one", () => {
      expect(servedCopyReadsTier("storage", ["database", "storage"])).toBe(
         true,
      );
      expect(servedCopyReadsTier("database", ["database", "storage"])).toBe(
         false,
      );
      expect(servedCopyReadsTier("database", ["database"])).toBe(true);
   });

   it("is the tier the served copy reads, for each freshness and mode", () => {
      const both = {
         storageBound: true,
         databaseBound: true,
         servedCopy: true,
         declaredStorage: false,
      };
      const rows = (
         [
            [true, true],
            [true, false],
            [false, true],
            [false, false],
         ] as const
      ).flatMap(([storageFresh, databaseFresh]) =>
         [true, false].map((storageServed) => [
            `storage ${storageFresh ? "fresh" : "stale"}, database ${databaseFresh ? "fresh" : "stale"}, ${storageServed ? "on" : "not on"}`,
            readTier({ ...both, storageFresh, databaseFresh, storageServed }),
         ]),
      );
      expect(Object.fromEntries(rows)).toEqual({
         "storage fresh, database fresh, on": "storage",
         "storage fresh, database fresh, not on": "database",
         "storage fresh, database stale, on": "storage",
         "storage fresh, database stale, not on": "database",
         "storage stale, database fresh, on": "database",
         "storage stale, database fresh, not on": "database",
         "storage stale, database stale, on": "storage",
         "storage stale, database stale, not on": "database",
      });
   });

   it("is the storage binding's for a leg that reads it alone, whatever the mode", () => {
      for (const storageServed of [true, false]) {
         expect(
            readTier({
               storageBound: true,
               storageFresh: true,
               storageServed,
               databaseBound: true,
               databaseFresh: true,
               servedCopy: false,
               declaredStorage: false,
            }),
         ).toBe("storage");
      }
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Which names share one stored table.

import { describe, expect, it } from "bun:test";

import { groupAliasesByName } from "./source_aliases";

describe("groupAliasesByName", () => {
   const plan = (
      ...pairs: [name: string, address: string][]
   ): { name?: string; sourceEntityId?: string }[] =>
      pairs.map(([name, sourceEntityId]) => ({ name, sourceEntityId }));

   it("groups the sources that share one address, keyed by each of their names", () => {
      const byName = groupAliasesByName(
         plan(["daily", "X"], ["daily_with_avg", "X"], ["monthly", "Y"]),
      );
      expect(byName["daily"]).toEqual(["daily", "daily_with_avg"]);
      expect(byName["daily_with_avg"]).toEqual(["daily", "daily_with_avg"]);
      expect(byName["monthly"]).toEqual(["monthly"]);
   });

   it("drops a name declared at more than one address", () => {
      // Source names are not unique in a package — the wire plan is keyed by
      // sourceID for exactly this reason. `ext` here could mean either table, so
      // it cannot be resolved by name and must alias nothing; picking one by map
      // order would eventually bind one name to two tables, put two
      // `source: ext` declarations in one serve shape, and take the storage tier
      // out for every model in the package.
      const byName = groupAliasesByName(
         plan(["daily", "X"], ["ext", "X"], ["other", "Z"], ["ext", "Z"]),
      );
      expect(byName["ext"]).toBeUndefined();
      // The unambiguous names keep their (now narrower) groups.
      expect(byName["daily"]).toEqual(["daily"]);
      expect(byName["other"]).toEqual(["other"]);
   });

   it("is independent of the order sources arrive in", () => {
      const forward = groupAliasesByName(
         plan(["daily", "X"], ["ext", "X"], ["other", "Z"], ["ext", "Z"]),
      );
      const reversed = groupAliasesByName(
         plan(["ext", "Z"], ["other", "Z"], ["ext", "X"], ["daily", "X"]),
      );
      expect(reversed).toEqual(forward);
   });

   it("skips sources missing a name or an address", () => {
      const byName = groupAliasesByName([
         { name: "daily", sourceEntityId: "X" },
         { name: undefined, sourceEntityId: "X" },
         { name: "nameless", sourceEntityId: undefined },
      ]);
      expect(byName).toEqual({ daily: ["daily"] });
   });
});

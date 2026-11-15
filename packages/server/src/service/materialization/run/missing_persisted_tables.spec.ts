// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A chained build's stored upstreams that no binding in its destination holds.

import { describe, expect, it } from "bun:test";

import { missingPersistedTables } from "./storage_build_strategy";

describe("missingPersistedTables", () => {
   const groups = {
      daily: ["daily", "daily_regional", "renamed"],
      daily_regional: ["daily", "daily_regional", "renamed"],
      renamed: ["daily", "daily_regional", "renamed"],
   };
   it("a name's table is present when any name sharing it is bound", () => {
      expect(
         missingPersistedTables(["daily_regional"], groups, new Set(["daily"])),
      ).toEqual([]);
   });
   it("and missing when none is", () => {
      expect(
         missingPersistedTables(["daily_regional", "sites"], groups, new Set()),
      ).toEqual(["daily_regional", "sites"]);
   });
   it("a name in no group is its own table", () => {
      expect(
         missingPersistedTables(["sites"], groups, new Set(["sites"])),
      ).toEqual([]);
   });
});

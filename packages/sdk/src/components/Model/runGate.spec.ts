// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { missingGivensHint, runGate } from "./runGate";

const values = (entries: Record<string, GivenValue>) =>
   new Map<string, GivenValue>(Object.entries(entries));

describe("runGate", () => {
   it("reports nothing with no declared givens", () => {
      expect(runGate([], values({}))).toEqual({ missing: [] });
   });

   it("reports nothing when every given is set", () => {
      const givens: Given[] = [{ name: "TENANT" }, { name: "REGION" }];
      expect(runGate(givens, values({ TENANT: "acme", REGION: "us" }))).toEqual(
         { missing: [] },
      );
   });

   it("lists unset givens that have no default", () => {
      const givens: Given[] = [{ name: "TENANT" }, { name: "REGION" }];
      expect(runGate(givens, values({})).missing).toEqual(["TENANT", "REGION"]);
   });

   it("treats a null default as no default", () => {
      // The API sends `default: null` for some types rather than omitting it.
      const givens = [
         { name: "TENANTS", type: "string[]", default: null },
      ] as unknown as Given[];
      expect(runGate(givens, values({}))).toEqual({ missing: ["TENANTS"] });
   });

   it("notes a default that an unset given fell back to", () => {
      const givens: Given[] = [{ name: "X", default: "5" }];
      expect(runGate(givens, values({}))).toEqual({
         missing: [],
         defaultsNote: "Ran with the default X = 5",
      });
   });

   it("names only the defaulted givens when some are set and some default", () => {
      const givens: Given[] = [
         { name: "X", default: "5" },
         { name: "Y", default: "'a'" },
         { name: "Z" },
      ];
      expect(runGate(givens, values({ Z: "set" }))).toEqual({
         missing: [],
         defaultsNote: "Ran with defaults X = 5, Y = a",
      });
   });

   it("spells defaults as the panel does and leaves empty filters out", () => {
      const givens: Given[] = [
         { name: "CATEGORY", type: "filter<string>", default: "f''" },
         { name: "SINCE", type: "date", default: "@2023-01-01" },
      ];
      expect(runGate(givens, values({})).defaultsNote).toBe(
         "Ran with the default SINCE = 2023-01-01",
      );
   });

   it("adds no note when every unset default is an empty filter", () => {
      const givens: Given[] = [
         { name: "CATEGORY", type: "filter<string>", default: "f''" },
      ];
      expect(runGate(givens, values({}))).toEqual({ missing: [] });
   });

   it("counts an empty string as set, since it is sent rather than defaulted", () => {
      const givens: Given[] = [
         { name: "TENANT" },
         { name: "REGION", type: "filter<string>", default: "f'West'" },
      ];
      expect(runGate(givens, values({ TENANT: "", REGION: "" }))).toEqual({
         missing: [],
      });
   });

   it("skips a spec with no name", () => {
      const givens: Given[] = [{ type: "string" }];
      expect(runGate(givens, values({}))).toEqual({ missing: [] });
   });
});

describe("missingGivensHint", () => {
   it("uses the singular wording for one given", () => {
      expect(missingGivensHint(["TENANT"])).toBe(
         "This source may need a value for the given TENANT. Set it in the parameters above.",
      );
   });

   it("uses the plural wording for several", () => {
      expect(missingGivensHint(["TENANT", "REGION"])).toBe(
         "This source may need values for the givens TENANT, REGION. Set them in the parameters above.",
      );
   });
});

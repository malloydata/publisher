// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { runGate } from "./runGate";

const values = (entries: Record<string, GivenValue>) =>
   new Map<string, GivenValue>(Object.entries(entries));

describe("runGate", () => {
   it("is ok with no declared givens", () => {
      expect(runGate([], values({}))).toEqual({ kind: "ok" });
   });

   it("is ok when every given is set", () => {
      const givens: Given[] = [{ name: "TENANT" }, { name: "REGION" }];
      expect(runGate(givens, values({ TENANT: "acme", REGION: "us" }))).toEqual(
         { kind: "ok" },
      );
   });

   it("blocks with the exact singular wording for one missing given", () => {
      const givens: Given[] = [{ name: "TENANT" }];
      expect(runGate(givens, values({}))).toEqual({
         kind: "blocked",
         reason:
            "This needs a value for the given TENANT. Set it in the parameters above.",
      });
   });

   it("blocks with the exact plural wording for two missing givens", () => {
      const givens: Given[] = [{ name: "TENANT" }, { name: "REGION" }];
      expect(runGate(givens, values({}))).toEqual({
         kind: "blocked",
         reason:
            "This needs a value for the givens TENANT, REGION. Set them in the parameters above.",
      });
   });

   it("runs with a note when an unset given has a default", () => {
      const givens: Given[] = [{ name: "X", default: "5" }];
      expect(runGate(givens, values({}))).toEqual({
         kind: "defaults",
         note: "Ran with the default X = 5",
      });
   });

   it("names only the defaulted givens when some are set and some default", () => {
      const givens: Given[] = [
         { name: "X", default: "5" },
         { name: "Y", default: "'a'" },
         { name: "Z" },
      ];
      expect(runGate(givens, values({ Z: "set" }))).toEqual({
         kind: "defaults",
         note: "Ran with defaults X = 5, Y = a",
      });
   });

   it("spells defaults as the panel does and leaves empty filters out", () => {
      const givens: Given[] = [
         { name: "CATEGORY", type: "filter<string>", default: "f''" },
         { name: "SINCE", type: "date", default: "@2023-01-01" },
      ];
      expect(runGate(givens, values({}))).toEqual({
         kind: "defaults",
         note: "Ran with the default SINCE = 2023-01-01",
      });
   });

   it("is ok when every unset default is an empty filter", () => {
      const givens: Given[] = [
         { name: "CATEGORY", type: "filter<string>", default: "f''" },
      ];
      expect(runGate(givens, values({}))).toEqual({ kind: "ok" });
   });

   it("treats a whitespace string as unset", () => {
      const givens: Given[] = [{ name: "TENANT" }];
      expect(runGate(givens, values({ TENANT: "   " }))).toEqual({
         kind: "blocked",
         reason:
            "This needs a value for the given TENANT. Set it in the parameters above.",
      });
   });

   it("blocks rather than defaults when both are present", () => {
      const givens: Given[] = [{ name: "X", default: "5" }, { name: "TENANT" }];
      expect(runGate(givens, values({}))).toEqual({
         kind: "blocked",
         reason:
            "This needs a value for the given TENANT. Set it in the parameters above.",
      });
   });

   it("skips a spec with no name", () => {
      const givens: Given[] = [{ type: "string" }, { name: "TENANT" }];
      expect(runGate(givens, values({ TENANT: "acme" }))).toEqual({
         kind: "ok",
      });
   });
});

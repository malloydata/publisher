// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Discovery under a lock: a source is omitted only when a `#(authorize)` term
 * keyed on a `#(secure)` given cannot admit the caller's value, or the secure
 * given was not supplied. Plain givens, row filters and an absent `givens`
 * hide nothing.
 */
import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { lockedForCaller } from "./discovery_lock";

type ApiSource = components["schemas"]["Source"];

const GIVENS: ApiSource["givens"] = [
   { name: "ROLES", type: "string[]", secure: true },
   { name: "ORGS", type: "string[]", secure: true },
   { name: "TIER", type: "string", secure: true },
   { name: "MODE", type: "string[]" },
];

const sources: ApiSource[] = [
   { name: "open", givens: GIVENS },
   { name: "chart", givens: GIVENS, authorize: ["'clinician' in $ROLES"] },
   { name: "billing", givens: GIVENS, authorize: ["'billing' in $ROLES"] },
   { name: "gold", givens: GIVENS, authorize: ["'gold' = $TIER"] },
   { name: "plain", givens: GIVENS, authorize: ["'debug' in $MODE"] },
   {
      name: "both",
      givens: GIVENS,
      authorize: ["'clinician' in $ROLES and 'debug' in $MODE"],
   },
   { name: "rows", givens: GIVENS, accessFilter: ["org in $ORGS"] },
   { name: "vault", givens: GIVENS, authorize: ["false"] },
   { name: "reopened", givens: GIVENS, authorize: ["true"] },
];

describe("lockedForCaller", () => {
   it("hides nothing with no givens", () => {
      expect(lockedForCaller(sources, undefined).size).toBe(0);
      expect(lockedForCaller(undefined, { ROLES: ["x"] }).size).toBe(0);
   });

   it("omits a source whose secure lock the caller's value does not satisfy", () => {
      const locked = lockedForCaller(sources, {
         ROLES: ["clinician"],
         TIER: "silver",
         MODE: ["debug"],
      });
      expect([...locked].sort()).toEqual(["billing", "gold", "vault"]);
   });

   it("omits a source whose secure lock reads a given the trusted tier did not supply", () => {
      const locked = lockedForCaller(sources, { TIER: "gold" });
      expect([...locked].sort()).toEqual(["billing", "both", "chart", "vault"]);
   });

   it("never hides on a plain given, and judges a mixed lock on its secure terms only", () => {
      const locked = lockedForCaller(sources, {
         ROLES: ["clinician", "billing"],
         TIER: "gold",
      });
      // `plain` reads only MODE; `both` reads ROLES (admitted) and MODE (ignored).
      expect([...locked].sort()).toEqual(["vault"]);
   });

   it("matches a scalar exactly and an array by membership", () => {
      expect(lockedForCaller(sources, { TIER: "Gold", ROLES: [] })).toContain(
         "gold",
      );
      expect(
         lockedForCaller(sources, { TIER: "gold", ROLES: ["admin"] }),
      ).toContain("chart");
      expect(
         lockedForCaller(sources, { TIER: "gold", ROLES: ["admin"] }),
      ).not.toContain("gold");
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { mergeGivens } from "./mergeGivens";

describe("mergeGivens", () => {
   it("keeps a given the report does not declare, such as a gate's ROLE", () => {
      expect(
         mergeGivens({ ROLE: "analyst", REGION: "West" }, { REGION: "East" }, [
            "REGION",
         ]),
      ).toEqual({ ROLE: "analyst", REGION: "East" });
   });

   it("drops a declared given the report cleared", () => {
      expect(
         mergeGivens({ ROLE: "analyst", REGION: "West" }, {}, ["REGION"]),
      ).toEqual({ ROLE: "analyst" });
   });
});

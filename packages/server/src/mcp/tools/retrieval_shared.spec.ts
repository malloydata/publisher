// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { scrubForEgress } from "./retrieval_shared";

describe("scrubForEgress", () => {
   it("drops access predicates and persist lines", () => {
      expect(
         scrubForEgress(
            "Revenue by region. #(access_filter) region = 'EU' #(authorize) group = 'x'",
         ),
      ).toBe("Revenue by region.");
      expect(scrubForEgress("Plain doc.\n#(authorize) role = 'a'\nMore.")).toBe(
         "Plain doc. More.",
      );
      expect(scrubForEgress("Keeps #(doc) inline marker text")).toContain(
         "Keeps",
      );
      expect(scrubForEgress("x #@ persist name=t")).toBe("x");
   });
});

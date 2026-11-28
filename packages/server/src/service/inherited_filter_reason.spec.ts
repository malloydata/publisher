// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { AccessDeniedError, errorReason } from "../errors";
import {
   INHERITED_FILTER_REASONS,
   inheritedFilterReasonOf,
} from "./filter_binding_guard";

describe("the inherited-filter guard's reason classes", () => {
   // Every refusal the guard throws, read from its source, so a new message
   // cannot fall into the catch-all class unnoticed.
   const source = fs.readFileSync(
      path.join(__dirname, "filter_binding_guard.ts"),
      "utf-8",
   );
   const thrown = [
      ...source.matchAll(/throw new Error\(\s*(["`])([\s\S]*?)\1,?\s*\)/g),
   ].map((m) => m[2]);

   it("finds the guard's refusals", () => {
      expect(thrown.length).toBeGreaterThanOrEqual(10);
   });

   // Each refusal's class, pinned: a message added to the guard that is not
   // listed here fails, so its class is decided rather than defaulted.
   const CLASSES: [string, string][] = [
      [
         ", which resolves to a different field on the executed source",
         "redefined_field",
      ],
      [", whose ON or where: inputs are not recorded", "unrecorded_join"],
      [
         "a row-security filter's declaring source could not be resolved",
         "unresolved_declaration",
      ],
      [
         "a row-security gate's declaring source could not be resolved",
         "unresolved_declaration",
      ],
      [
         "a row-security gate's entry point could not be resolved",
         "unresolved_declaration",
      ],
      ["a query source's base could not be resolved", "unresolved_declaration"],
      ["field-usage closure exceeded the resolution bound", "resolution_bound"],
      ["composite nesting exceeded the resolution bound", "resolution_bound"],
      [
         "row-security filter recursion exceeded the join-depth resolution bound",
         "resolution_bound",
      ],
   ];

   it("classes each one as pinned", () => {
      for (const message of thrown) {
         const listed = CLASSES.filter(([key]) => message.includes(key));
         expect([message, listed.length]).toEqual([message, 1]);
         expect([message, inheritedFilterReasonOf(message)]).toEqual([
            message,
            listed[0][1],
         ]);
         expect(INHERITED_FILTER_REASONS as readonly string[]).toContain(
            listed[0][1],
         );
      }
   });
});

describe("the inherited-filter denial", () => {
   const err = AccessDeniedError.inheritedFilterUnverified(
      "funnel_deals",
      "redefined_field",
   );

   it("answers 403 with its own reason, distinct from a gate's", () => {
      expect(errorReason(err)).toBe("INHERITED_FILTER_UNVERIFIED");
      expect(errorReason(AccessDeniedError.forSource("funnel_deals"))).toBe(
         "ACCESS_DENIED",
      );
   });

   it("names the source and the reason class, says it is no permission rule, and nothing else", () => {
      expect(err.message).toStartWith(
         'Access denied for source "funnel_deals"',
      );
      expect(err.message).toContain("reason: redefined_field");
      expect(err.message).toContain("This is not a permission rule");
      expect(err.source).toBe("funnel_deals");
      expect(err.gate).toBe("row_filter");
   });
});

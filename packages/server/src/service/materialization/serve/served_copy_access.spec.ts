// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { keepsAccess } from "./served_copy_serve";

/**
 * The load-time access backstop (P3): a re-rooted source is carried only when
 * every field it has is exactly as hidden as the author's field of that name,
 * and a field only the served copy has is never public.
 */
describe("keepsAccess", () => {
   const def = (...fields: [string, string?][]) => ({
      fields: fields.map(([name, accessModifier]) => ({
         name,
         ...(accessModifier ? { accessModifier } : {}),
      })),
   });
   const author = def(
      ["amount"],
      ["org_id", "internal"],
      ["secret", "private"],
   );

   it("carries a source whose fields keep the author's modifiers", () => {
      expect(
         keepsAccess(
            author,
            def(
               ["amount", "public"],
               ["org_id", "internal"],
               ["secret", "private"],
            ),
         ),
      ).toBe(true);
   });

   it("refuses a hidden field served public, or private served internal", () => {
      expect(
         keepsAccess(
            author,
            def(["amount"], ["org_id"], ["secret", "private"]),
         ),
      ).toBe(false);
      expect(
         keepsAccess(
            author,
            def(["amount"], ["org_id", "internal"], ["secret", "internal"]),
         ),
      ).toBe(false);
   });

   it("reads an unknown modifier as private", () => {
      expect(keepsAccess(def(["x", "protected"]), def(["x", "private"]))).toBe(
         true,
      );
      expect(keepsAccess(def(["x", "protected"]), def(["x"]))).toBe(false);
   });

   it("allows a hidden field only the served copy has, never a public one", () => {
      expect(
         keepsAccess(
            def(["order_month"]),
            def(["order_month"], ["order_month__stored", "internal"]),
         ),
      ).toBe(true);
      expect(keepsAccess(def(["amount"]), def(["amount"], ["org_id"]))).toBe(
         false,
      );
   });

   it("fails closed on an unreadable source", () => {
      expect(keepsAccess(undefined, def(["a"]))).toBe(false);
      expect(keepsAccess(def(["a"]), {})).toBe(false);
   });
});

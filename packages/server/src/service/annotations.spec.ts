import { describe, expect, it } from "bun:test";
import { isReservedRoute } from "./annotations";

describe("isReservedRoute", () => {
   it("treats the empty route (MOTLY / render config) as reserved", () => {
      expect(isReservedRoute("")).toBe(true);
   });

   it("treats Malloy's punctuation sigils as reserved", () => {
      // Form 2 reserves the entire punct-only namespace, not just the
      // currently-claimed sigils (`!` `@` `"` `:`).
      for (const route of ["!", "@", '"', ":", "%", "-", "::"]) {
         expect(isReservedRoute(route)).toBe(true);
      }
   });

   it("treats bracketed app routes as not reserved", () => {
      for (const route of [
         "doc",
         "label",
         "filter",
         "bar-chart",
         "https://example.com/ns",
         "123",
         "══SECURITY══",
      ]) {
         expect(isReservedRoute(route)).toBe(false);
      }
   });
});

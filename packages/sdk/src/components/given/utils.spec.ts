import { describe, expect, it } from "bun:test";
import { renderGivenDefault } from "./utils";

describe("renderGivenDefault", () => {
   it("unquotes a string literal", () => {
      expect(renderGivenDefault("string", "'WN'")).toBe("WN");
   });

   it("undoubles escaped quotes in a string literal", () => {
      expect(renderGivenDefault("string", "'O''Hare'")).toBe("O'Hare");
   });

   it("drops the @ sigil from a date literal", () => {
      expect(renderGivenDefault("date", "@2024-01-01")).toBe("2024-01-01");
      expect(renderGivenDefault("timestamp", "@2024-01-01 12:00:00")).toBe(
         "2024-01-01 12:00:00",
      );
   });

   it("unwraps a filter literal", () => {
      expect(renderGivenDefault("filter<string>", "f'WN'")).toBe("WN");
   });

   it("shows numbers and booleans verbatim", () => {
      expect(renderGivenDefault("number", "2003")).toBe("2003");
      expect(renderGivenDefault("boolean", "true")).toBe("true");
   });

   it("returns undefined when there is no default", () => {
      expect(renderGivenDefault("string", undefined)).toBeUndefined();
      expect(renderGivenDefault("string", "")).toBeUndefined();
      expect(renderGivenDefault("string", "   ")).toBeUndefined();
   });

   it("distinguishes an explicit empty-string default from no default", () => {
      // `given: x :: string is ''` -> literal "''" -> "" (present, not undefined),
      // so the caller can show it as "(empty)" rather than suppress it.
      expect(renderGivenDefault("string", "''")).toBe("");
   });

   it("leaves a non-quoted value untouched", () => {
      // Defensive: a malformed/already-rendered default isn't mangled.
      expect(renderGivenDefault("string", "WN")).toBe("WN");
   });
});

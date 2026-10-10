// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { malloyLiteral } from "./malloyLiteral";

describe("malloyLiteral", () => {
   it("spells a value the way its given's type reads it", () => {
      expect(malloyLiteral("Nike", "filter<string>")).toBe("f'Nike'");
      expect(malloyLiteral(">= 10", "filter<number>")).toBe("f'>= 10'");
      expect(malloyLiteral("O'Neil", "string")).toBe("'O\\'Neil'");
      expect(malloyLiteral("42", "number")).toBe("42");
      expect(malloyLiteral("forty", "number")).toBeUndefined();
      expect(malloyLiteral(true, "boolean")).toBe("true");
      expect(malloyLiteral("2024-01-31", "date")).toBe("@2024-01-31");
      expect(malloyLiteral(new Date("2024-01-31T09:30:00Z"), "timestamp")).toBe(
         "@2024-01-31 09:30:00",
      );
   });

   it("spells a value by its own type when no given is involved", () => {
      expect(malloyLiteral("Jeans")).toBe("'Jeans'");
      expect(malloyLiteral("Ben's & Jerry\\s")).toBe("'Ben\\'s & Jerry\\\\s'");
      expect(malloyLiteral(42)).toBe("42");
      expect(malloyLiteral(true)).toBe("true");
      expect(malloyLiteral(new Date("2024-03-05T00:00:00Z"))).toBe(
         "@2024-03-05",
      );
      expect(malloyLiteral(new Date("2024-03-05T13:45:00Z"))).toBe(
         "@2024-03-05 13:45:00",
      );
   });

   it("has nothing to write for an empty value or a type it cannot spell", () => {
      expect(malloyLiteral("", "filter<string>")).toBeUndefined();
      expect(malloyLiteral(null, "filter<string>")).toBeUndefined();
      expect(malloyLiteral("x", "geometry")).toBeUndefined();
      expect(malloyLiteral(null)).toBeUndefined();
      expect(malloyLiteral(Number.NaN)).toBeUndefined();
      expect(malloyLiteral({})).toBeUndefined();
      expect(malloyLiteral(new Date("nope"))).toBeUndefined();
   });
});

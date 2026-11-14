// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { widthLabel } from "./widthLabel";

describe("widthLabel", () => {
   it("names the common fractions of the grid", () => {
      expect(widthLabel(6, 12)).toBe("6 of 12 · Half");
      expect(widthLabel(8, 12)).toBe("8 of 12 · Two-thirds");
      expect(widthLabel(4, 12)).toBe("4 of 12 · A third");
      expect(widthLabel(3, 12)).toBe("3 of 12 · A quarter");
      expect(widthLabel(9, 12)).toBe("9 of 12 · Three-quarters");
      expect(widthLabel(2, 4)).toBe("2 of 4 · Half");
   });

   it("says the count alone where the width is no common fraction", () => {
      expect(widthLabel(5, 12)).toBe("5 of 12");
      expect(widthLabel(1, 12)).toBe("1 of 12");
   });

   it("calls the whole grid full width", () => {
      expect(widthLabel(12, 12)).toBe("12 of 12 · Full width");
   });
});

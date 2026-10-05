// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { defaultTileSpan } from "./DashboardGrid";

// A 16-wide grid of 16 untagged tiles rendered as one row of ~45px slivers.
describe("defaultTileSpan", () => {
   it("widens a one-column tile in a 16-column grid until it is readable", () => {
      expect(defaultTileSpan(16, 1000, 240)).toBe(5);
   });

   it("widens a one-column tile in a 12-column grid", () => {
      expect(defaultTileSpan(12, 1000, 240)).toBe(4);
   });

   it("leaves a roomy grid alone", () => {
      expect(defaultTileSpan(2, 1000, 240)).toBe(1);
   });

   it("changes nothing before the width is measured", () => {
      expect(defaultTileSpan(16, 0, 240)).toBe(1);
   });

   it("never exceeds the grid width", () => {
      expect(defaultTileSpan(4, 100, 240)).toBe(4);
   });

   it("leaves a one-column grid alone", () => {
      expect(defaultTileSpan(1, 100, 240)).toBe(1);
   });
});

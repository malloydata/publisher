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

describe("useDefaultTileSpan", () => {
   it("is the reader's span for the measured box, so the builder draws an unset tile as the reader does", async () => {
      const { renderHook } = await import("@testing-library/react");
      const { useDefaultTileSpan } = await import("./DashboardGrid");
      const box = document.createElement("div");
      box.getBoundingClientRect = () => ({ width: 1200 }) as DOMRect;
      const { result } = renderHook(() =>
         useDefaultTileSpan({ current: box }, 12),
      );
      // Not 1: a one-column tile beside a colspan=6 one was a ~80px sliver.
      expect(result.current).toBe(defaultTileSpan(12, 1200, 240));
      expect(result.current).toBeGreaterThan(1);
   });

   it("is one column while nothing is measured", async () => {
      const { renderHook } = await import("@testing-library/react");
      const { useDefaultTileSpan } = await import("./DashboardGrid");
      const { result } = renderHook(() =>
         useDefaultTileSpan({ current: null }, 12),
      );
      expect(result.current).toBe(1);
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import { useSettled } from "./useSettled";

// Bun runs it; its typings do not declare it.
const advance = (ms: number) =>
   (
      jest as unknown as { advanceTimersByTime(ms: number): void }
   ).advanceTimersByTime(ms);

describe("useSettled", () => {
   beforeEach(() => {
      jest.useFakeTimers();
   });
   afterEach(() => {
      jest.useRealTimers();
   });

   it("returns the first value at once and a changed one only after it has stayed put", () => {
      const view = renderHook(({ value }) => useSettled(value, 400), {
         initialProps: { value: "a" },
      });
      expect(view.result.current).toBe("a");

      // A burst of changes, each inside the window of the last, never settles.
      for (const value of ["b", "bc", "bcd"]) {
         view.rerender({ value });
         act(() => {
            advance(399);
         });
         expect(view.result.current).toBe("a");
      }
      act(() => {
         advance(1);
      });
      expect(view.result.current).toBe("bcd");
   });
});

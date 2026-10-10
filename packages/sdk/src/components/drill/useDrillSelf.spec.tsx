// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { renderHook } from "@testing-library/react";
import type { GivenValue } from "../../hooks/givenValue";
import { useDrillSelf } from "./useDrillSelf";

const TYPES = new Map<string, string | undefined>([
   ["REGION", "filter<string>"],
   ["MIN_AMOUNT", "number"],
]);

function hook(setGiven = mock((_n: string, _v: GivenValue) => {})) {
   const view = renderHook(() =>
      useDrillSelf({
         declaredTypes: TYPES,
         setGiven,
         documentName: "overview",
      }),
   );
   return { ...view.result.current, setGiven };
}

describe("useDrillSelf", () => {
   let warn: ReturnType<typeof spyOn> | undefined;
   afterEach(() => warn?.mockRestore());

   it("resolves a tag's name as declared, or by folding case", () => {
      const { resolveGiven, canSelf } = hook();
      expect(resolveGiven("REGION")).toBe("REGION");
      expect(resolveGiven("region")).toBe("REGION");
      expect(resolveGiven("Min_Amount")).toBe("MIN_AMOUNT");
      expect(resolveGiven("STATE")).toBeUndefined();
      expect(canSelf("region")).toBe(true);
      expect(canSelf("STATE")).toBe(false);
   });

   it("sets the given under the DECLARED name, encoded for its type", () => {
      const { onSelf, setGiven } = hook();
      onSelf("region", "West");
      expect(setGiven).toHaveBeenCalledWith("REGION", "West");
      onSelf("min_amount", 42);
      expect(setGiven).toHaveBeenLastCalledWith("MIN_AMOUNT", 42);
   });

   it("declines, aloud, a name the document does not declare", () => {
      warn = spyOn(console, "warn").mockImplementation(() => {});
      const { onSelf, setGiven } = hook();
      onSelf("STATE", "CA");
      expect(setGiven).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("'overview'"));
   });

   it("declines, aloud, a value the given's type cannot hold", () => {
      warn = spyOn(console, "warn").mockImplementation(() => {});
      const { onSelf, setGiven } = hook();
      onSelf("MIN_AMOUNT", "not a number");
      expect(setGiven).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith(
         expect.stringContaining("Drill declined"),
      );
   });
});

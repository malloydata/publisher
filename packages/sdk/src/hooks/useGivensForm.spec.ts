import { describe, expect, it } from "bun:test";
import { act, renderHook } from "@testing-library/react";
import type { Given } from "../client";
import { useGivensForm } from "./useGivensForm";

/**
 * Hook-testing pattern for this package: `renderHook` to mount the hook,
 * `act()` around anything that sets state, then assert on `result.current`.
 * No JSX is involved, so this is a plain `.spec.ts`.
 *
 * A migration note, since #935 replaces this hook with `useGivensState`:
 * these tests describe behaviours a replacement has to preserve, not an API
 * contract worth keeping, so rewrite them alongside the hook. The last one
 * matters most. It guards a defect that was live here, and a replacement that
 * rebuilds its state on every run of the sync effect will reintroduce it.
 */

const givens = (...names: string[]): Given[] =>
   names.map((name) => ({ name, type: "string" }));

describe("useGivensForm", () => {
   it("starts with no overrides, so every given uses its model default", () => {
      const { result } = renderHook(() => useGivensForm(givens("region")));
      expect(result.current.givenValues.size).toBe(0);
   });

   it("records an override and reports it as active", () => {
      const { result } = renderHook(() => useGivensForm(givens("region")));

      act(() => result.current.updateGiven("region", "CA"));

      expect(result.current.givenValues.get("region")).toBe("CA");
      expect([...result.current.getActiveGivens()]).toEqual([["region", "CA"]]);
   });

   it("treats null as a revert, deleting the key rather than storing null", () => {
      const { result } = renderHook(() => useGivensForm(givens("region")));

      act(() => result.current.updateGiven("region", "CA"));
      act(() => result.current.updateGiven("region", null));

      // Deleted, not set to null. A stored null would still be an entry, and
      // the request builder would send the given rather than omitting it.
      expect(result.current.givenValues.has("region")).toBe(false);
   });

   it("keeps an explicit empty string, which is an override not a revert", () => {
      const { result } = renderHook(() => useGivensForm(givens("region")));

      act(() => result.current.updateGiven("region", ""));

      expect(result.current.givenValues.get("region")).toBe("");
      expect(result.current.getActiveGivens().has("region")).toBe(true);
   });

   it("clearAll drops every override", () => {
      const { result } = renderHook(() => useGivensForm(givens("a", "b")));

      act(() => result.current.updateGiven("a", "1"));
      act(() => result.current.updateGiven("b", "2"));
      act(() => result.current.clearAll());

      expect(result.current.givenValues.size).toBe(0);
   });

   it("drops overrides for givens the model no longer declares", () => {
      const { result, rerender } = renderHook(
         ({ declared }) => useGivensForm(declared),
         { initialProps: { declared: givens("a", "b") } },
      );

      act(() => result.current.updateGiven("a", "1"));
      act(() => result.current.updateGiven("b", "2"));

      // The model reloaded and "b" is gone.
      rerender({ declared: givens("a") });

      expect(result.current.givenValues.get("a")).toBe("1");
      expect(result.current.givenValues.has("b")).toBe(false);
   });

   it("settles when `givens` is a fresh array on every render", () => {
      // This hook is a published SDK export, so a consumer can reasonably call
      // it with an array literal instead of a memoized one. The sync effect
      // keys on `givens` identity, so an unmemoized array re-fires it on every
      // render; if it returns a new Map each time, state identity changes,
      // that re-renders, and the two drive each other until React throws
      // "Maximum update depth exceeded". In-repo callers are shielded only
      // because `useModelGivens` happens to memoize.
      let renders = 0;
      const { result } = renderHook(() => {
         renders++;
         return useGivensForm([{ name: "region", type: "string" }]);
      });

      expect(result.current.givenValues.size).toBe(0);
      expect(renders).toBeLessThan(5);
   });
});

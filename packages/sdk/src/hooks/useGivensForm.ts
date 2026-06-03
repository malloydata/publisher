import { useCallback, useEffect, useState } from "react";
import { Given } from "../client";

/**
 * UI-side value for a given. Mirrors the JS shapes the server accepts
 * for `givens` runtime values, plus `Date` (serialized to ISO before send).
 */
export type GivenValue =
   | string
   | number
   | boolean
   | Date
   | string[]
   | number[]
   | null;

/**
 * Result from the useGivensForm hook
 */
export interface UseGivensFormResult {
   /** Current value for each given, keyed by given name. Missing keys mean "use the model default". */
   givenValues: Map<string, GivenValue>;
   /** Update a given's value. Pass `null` to revert to model default. */
   updateGiven: (name: string, value: GivenValue) => void;
   /** Remove a single given override. */
   clearGiven: (name: string) => void;
   /** Remove all overrides. */
   clearAll: () => void;
   /** Map of just the givens that have a user-supplied (non-null) override. */
   getActiveGivens: () => Map<string, Exclude<GivenValue, null>>;
}

/**
 * Manages user-supplied values for model `given:` parameters.
 *
 * Mirrors the shape of [`useDimensionFilters`](./useDimensionFilters.ts):
 * one entry per declared given, keyed by name (no source qualifier — givens
 * are model-level, not per-source).
 *
 * When `givens` (the introspected list) changes (e.g., after the notebook
 * loads), existing user overrides are preserved for matching names and new
 * givens are added as empty.
 */
export function useGivensForm(givens: Given[]): UseGivensFormResult {
   const [givenValues, setGivenValues] = useState<Map<string, GivenValue>>(
      () => new Map(),
   );

   // Sync the form when the declared givens list changes (e.g., model reloaded).
   // Preserve existing values for givens that still exist; drop values for givens
   // that no longer exist.
   useEffect(() => {
      setGivenValues((prev) => {
         const declared = new Set(
            givens.map((g) => g.name).filter((n): n is string => !!n),
         );
         const next = new Map<string, GivenValue>();
         prev.forEach((value, name) => {
            if (declared.has(name)) next.set(name, value);
         });
         return next;
      });
   }, [givens]);

   const updateGiven = useCallback((name: string, value: GivenValue) => {
      setGivenValues((prev) => {
         const next = new Map(prev);
         if (value === null) {
            next.delete(name);
         } else {
            next.set(name, value);
         }
         return next;
      });
   }, []);

   const clearGiven = useCallback((name: string) => {
      setGivenValues((prev) => {
         if (!prev.has(name)) return prev;
         const next = new Map(prev);
         next.delete(name);
         return next;
      });
   }, []);

   const clearAll = useCallback(() => {
      setGivenValues((prev) => (prev.size === 0 ? prev : new Map()));
   }, []);

   const getActiveGivens = useCallback((): Map<
      string,
      Exclude<GivenValue, null>
   > => {
      const active = new Map<string, Exclude<GivenValue, null>>();
      givenValues.forEach((value, name) => {
         if (value !== null && value !== undefined) {
            active.set(name, value);
         }
      });
      return active;
   }, [givenValues]);

   return {
      givenValues,
      updateGiven,
      clearGiven,
      clearAll,
      getActiveGivens,
   };
}

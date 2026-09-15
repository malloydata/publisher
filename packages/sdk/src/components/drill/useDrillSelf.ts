// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo } from "react";
import type { GivenValue } from "../../hooks/givenValue";
import { encodeDrillValue } from "./resolveDrill";

export interface UseDrillSelfOptions {
   /** Declared type per given name: what this document can set. */
   declaredTypes: ReadonlyMap<string, string | undefined>;
   /** The control row's setter, from `useGivensState`. */
   setGiven: (name: string, value: GivenValue) => void;
   /**
    * How the document names itself in the warning for a tag that names a given
    * it does not declare: a dashboard's slug, a notebook's path.
    */
   documentName: string;
}

export interface UseDrillSelfResult {
   /** The declared given a drill tag's name refers to, or undefined. */
   resolveGiven: (given: string) => string | undefined;
   /** For `useDrill`: whether a `to=self` on this given can be honoured. */
   canSelf: (given: string) => boolean;
   /** For `useDrill`: set the given to the clicked value, encoded for its type. */
   onSelf: (given: string, rawValue: unknown) => void;
}

/**
 * The `# drill { to=self }` half of a document's drill wiring: which of its
 * givens a tag can set, and setting one from a clicked cell.
 *
 * Shared by the dashboard and the notebook so one tag behaves identically on
 * both surfaces, which used to be two copies of this code with the same
 * warning string in each. Three rules live here:
 *
 * - **Names fold case.** `# drill` with no `given=` falls back to the
 *   DIMENSION's spelling, conventionally lower_snake, while givens are
 *   conventionally SHOUTED; neither convention can be assumed, so a tag naming
 *   `region` finds a declared `REGION` and vice versa. First declaration wins
 *   when a model declares both spellings.
 * - **The value is encoded against the DECLARED type**, which is knowable here
 *   and is not knowable at the click: `useDrill` hands over the raw cell value
 *   for exactly this reason. Passing it straight to `setGiven` skipped the
 *   encoder, so a clicked date reached a `number` given as epoch milliseconds
 *   and a filter value went unescaped. It is set under the name the MODEL
 *   declares, so it reaches the URL and the request under the one name the
 *   server knows.
 * - **A refusal is said aloud.** `canSelf` checks only that the name resolves,
 *   so a `given=` naming a type the clicked value cannot become still paints a
 *   whole column clickable and would then drop every click in silence. Both
 *   refusals warn, because each is an authoring mistake with no other symptom.
 */
export function useDrillSelf({
   declaredTypes,
   setGiven,
   documentName,
}: UseDrillSelfOptions): UseDrillSelfResult {
   const givenNamesByFold = useMemo(() => {
      const byFold = new Map<string, string>();
      for (const name of declaredTypes.keys()) {
         if (!byFold.has(name.toLowerCase()))
            byFold.set(name.toLowerCase(), name);
      }
      return byFold;
   }, [declaredTypes]);

   const resolveGiven = useCallback(
      (given: string) =>
         declaredTypes.has(given)
            ? given
            : givenNamesByFold.get(given.toLowerCase()),
      [declaredTypes, givenNamesByFold],
   );

   // Asked before a cell is painted as drillable, so the affordance matches
   // what a click can do: sending a given the document cannot bind would fail
   // every query on the page.
   const canSelf = useCallback(
      (given: string) => resolveGiven(given) !== undefined,
      [resolveGiven],
   );

   const onSelf = useCallback(
      (given: string, rawValue: unknown) => {
         const declared = resolveGiven(given);
         if (declared === undefined) {
            console.warn(
               `# drill { to=self } tried to set '${given}', which ` +
                  `'${documentName}' does not declare as a given. Name the ` +
                  `given with 'given=' on the drill tag.`,
            );
            return;
         }
         const declaredType = declaredTypes.get(declared);
         const value = encodeDrillValue(rawValue, declaredType);
         if (value === undefined) {
            console.warn(
               `Drill declined: ${JSON.stringify(rawValue)} cannot be a value for given "${declared}"` +
                  (declaredType ? ` of type ${declaredType}` : ""),
            );
            return;
         }
         setGiven(declared, value);
      },
      [declaredTypes, documentName, resolveGiven, setGiven],
   );

   return { resolveGiven, canSelf, onSelf };
}

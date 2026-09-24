// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { renderGivenDefault } from "../given/utils";

/**
 * Whether a Run should proceed, given the model's declared `given:`s and what
 * the control row currently holds.
 *
 * - `ok`: every given is either set or has a default; run as normal.
 * - `defaults`: nothing is missing, but one or more unset givens will fall
 *   back to their model default, which is worth saying since the reader chose
 *   nothing for them.
 * - `blocked`: an unset given has no default, so the server would 403 (or
 *   reject outright) rather than run. Refused here instead, with a message
 *   that names what to fill in, rather than round-tripping to the server for
 *   the same answer.
 */
export type RunGate =
   | { kind: "ok" }
   | { kind: "defaults"; note: string }
   | { kind: "blocked"; reason: string };

/** Set means a real value, not merely present: a blank string is not a choice. */
function isSet(value: GivenValue | undefined): boolean {
   if (value === null || value === undefined) return false;
   if (typeof value === "string" && value.trim() === "") return false;
   return true;
}

export function runGate(
   givens: readonly Given[],
   values: ReadonlyMap<string, GivenValue>,
): RunGate {
   const missing: Given[] = [];
   const defaulted: Given[] = [];
   for (const given of givens) {
      // A spec with no name cannot be bound to a value at all, so it is
      // neither missing nor defaulted; it is simply not this gate's business.
      if (given.name === undefined) continue;
      if (isSet(values.get(given.name))) continue;
      if (given.default === undefined) missing.push(given);
      else defaulted.push(given);
   }

   if (missing.length > 0) {
      const names = missing.map((given) => given.name).join(", ");
      return missing.length === 1
         ? {
              kind: "blocked",
              reason: `This needs a value for the given ${names}. Set it in the parameters above.`,
           }
         : {
              kind: "blocked",
              reason: `This needs a value for the givens ${names}. Set them in the parameters above.`,
           };
   }

   // Spelled as the panel's "Default:" caption; an empty default (an unset
   // filter, say) changes nothing, so the note leaves it out.
   const shown = defaulted.flatMap((given) => {
      const display = renderGivenDefault(given.type ?? "string", given.default);
      return display ? [`${given.name} = ${display}`] : [];
   });
   if (shown.length > 0) {
      const pairs = shown.join(", ");
      return {
         kind: "defaults",
         note:
            shown.length === 1
               ? `Ran with the default ${pairs}`
               : `Ran with defaults ${pairs}`,
      };
   }

   return { kind: "ok" };
}

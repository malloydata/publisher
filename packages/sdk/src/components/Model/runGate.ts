// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { renderGivenDefault } from "../given/utils";

/**
 * What a Run is about to send, measured against the model's declared givens.
 *
 * Nothing here refuses a Run. Givens are declared model-wide but read per
 * source, and nothing on the client says which source reads which, so a blank
 * given with no default may be exactly right for the source being explored.
 * The server decides; this only supplies the words around its answer.
 */
export interface RunGate {
   /** Unset givens with no default: named in a hint when the server refuses. */
   missing: string[];
   /** Set when an unset given fell back to a non-empty default. */
   defaultsNote?: string;
}

/**
 * Unset means exactly what `givensToRequest` leaves out of the request. An
 * empty string is not unset: for a string or filter given it is a value the
 * reader typed, and it is sent, so the default does not apply.
 */
function isSet(value: GivenValue | undefined): boolean {
   return value !== null && value !== undefined;
}

export function runGate(
   givens: readonly Given[],
   values: ReadonlyMap<string, GivenValue>,
): RunGate {
   const missing: string[] = [];
   const defaults: string[] = [];
   for (const given of givens) {
      if (given.name === undefined) continue;
      if (isSet(values.get(given.name))) continue;
      // `==`, not `===`: the API sends a missing default as null for some types.
      if (given.default == null) {
         missing.push(given.name);
         continue;
      }
      // Spelled as the panel's "Default:" caption; an empty default (an unset
      // filter, say) changes nothing, so the note leaves it out.
      const display = renderGivenDefault(given.type ?? "string", given.default);
      if (display) defaults.push(`${given.name} = ${display}`);
   }

   const gate: RunGate = { missing };
   if (defaults.length > 0) {
      gate.defaultsNote =
         defaults.length === 1
            ? `Ran with the default ${defaults[0]}`
            : `Ran with defaults ${defaults.join(", ")}`;
   }
   return gate;
}

/** The hint shown under a refusal when the reader left no-default givens blank. */
export function missingGivensHint(missing: readonly string[]): string {
   return missing.length === 1
      ? `This source may need a value for the given ${missing[0]}. Set it in the parameters above.`
      : `This source may need values for the givens ${missing.join(", ")}. Set them in the parameters above.`;
}

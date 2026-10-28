// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenInfo, GivenRequest } from "./modelGivens";

/**
 * Whether a run from the explorer can be honest about its inputs.
 *
 * A run sends the given values the reader set in the page's Givens section
 * (the dashboard's own controls). A given the reader left unset falls back
 * to its declared default, and the card names every default it ran at, the
 * way a report's panel shows the values that produced its cells. An unset
 * given with no default, or a `#(filter … required)` on the source (the
 * explorer has no filter controls), cannot run at all, so the card says what
 * is missing instead of showing a bare 400.
 */
export type RunGate =
   | { kind: "ok" }
   | { kind: "defaults"; note: string }
   | { kind: "blocked"; reason: string };

export function runGate(
   givens: ReadonlyArray<GivenInfo> | undefined,
   requiredFilters: ReadonlyArray<string> | undefined,
   /** Values the reader set, as a run sends them; an absent entry means "use the
    *  default". An empty string is a value, as on a dashboard (for a
    *  `filter<…>`, "All"). */
   values: GivenRequest | undefined = undefined,
): RunGate {
   // A given the host fills is never in `givens`: extractCompiledModelGivens
   // leaves it out.
   const isSet = (g: GivenInfo) => values?.[g.name] !== undefined;
   const missingGivens = (givens ?? [])
      .filter((g) => !isSet(g) && g.default === undefined)
      .map((g) => g.name);
   const required = requiredFilters ?? [];
   if (missingGivens.length > 0 || required.length > 0) {
      // Two different remedies, said separately: a given can be set in the
      // panel above this card; a required filter has no control here.
      const givenPart =
         missingGivens.length > 0
            ? `a value for ${missingGivens.length === 1 ? "the given" : "the givens"} ${missingGivens.join(", ")}`
            : "";
      const filterPart =
         required.length > 0
            ? `${required.length === 1 ? "the required filter" : "the required filters"} ${required.join(", ")}`
            : "";
      let reason: string;
      if (filterPart === "") {
         reason = `This needs ${givenPart}. Set ${missingGivens.length === 1 ? "it" : "them"} in the Givens section above.`;
      } else if (givenPart === "") {
         reason = `This needs ${filterPart}, which this page can't set; open the source in a dashboard or notebook.`;
      } else {
         reason = `This needs ${givenPart} (set ${missingGivens.length === 1 ? "it" : "them"} in the Givens section above) and ${filterPart}, which this page can't set; open the source in a dashboard or notebook.`;
      }
      return { kind: "blocked", reason };
   }
   const defaulted = (givens ?? []).filter(
      (g) => !isSet(g) && g.default !== undefined,
   );
   if (defaulted.length > 0) {
      return {
         kind: "defaults",
         note: `Ran with ${defaulted.length === 1 ? "the default" : "defaults"} ${defaulted
            .map((g) => `${g.name} = ${g.default}`)
            .join(", ")}`,
      };
   }
   return { kind: "ok" };
}

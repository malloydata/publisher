// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describeGivenValue } from "../given/filterSummary";
import type { GivenInfo, GivenRequest, GivenRequestValue } from "./modelGivens";

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
 *
 * Each given is named as its control on the page words it, `Ordered since:
 * Jan 1, 2023`, never as a Malloy literal. A given the page can't tie to the
 * source either way (`unknown`) is named apart: the card says it can't tell
 * whether the source reads it, and what the run had for it where it does,
 * rather than calling its default one the run ran at. Values are named whole
 * (`describeGivenValue`), never cut to a button's width: the note is the record
 * of what produced the result.
 *
 * A run held only for givens the page can't tie to the source can run anyway
 * (`canRunAnyway`), at the reader's word: if the source does read one, the run
 * fails, and the card has already said why.
 */
export type RunGate =
   | { kind: "ok" }
   | { kind: "noted"; note: string }
   | { kind: "blocked"; reason: string; canRunAnyway: boolean };

/** `A`, `A or B`, `A, B or C`. */
export function wordList(
   words: readonly string[],
   conjunction: string,
): string {
   if (words.length <= 1) return words.join("");
   return `${words.slice(0, -1).join(", ")} ${conjunction} ${words.at(-1)}`;
}

/** A given and its value as its control words them, whole: `Region: West`, `Category: All`. */
function givenWords(given: GivenInfo, value?: GivenRequestValue): string {
   return `${given.label ?? given.name}: ${describeGivenValue(given, value)}`;
}

const labelOf = (given: GivenInfo) => given.label ?? given.name;

export function runGate(
   givens: ReadonlyArray<GivenInfo> | undefined,
   requiredFilters: ReadonlyArray<string> | undefined,
   /** Values the reader set, as a run sends them; an absent entry means "use the
    *  default". An empty string is a value, as on a dashboard (for a
    *  `filter<…>`, "All"). */
   values: GivenRequest | undefined = undefined,
   /** Names of the givens the page can't tie to the source either way
    *  (scanGivensForSource). */
   unknown: ReadonlySet<string> | undefined = undefined,
   /** The reader chose to run without the missing givens the page can't tie
    *  to the source; honored only when those are all that holds the run. */
   runAnyway = false,
   /** What the run reads from: its source, or a named query with none. */
   subject: "source" | "query" = "source",
): RunGate {
   // A given the host fills is never in `givens`: extractCompiledModelGivens
   // leaves it out.
   const all = givens ?? [];
   const isSet = (g: GivenInfo) => values?.[g.name] !== undefined;
   const isUnknown = (g: GivenInfo) => unknown?.has(g.name) === true;
   const missing = all.filter((g) => !isSet(g) && g.default === undefined);
   const required = requiredFilters ?? [];
   const canRunAnyway =
      required.length === 0 &&
      missing.length > 0 &&
      missing.every((g) => isUnknown(g));
   const skipped = runAnyway && canRunAnyway ? missing : [];
   if (skipped.length === 0 && (missing.length > 0 || required.length > 0)) {
      // Two different remedies, said separately: a given can be set in the
      // panel above this card; a required filter has no control here.
      const givenPart =
         missing.length > 0
            ? `a value for ${missing.length === 1 ? "the given" : "the givens"} ${missing.map(labelOf).join(", ")}`
            : "";
      const filterPart =
         required.length > 0
            ? `${required.length === 1 ? "the required filter" : "the required filters"} ${required.join(", ")}`
            : "";
      // A run that may not read the given still waits for it: sent without one
      // it does read, it fails with no control to fix it.
      const maybeMissing = missing.filter(isUnknown);
      const maybe =
         maybeMissing.length === 0
            ? ""
            : maybeMissing.length === missing.length
              ? ` This page can't tell whether this ${subject} reads ${missing.length === 1 ? "it" : "them"}.`
              : ` This page can't tell whether this ${subject} reads ${wordList(maybeMissing.map(labelOf), "or")}.`;
      let reason: string;
      if (filterPart === "") {
         reason = `This needs ${givenPart}. Set ${missing.length === 1 ? "it" : "them"} in the Givens section above.${maybe}`;
      } else if (givenPart === "") {
         reason = `This needs ${filterPart}, which this page can't set; open the source in a dashboard or notebook.`;
      } else {
         reason = `This needs ${givenPart} (set ${missing.length === 1 ? "it" : "them"} in the Givens section above) and ${filterPart}, which this page can't set; open the source in a dashboard or notebook.${maybe}`;
      }
      return { kind: "blocked", reason, canRunAnyway };
   }
   const defaultsOf = (list: GivenInfo[]) =>
      list.length > 0
         ? `${list.length === 1 ? "the default" : "the defaults"} ${list
              .map((g) => givenWords(g))
              .join(", ")}`
         : "";
   const notes: string[] = [];
   if (skipped.length > 0) {
      notes.push(
         `Ran without a value for ${skipped.length === 1 ? "the given" : "the givens"} ${wordList(skipped.map(labelOf), "and")}, which this page can't tell this ${subject} reads.`,
      );
   }
   const ran = all.filter((g) => !skipped.includes(g));
   const known = ran.filter((g) => !isUnknown(g));
   const knownDefaults = defaultsOf(known.filter((g) => !isSet(g)));
   if (knownDefaults !== "") notes.push(`Ran with ${knownDefaults}.`);
   const maybe = ran.filter(isUnknown);
   if (maybe.length > 0) {
      const had = [
         maybe
            .filter(isSet)
            .map((g) => givenWords(g, values?.[g.name]))
            .join(", "),
         defaultsOf(maybe.filter((g) => !isSet(g))),
      ]
         .filter((part) => part !== "")
         .join(" and ");
      notes.push(
         maybe.length === ran.length
            ? `This page can't tell which givens this ${subject} reads. Where it reads one, it ran with ${had}.`
            : `This page can't tell whether this ${subject} reads ${wordList(maybe.map(labelOf), "or")}. Where it does, it ran with ${had}.`,
      );
   }
   return notes.length > 0
      ? { kind: "noted", note: notes.join(" ") }
      : { kind: "ok" };
}

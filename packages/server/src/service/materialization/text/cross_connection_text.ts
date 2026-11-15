// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/**
 * Joins a served copy moves across connections (question 11).
 *
 * Malloy refuses a join whose source is on another connection than the source
 * it joins into (`join-connection-mismatch`), and it
 * refuses it for the whole file. A served copy re-roots a source onto its
 * storage destination, and a reference join it keeps may read a source that
 * is not stored there. The served copy leaves that join out, so the queries
 * that do not use it still read the table, and a query that does skips it
 * with reason `cross_connection`. An author's own file that joins sources on
 * two origin connections is not rescued: it fails to load with Malloy's own
 * diagnostic.
 *
 * The text operation: blank out the join's definition, compile again, and
 * blank out each definition that then fails because it read what was left out
 * (a dimension or measure through the join, a view over that measure, a join
 * keyed on it), until the text compiles. Blanking keeps every other
 * character's line and column, so the compiled locations of everything kept
 * still index the author's file.
 *
 * Only a refusal of exactly this kind starts it, and nothing else is ever left
 * out: the first compile may report, beside the refused joins, only problems
 * that follow from them (an importer that cannot find a source whose file
 * failed), and each of those must be gone once the joins are out. A file with
 * any other error is left to fail with its own error. Later passes leave out
 * only definitions that fail for reading something missing; any other problem
 * stops it.
 *
 * There is no compiler facility for this: Malloy has no "compile without these
 * joins", and a file that does not compile has no IR to read ranges from, so
 * the edit is made on the text, with the rewriters' lexer (malloy_lexer.ts).
 *
 * Read by the served copy (`serve/`), which compiles through what this is
 * handed; it never itself reads a file.
 */

import { MalloyError } from "@malloydata/malloy";
import { offsetOf } from "./malloy_lexer";
import {
   type Cut,
   definitionCutAt,
   type LeftOutDefinition,
} from "./definition_cut";

/** Malloy's problem code for a join into a source on another connection. */
export const JOIN_CONNECTION_MISMATCH = "join-connection-mismatch";

/**
 * Malloy's codes for a definition that fails because it reads something
 * missing, directly or through another definition that failed: what leaving a
 * join out causes, and the only problems a pass leaves a definition out for.
 */
const FOLLOWS_FROM_LEFT_OUT: ReadonlySet<string> = new Set([
   "field-not-found",
   "source-not-found",
   "source-or-query-not-found",
   "aggregate-source-not-found",
   "aggregate-of-scalar",
   "scalar-in-measure",
   "ungroup-of-non-aggregate",
]);

/**
 * Malloy's codes for a statement in an importer that fails because the file
 * it imports failed: an `export` of a name that file no longer defines. Each
 * follows from the refused joins on the first compile and must be gone once
 * they are out; a pass never leaves one out, since an `export` is not a
 * definition and no cut of the text can place it.
 */
const FOLLOWS_FROM_FAILED_IMPORT: ReadonlySet<string> = new Set([
   "export-name-not-defined",
]);

/** Whether a problem's code is Malloy refusing a join across connections. */
export function isJoinConnectionMismatch(code: string | undefined): boolean {
   return code === JOIN_CONNECTION_MISMATCH;
}

/**
 * Whether a problem's code is one a join left out across connections can
 * cause in what reads it ({@link FOLLOWS_FROM_LEFT_OUT}).
 */
export function mayFollowFromLeftOut(code: string | undefined): boolean {
   return code !== undefined && FOLLOWS_FROM_LEFT_OUT.has(code);
}

/**
 * Whether `error` is Malloy refusing joins across connections, and nothing
 * else but what may follow from that refusal (see the file's doc).
 */
export function isCrossConnectionRefusal(error: unknown): boolean {
   const problems = errorProblems(error);
   return (
      problems !== undefined &&
      problems.some((p) => p.code === JOIN_CONNECTION_MISMATCH) &&
      problems.every(
         (p) =>
            p.code === JOIN_CONNECTION_MISMATCH ||
            (p.code !== undefined &&
               (FOLLOWS_FROM_LEFT_OUT.has(p.code) ||
                  FOLLOWS_FROM_FAILED_IMPORT.has(p.code))),
      )
   );
}

/** A problem's identity across compiles of the same text: where, and what. */
function problemKey(p: ProblemLike): string {
   const start = p.at?.range?.start;
   return `${p.at?.url}\u0000${start?.line}:${start?.character}\u0000${p.code}`;
}

interface ProblemLike {
   code?: string;
   severity?: string;
   at?: {
      url?: string;
      range?: { start: { line: number; character: number } };
   };
}

/** A compile failure's error-severity problems, or undefined for a non-Malloy error. */
export function errorProblems(error: unknown): ProblemLike[] | undefined {
   if (!(error instanceof MalloyError)) return undefined;
   return (error.problems as ProblemLike[]).filter(
      (p) => (p.severity ?? "error") === "error",
   );
}

/**
 * Compile with joins across connections left out. `firstError` is the failure
 * of a compile with `overlay` as given; when it is anything but Malloy refusing
 * joins across connections, nothing is left out and the result is undefined.
 * Otherwise each pass blanks the definitions the last failure points at and
 * compiles again, through `compile` (which throws the compiler's error). The
 * result holds the overlay that compiled and what it left out; undefined when
 * no number of passes compiles, or a failure points at something that is not a
 * definition (a `where:`, a `primary_key:`).
 */
export async function leaveOutCrossConnectionJoins(args: {
   firstError: unknown;
   /** Files already served in place of the ones on disk, by URL. */
   overlay?: ReadonlyMap<string, string>;
   /** A file's text by URL, when the overlay does not hold it. */
   readText: (url: string) => string | undefined;
   compile: (overlay: ReadonlyMap<string, string>) => Promise<void>;
   maxPasses?: number;
}): Promise<
   { overlay: Map<string, string>; leftOut: LeftOutDefinition[] } | undefined
> {
   if (!isCrossConnectionRefusal(args.firstError)) return undefined;
   const overlay = new Map(args.overlay ?? []);
   const leftOut: LeftOutDefinition[] = [];
   const first = errorProblems(args.firstError) ?? [];
   // What the first compile reported beside the refused joins must follow
   // from them (an importer that could not find a source whose file failed),
   // so it is never left out itself: each pass leaves out what fails anew,
   // and once nothing else fails, one still there is the author's own error
   // and the file is left to fail with it.
   const followOn = new Set(
      first.filter((p) => p.code !== JOIN_CONNECTION_MISMATCH).map(problemKey),
   );
   let problems = first;
   const maxPasses = args.maxPasses ?? 8;
   for (let pass = 0; pass < maxPasses; pass++) {
      // The refused joins go first, alone.
      const refusedJoins = problems.filter(
         (p) => p.code === JOIN_CONNECTION_MISMATCH,
      );
      const toCut =
         refusedJoins.length > 0
            ? refusedJoins
            : problems.filter((p) => !followOn.has(problemKey(p)));
      if (
         toCut.length === 0 ||
         toCut.some(
            (p) =>
               p.code !== JOIN_CONNECTION_MISMATCH &&
               (p.code === undefined || !FOLLOWS_FROM_LEFT_OUT.has(p.code)),
         )
      ) {
         return undefined;
      }
      // Every cut of a pass is found in the same text, before any is applied,
      // so two errors in one definition leave it out once.
      const byUrl = new Map<string, ProblemLike[]>();
      for (const p of toCut) {
         const url = p.at?.url;
         if (!url || !p.at?.range) return undefined;
         const list = byUrl.get(url) ?? [];
         list.push(p);
         byUrl.set(url, list);
      }
      for (const [url, list] of byUrl) {
         const text = overlay.get(url) ?? args.readText(url);
         if (text === undefined) return undefined;
         const cuts: Cut[] = [];
         for (const p of list) {
            const offset = offsetOf(
               text,
               p.at?.range?.start ?? {
                  line: -1,
                  character: 0,
               },
            );
            if (offset < 0) return undefined;
            const cut = definitionCutAt(text, offset);
            if (!cut) return undefined;
            if (cuts.some((c) => c.start === cut.start && c.end === cut.end)) {
               continue;
            }
            cuts.push(cut);
            leftOut.push({
               url,
               ...cut.left,
               crossConnection: p.code === JOIN_CONNECTION_MISMATCH,
            });
         }
         overlay.set(url, blank(text, cuts));
      }
      try {
         await args.compile(overlay);
         return { overlay, leftOut };
      } catch (error) {
         const next = errorProblems(error);
         if (!next || next.length === 0) return undefined;
         problems = next;
      }
   }
   return undefined;
}

/** `text` with each cut's characters turned to spaces, newlines kept. */
function blank(text: string, cuts: readonly Cut[]): string {
   let out = text;
   for (const c of [...cuts].sort((a, b) => b.start - a.start)) {
      out =
         out.slice(0, c.start) +
         out.slice(c.start, c.end).replace(/[^\n]/g, " ") +
         out.slice(c.end);
   }
   return out;
}

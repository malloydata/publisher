// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/**
 * Joins a served copy moves across connections.
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

/**
 * The two connections Malloy's refusal of a join across connections names:
 * `Cannot join 'x', which is on connection 'a', into a source on connection
 * 'b'`. Undefined for a message in any other words.
 */
export function refusedJoinConnections(
   message: string | undefined,
): { joined: string; into: string } | undefined {
   const named =
      /which is on connection '([^']*)', into a source on connection '([^']*)'/.exec(
         message ?? "",
      );
   return named ? { joined: named[1], into: named[2] } : undefined;
}

/**
 * The two connections each join refusal in `error` names, when `error` is
 * Malloy refusing joins across connections ({@link isCrossConnectionRefusal});
 * undefined otherwise, or when a refusal's words name no two connections.
 */
export function refusedJoinPairs(
   error: unknown,
): { joined: string; into: string }[] | undefined {
   if (!isCrossConnectionRefusal(error)) return undefined;
   const pairs: { joined: string; into: string }[] = [];
   for (const p of errorProblems(error) ?? []) {
      if (p.code !== JOIN_CONNECTION_MISMATCH) continue;
      const pair = refusedJoinConnections(p.message);
      if (!pair) return undefined;
      pairs.push(pair);
   }
   return pairs;
}

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
   message?: string;
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
   // A refused join that cannot be left out alone (what reads it sits where
   // no definition can be cut, or the text does not compile without it)
   // costs the source or query declaring it, and only that one: every other
   // refused join is still left out alone, so a source whose join can be
   // cut keeps serving beside one that cannot. Each attempt that is blocked
   // names the declaration that blocked it, which the next attempt leaves
   // out whole; a block no declaration with a refused join answers falls
   // back to leaving out every declaring source whole.
   const escalated = new Set<string>();
   for (let attempt = 0; attempt < 8; attempt++) {
      const result = await leaveOut(args, escalated);
      if ("overlay" in result) return result;
      const key = result.blocked;
      if (key === undefined || escalated.has(key)) break;
      escalated.add(key);
   }
   const all = await leaveOut(args, "all");
   return "overlay" in all ? all : undefined;
}

/** A top-level declaration's identity: its file and where it starts. */
function declarationKey(url: string, start: number): string {
   return `${url}\u0000${start}`;
}

/**
 * One attempt of {@link leaveOutCrossConnectionJoins}: each refused join cut
 * as its own definition, or, in a declaration `escalated` names (every one,
 * for `all`), as the top-level source or query declaring it. When it does
 * not compile, `blocked` names the declaration holding what stopped it, if
 * that declaration declares a refused join.
 */
async function leaveOut(
   args: Parameters<typeof leaveOutCrossConnectionJoins>[0],
   escalated: ReadonlySet<string> | "all",
): Promise<
   | { overlay: Map<string, string>; leftOut: LeftOutDefinition[] }
   | { blocked?: string }
> {
   // The declarations that declare a refused join, by key.
   const declaring = new Set<string>();
   // Where an attempt stopped, as the declaration around it (if it declares
   // a refused join).
   const blockedAt = (
      url: string | undefined,
      text: string | undefined,
      offset: number,
   ): { blocked?: string } => {
      if (!url || text === undefined || offset < 0) return {};
      const around = definitionCutAt(text, offset, true);
      if (!around) return {};
      const key = declarationKey(url, around.start);
      return declaring.has(key) ? { blocked: key } : {};
   };
   const textOf = (url: string | undefined): string | undefined =>
      url === undefined ? undefined : (overlay.get(url) ?? args.readText(url));
   const offsetIn = (p: ProblemLike): number => {
      const text = textOf(p.at?.url);
      return text === undefined
         ? -1
         : offsetOf(text, p.at?.range?.start ?? NOWHERE);
   };
   const overlay = new Map(args.overlay ?? []);
   const leftOut: LeftOutDefinition[] = [];
   const first = errorProblems(args.firstError) ?? [];
   // What a compile reports beside refused joins must follow from them (an
   // importer that could not find a source whose file failed), so it is
   // never left out itself: each pass leaves out what fails anew, and once
   // nothing else fails, one still there is the author's own error and the
   // file is left to fail with it. Reset by each compile that reports refused
   // joins: an imported file's refusal hides its importer's own refusals
   // behind what follows from the failed import, and those follow-ons are
   // answered by the importer's joins, refused only on a later pass.
   let reported: ProblemLike[] = [];
   let followOn = new Set<string>();
   let problems = first;
   const maxPasses = args.maxPasses ?? 8;
   for (let pass = 0; pass < maxPasses; pass++) {
      // The refused joins go first, alone.
      const refusedJoins = problems.filter(
         (p) => p.code === JOIN_CONNECTION_MISMATCH,
      );
      if (refusedJoins.length > 0) {
         reported = problems.filter((p) => p.code !== JOIN_CONNECTION_MISMATCH);
         followOn = new Set(reported.map(problemKey));
      }
      const toCut =
         refusedJoins.length > 0
            ? refusedJoins
            : problems.filter((p) => !followOn.has(problemKey(p)));
      if (toCut.length === 0) return {};
      const notFollowing = toCut.find(
         (p) =>
            p.code !== JOIN_CONNECTION_MISMATCH &&
            (p.code === undefined || !FOLLOWS_FROM_LEFT_OUT.has(p.code)),
      );
      if (notFollowing) {
         return blockedAt(
            notFollowing.at?.url,
            textOf(notFollowing.at?.url),
            offsetIn(notFollowing),
         );
      }
      // Every cut of a pass is found in the same text, before any is applied,
      // so two errors in one definition leave it out once.
      const byUrl = new Map<string, ProblemLike[]>();
      for (const p of toCut) {
         const url = p.at?.url;
         if (!url || !p.at?.range) return {};
         const list = byUrl.get(url) ?? [];
         list.push(p);
         byUrl.set(url, list);
      }
      for (const [url, list] of byUrl) {
         const text = overlay.get(url) ?? args.readText(url);
         if (text === undefined) return {};
         const cuts: Cut[] = [];
         for (const p of list) {
            const offset = offsetOf(
               text,
               p.at?.range?.start ?? {
                  line: -1,
                  character: 0,
               },
            );
            if (offset < 0) return {};
            const refused = p.code === JOIN_CONNECTION_MISMATCH;
            const own = definitionCutAt(text, offset);
            const declaration = refused
               ? definitionCutAt(text, offset, true)
               : undefined;
            if (declaration) {
               declaring.add(declarationKey(url, declaration.start));
            }
            const cut =
               declaration &&
               (escalated === "all" ||
                  escalated.has(declarationKey(url, declaration.start)))
                  ? declaration
                  : own;
            if (!cut) return blockedAt(url, text, offset);
            // A declaration left out whole must not take with it a problem
            // its compile reported beside the refusal: that one must be gone
            // because the join is out, not because its text is.
            if (
               cut !== own &&
               reported.some((q) => {
                  if (q.at?.url !== url) return false;
                  const at = offsetOf(text, q.at?.range?.start ?? NOWHERE);
                  return at >= cut.start && at < cut.end;
               })
            ) {
               return {};
            }
            if (cuts.some((c) => c.start === cut.start && c.end === cut.end)) {
               continue;
            }
            cuts.push(cut);
            const connections = refused
               ? refusedJoinConnections(p.message)
               : undefined;
            leftOut.push({
               url,
               ...cut.left,
               crossConnection: refused,
               ...(connections ? { connections } : {}),
               ...(cut !== own && own?.left.keyword.startsWith("join_")
                  ? { join: own.left.name }
                  : {}),
            });
         }
         overlay.set(url, blank(text, wholeStatements(cuts)));
      }
      try {
         await args.compile(overlay);
         return { overlay, leftOut };
      } catch (error) {
         const next = errorProblems(error);
         if (!next || next.length === 0) return {};
         problems = next;
      }
   }
   return {};
}

/**
 * `cuts` with every set that leaves out each definition of one statement
 * (`join_one: a is …, b is …`, both refused) replaced by that statement
 * whole: blanking each alone would leave its keyword with nothing after it.
 */
function wholeStatements(cuts: readonly Cut[]): Cut[] {
   const byStatement = new Map<number, Cut[]>();
   for (const c of cuts) {
      if (!c.statement) continue;
      const list = byStatement.get(c.statement.start) ?? [];
      list.push(c);
      byStatement.set(c.statement.start, list);
   }
   const out = cuts.filter((c) => !c.statement);
   for (const list of byStatement.values()) {
      const { start, end, definitions } = list[0].statement!;
      out.push(
         ...(list.length >= definitions
            ? [{ start, end, left: list[0].left }]
            : list),
      );
   }
   return out;
}

/** A position no text holds, for a problem that names none. */
const NOWHERE = { line: -1, character: 0 };

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

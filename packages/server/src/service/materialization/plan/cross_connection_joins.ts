// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Joins across connections (materialization v2, phase 3: ms2data/service#6982
 * and the plan's question 11).
 *
 * Malloy refuses a join whose source is on another connection than the source
 * it joins into (`join-connection-mismatch`, malloydata/malloy#3032), and it
 * refuses it for the whole file. Two cases reach that refusal:
 *
 * - The author's own file joins sources from two origin connections (#6982).
 *   It never compiles live. With both sides stored in one destination the
 *   served copy compiles, so builds are planned, and the model loads, from the
 *   author's file with those joins left out (the "build view").
 * - A served copy re-roots a source onto its storage destination, and a
 *   reference join it keeps reads a source that is not stored there (question
 *   11). The served copy leaves that join out, so the queries that do not use
 *   it still read the table, and a query that does skips it with reason
 *   `cross_connection`.
 *
 * Either way the same text operation applies: blank out the join's definition,
 * compile again, and blank out each definition that then fails because it read
 * what was left out (a dimension or measure through the join, a view over that
 * measure, a join keyed on it), until the text compiles. Blanking keeps every
 * other character's line and column, so the compiled locations of everything
 * kept still index the author's file.
 *
 * Only a refusal of exactly this kind starts it, and nothing else is ever left
 * out: the first compile may report, beside the refused joins, only problems
 * that follow from them (an importer that cannot find a source whose file
 * failed), and each of those must be gone once the joins are out. A file with
 * any other error is left to fail as it does today. Later passes leave out
 * only definitions that fail for reading something missing; any other problem
 * stops it.
 *
 * There is no compiler facility for this: Malloy has no "compile without these
 * joins", and a file that does not compile has no IR to read ranges from, so
 * the edit is made on the text, with the rewriters' lexer (../malloy_lexer.ts).
 */

import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { MalloyError } from "@malloydata/malloy";
import { getMaterializeEnabled } from "../../../config";
import { overlayKey } from "../../overlay_key";
import {
   isName,
   lexAll,
   offsetOf,
   statementHead,
   statementStarts,
   type Token,
} from "../malloy_lexer";

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

/** The statements a definition can be left out of. */
const DEFINITION_KEYWORDS = new Set([
   "dimension",
   "measure",
   "join_one",
   "join_many",
   "join_cross",
   "view",
   "source",
   "query",
]);

/** One definition left out of a file. */
export interface LeftOutDefinition {
   /** The file it was left out of, as the compiler names it. */
   url: string;
   /** Its statement's keyword: `join_many`, `dimension`, `view`, `source`, … */
   keyword: string;
   /** The definition's name. */
   name: string;
   /** The top-level source or query it sits in, when it is not one itself. */
   declaration?: string;
   /** True for a join Malloy refused across connections; false for what read it. */
   crossConnection: boolean;
}

/** A half-open character range of a file to blank. */
interface Cut {
   start: number;
   end: number;
   left: Omit<LeftOutDefinition, "url" | "crossConnection">;
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
            (p.code !== undefined && FOLLOWS_FROM_LEFT_OUT.has(p.code)),
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

/** A statement: its keyword token, its colon, and where it ends. */
interface Statement {
   keyword: Token;
   colon: Token;
   /** Offset its text ends at (exclusive): the next statement or its block's close. */
   end: number;
}

/** Every statement in `tokens` (`<word>:` at any depth), with its extent. */
function statementsOf(text: string, tokens: readonly Token[]): Statement[] {
   const out: Statement[] = [];
   for (const k of statementStarts(tokens)) {
      const keyword = tokens[k];
      const d = keyword.depth;
      let end = text.length;
      for (let j = k + 2; j < tokens.length; j++) {
         const t = tokens[j];
         if (t.depth < d || (t.kind === "close" && t.depth === d - 1)) {
            end = t.start;
            break;
         }
         if (
            t.depth === d &&
            t.kind === "word" &&
            tokens[j + 1]?.text === ":" &&
            tokens[j + 1].start === t.end &&
            tokens[j + 2]?.text !== ":"
         ) {
            // The next statement's access label is its own (statementHead).
            end = statementHead(tokens, j).start;
            break;
         }
      }
      out.push({ keyword, colon: tokens[k + 1], end });
   }
   return out;
}

/**
 * The definition at `offset`: the innermost definition statement around it
 * (`dimension:`, `measure:`, a join, `view:`, or a top-level `source:` or
 * `query:`), and within it the one `<name> is …` (or `<name> with …`, `<name>
 * on …`) that holds the offset. Undefined when no such statement holds it.
 */
export function definitionCutAt(text: string, offset: number): Cut | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const statements = statementsOf(text, tokens);
   const holding = statements.filter(
      (s) =>
         DEFINITION_KEYWORDS.has(s.keyword.text) &&
         s.keyword.start <= offset &&
         offset < s.end &&
         // `source:` and `query:` are definitions only at the top level.
         (s.keyword.depth === 0 ||
            (s.keyword.text !== "source" && s.keyword.text !== "query")),
   );
   if (holding.length === 0) return undefined;
   const stmt = holding.reduce((a, b) =>
      b.keyword.depth > a.keyword.depth ||
      (b.keyword.depth === a.keyword.depth && b.keyword.start > a.keyword.start)
         ? b
         : a,
   );
   const d = stmt.keyword.depth;
   const body = tokens.filter(
      (t) => t.depth === d && t.start > stmt.colon.start && t.start < stmt.end,
   );
   const inside = tokens.filter(
      (t) => t.start > stmt.colon.start && t.start < stmt.end,
   );
   if (body.length === 0 || inside.length === 0) return undefined;
   const stmtLast = inside[inside.length - 1];
   // A definition opens the statement, follows a comma, or starts a new line
   // as `<name> is|with|on`.
   const starts: number[] = [];
   for (let k = 0; k < body.length; k++) {
      const t = body[k];
      const prev = body[k - 1];
      const next = body[k + 1];
      if (k === 0) {
         starts.push(k);
      } else if (prev?.text === "," && isName(t)) {
         starts.push(k);
      } else if (
         isName(t) &&
         next?.kind === "word" &&
         (next.text === "is" || next.text === "with" || next.text === "on") &&
         text.slice(prev.end, t.start).includes("\n")
      ) {
         starts.push(k);
      }
   }
   const defs = starts.map((k, i) => {
      const from = body[k].start;
      const to = i + 1 < starts.length ? body[starts[i + 1]].start : stmt.end;
      const toks = inside.filter((t) => t.start >= from && t.start < to);
      // The separating comma belongs between definitions, not to one.
      const comma =
         toks[toks.length - 1]?.text === "," && i + 1 < starts.length;
      const last = comma ? toks[toks.length - 2] : toks[toks.length - 1];
      return {
         name: body[k],
         from,
         lastEnd: last?.end ?? body[k].end,
         // Through the comma after it, when one separates it from the next.
         fullEnd: toks[toks.length - 1]?.end ?? body[k].end,
      };
   });
   const at = defs.findIndex(
      (def, i) =>
         def.from <= offset &&
         (i + 1 < defs.length ? offset < defs[i + 1].from : true),
   );
   // An offset on the keyword itself is the statement's first definition.
   const index = at < 0 ? 0 : at;
   const def = defs[index];
   const name = def.name.text.startsWith("`")
      ? def.name.text.slice(1, -1)
      : def.name.text;
   const declaration =
      d === 0 ? undefined : enclosingDeclaration(statements, tokens, offset);
   const left = {
      keyword: stmt.keyword.text,
      name,
      ...(declaration && declaration !== name ? { declaration } : {}),
   };
   if (defs.length === 1) {
      // The whole statement, with its access label and the annotation lines
      // written on it.
      const head = statementHead(tokens, tokens.indexOf(stmt.keyword));
      const before = tokens.filter((t) => t.end <= head.start);
      return {
         start: annotationsStart(
            text,
            before[before.length - 1]?.end ?? 0,
            head.start,
         ),
         end: stmtLast.end,
         left,
      };
   }
   if (index === 0) {
      // From the colon, so its own annotation lines go with it, through its
      // comma; the next definition keeps the annotation lines written on it.
      return { start: stmt.colon.end, end: def.fullEnd, left };
   }
   // From the end of the definition before it (taking the comma between them,
   // and this one's annotation lines).
   return { start: defs[index - 1].lastEnd, end: def.lastEnd, left };
}

/**
 * Where the annotation lines written on a statement at `keyword` begin: the
 * run of `#` lines (and blank lines) right above it, no further back than
 * `floor`. A `##` model annotation is the file's, not the statement's, so the
 * run stops at one; nothing on the statement's own line before it moves.
 */
function annotationsStart(
   text: string,
   floor: number,
   keyword: number,
): number {
   const lineStart = text.lastIndexOf("\n", keyword - 1) + 1;
   if (lineStart < floor || text.slice(lineStart, keyword).trim() !== "") {
      return keyword;
   }
   let start = lineStart;
   while (start > floor) {
      const prevStart = text.lastIndexOf("\n", start - 2) + 1;
      if (prevStart < floor) break;
      const line = text.slice(prevStart, start - 1).trim();
      if (line !== "" && !(line.startsWith("#") && !line.startsWith("##"))) {
         break;
      }
      start = prevStart;
   }
   return start;
}

/** The name of the top-level `source:` or `query:` holding `offset`. */
function enclosingDeclaration(
   statements: readonly Statement[],
   tokens: readonly Token[],
   offset: number,
): string | undefined {
   const top = statements.find(
      (s) =>
         s.keyword.depth === 0 &&
         (s.keyword.text === "source" || s.keyword.text === "query") &&
         s.keyword.start <= offset &&
         offset < s.end,
   );
   if (!top) return undefined;
   const name = tokens.find((t) => t.start > top.colon.start && isName(t));
   if (!name) return undefined;
   return name.text.startsWith("`") ? name.text.slice(1, -1) : name.text;
}

/**
 * A file's text by URL, as a compile read it: `overlay`'s (keyed by
 * {@link overlayKey}) when it holds the file, else the file on disk.
 */
export function compiledFileText(
   url: string,
   overlay?: ReadonlyMap<string, string>,
): string | undefined {
   const held = overlay?.get(overlayKey(url));
   if (held !== undefined) return held;
   try {
      return readFileSync(fileURLToPath(url), "utf8");
   } catch {
      return undefined;
   }
}

/**
 * Load a model whose files join sources across connections (#6982): with the
 * materialize switch on, `load` again with those joins, and what reads them,
 * left out (the build view), so builds are planned from it and the model
 * serves every query that does not need them. `firstError` is the failure of
 * `load` with no overlay; `readText` gives each file's text as that compile
 * read it, which the problems' positions index. Undefined when the switch is
 * off, it is any other failure, or the build view does not compile either;
 * the caller then reports `firstError`.
 *
 * The one entry point for every load of an author's model: the package-load
 * worker, `Model.create` and the main-thread build plan, so discovery, gates,
 * the build plan and live all read the same build view.
 */
export async function loadLeavingOutCrossConnectionJoins<T>(args: {
   firstError: unknown;
   readText: (url: string) => string | undefined;
   load: (overlay: ReadonlyMap<string, string>) => Promise<T>;
}): Promise<
   | { result: T; overlay: Map<string, string>; leftOut: LeftOutDefinition[] }
   | undefined
> {
   if (!getMaterializeEnabled()) return undefined;
   let result: T | undefined;
   let loaded = false;
   const kept = await leaveOutCrossConnectionJoins({
      firstError: args.firstError,
      readText: args.readText,
      compile: async (overlay) => {
         result = await args.load(overlay);
         loaded = true;
      },
   }).catch(() => undefined);
   if (!kept || !loaded) return undefined;
   return { result: result as T, overlay: kept.overlay, leftOut: kept.leftOut };
}

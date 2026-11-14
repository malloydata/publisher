// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where one definition lies in a Malloy file's text, so a rewrite can leave it
 * out: the innermost definition statement around an offset, and within it the
 * one `<name> is …` holding the offset, with its annotations.
 *
 * `definitionCutAt` is read by the cross-connection join blanking
 * (plan/cross_connection_joins.ts) and the inlined-join rewrite
 * (inlined_join_text.ts). A cut is undefined when no definition holds the
 * offset or the text does not lex; never a guess.
 *
 * Pure.
 */

import {
   isName,
   lexAll,
   statementHead,
   statementStarts,
   type Token,
} from "./malloy_lexer";

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
export interface Cut {
   start: number;
   end: number;
   left: Omit<LeftOutDefinition, "url" | "crossConnection">;
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The textual reading of a dashboard file that the reader and the writer
 * share: where declarations are, and how a tile expression splits.
 *
 * Text, not the parser's symbol tree, because the tree is unreliable inside a
 * source — measured, a refinement spelled `+ { limit: 5, where: … }` makes it
 * report the refined view's base as a child and drop the next declaration —
 * and does not cover `given:` at all. A `<keyword>: <name> is` line under a
 * `source: <name> is` line is unambiguous, and Malloy has no nested sources
 * to confuse it. Both sides reading the same lines the same way is also what
 * lets the writer patch exactly what the reader read.
 */
const IDENT = "[A-Za-z_][A-Za-z0-9_]*";

export const isIdentifier = (text: string) =>
   new RegExp(`^${IDENT}$`).test(text);

/** A one-line `<keyword>: <name> is <rest>` declaration. */
export interface DeclarationAt {
   line: number;
   /** Everything after `is`, trimmed: a base view, an expression, or `{`. */
   rest: string;
}

/**
 * The `<keyword>:` declarations under `source: <owner>`, in file order. The
 * body of a source ends at the next top-level declaration or `##` line.
 */
export function declarationsUnder(
   lines: string[],
   owner: string,
   keyword: "view" | "dimension",
): Map<string, DeclarationAt> {
   const found = new Map<string, DeclarationAt>();
   const declaration = new RegExp(
      `^\\s*${keyword}:\\s*(${IDENT})\\s+is\\b\\s*(.*)$`,
   );
   let current: string | undefined;
   for (let line = 0; line < lines.length; line++) {
      const text = lines[line];
      const source = new RegExp(`^\\s*source:\\s*(${IDENT})\\s+is\\b`).exec(
         text,
      );
      if (source) {
         current = source[1];
         continue;
      }
      if (/^\s*(query|run|import|given)\b/.test(text) || text.startsWith("##"))
         current = undefined;
      if (current !== owner) continue;
      const m = declaration.exec(text);
      if (m) found.set(m[1], { line, rest: m[2].trim() });
   }
   return found;
}

/**
 * The extent of the declaration starting at `line`: the last line of its body
 * for one that opens a `{ … }` block, or `line` itself for one that never does
 * (`view: x is y`). `opened` says which; `unreadable` replaces both when the
 * scan cannot trust its own brace count, which only a `"""` span can cause.
 */
export type DeclarationExtent =
   | { end: number; opened: boolean }
   | { unreadable: string };

/**
 * Scans forward from `line`, tracking `{`/`}` depth and `"""` spans so a
 * declaration's body is found even when its block opens on a later line — a
 * source whose base is a multi-line `duckdb.sql("""…""")` literal is the case
 * that matters: the literal's own line carries no `{`, and the one that follows
 * is still SQL text, not the block.
 *
 * A `#`/`//` line is skipped outright rather than scanned for braces: a `#
 * drill { to=… }` tag or a `# label="…"` comment sits between one declaration
 * and the next, and counting its brace would attribute it to the wrong one —
 * or, for a blockless declaration such as `view: x is y`, extend the scan past
 * where it should have stopped.
 *
 * Past the start line, a line beginning `<word>:` while no block has opened
 * yet is the NEXT declaration, not more of this one — checked before that
 * line's own braces are counted, so `view: c is d + { … }` right after a
 * blockless `view: b is a` is never mistaken for `b`'s body. The same
 * reasoning covers running off the end of the block that CONTAINS this
 * declaration: a bare `}` seen before any `{` of our own belongs to that
 * enclosing block, not to us, so it stops the scan rather than being counted.
 *
 * A `{` found inside a `"""` span is refused rather than guessed at: Malloy's
 * dashboard grammar has never asked this scan to look inside a SQL literal,
 * and a brace there (`select {'region': 'West'} as s`) cannot be told apart
 * from the block this scan is hunting for. Getting it wrong here is the one
 * case the round-trip gate cannot catch, because a `view:` spliced into the
 * literal reads back as a `view:` under the source above it either way.
 */
export function declarationExtent(
   lines: string[],
   line: number,
): DeclarationExtent {
   let depth = 0;
   let opened = false;
   let tripleQuote = false;
   let lastLine = -1;
   for (let i = line; i < lines.length; i++) {
      const raw = lines[i];
      // The code text to brace-scan for this line: everything but a trailing
      // `//` comment, and everything but the `"""` string content when a span
      // opened on an earlier line. `splitTrailingComment` is single-line, so a
      // span already open at the start of this line has to be closed first —
      // running it on the raw line would read SQL text as code.
      let scan: string;
      if (tripleQuote) {
         const close = raw.indexOf('"""');
         const stringContent = close < 0 ? raw : raw.slice(0, close);
         if (stringContent.includes("{") || stringContent.includes("}"))
            return {
               unreadable:
                  `line ${i + 1} holds a brace inside a """ string, which ` +
                  `this scan cannot tell apart from the block it is looking for`,
            };
         if (close < 0) {
            lastLine = i;
            continue;
         }
         tripleQuote = false;
         scan = splitTrailingComment(raw.slice(close + 3)).code;
      } else {
         const trimmed = raw.trim();
         // A blank line separating declarations is not part of either one's
         // body — skipped like a tag or comment, so it is never swallowed into
         // a blockless declaration's reported end.
         if (
            trimmed === "" ||
            trimmed.startsWith("#") ||
            trimmed.startsWith("//")
         )
            continue;
         // Only outside a `"""` span: inside one, every line is string content,
         // and SQL reaches for `<word>:` readily enough — `file:///data/orders.csv`
         // in a `read_csv(…)` reads as the next declaration and ends the source
         // at its first line.
         if (i > line && !opened && /^\s*[a-z_]+:/.test(raw))
            return { end: lastLine, opened: false };
         scan = splitTrailingComment(raw).code;
      }

      let pos = 0;
      let overshoot = false;
      while (pos < scan.length) {
         if (tripleQuote) {
            const close = scan.indexOf('"""', pos);
            const segment =
               close < 0 ? scan.slice(pos) : scan.slice(pos, close);
            if (segment.includes("{") || segment.includes("}"))
               return {
                  unreadable:
                     `line ${i + 1} holds a brace inside a """ string, which ` +
                     `this scan cannot tell apart from the block it is looking for`,
               };
            if (close < 0) {
               pos = scan.length;
               break;
            }
            tripleQuote = false;
            pos = close + 3;
            continue;
         }
         if (scan.startsWith('"""', pos)) {
            tripleQuote = true;
            pos += 3;
            continue;
         }
         const ch = scan[pos];
         if (ch === "{") {
            depth++;
            opened = true;
         } else if (ch === "}") {
            if (!opened) {
               overshoot = true;
               break;
            }
            depth--;
         }
         pos++;
      }
      if (overshoot) return { end: lastLine, opened: false };
      lastLine = i;
      if (opened && depth <= 0) return { end: i, opened: true };
   }
   return { end: lastLine, opened };
}

/**
 * The body's FIRST stage: the lines from `declLine` through the matching
 * close of the `{ … }` that follows `is`, and the depth-1 `where:` lines
 * inside it — the only lines a builder-managed binding can occupy (a `nest:`'s
 * own `where:` is depth 2 and never appears here).
 *
 * Stops the instant that brace closes, even when the same line goes on to
 * reopen one (`} -> { …`, a second pipeline stage): a plain running depth
 * count would read straight through a same-line reopen and misread a second
 * stage's own `where:` as the tile's. `extentEnd` bounds the scan and is
 * {@link declarationExtent}'s own `end`; the caller has already used it to
 * confirm no brace hides inside a `"""` span in this declaration, so this
 * scan does not repeat that check.
 *
 * `oneLiner` is set when the first stage opens and closes on `declLine`
 * itself (`view: x is { aggregate: n is count() }`) — there is no separate
 * line to add or patch a binding on, so the caller rewrites the braces'
 * content as a whole instead of a line.
 *
 * `more` is set when text follows the first stage that this function does not
 * cover: a `->` second stage, or a `{ … } + { … }` compound refinement.
 * Either makes "the first stage" an ambiguous place to write a binding, which
 * is for the caller to refuse.
 */
export interface ViewBodyStage1 {
   end: number;
   whereLines: Array<{ line: number; code: string }>;
   oneLiner: { openCol: number; closeCol: number; content: string } | undefined;
   more: boolean;
}

export function viewBodyStage1(
   lines: string[],
   declLine: number,
   extentEnd: number,
): ViewBodyStage1 {
   let depth = 0;
   let openAt = -1;
   const whereLines: Array<{ line: number; code: string }> = [];
   for (let i = declLine; i <= extentEnd && i < lines.length; i++) {
      const raw = lines[i];
      const trimmed = raw.trim();
      if (trimmed === "" || trimmed.startsWith("#") || trimmed.startsWith("//"))
         continue;
      const startDepth = depth;
      const { code } = splitTrailingComment(raw);
      let closedAt = -1;
      for (let pos = 0; pos < code.length; pos++) {
         const ch = code[pos];
         if (ch === "{") {
            if (depth === 0 && openAt < 0 && i === declLine) openAt = pos;
            depth++;
         } else if (ch === "}") {
            depth--;
            if (depth === 0) {
               closedAt = pos;
               break;
            }
         }
      }
      if (closedAt >= 0) {
         const oneLiner =
            i === declLine && openAt >= 0
               ? {
                    openCol: openAt,
                    closeCol: closedAt,
                    content: raw.slice(openAt + 1, closedAt),
                 }
               : undefined;
         const afterClose = splitTrailingComment(raw.slice(closedAt + 1)).code;
         let more = afterClose.trim().length > 0;
         if (!more) {
            for (let j = i + 1; j < lines.length; j++) {
               const nextTrimmed = lines[j].trim();
               if (
                  nextTrimmed === "" ||
                  nextTrimmed.startsWith("#") ||
                  nextTrimmed.startsWith("//")
               )
                  continue;
               more =
                  nextTrimmed.startsWith("->") || nextTrimmed.startsWith("+");
               break;
            }
         }
         return { end: i, whereLines, oneLiner, more };
      }
      // startDepth 1 is a statement of the body ITSELF, one level inside the
      // `{` that opened it; a `nest: y is { where: … }` puts its own `where:`
      // at startDepth 2, which is why this check runs before this line's own
      // braces (if any) are counted.
      if (startDepth === 1 && code.trim().startsWith("where:"))
         whereLines.push({ line: i, code: code.trim() });
   }
   // declarationExtent already guarantees the block closes somewhere at or
   // before extentEnd; falling through here only means the caller passed an
   // extentEnd that does not belong to this declaration.
   return { end: extentEnd, whereLines, oneLiner: undefined, more: false };
}

/** The line declaring `<keyword>: <name> is`, or -1. */
export function declarationLine(
   lines: string[],
   keyword: "view" | "source",
   name: string,
): number {
   const re = new RegExp(`^\\s*${keyword}:\\s*${name}\\s+is\\b`);
   return lines.findIndex((l) => re.test(l));
}

/** The line carrying the model-level `## artifact` tag, or -1. */
export const artifactLine = (lines: string[]) =>
   lines.findIndex(
      (l) => l.trimStart().startsWith("##") && l.includes("artifact"),
   );

/** A `given:` declaration: its line, and the block header when it is in one. */
export interface GivenAt {
   line: number;
   blockHeader?: number;
   /** `NAME :: type is default`, as written. */
   declaration: string;
}

/**
 * Every `given:` declaration, by name, in both spellings Malloy accepts:
 *
 *     given: CATEGORY :: filter<string> is f''   // one per line
 *
 *     given:                                     // a block
 *       CATEGORY :: filter<string> is f''
 *       SINCE :: date is @2023-01-01
 */
export function givenDeclarations(lines: string[]): Map<string, GivenAt> {
   const found = new Map<string, GivenAt>();
   const nameOf = (declaration: string) =>
      /^([A-Z_][A-Z0-9_]*)\s*::/.exec(declaration)?.[1];
   const add = (line: number, declaration: string, blockHeader?: number) => {
      const text = declaration.trim();
      const name = nameOf(text);
      if (name)
         found.set(name, {
            line,
            declaration: text,
            ...(blockHeader === undefined ? {} : { blockHeader }),
         });
   };
   for (let i = 0; i < lines.length; i++) {
      const text = lines[i].trim();
      if (text === "given:") {
         for (let j = i + 1; j < lines.length; j++) {
            const inner = lines[j].trim();
            if (
               inner === "" ||
               /^(source|query|import|run|given|##)/.test(inner)
            )
               break;
            add(j, inner, i);
         }
      } else if (text.startsWith("given:")) {
         add(i, text.slice("given:".length));
      }
   }
   return found;
}

/**
 * A line split at the `//` that starts its trailing comment, quotes respected.
 *
 * The writer appends to a declaration line, and a line ending in a comment is
 * how people annotate a tile. Appended blind, the refinement lands INSIDE the
 * comment: with nothing to append to it the save is refused, and with a
 * refinement already there the greedy read finds the clause anyway and the gate
 * passes a file where Malloy sees no filter at all.
 */
export function splitTrailingComment(line: string): {
   /** Everything before the comment, including the gap that separated them. */
   code: string;
   /** The comment from its `//`, or "". */
   comment: string;
} {
   let quote: '"' | "'" | '"""' | undefined;
   for (let i = 0; i < line.length; i++) {
      if (quote === '"""') {
         if (line.startsWith('"""', i)) {
            quote = undefined;
            i += 2;
         }
         continue;
      }
      if (quote !== undefined) {
         if (line[i] === "\\") i++;
         else if (line[i] === quote) quote = undefined;
         continue;
      }
      if (line.startsWith('"""', i)) {
         quote = '"""';
         i += 2;
      } else if (line[i] === '"' || line[i] === "'") {
         quote = line[i] as '"' | "'";
      } else if (line.startsWith("//", i)) {
         return { code: line.slice(0, i), comment: line.slice(i) };
      }
   }
   return { code: line, comment: "" };
}

/** A tile expression's steps: `orders -> by_brand + { limit: 2 }`. */
export interface TileSteps {
   source: string;
   view: string;
   /** The refinement on the view, `+ { … }`, when the expression carries one. */
   refinement?: string;
}

/**
 * `source -> view`, optionally refined — the one form the builder lays out.
 * Undefined for anything else: an inline stage, or a longer pipeline.
 */
export function tileSteps(
   expression: string | undefined,
): TileSteps | undefined {
   const parts = (expression ?? "").split("->").map((p) => p.trim());
   if (parts.length !== 2) return undefined;
   const plus = parts[1].indexOf("+");
   const view = plus < 0 ? parts[1] : parts[1].slice(0, plus).trim();
   const refinement = plus < 0 ? undefined : parts[1].slice(plus).trim();
   return {
      source: parts[0],
      view,
      ...(refinement === undefined ? {} : { refinement }),
   };
}

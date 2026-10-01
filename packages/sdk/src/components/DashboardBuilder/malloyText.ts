// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The two small grammars that are NOT Malloy's, and so are not the parser's to
 * answer: a `tiles=[…]` entry inside an annotation, and the model-level `##`
 * lines. Everything about Malloy itself is located in `malloyTree`.
 */

const IDENT = "[A-Za-z_][A-Za-z0-9_]*";

export const isIdentifier = (text: string) =>
   new RegExp(`^${IDENT}$`).test(text);

/** Statement keywords: they lex as keywords only before `:` (`where: …`), so they stand bare as a source, view or tile name but not as a given name or filter field. */
const STATEMENT_KEYWORDS = new Set(
   `accept aggregate calculate calculation connection declare dimension drill except given group_by grouped_by having index join_cross join_many join_one limit measure nest order_by partition_by primary_key query rename run sample select timezone top type view where`.split(
      " ",
   ),
);

/** Keywords that fail to compile as a bare name anywhere (case-insensitive), from Malloy's lexer. Static so the main entry never imports the compiler. */
const ALWAYS_RESERVED = new Set(
   `all and as asc avg boolean by case cast compose count date day desc distinct else end exclude export extend false filter for from full has hour import in include inner internal is json left like max min minute month not now null number on or pick private public quarter right second source sql string sum table then this timestamp timestamptz to true virtual week when with year`.split(
      " ",
   ),
);

/** A name that can stand bare as a source, view or tile name: identifier-shaped and never reserved. */
export const isBareName = (text: string) =>
   isIdentifier(text) && !ALWAYS_RESERVED.has(text.toLowerCase());

/** A name that can stand bare as a given name: also not a statement keyword, since `NAME ::` is not a statement. */
export const isStrictName = (text: string) =>
   isBareName(text) && !STATEMENT_KEYWORDS.has(text.toLowerCase());

/** A filter field as written after `where:`: a lone reserved name is back-quoted; a dotted path (`.year` is reserved too) or an expression stays verbatim. */
export const malloyPath = (path: string) =>
   isIdentifier(path) && !isBareName(path) ? `\`${path}\`` : path;

/** The server's rule for the tag line: `## artifact` at the start of a line. */
export const ARTIFACT_LINE = /^##[ \t]*artifact\b/;

/**
 * A note's markdown route, by Malloy's prefix rule: the first whitespace-delimited token is
 * `#`/`##`, an optional `|`, and `(markdown)` or its `<>`, `[]`, `{}` twin. `#(markdown)hi` has
 * trailing junk on the prefix, which Malloy calls malformed and the server ignores.
 */
export function markdownNote(
   line: string,
): { level: 1 | 2; block: boolean } | undefined {
   const token = line.trim().split(/[ \t\r]/, 1)[0];
   const m =
      /^(#{1,2})(\|?)(?:\(markdown\)|<markdown>|\[markdown\]|\{markdown\})$/.exec(
         token,
      );
   return m ? { level: m[1].length as 1 | 2, block: m[2] === "|" } : undefined;
}

/**
 * Whether `line` closes a block annotation as Malloy's lexer reads it: the closer sits at the
 * opener's own column, and `|##` never closes a `#|` block. An unknown `column` (a cell's first
 * line has lost its indentation) accepts any indent.
 */
export function closesBlock(
   line: string,
   column: number | undefined,
   closer: "|#" | "|##",
): boolean {
   const indent = /^[ \t]*/.exec(line)[0].length;
   if (column !== undefined && indent !== column) return false;
   const rest = line.slice(indent);
   return (
      rest.startsWith(closer) && (closer !== "|#" || rest.charAt(2) !== "#")
   );
}

/**
 * The `##|` / `#|` blocks that have a closer, as `[opener line, closer line]`;
 * an opener with none is not a block. An opener on a `skip` line (inside a
 * block comment) opens nothing.
 */
export function blockSpans(
   lines: string[],
   skip?: (line: number) => boolean,
): [number, number][] {
   const spans: [number, number][] = [];
   for (let i = 0; i < lines.length; i++) {
      if (skip?.(i)) continue;
      const opener = /^([ \t]*)(#{1,2})\|/.exec(lines[i]);
      if (!opener) continue;
      const closer = opener[2] === "#" ? "|#" : "|##";
      for (let j = i + 1; j < lines.length; j++) {
         if (closesBlock(lines[j], opener[1].length, closer)) {
            spans.push([i, j]);
            i = j;
            break;
         }
      }
   }
   return spans;
}

/** The lines that sit inside or on a block, where prose is not a tag or a note. */
export function blockLines(lines: string[]): Set<number> {
   const out = new Set<number>();
   for (const [from, to] of blockSpans(lines))
      for (let i = from; i <= to; i++) out.add(i);
   return out;
}

/**
 * The `#(markdown)` lines and `#|(markdown)` blocks: prose attached to a declaration, not tags.
 * A line inside a block comment (`skip`) or inside another block is no annotation.
 */
export function markdownLines(
   lines: string[],
   skip?: (line: number) => boolean,
): Set<number> {
   const out = new Set<number>();
   const inside = new Set<number>();
   for (const [from, to] of blockSpans(lines, skip)) {
      const attached = markdownNote(lines[from])?.level === 1;
      for (let i = from; i <= to; i++) {
         inside.add(i);
         if (attached) out.add(i);
      }
   }
   lines.forEach((l, i) => {
      if (inside.has(i) || skip?.(i)) return;
      const note = markdownNote(l);
      if (note?.level === 1 && !note.block) out.add(i);
   });
   return out;
}

/** The line carrying the model-level `## artifact` tag, or -1. */
export const artifactLine = (lines: string[]) => {
   const inBlock = blockLines(lines);
   return lines.findIndex(
      (l, i) => !inBlock.has(i) && ARTIFACT_LINE.test(l.trim()),
   );
};

/** An unnamed `"` note line; `##"word` is a malformed route Malloy drops. */
const DOC_NOTE = /^##"([ \t]|$)/;

/**
 * The lines a dashboard's description is read from, by the server's rule: the
 * notes above the artifact tag, or, when those carry no prose, the notes below
 * it, `##"` lines and `##|"` blocks alike. `blankAbove` are the prose-less
 * `##"` notes above when the text is read from below, which a rewrite has to
 * clear; `below` are the `##"` notes under the tag, and `belowBlock` is true
 * when a `##|"` block with prose sits there. `inBlock` is true when the text
 * that is read is held in a block, which a rewrite cannot patch.
 */
export function descriptionNotes(lines: string[]): {
   read: number[];
   blankAbove: number[];
   below: number[];
   belowBlock: boolean;
   inBlock: boolean;
   text?: string;
} {
   const artifactAt = artifactLine(lines);
   const inside = blockLines(lines);
   const isAbove = (i: number) => artifactAt < 0 || i < artifactAt;
   const isBelow = (i: number) => artifactAt >= 0 && i > artifactAt;
   type Item = { at: number; text: string; block: boolean };
   const items: Item[] = [];
   lines.forEach((l, i) => {
      if (!inside.has(i) && DOC_NOTE.test(l.trim()))
         items.push({ at: i, text: l.trim().slice(3).trim(), block: false });
   });
   for (const [from, to] of blockSpans(lines)) {
      if (!/^[ \t]*##\|"([ \t]|$)/.test(lines[from])) continue;
      const text = [lines[from].trim().slice(4), ...lines.slice(from + 1, to)]
         .map((l) => l.trim())
         .join("\n")
         .trim();
      if (text.length > 0) items.push({ at: from, text, block: true });
   }
   items.sort((a, b) => a.at - b.at);
   const above = items.filter((n) => isAbove(n.at));
   const below = items.filter((n) => isBelow(n.at));
   const chosen = above.some((n) => n.text.trim()) ? above : below;
   const text = chosen.map((n) => n.text).join("\n");
   const lineAts = (from: Item[]) =>
      from.filter((n) => !n.block).map((n) => n.at);
   return {
      read: lineAts(chosen),
      blankAbove: chosen === below ? lineAts(above) : [],
      below: lineAts(below),
      belowBlock: below.some((n) => n.block),
      inBlock: chosen.some((n) => n.block),
      ...(text.trim() ? { text } : {}),
   };
}

/** Whether `tiles=[…]` holds an entry that is not a quoted run expression, such as a `kind=text` tile. */
export function hasNonQuotedTiles(artifactText: string): boolean {
   const key = artifactText.search(/tiles\s*=\s*\[/);
   if (key < 0) return false;
   const open = artifactText.indexOf("[", key);
   const close = artifactText.indexOf("]", open);
   if (close < 0) return false;
   return (
      artifactText
         .slice(open + 1, close)
         .replace(/"[^"]*"/g, "")
         .replace(/[\s,]/g, "").length > 0
   );
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

/** The inverse of {@link malloyPath}: a field that is exactly one back-quoted identifier is read as the plain name; anything else is verbatim. */
export const readPath = (path: string) =>
   /^`([A-Za-z_][A-Za-z0-9_]*)`$/.test(path) ? path.slice(1, -1) : path;

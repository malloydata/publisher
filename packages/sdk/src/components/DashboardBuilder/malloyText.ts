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

/** Malloy's keywords (case-insensitive), from the lexer; a name spelled like one needs back-quotes. Static so the main entry never imports the compiler. */
const RESERVED = new Set(
   `accept aggregate all and as asc avg boolean by calculate calculation case cast compose connection count date day declare desc dimension distinct drill else end except exclude export extend false filter for from full given group_by grouped_by has having hour import in include index inner internal is join_cross join_many join_one json left like limit max measure min minute month nest not now null number on or order_by partition_by pick primary_key private public quarter query rename right run sample second select source sql string sum table then this timestamp timestamptz timezone to top true type view virtual week when where with year`.split(
      " ",
   ),
);

/** A name that can stand bare in Malloy text: identifier-shaped and not a keyword. */
export const isBareName = (text: string) =>
   isIdentifier(text) && !RESERVED.has(text.toLowerCase());

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

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

/** The server's rule for the tag line: `## artifact` at the start of a line. */
export const ARTIFACT_LINE = /^##[ \t]*artifact\b/;

/** The line carrying the model-level `## artifact` tag, or -1. */
export const artifactLine = (lines: string[]) =>
   lines.findIndex((l) => ARTIFACT_LINE.test(l.trim()));

/** An unnamed `"` note line; `##"word` is a malformed route Malloy drops. */
const DOC_NOTE = /^##"([ \t]|$)/;

/** The `##|"` blocks that have a closer, with their body text; an unclosed opener is no block. */
function docBlocks(lines: string[]): { start: number; text: string }[] {
   const out: { start: number; text: string }[] = [];
   for (let i = 0; i < lines.length; i++) {
      if (!/^##\|"([ \t]|$)/.test(lines[i].trim())) continue;
      let end = -1;
      for (let j = i + 1; j < lines.length; j++)
         if (lines[j].trimStart().startsWith("|##")) {
            end = j;
            break;
         }
      if (end < 0) continue;
      out.push({
         start: i,
         text: [lines[i].trim().slice(4), ...lines.slice(i + 1, end)]
            .map((l) => l.trim())
            .join("\n")
            .trim(),
      });
      i = end;
   }
   return out;
}

/**
 * The lines a dashboard's description is read from, by the server's rule: the
 * notes above the artifact tag, or, when those carry no prose, the `##"` notes
 * below it. `blankAbove` are the prose-less notes above when the text is read
 * from below, which a rewrite has to clear so the new text is what sits above;
 * `below` are the `##"` notes under the tag. `inBlock` is true when a `##|"`
 * block above the tag holds part of the text, which a rewrite cannot patch.
 */
export function descriptionNotes(lines: string[]): {
   read: number[];
   blankAbove: number[];
   below: number[];
   inBlock: boolean;
   text?: string;
} {
   const artifactAt = artifactLine(lines);
   const isAbove = (i: number) => artifactAt < 0 || i < artifactAt;
   const notes = lines
      .map((l, i) => (DOC_NOTE.test(l.trim()) ? i : -1))
      .filter((i) => i >= 0);
   const textOf = (idx: number[]) =>
      idx.map((i) => lines[i].trim().slice(3).trim()).join("\n");
   const above = notes.filter(isAbove);
   const below = notes.filter((i) => artifactAt >= 0 && i > artifactAt);
   const blocks = docBlocks(lines).filter(
      (b) => isAbove(b.start) && b.text.length > 0,
   );
   if (blocks.length > 0) {
      const parts = [
         ...above.map((i) => ({
            at: i,
            text: lines[i].trim().slice(3).trim(),
         })),
         ...blocks.map((b) => ({ at: b.start, text: b.text })),
      ].sort((a, b) => a.at - b.at);
      return {
         read: above,
         blankAbove: [],
         below,
         inBlock: true,
         text: parts.map((p) => p.text).join("\n"),
      };
   }
   const read = textOf(above).trim() ? above : below;
   const text = textOf(read);
   return {
      read,
      blankAbove: read === above ? [] : above,
      below,
      inBlock: false,
      ...(text.trim() ? { text } : {}),
   };
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The two small grammars that are NOT Malloy's, and so are not the parser's to
 * answer: a `tiles=[…]` entry inside an annotation, and the model-level `##`
 * lines. Everything about Malloy itself is located in `malloyTree`.
 */

const IDENT = "[A-Za-z_][A-Za-z0-9_]*";

/** A file's lines, whatever its line endings: a CRLF line otherwise keeps a `\r` that a `$` anchor trips on. */
export const splitSourceLines = (source: string): string[] =>
   source.split(/\r\n|\r|\n/);

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

/**
 * Whether a `##` note or `##|` block sets `artifact` as a top-level property, anywhere among its
 * properties, as the tag parser reads it: not inside a string, a nested `{…}` or `[…]`, or a `#`
 * comment, and not as a value (`title=artifact`) or a dotted path's tail. The server's rule too.
 */
export function setsArtifactProperty(note: string): boolean {
   // A routed note (`##(markdown)`, `##"`) or a flag (`##!`) is never a tag.
   const prefix = /^##(?:\|\s*|[ \t]*)(?=[A-Za-z_])/.exec(note);
   if (!prefix) return false;
   let depth = 0;
   let before = "";
   for (let i = prefix[0].length; i < note.length; i++) {
      const c = note[i];
      if (/\s/.test(c)) continue;
      if (c === '"' || c === "'") {
         const fence = note.startsWith(c.repeat(3), i) ? c.repeat(3) : c;
         i += fence.length;
         while (i < note.length && !note.startsWith(fence, i))
            i += note[i] === "\\" ? 2 : 1;
         i += fence.length - 1;
      } else if (c === "#") {
         const eol = note.indexOf("\n", i);
         if (eol < 0) return false;
         i = eol;
         continue;
      } else if (c === "{" || c === "[") depth++;
      else if (c === "}" || c === "]") depth--;
      else if (/\w/.test(c)) {
         const word = /^\w+/.exec(note.slice(i))?.[0] ?? c;
         if (depth === 0 && word === "artifact" && !/[=.-]/.test(before))
            return true;
         i += word.length - 1;
      }
      before = c;
   }
   return false;
}

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
   // Each closer's lines by its column, so a file of unclosed openers is not rescanned to its end per opener.
   const closers = new Map<string, number[]>();
   lines.forEach((line, at) => {
      const indent = /^[ \t]*/.exec(line)?.[0].length ?? 0;
      for (const closer of ["|##", "|#"] as const)
         if (closesBlock(line, indent, closer)) {
            const key = `${indent}${closer}`;
            const ats = closers.get(key);
            if (ats) ats.push(at);
            else closers.set(key, [at]);
            break;
         }
   });
   const cursor = new Map<string, number>();
   const spans: [number, number][] = [];
   for (let i = 0; i < lines.length; i++) {
      if (skip?.(i)) continue;
      const opener = /^([ \t]*)(#{1,2})\|/.exec(lines[i]);
      if (!opener) continue;
      const key = `${opener[1].length}${opener[2] === "#" ? "|#" : "|##"}`;
      const ats = closers.get(key) ?? [];
      let k = cursor.get(key) ?? 0;
      while (k < ats.length && ats[k] <= i) k++;
      cursor.set(key, k);
      if (k < ats.length) {
         spans.push([i, ats[k]]);
         i = ats[k];
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

/** Where the model-level artifact tag sits, written as one `## artifact { … }` line or as a `##|` … `|##` block. */
export interface ArtifactTag {
   /** The tag's first line: the `## artifact` line, or a block's `##|` opener. */
   from: number;
   /** The last line the tag occupies: the same line when single, else the `|##` closer. */
   to: number;
   block: boolean;
   /** The tag as written: its line, or a block's lines from the opener up to, not including, the closer. */
   text: string;
}

export function artifactTag(lines: string[]): ArtifactTag | undefined {
   const inBlock = blockLines(lines);
   const single = lines.findIndex(
      (l, i) =>
         !inBlock.has(i) &&
         // An unclosed `##|` opener holds no block, so it is no tag either.
         !/^\s*##\|/.test(l) &&
         setsArtifactProperty(l.trim()),
   );
   const span = blockSpans(lines).find(([from, to]) =>
      setsArtifactProperty(lines.slice(from, to).join("\n").trim()),
   );
   if (span && (single < 0 || span[0] < single))
      return {
         from: span[0],
         to: span[1],
         block: true,
         text: lines.slice(span[0], span[1]).join("\n"),
      };
   return single < 0
      ? undefined
      : { from: single, to: single, block: false, text: lines[single] };
}

/** Whether `artifact { … }` is the tag's first property, the one spelling the builder's tag rewrites can edit. */
export const artifactLeads = (tagText: string) =>
   /^\s*##(?:\|\s*|[ \t]*)artifact\b/.test(tagText);

/** Why a tag the server serves is not opened when `artifact` is not its first property. */
export const ARTIFACT_NOT_FIRST =
   "The `artifact` property is not the first on its tag, so the builder cannot rewrite the tag without risking the properties before it. Move `artifact { … }` first on the tag to edit this file here.";

/** The line carrying the model-level artifact tag (a block's opener), or -1. */
export const artifactLine = (lines: string[]) => artifactTag(lines)?.from ?? -1;

/** A tag's text as the annotation parser takes it: one `#` annotation, whatever the spelling. */
export const tagAnnotation = (tagText: string) =>
   tagText.replace(/^\s*##\|?\s*/, "# ");

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

/** A text tile's name: a bare word, as the server reads a `tiles=[…]` entry. */
export const TEXT_TILE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * A floating `##|(markdown)` / `##|(text)` opener line: its route, and what follows the route.
 * `name` is that text when it is a lone bare word; any other text on the opener is body.
 */
export function textBlockOpener(
   line: string,
): { route: "markdown" | "text"; rest: string; name?: string } | undefined {
   const trimmed = line.trim();
   const token = trimmed.split(/[ \t\r]/, 1)[0];
   const m =
      /^##\|(?:\((markdown|text)\)|<(markdown|text)>|\[(markdown|text)\]|\{(markdown|text)\})$/.exec(
         token,
      );
   if (!m) return undefined;
   const rest = trimmed.slice(token.length).trim();
   return {
      route: (m[1] ?? m[2] ?? m[3] ?? m[4]) as "markdown" | "text",
      rest,
      ...(TEXT_TILE_NAME.test(rest) ? { name: rest } : {}),
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

/** The inverse of {@link malloyPath}: a field that is exactly one back-quoted identifier is read as the plain name; anything else is verbatim. */
export const readPath = (path: string) =>
   /^`([A-Za-z_][A-Za-z0-9_]*)`$/.test(path) ? path.slice(1, -1) : path;

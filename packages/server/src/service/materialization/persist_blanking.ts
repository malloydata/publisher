// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The author's files with every SOURCE-level `#@` note blanked, for a
 * generated companion that must read the rows a live query reads, never a
 * source's own stored table: a `#@ persist` view's build and serve variant
 * (P14) and a `#(preaggregate)` rollup's (#6293) both compile over it.
 *
 * The spans are the compiler's own (each note's `at.range`), never a text
 * search. Each is checked before it is blanked: it lies on one line, it holds
 * its note's text, and the file lexes to the same tokens once it is blanked
 * (`malloy_lexer.ts`), so only annotation text was removed and every position
 * is kept. A note with no span, a file that cannot be read, or a span that
 * fails a check gives `undefined`: nothing is blanked by guess, and the caller
 * builds or serves nothing rather than read a stored base it meant to leave
 * out.
 *
 * Neutral ground for the materialization legs (plan/ and serve/ both import
 * it); keep its imports to the lexer, the source map and the overlay key.
 */

import { routeOfNote } from "../annotation_reader";
import { overlayKey } from "../overlay_key";
import { lexAll, offsetOf } from "./malloy_lexer";
import { SourceMap } from "./plan/source_map";

/** A zero-based range, as a note's `at.range` carries it. */
interface NoteRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** One note to blank: its text and the compiler's span for it. */
export interface NoteSpan {
   text: string;
   range: NoteRange;
}

interface NoteLike {
   text: string;
   at?: { url: string; range: NoteRange };
}

/** Every source-level note on an annotation chain, inherited levels included. */
function sourceNotes(annotations: unknown, depth = 0): NoteLike[] {
   if (!annotations || typeof annotations !== "object" || depth > 64) return [];
   const a = annotations as {
      notes?: NoteLike[];
      blockNotes?: NoteLike[];
      inherits?: unknown;
   };
   return [
      ...(a.blockNotes ?? []),
      ...(a.notes ?? []),
      ...sourceNotes(a.inherits, depth + 1),
   ];
}

/**
 * The source-level `#@` notes of every source `modelDef` reaches, by the URL
 * of the file each was parsed in. Undefined when a note carries no span.
 */
function persistNoteSpans(
   modelDef: unknown,
): Map<string, NoteSpan[]> | undefined {
   const byFile = new Map<string, NoteSpan[]>();
   const seen = new Set<string>();
   for (const node of SourceMap.of(modelDef).nodes()) {
      const annotations = (node.def as { annotations?: unknown }).annotations;
      for (const note of sourceNotes(annotations)) {
         if (routeOfNote(note.text) !== "@") continue;
         if (!note.at) return undefined;
         const r = note.at.range;
         const key = `${note.at.url}#${r.start.line}:${r.start.character}`;
         if (seen.has(key)) continue;
         seen.add(key);
         const spans = byFile.get(note.at.url) ?? [];
         spans.push({ text: note.text, range: r });
         byFile.set(note.at.url, spans);
      }
   }
   return byFile;
}

/**
 * `text` with each note's span replaced by spaces (line breaks and every
 * position kept), or undefined when a span does not hold its note (see the
 * module doc).
 */
export function blankNoteSpans(
   text: string,
   notes: readonly NoteSpan[],
): string | undefined {
   const before = lexAll(text);
   if (!before) return undefined;
   let out = text;
   for (const { text: note, range: r } of notes) {
      // A source-level note is one line: `#@ …` to the end of its line.
      if (r.start.line !== r.end.line) return undefined;
      const lineStart = offsetOf(out, { line: r.start.line, character: 0 });
      if (lineStart < 0) return undefined;
      const newline = out.indexOf("\n", lineStart);
      const lineEnd = newline < 0 ? out.length : newline;
      const from = lineStart + r.start.character;
      const to = Math.min(lineStart + r.end.character, lineEnd);
      if (from > lineEnd || from > to) return undefined;
      const held = out.slice(from, to);
      if (!held.startsWith("#@") || held.trimEnd() !== note.trimEnd()) {
         return undefined;
      }
      out = out.slice(0, from) + " ".repeat(to - from) + out.slice(to);
   }
   const after = lexAll(out);
   if (
      !after ||
      after.length !== before.length ||
      after.some(
         (t, k) => t.text !== before[k].text || t.start !== before[k].start,
      )
   ) {
      return undefined;
   }
   return out;
}

/**
 * The author's files with every source-level `#@` note blanked, keyed for an
 * overlay, for each file of `modelDef` that carries one: empty when none
 * does, the common case. `readText` reads a file by URL (the reader the
 * companion compiles with). Undefined when a file cannot be read or a span
 * does not hold its note.
 */
export async function persistBlankedFiles(
   modelDef: unknown,
   readText: (url: string) => string | undefined | Promise<string | undefined>,
): Promise<Map<string, string> | undefined> {
   const spans = persistNoteSpans(modelDef);
   if (!spans) return undefined;
   const out = new Map<string, string>();
   for (const [url, notes] of spans) {
      const text = await readText(url);
      const blanked =
         text === undefined ? undefined : blankNoteSpans(text, notes);
      if (blanked === undefined) return undefined;
      out.set(overlayKey(url), blanked);
   }
   return out;
}

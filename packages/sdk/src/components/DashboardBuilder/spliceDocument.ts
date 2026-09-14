// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument, DashboardTile } from "./document";
import { blockAbove, readDashboardDocument, readFailed } from "./readDocument";

/**
 * Write a change back into a `dashboards/*.malloy` file by PATCHING it.
 *
 * The builder does not regenerate the file. It rewrites the bytes it owns and
 * leaves every other byte exactly where it was, which is what lets it edit a
 * dashboard somebody else wrote: comments, formatting, declaration order and
 * unmodelled Malloy all survive an edit because nothing rewrites them.
 *
 * The safety property is a SEMANTIC round-trip, not a byte one. After splicing,
 * the result is read back and compared against the document that was asked for;
 * a mismatch refuses the write. An earlier design gated on byte-identity with a
 * freshly generated file, which is a much weaker thing to know and which made
 * every commented file read-only.
 *
 * A refused write NEVER discards the edit. The caller keeps its document and is
 * told the writer produced something it could not read back — that is a defect
 * in this module, not a mistake by the person editing.
 *
 * SCOPE, deliberately: property edits only. Changing a tile's presentation, its
 * filter binding, or the page's own settings. Adding, removing or reordering
 * tiles moves declarations around, and no file says whether the comment above a
 * tile belongs to the tile, to the row, or to the file — so those are refused
 * here rather than guessed at, and will arrive with a diff preview.
 */

export interface SpliceFailure {
   ok: false;
   reason: string;
}

export type SpliceResult = { ok: true; source: string } | SpliceFailure;

/** Narrow to the failure arm; see {@link readFailed} for why a guard. */
export const spliceFailed = (result: SpliceResult): result is SpliceFailure =>
   result.ok === false;

/**
 * A stable serialisation for comparing documents.
 *
 * `JSON.stringify` is key-ORDER sensitive, and the two documents being compared
 * are built differently: the reader emits properties in its own canonical order,
 * while a caller that assigns `tile.label = …` appends that key at the end. The
 * gate would then refuse a correct edit and report a writer defect that is
 * really a serialisation artefact. Sorting keys compares the documents rather
 * than the way they happen to be spelled.
 */
function canonical(value: unknown): string {
   const walk = (node: unknown): unknown => {
      if (Array.isArray(node)) return node.map(walk);
      if (node && typeof node === "object") {
         const out: Record<string, unknown> = {};
         for (const key of Object.keys(node as object).sort())
            out[key] = walk((node as Record<string, unknown>)[key]);
         return out;
      }
      return node;
   };
   return JSON.stringify(walk(value));
}

/** A byte-range replacement. Applied last-first so earlier offsets stay valid. */
interface Edit {
   start: number;
   end: number;
   text: string;
}

/** Start offset of each line, so a line number can become a byte range. */
function lineStarts(source: string): number[] {
   const starts = [0];
   for (let i = 0; i < source.length; i++)
      if (source[i] === "\n") starts.push(i + 1);
   return starts;
}

function applyEdits(source: string, edits: Edit[]): string {
   let out = source;
   for (const edit of [...edits].sort((a, b) => b.start - a.start))
      out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
   return out;
}

/** The `#` tags a tile's presentation implies, in the order they are written. */
function tagsFor(tile: DashboardTile): string[] {
   const tags: string[] = [];
   if (tile.colspan !== undefined) tags.push(`# colspan=${tile.colspan}`);
   if (tile.break) tags.push("# break");
   if (tile.borderless) tags.push("# borderless");
   if (tile.label !== undefined) tags.push(`# label="${tile.label}"`);
   if (tile.subtitle !== undefined) tags.push(`# subtitle="${tile.subtitle}"`);
   return tags;
}

/** Which property a tag line carries, so an existing line can be replaced. */
function tagKey(text: string): string | undefined {
   const m = /^#\s*([a-z_]+)/.exec(text);
   return m?.[1];
}

const isSameDocumentExceptTiles = (
   a: DashboardDocument,
   b: DashboardDocument,
) =>
   a.title === b.title &&
   a.description === b.description &&
   a.columns === b.columns &&
   a.autorun === b.autorun &&
   canonical(a.imports) === canonical(b.imports) &&
   canonical(a.sources) === canonical(b.sources) &&
   canonical(a.localGivens) === canonical(b.localGivens) &&
   canonical(a.drills) === canonical(b.drills) &&
   canonical(a.startingGivens) === canonical(b.startingGivens);

/** The tile list, ignoring presentation: identity, order and what they read. */
const tileIdentity = (document: DashboardDocument) =>
   canonical(document.tiles.map((t) => [t.name, t.source, t.declaration]));

export async function spliceDashboardDocument(
   sourceText: string,
   next: DashboardDocument,
): Promise<SpliceResult> {
   const before = await readDashboardDocument(sourceText);
   if (readFailed(before)) {
      return {
         ok: false,
         reason: `Cannot edit a file that will not open: ${before.reason}`,
      };
   }
   const current = before.document;

   // Structural change: adding, removing or reordering tiles moves declarations
   // and their comment blocks, and the file cannot say who a comment belongs to.
   // Refused rather than guessed; a diff preview is what makes it safe later.
   if (tileIdentity(current) !== tileIdentity(next)) {
      return {
         ok: false,
         reason:
            "Adding, removing or reordering tiles is not supported yet — only " +
            "changing what a tile already shows.",
      };
   }
   if (!isSameDocumentExceptTiles(current, next)) {
      return {
         ok: false,
         reason:
            "Only a tile's presentation and filters can be changed so far, not " +
            "the page's imports, sources, givens or settings.",
      };
   }

   const lines = sourceText.split("\n");
   const starts = lineStarts(sourceText);
   const wholeLine = (line: number): { start: number; end: number } => ({
      start: starts[line],
      end: line + 1 < starts.length ? starts[line + 1] : sourceText.length,
   });
   const indentOf = (line: number) => /^\s*/.exec(lines[line])?.[0] ?? "";

   const edits: Edit[] = [];

   for (const [index, tile] of next.tiles.entries()) {
      const was = current.tiles[index];
      if (canonical(was) === canonical(tile)) continue;

      // An inherited tile is declared in the model, not here, so there is
      // nothing in this file to patch. Saying so beats writing a tag that would
      // land on the wrong object.
      if (tile.declaration.kind === "inherited") {
         return {
            ok: false,
            reason:
               `\`${tile.source} -> ${tile.name}\` is declared on its source, ` +
               `not in this dashboard, so its presentation cannot be changed here.`,
         };
      }

      const declLine = lines.findIndex((l) =>
         new RegExp(`view:\\s*${tile.name}\\s+is\\b`).test(l),
      );
      if (declLine < 0) {
         return {
            ok: false,
            reason: `Could not find where \`${tile.name}\` is declared.`,
         };
      }

      const { tags } = blockAbove(lines, declLine);
      const indent = indentOf(declLine);
      const wanted = tagsFor(tile);
      const wantedByKey = new Map(
         wanted.map((text) => [tagKey(text) ?? text, text]),
      );
      const seen = new Set<string>();

      // Existing tag lines are patched or removed IN PLACE, so anything else in
      // the block — a comment explaining the tile — keeps its position.
      for (const tag of tags) {
         const key = tagKey(tag.text);
         if (key === undefined) continue;
         const want = wantedByKey.get(key);
         if (want === undefined) {
            // Removed: take the whole line, including its newline.
            edits.push({ ...wholeLine(tag.line), text: "" });
            continue;
         }
         seen.add(key);
         if (want !== tag.text)
            edits.push({ ...wholeLine(tag.line), text: `${indent}${want}\n` });
      }

      // New tags go immediately above the declaration, which is where a reader
      // looks for them and where `blockAbove` will find them again.
      const added = wanted.filter((text) => !seen.has(tagKey(text) ?? text));
      if (added.length > 0) {
         const at = starts[declLine];
         edits.push({
            start: at,
            end: at,
            text: added.map((text) => `${indent}${text}\n`).join(""),
         });
      }

      // The filter binding lives in the declaration itself, as a refinement.
      const refinement = (tile.filters ?? [])
         .map((f) => `where: ${f.field} ~ $${f.given}`)
         .join(", ");
      const withoutRefinement = lines[declLine].replace(
         /\s*\+\s*\{[^}]*\}\s*$/,
         "",
      );
      const rewritten =
         refinement === ""
            ? withoutRefinement
            : `${withoutRefinement} + { ${refinement} }`;
      if (rewritten !== lines[declLine])
         edits.push({ ...wholeLine(declLine), text: `${rewritten}\n` });
   }

   if (edits.length === 0) return { ok: true, source: sourceText };

   const spliced = applyEdits(sourceText, edits);

   // The gate. Read back what was actually written and compare it against what
   // was asked for. Comments survived because they were never rewritten;
   // correctness is established here rather than assumed.
   const after = await readDashboardDocument(spliced);
   if (readFailed(after)) {
      return {
         ok: false,
         reason: `The edit produced a file that cannot be read back: ${after.reason}`,
      };
   }
   if (canonical(after.document) !== canonical(next)) {
      return {
         ok: false,
         reason:
            "The edit did not produce the dashboard that was asked for, so it " +
            "was not written. Your changes are still here.",
      };
   }

   return { ok: true, source: spliced };
}

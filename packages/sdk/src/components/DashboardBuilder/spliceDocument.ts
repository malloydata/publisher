// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument, DashboardTile } from "./document";
import type { LocalGiven } from "./document";
import {
   BINDING_CLAUSE,
   blockAbove,
   readDashboardDocument,
   readFailed,
} from "./readDocument";

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
 * SCOPE, deliberately: a tile's presentation, its filter bindings, the order of
 * tiles, and the dashboard's OWN givens — added, removed or retagged. Never an
 * import, and never a model file: a filter the builder adds is a declaration in
 * this file, which is the convention {@link LocalGiven} describes. Adding or
 * removing TILES moves view declarations around, and no file says whether the
 * comment above a tile belongs to the tile, to the row, or to the file — so
 * those are refused rather than guessed at, and will arrive with a diff preview.
 *
 * A given is different from a tile in exactly the way that matters there: the
 * `#` tags above its declaration are its control contract and have no other
 * owner, so removing the declaration can take them with it, and a `//` comment
 * in the same block is left where it is.
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
   canonical(a.drills) === canonical(b.drills) &&
   canonical(a.startingGivens) === canonical(b.startingGivens);

/**
 * A given's tag line, composed from its control contract. One line, in the
 * order `givens.malloy` writes them, so a file the builder wrote reads like one
 * a person wrote.
 */
export function givenTagLine(given: LocalGiven): string | undefined {
   const parts: string[] = [];
   if (given.label !== undefined) parts.push(`label="${given.label}"`);
   if (given.description !== undefined)
      parts.push(`description="${given.description}"`);
   if (given.control !== undefined) parts.push(`control=${given.control}`);
   if (given.suggest) {
      const by =
         given.suggest.source !== undefined
            ? `source=${given.suggest.source}`
            : given.suggest.query !== undefined
              ? `query=${given.suggest.query}`
              : undefined;
      parts.push(
         `suggest { ${by ? `${by} ` : ""}dimension=${given.suggest.dimension} }`,
      );
   }
   if (given.rangeMin !== undefined) parts.push(`range_min=${given.rangeMin}`);
   if (given.rangeMax !== undefined) parts.push(`range_max=${given.rangeMax}`);
   return parts.length > 0 ? `# ${parts.join(" ")}` : undefined;
}

/** `given: NAME :: type is default`, the one-line spelling the builder writes. */
export const givenDeclaration = (given: LocalGiven) =>
   `given: ${given.name} :: ${given.type} is ${given.default}`;

/**
 * Where each given is declared: the line, and whether it sits inside a
 * `given:` block (whose header has to go if its last declaration does).
 */
function givenLines(
   lines: string[],
): Map<string, { line: number; blockHeader?: number }> {
   const out = new Map<string, { line: number; blockHeader?: number }>();
   const nameOf = (declaration: string) =>
      /^([A-Z_][A-Z0-9_]*)\s*::/.exec(declaration.trim())?.[1];
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
            const name = nameOf(inner);
            if (name) out.set(name, { line: j, blockHeader: i });
         }
      } else if (text.startsWith("given:")) {
         const name = nameOf(text.slice("given:".length));
         if (name) out.set(name, { line: i });
      }
   }
   return out;
}

/** One tile's identity, ignoring presentation and position. */
const tileKey = (t: DashboardTile) =>
   canonical([t.name, t.source, t.declaration]);

/** The tile list as identities, IN ORDER. Differs under a reorder. */
const tileIdentity = (document: DashboardDocument) =>
   canonical(document.tiles.map(tileKey));

/** The same identities as a SET. Differs only when a tile is added or removed. */
const tileMembership = (document: DashboardDocument) =>
   canonical([...document.tiles.map(tileKey)].sort());

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

   // Adding or removing a tile means inserting or deleting a view declaration,
   // which carries the comment block above it — and the file cannot say whether
   // that comment belongs to the tile, the row, or the file. Refused rather
   // than guessed; a diff preview is what makes it safe later.
   //
   // REORDERING is not in that class, and this used to refuse it with them. A
   // tile's position is not where its view is declared: order comes from the
   // `tiles=[…]` array on the `## artifact` tag, which is what the reader walks.
   // So a reorder rewrites that one array and moves no declaration and no
   // comment — the ambiguity above simply does not arise. It is handled below.
   if (tileMembership(current) !== tileMembership(next)) {
      return {
         ok: false,
         reason:
            "Adding or removing tiles is not supported yet — only reordering " +
            "them and changing what one already shows.",
      };
   }
   const reordered = tileIdentity(current) !== tileIdentity(next);
   if (!isSameDocumentExceptTiles(current, next)) {
      return {
         ok: false,
         reason:
            "Only tiles and this dashboard's own filters can be changed so far, " +
            "not the page's imports, sources or settings.",
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

   // A reorder is one rewritten array on the `## artifact` line. Each entry is
   // re-emitted AS IT WAS WRITTEN rather than rebuilt from `source` and `name`,
   // so a file that spells a tile `overview->kpis` keeps its spelling and the
   // diff is the reordering and nothing else.
   if (reordered) {
      const artifactLine = lines.findIndex(
         (l) => l.trimStart().startsWith("##") && l.includes("artifact"),
      );
      if (artifactLine < 0) {
         return {
            ok: false,
            reason: "Could not find the `## artifact` tag to reorder.",
         };
      }
      const written = [...lines[artifactLine].matchAll(/"([^"]+)"/g)].map(
         (m) => m[1],
      );
      const byKey = new Map<string, string>();
      for (const entry of written) {
         const parts = entry.split("->").map((part) => part.trim());
         if (parts.length === 2) byKey.set(`${parts[0]}->${parts[1]}`, entry);
      }
      const nextEntries = next.tiles.map((tile) =>
         byKey.get(`${tile.source}->${tile.name}`),
      );
      if (nextEntries.some((entry) => entry === undefined)) {
         return {
            ok: false,
            reason:
               "A tile in the new order is not one the `## artifact` tag names.",
         };
      }
      const list = `tiles=[${nextEntries.map((e) => `"${e}"`).join(", ")}]`;
      // The `## artifact` tag must stay on ONE line or the package fails to
      // compile, so the array is replaced in place rather than reformatted.
      const rewritten = lines[artifactLine].replace(
         /tiles\s*=\s*\[[\s\S]*?\]/,
         list,
      );
      if (rewritten !== lines[artifactLine])
         edits.push({ ...wholeLine(artifactLine), text: `${rewritten}\n` });
   }

   // THE DASHBOARD'S OWN GIVENS. Added, removed, or retagged — by name, since
   // a given's name is its identity in every `where:` that reads it.
   const givensBefore = new Map(
      (current.localGivens ?? []).map((g) => [g.name, g]),
   );
   const givensAfter = new Map(
      (next.localGivens ?? []).map((g) => [g.name, g]),
   );
   const declared = givenLines(lines);
   const removedLines = new Set<number>();

   for (const [name, was] of givensBefore) {
      const want = givensAfter.get(name);
      if (want !== undefined && canonical(want) === canonical(was)) continue;
      const at = declared.get(name);
      if (at === undefined) {
         return {
            ok: false,
            reason: `Could not find where the given \`${name}\` is declared.`,
         };
      }
      // Its tags are its control contract, with no other owner, so they go
      // with it (or are replaced with it). A `//` comment in the block stays.
      const { tags } = blockAbove(lines, at.line);
      for (const tag of tags) {
         edits.push({ ...wholeLine(tag.line), text: "" });
         removedLines.add(tag.line);
      }
      if (want === undefined) {
         edits.push({ ...wholeLine(at.line), text: "" });
         removedLines.add(at.line);
         // A declaration set off by blank lines takes one of them with it, or
         // the two separators meet and the file gains an empty line per edit.
         const first = Math.min(at.line, ...tags.map((tag) => tag.line));
         const above = at.blockHeader ?? first;
         const belowIsBlank = (lines[at.line + 1] ?? "x").trim() === "";
         const aboveIsBlank = above === 0 || lines[above - 1].trim() === "";
         const lastInBlock =
            at.blockHeader === undefined ||
            ![...declared.values()].some(
               (other) =>
                  other.blockHeader === at.blockHeader &&
                  other.line > at.line &&
                  givensAfter.has(
                     [...declared.entries()].find(
                        ([, v]) => v === other,
                     )?.[0] ?? "",
                  ),
            );
         if (
            belowIsBlank &&
            aboveIsBlank &&
            lastInBlock &&
            !removedLines.has(at.line + 1)
         ) {
            edits.push({ ...wholeLine(at.line + 1), text: "" });
            removedLines.add(at.line + 1);
         }
         continue;
      }
      // Retagged, or redeclared: the tag line is rewritten above the
      // declaration, and the declaration itself only if its type or default
      // changed — a block-form declaration keeps its own spelling.
      const indent = indentOf(at.line);
      const tagLine = givenTagLine(want);
      const declarationChanged =
         want.type !== was.type || want.default !== was.default;
      const declaration = declarationChanged
         ? at.blockHeader === undefined
            ? givenDeclaration(want)
            : `${want.name} :: ${want.type} is ${want.default}`
         : lines[at.line].trim();
      edits.push({
         ...wholeLine(at.line),
         text:
            (tagLine ? `${indent}${tagLine}\n` : "") +
            `${indent}${declaration}\n`,
      });
   }

   // A block whose every declaration went has to lose its `given:` header too,
   // or the file stops compiling on an empty block.
   for (const [name, at] of declared) {
      if (at.blockHeader === undefined || givensAfter.has(name)) continue;
      const siblingsLeft = [...declared.values()].some(
         (other) =>
            other.blockHeader === at.blockHeader &&
            !removedLines.has(other.line),
      );
      if (!siblingsLeft && !removedLines.has(at.blockHeader)) {
         edits.push({ ...wholeLine(at.blockHeader), text: "" });
         removedLines.add(at.blockHeader);
      }
   }

   const added = [...givensAfter.values()].filter(
      (g) => !givensBefore.has(g.name),
   );
   if (added.length > 0) {
      // A new given goes with the others: after the last one declared, or —
      // for a file declaring its first — after the imports, where a reader
      // expects the page's own declarations to begin.
      const lastGiven = Math.max(
         -1,
         ...[...declared.values()].map((at) => at.line),
      );
      const lastImport = (() => {
         let found = -1;
         for (let i = 0; i < lines.length; i++) {
            const text = lines[i].trim();
            if (text.startsWith("import ")) found = i;
            // A multi-line `import { … } from "…"` ends on its `from` line.
            else if (found >= 0 && /^}\s*from\s/.test(text)) found = i;
         }
         return found;
      })();
      const anchor = lastGiven >= 0 ? lastGiven : lastImport;
      const block = added
         .map((given) => {
            const tagLine = givenTagLine(given);
            return (
               (tagLine ? `${tagLine}\n` : "") + `${givenDeclaration(given)}\n`
            );
         })
         .join("\n");
      const at = anchor >= 0 ? wholeLine(anchor).end : wholeLine(0).end;
      edits.push({ start: at, end: at, text: `\n${block}` });

      // A file declaring a given needs the experiment switched on, at the top,
      // where every file in this repository that declares one puts it.
      const hasSwitch = lines.some((l) =>
         l.trim().startsWith("##! experimental.givens"),
      );
      if (!hasSwitch)
         edits.push({ start: 0, end: 0, text: "##! experimental.givens\n" });
   }

   // Presentation edits are matched by IDENTITY, not by position: after a
   // reorder `next.tiles[i]` and `current.tiles[i]` are different tiles, and
   // comparing them pairwise would report every moved tile as changed and
   // rewrite tags that nobody touched.
   const currentByKey = new Map(current.tiles.map((t) => [tileKey(t), t]));

   for (const tile of next.tiles) {
      const was = currentByKey.get(tileKey(tile));
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

      // The filter bindings live in the declaration itself, as a refinement.
      // Only the BINDING clauses are ours to rewrite: a `limit:`, an `order_by:`
      // or a `where:` on a literal that someone put in the same refinement is
      // unmodelled Malloy and stays, ahead of the bindings, exactly as written.
      const existing =
         /\+\s*\{([\s\S]*)\}\s*$/.exec(lines[declLine])?.[1] ?? "";
      const kept = existing
         .replace(BINDING_CLAUSE, "")
         .replace(/\s*,\s*,\s*/g, ", ")
         .replace(/^[\s,;]+|[\s,;]+$/g, "");
      const bindings = (tile.filters ?? []).map(
         (f) => `where: ${f.field} ${f.op ?? "~"} $${f.given}`,
      );
      const clauses = [...(kept ? [kept] : []), ...bindings];
      const withoutRefinement = lines[declLine].replace(
         /\s*\+\s*\{[\s\S]*\}\s*$/,
         "",
      );
      const rewritten =
         clauses.length === 0
            ? withoutRefinement
            : `${withoutRefinement} + { ${clauses.join(", ")} }`;
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

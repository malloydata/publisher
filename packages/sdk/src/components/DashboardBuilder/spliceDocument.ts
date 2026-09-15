// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   DashboardDocument,
   DashboardDrill,
   DashboardSource,
   DashboardTile,
} from "./document";
import type { LocalGiven } from "./document";
import {
   artifactLine,
   declarationLine,
   declarationsUnder,
   givenDeclarations,
} from "./malloyText";
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
 * this file, which is the convention {@link LocalGiven} describes. And tiles
 * ADDED or REMOVED: a new `view:` inside the extension of the source it reads
 * (or a new extension, when the file imports that source by name), a removed
 * one deleted with its `#` tags. Those moves are the one place the file cannot
 * say who owns the comment beside a declaration, so the builder shows the diff
 * before a structural save and the `//` comments are left where they were.
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

/**
 * The tag keys the document MODELS, and therefore the only tag lines the
 * writer may rewrite or remove. Every other `#` line on a tile — `# big_value`,
 * `# bar_chart`, `# currency`, a host's own tag — is the renderer's, not ours,
 * and survives an edit like any other unmodelled Malloy.
 *
 * Measured before this existed: unticking one filter on the storefront
 * overview's KPI strip deleted its `# big_value`, and the strip came back as a
 * one-row table. The round-trip gate cannot catch that, because the projection
 * never held the tag it lost.
 */
const MODELLED_TAG_KEYS: ReadonlySet<string> = new Set([
   "colspan",
   "break",
   "borderless",
   "label",
   "subtitle",
]);

const isSameDocumentExceptTiles = (
   a: DashboardDocument,
   b: DashboardDocument,
) => canonical(a.imports) === canonical(b.imports);

/**
 * A given's tag line, composed from its control contract. One line, in the
 * order `givens.malloy` writes them, so a file the builder wrote reads like one
 * a person wrote.
 */
function givenTagLine(given: LocalGiven): string | undefined {
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
const givenDeclaration = (given: LocalGiven) =>
   `given: ${given.name} :: ${given.type} is ${given.default}`;

/**
 * The last line of the declaration starting at `line`: the line itself for a
 * one-line `view: x is y + { … }`, or the matching closing brace for a body
 * (`view: x is {`, `source: s is b extend {`). Braces inside strings are not
 * a concern Malloy dashboards have raised, so the count is plain.
 */
function declarationEnd(lines: string[], line: number): number {
   let depth = 0;
   let opened = false;
   for (let i = line; i < lines.length; i++) {
      for (const ch of lines[i]) {
         if (ch === "{") {
            depth++;
            opened = true;
         } else if (ch === "}") depth--;
      }
      if (opened && depth <= 0) return i;
      if (!opened && i === line) return i;
   }
   return lines.length - 1;
}

/** `# drill { to=… given=… }`, one line, as the reader spells it back. */
function drillTagLine(drill: DashboardDrill): string {
   const to =
      drill.to.length === 1
         ? drill.to[0]
         : `[${drill.to.map((d) => `"${d}"`).join(", ")}]`;
   return `# drill { to=${to}${drill.given ? ` given=${drill.given}` : ""} }`;
}

const drillKey = (d: DashboardDrill) => `${d.source}.${d.name}`;

/**
 * One tile's identity, ignoring presentation and position — but not its
 * declaration: a tile redeclared from another view is a different tile to
 * the file, removed and added, which `document.tileKey` does not say.
 */
const tileKey = (t: DashboardTile) =>
   canonical([t.name, t.source, t.declaration]);

/** The tile list as identities, IN ORDER. Differs under a reorder. */
const tileIdentity = (document: DashboardDocument) =>
   canonical(document.tiles.map(tileKey));

interface TileMembership {
   currentKeys: Set<string>;
   nextKeys: Set<string>;
   removedTiles: DashboardTile[];
   addedTiles: DashboardTile[];
   newSources: DashboardSource[];
   currentSources: Map<string, DashboardSource>;
   reordered: boolean;
}

/** Everything a planner reads, and the edits it adds to. */
interface SpliceContext extends TileMembership {
   sourceText: string;
   lines: string[];
   starts: number[];
   wholeLine: (line: number) => { start: number; end: number };
   indentOf: (line: number) => string;
   current: DashboardDocument;
   next: DashboardDocument;
   edits: Edit[];
}

/**
 * Which tiles and sources come and go between the two documents, and whether
 * the change is one the builder may write at all.
 */
function checkShape(
   current: DashboardDocument,
   next: DashboardDocument,
): SpliceFailure | TileMembership {
   // Tiles ADDED and REMOVED, by identity. A removed tile's declaration goes,
   // with its `#` tags; a `//` comment above it stays, because the file cannot
   // say whether it belonged to the tile, the row or the page, and a comment
   // left behind is a smaller wrong than one destroyed — and the builder shows
   // this diff before it saves. An added tile is a `view:` in the extension of
   // the source it reads, or a new extension when the file has none yet.
   //
   // REORDERING is neither: a tile's position is the `tiles=[…]` array on the
   // `## artifact` tag, and moving a tile there moves no declaration.
   const currentKeys = new Set(current.tiles.map(tileKey));
   const nextKeys = new Set(next.tiles.map(tileKey));
   const removedTiles = current.tiles.filter((t) => !nextKeys.has(tileKey(t)));
   const addedTiles = next.tiles.filter((t) => !currentKeys.has(tileKey(t)));
   const membershipChanged = removedTiles.length > 0 || addedTiles.length > 0;
   const reordered =
      membershipChanged || tileIdentity(current) !== tileIdentity(next);
   for (const tile of addedTiles) {
      if (tile.declaration.kind !== "reference") {
         return {
            ok: false,
            reason:
               `A new tile names a view of a source; \`${tile.name}\` is ` +
               `${tile.declaration.kind === "inline" ? "an inline query" : "not declared here"}, ` +
               `which the builder cannot write.`,
         };
      }
   }
   // Sources may only be ADDED, and only for a tile being added on them — the
   // builder never renames or removes an extension, and never edits imports,
   // so a new extension's base has to be a source the file already imports by
   // name (or already extends).
   const currentSources = new Map(current.sources.map((s) => [s.name, s]));
   const importedByName = new Set(
      current.imports.flatMap((i) => (i.kind === "names" ? i.names : [])),
   );
   for (const source of current.sources) importedByName.add(source.base);
   const newSources = next.sources.filter((s) => !currentSources.has(s.name));
   for (const source of current.sources) {
      const still = next.sources.find((s) => s.name === source.name);
      if (!still || still.base !== source.base) {
         return {
            ok: false,
            reason: `The source \`${source.name}\` cannot be changed or removed here.`,
         };
      }
   }
   for (const source of newSources) {
      if (!addedTiles.some((t) => t.source === source.name)) {
         return {
            ok: false,
            reason: `A new source \`${source.name}\` needs a tile on it.`,
         };
      }
      if (!importedByName.has(source.base)) {
         return {
            ok: false,
            reason:
               `\`${source.base}\` is not imported by name in this file, so a ` +
               `tile cannot be put on it. The builder does not add imports: ` +
               `import { ${source.base} } from the model first.`,
         };
      }
   }
   if (!isSameDocumentExceptTiles(current, next)) {
      return {
         ok: false,
         reason: "The dashboard's imports cannot be changed here.",
      };
   }
   return {
      currentKeys,
      nextKeys,
      removedTiles,
      addedTiles,
      newSources,
      currentSources,
      reordered,
   };
}

function planOrder(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, wholeLine, next, reordered, currentKeys, nextKeys, edits } =
      ctx;
   // A reorder is one rewritten array on the `## artifact` line. Each entry is
   // re-emitted AS IT WAS WRITTEN rather than rebuilt from `source` and `name`,
   // so a file that spells a tile `overview->kpis` keeps its spelling and the
   // diff is the reordering and nothing else.
   if (reordered) {
      const artifactAt = artifactLine(lines);
      if (artifactAt < 0) {
         return {
            ok: false,
            reason: "Could not find the `## artifact` tag to reorder.",
         };
      }
      const written = [...lines[artifactAt].matchAll(/"([^"]+)"/g)].map(
         (m) => m[1],
      );
      const byKey = new Map<string, string>();
      for (const entry of written) {
         const parts = entry.split("->").map((part) => part.trim());
         if (parts.length === 2) byKey.set(`${parts[0]}->${parts[1]}`, entry);
      }
      // An existing tile keeps its spelling; a new one is written canonically.
      const nextEntries = next.tiles.map(
         (tile) =>
            byKey.get(`${tile.source}->${tile.name}`) ??
            (nextKeys.has(tileKey(tile)) && !currentKeys.has(tileKey(tile))
               ? `${tile.source} -> ${tile.name}`
               : undefined),
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
      const rewritten = lines[artifactAt].replace(
         /tiles\s*=\s*\[[\s\S]*?\]/,
         list,
      );
      if (rewritten !== lines[artifactAt])
         edits.push({ ...wholeLine(artifactAt), text: `${rewritten}\n` });
   }
   return undefined;
}

function planSettings(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, wholeLine, current, next, edits } = ctx;
   // THE PAGE'S OWN SETTINGS. Title, autorun and starting values are
   // properties on the one-line `## artifact { … }` tag; the grid width is the
   // `dashboard { columns=N }` beside it; the description is the run of `##"`
   // lines above. Each is patched in place on its own line, so the tag stays
   // on one line — the package fails to compile otherwise — and a property
   // the file spells its own way keeps that spelling when it did not change.
   if (
      current.title !== next.title ||
      current.autorun !== next.autorun ||
      current.columns !== next.columns ||
      canonical(current.startingGivens) !== canonical(next.startingGivens)
   ) {
      const artifactAt = artifactLine(lines);
      if (artifactAt < 0) {
         return {
            ok: false,
            reason:
               "Could not find the `## artifact` tag to change the page's settings.",
         };
      }
      // Whatever the reorder wrote to this line is the text to patch further.
      const already = edits.find(
         (edit) =>
            edit.start === wholeLine(artifactAt).start &&
            edit.end === wholeLine(artifactAt).end,
      );
      let line = already ? already.text.replace(/\n$/, "") : lines[artifactAt];
      // The artifact tag's braces: everything up to the matching `}`.
      const open = line.indexOf("artifact");
      const braceOpen = line.indexOf("{", open);
      let depth = 0;
      let braceClose = -1;
      for (let i = braceOpen; i < line.length; i++) {
         if (line[i] === "{") depth++;
         else if (line[i] === "}" && --depth === 0) {
            braceClose = i;
            break;
         }
      }
      if (braceOpen < 0 || braceClose < 0) {
         return {
            ok: false,
            reason: "Could not read the `## artifact { … }` tag.",
         };
      }
      let inner = line.slice(braceOpen + 1, braceClose);
      const setProperty = (key: string, value: string | undefined) => {
         const re = new RegExp(
            `\\s*\\b${key}=(?:"(?:[^"\\\\]|\\\\.)*"|[^\\s}]+)`,
         );
         if (value === undefined) inner = inner.replace(re, "");
         else if (re.test(inner)) inner = inner.replace(re, ` ${key}=${value}`);
         else inner = `${inner.replace(/\s+$/, "")} ${key}=${value} `;
      };
      if (current.title !== next.title)
         setProperty(
            "title",
            next.title ? `"${next.title.replace(/"/g, '\\"')}"` : undefined,
         );
      if (current.autorun !== next.autorun)
         setProperty(
            "autorun",
            next.autorun === undefined ? undefined : String(next.autorun),
         );
      if (
         canonical(current.startingGivens) !== canonical(next.startingGivens)
      ) {
         inner = inner.replace(/\s*\bgivens\s*\{[^}]*\}/, "");
         const entries = Object.entries(next.startingGivens ?? {});
         // Quoted: a tag value is a string, and the reader hands back the
         // string's text — `CATEGORY="Jeans"` reads as `Jeans`, which is
         // what the document holds and what is written back here.
         if (entries.length > 0)
            inner = `${inner.replace(/\s+$/, "")} givens { ${entries
               .map(([k, v]) => `${k}="${v.replace(/"/g, '\\"')}"`)
               .join(" ")} }`;
      }
      inner = inner.replace(/\s{2,}/g, " ");
      line = `${line.slice(0, braceOpen + 1)}${inner.startsWith(" ") ? inner : ` ${inner}`}${inner.endsWith(" ") ? "" : " "}${line.slice(braceClose)}`;
      if (current.columns !== next.columns) {
         line = line.replace(/\s*dashboard\s*\{[^}]*\}/, "");
         if (next.columns !== undefined)
            line = `${line.trimEnd()} dashboard { columns=${next.columns} }`;
      }
      if (already) already.text = `${line}\n`;
      else edits.push({ ...wholeLine(artifactAt), text: `${line}\n` });
   }
   if (current.description !== next.description) {
      // The run of `##"` lines, wherever it is; a new one goes above the tag.
      const docLines = lines
         .map((l, i) => (l.trim().startsWith('##"') ? i : -1))
         .filter((i) => i >= 0);
      const text = (next.description ?? "")
         .split("\n")
         .map((para) => (para.trim() === "" ? '##"' : `##" ${para.trim()}`))
         .join("\n");
      if (docLines.length > 0) {
         const first = docLines[0];
         const last = docLines[docLines.length - 1];
         edits.push({
            start: wholeLine(first).start,
            end: wholeLine(last).end,
            text: next.description === undefined ? "" : `${text}\n`,
         });
      } else if (next.description !== undefined) {
         const artifactAt = artifactLine(lines);
         const at = wholeLine(artifactAt).start;
         edits.push({ start: at, end: at, text: `${text}\n` });
      }
   }
   return undefined;
}

function planGivens(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, wholeLine, indentOf, current, next, edits } = ctx;
   // THE DASHBOARD'S OWN GIVENS. Added, removed, or retagged — by name, since
   // a given's name is its identity in every `where:` that reads it.
   const givensBefore = new Map(
      (current.localGivens ?? []).map((g) => [g.name, g]),
   );
   const givensAfter = new Map(
      (next.localGivens ?? []).map((g) => [g.name, g]),
   );
   const declared = givenDeclarations(lines);
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
   return undefined;
}

function planDrills(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, wholeLine, indentOf, current, next, edits } = ctx;
   // DRILLS. A drill is one `# drill` tag on a dimension THIS FILE declares —
   // added above the declaration, rewritten, or taken off. The dimension itself
   // is never written: a dimension no view reads is a dead drill, and the
   // builder does not edit views, so the author declares the dimension and the
   // builder makes it clickable.
   const drillsBefore = new Map(
      (current.drills ?? []).map((d) => [drillKey(d), d]),
   );
   const drillsAfter = new Map(
      (next.drills ?? []).map((d) => [drillKey(d), d]),
   );
   for (const key of new Set([...drillsBefore.keys(), ...drillsAfter.keys()])) {
      const was = drillsBefore.get(key);
      const want = drillsAfter.get(key);
      if (was && want && canonical(was) === canonical(want)) continue;
      const drill = (want ?? was) as DashboardDrill;
      if (want && want.to.length === 0) {
         return {
            ok: false,
            reason: `The drill on \`${key}\` names no destination.`,
         };
      }
      const at = declarationsUnder(lines, drill.source, "dimension").get(
         drill.name,
      );
      if (at === undefined) {
         return {
            ok: false,
            reason:
               `\`${drill.name}\` is not a dimension \`${drill.source}\` declares ` +
               `in this file, so a drill cannot be put on it here.`,
         };
      }
      const { tags } = blockAbove(lines, at.line);
      const existing = tags.find((tag) => /^#\s*drill\b/.test(tag.text));
      const indent = indentOf(at.line);
      if (want === undefined) {
         if (existing) edits.push({ ...wholeLine(existing.line), text: "" });
      } else if (existing) {
         edits.push({
            ...wholeLine(existing.line),
            text: `${indent}${drillTagLine(want)}\n`,
         });
      } else {
         // Directly above the declaration, under any other tags it has.
         const start = wholeLine(at.line).start;
         edits.push({
            start,
            end: start,
            text: `${indent}${drillTagLine(want)}\n`,
         });
      }
   }
   return undefined;
}

function planRemovedTiles(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, wholeLine, removedTiles, edits } = ctx;
   // REMOVED TILES: the declaration and its `#` tags. An inline view's body
   // runs to its closing brace; a reference is one line. An inherited tile has
   // nothing here to remove — its entry left the artifact list above.
   for (const tile of removedTiles) {
      if (tile.declaration.kind === "inherited") continue;
      const declLine = declarationLine(lines, "view", tile.name);
      if (declLine < 0) {
         return {
            ok: false,
            reason: `Could not find where \`${tile.name}\` is declared.`,
         };
      }
      const endLine = declarationEnd(lines, declLine);
      const { tags } = blockAbove(lines, declLine);
      const first = Math.min(declLine, ...tags.map((t) => t.line));
      for (const tag of tags) edits.push({ ...wholeLine(tag.line), text: "" });
      edits.push({
         start: wholeLine(declLine).start,
         end: wholeLine(endLine).end,
         text: "",
      });
      // The blank line after it goes too when what came before was a blank
      // line or the extension's opening brace — otherwise two separators meet,
      // or the body starts with an empty line. It STAYS after a `//` comment
      // that is being left behind: closing the gap would hand that comment to
      // the next tile, which is the ownership guess this whole rule avoids.
      const before = first === 0 ? "" : lines[first - 1].trim();
      if (
         (lines[endLine + 1] ?? "x").trim() === "" &&
         (first === 0 || before === "" || before.endsWith("{"))
      )
         edits.push({ ...wholeLine(endLine + 1), text: "" });
   }
   return undefined;
}

function planAddedTiles(ctx: SpliceContext): SpliceFailure | undefined {
   const {
      sourceText,
      lines,
      wholeLine,
      current,
      addedTiles,
      newSources,
      currentSources,
      edits,
   } = ctx;
   // ADDED TILES: a `view:` with its tags, inside the extension of the source
   // the tile reads — before that extension's closing brace — or in a new
   // extension after the last one, when the file has none for that source.
   const byExtension = new Map<string, DashboardTile[]>();
   for (const tile of addedTiles) {
      const list = byExtension.get(tile.source) ?? [];
      list.push(tile);
      byExtension.set(tile.source, list);
   }
   const declarationOf = (tile: DashboardTile, indent: string) => {
      const from =
         tile.declaration.kind === "reference"
            ? tile.declaration.from
            : tile.name;
      const bindings = (tile.filters ?? [])
         .map((f) => `where: ${f.field} ${f.op ?? "~"} $${f.given}`)
         .join(", ");
      return [
         ...tagsFor(tile).map((tag) => `${indent}${tag}`),
         `${indent}view: ${tile.name} is ${from}${bindings ? ` + { ${bindings} }` : ""}`,
      ].join("\n");
   };
   let lastExtensionEnd = -1;
   for (const source of current.sources) {
      const open = declarationLine(lines, "source", source.name);
      if (open >= 0)
         lastExtensionEnd = Math.max(
            lastExtensionEnd,
            declarationEnd(lines, open),
         );
   }
   for (const [sourceName, tiles] of byExtension) {
      if (currentSources.has(sourceName)) {
         const open = declarationLine(lines, "source", sourceName);
         if (open < 0) {
            return {
               ok: false,
               reason: `Could not find where the source \`${sourceName}\` is declared.`,
            };
         }
         const close = declarationEnd(lines, open);
         // The indent the extension already uses for its views, else two spaces.
         let indent = "  ";
         for (let i = open + 1; i < close; i++) {
            const m = /^(\s+)view:/.exec(lines[i]);
            if (m) {
               indent = m[1];
               break;
            }
         }
         // Before the closing brace, set off by a blank line from what precedes.
         const at = wholeLine(close).start;
         const precededByBlank = (lines[close - 1] ?? "").trim() === "";
         edits.push({
            start: at,
            end: at,
            text:
               (precededByBlank ? "" : "\n") +
               tiles.map((tile) => declarationOf(tile, indent)).join("\n\n") +
               "\n",
         });
      } else {
         const source = newSources.find((s) => s.name === sourceName);
         if (!source) {
            return {
               ok: false,
               reason: `The tile's source \`${sourceName}\` is not declared.`,
            };
         }
         const block =
            `\nsource: ${source.name} is ${source.base} extend {\n` +
            tiles.map((tile) => declarationOf(tile, "  ")).join("\n\n") +
            "\n}\n";
         // After the last extension; failing that, after the last given or
         // import, where the page's own declarations begin.
         let anchor = lastExtensionEnd;
         if (anchor < 0) {
            for (let i = 0; i < lines.length; i++) {
               const text = lines[i].trim();
               if (
                  /^(import\s|given:|}\s*from\s)/.test(text) ||
                  /^[A-Z_]+\s*::/.test(text)
               )
                  anchor = i;
            }
         }
         const at = anchor >= 0 ? wholeLine(anchor).end : sourceText.length;
         // A file that ends without a newline needs one before the block.
         const separator =
            at === sourceText.length && !sourceText.endsWith("\n") ? "\n" : "";
         edits.push({ start: at, end: at, text: separator + block });
         lastExtensionEnd = anchor;
      }
   }
   return undefined;
}

function planTilePresentation(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, starts, wholeLine, indentOf, current, next, edits } = ctx;
   // Presentation edits are matched by IDENTITY, not by position: after a
   // reorder `next.tiles[i]` and `current.tiles[i]` are different tiles, and
   // comparing them pairwise would report every moved tile as changed and
   // rewrite tags that nobody touched.
   const currentByKey = new Map(current.tiles.map((t) => [tileKey(t), t]));

   for (const tile of next.tiles) {
      const was = currentByKey.get(tileKey(tile));
      // Written whole above, tags and all.
      if (was === undefined) continue;
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
         // Not a property this document models: not ours to touch.
         if (key === undefined || !MODELLED_TAG_KEYS.has(key)) continue;
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
   return undefined;
}

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
   const shape = checkShape(current, next);
   if ("reason" in shape) return shape;

   const lines = sourceText.split("\n");
   const starts = lineStarts(sourceText);
   const wholeLine = (line: number): { start: number; end: number } => ({
      start: starts[line],
      end: line + 1 < starts.length ? starts[line + 1] : sourceText.length,
   });
   const indentOf = (line: number) => /^\s*/.exec(lines[line])?.[0] ?? "";

   const edits: Edit[] = [];
   const ctx: SpliceContext = {
      ...shape,
      sourceText,
      lines,
      starts,
      wholeLine,
      indentOf,
      current,
      next,
      edits,
   };

   // Each concern plans its own edits against the file as it stands; the
   // order matters only where one patches a line another rewrote, which the
   // settings planner handles by patching the reorder's text.
   for (const plan of [
      planOrder,
      planSettings,
      planGivens,
      planDrills,
      planRemovedTiles,
      planAddedTiles,
      planTilePresentation,
   ]) {
      const failure = plan(ctx);
      if (failure) return failure;
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
   // Drills are compared as a set: their order is the file's, and a caller
   // appending one to the array has no way to know where its dimension sits.
   const comparable = (document: DashboardDocument): DashboardDocument => ({
      ...document,
      ...(document.drills
         ? {
              drills: [...document.drills].sort((a, b) =>
                 drillKey(a).localeCompare(drillKey(b)),
              ),
           }
         : {}),
   });
   if (canonical(comparable(after.document)) !== canonical(comparable(next))) {
      return {
         ok: false,
         reason:
            "The edit did not produce the dashboard that was asked for, so it " +
            "was not written. Your changes are still here.",
      };
   }

   return { ok: true, source: spliced };
}

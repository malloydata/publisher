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
   declarationExtent,
   declarationLine,
   declarationsUnder,
   givenDeclarations,
   maskQuoted,
   splitTrailingComment,
   viewBodyStage1,
} from "./malloyText";
import {
   blockAbove,
   cleanBindingClauses,
   isBindingOnly,
   readDashboardDocument,
   readFailed,
} from "./readDocument";

/**
 * The syntax errors Malloy's own parser reports for `text`, as a multiset of
 * messages.
 *
 * The first `translate()` is enough and is all we want: a document with
 * imports comes back NOT final, asking for the urls it needs, and by then the
 * parse has already happened while nothing has been resolved. So this sees
 * every syntax error and none of the semantic ones, which are not this
 * writer's business -- a dashboard referring to a source in a file we decline
 * to hand over is not damage we caused.
 */
export async function syntaxErrors(text: string): Promise<string[]> {
   // Imported dynamically, never statically: `builder-entry.ts` installs the
   // `process.env` shim the parser's dependencies read at module scope, and a
   // static import here would be evaluated before that shim runs. The reader
   // loads the parser the same way, for the same reason.
   const { MalloyTranslator } = await import("@malloydata/malloy");
   const url = "file://splice-check.malloy";
   const result = new MalloyTranslator(url, null, {
      urls: { [url]: text },
   }).translate() as { problems?: Array<{ code?: string; message?: string }> };
   return (result.problems ?? [])
      .filter((p) => p.code === "syntax-error")
      .map((p) => p.message ?? "")
      .sort();
}

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
 * a mismatch refuses the write. Byte-identity with a regenerated file would be
 * a weaker thing to know, and would make every commented file read-only.
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
   // Back to front, so an earlier edit's offsets still address the text it was
   // planned against. Where two share a start — an inserted tag line and a
   // rewrite of the declaration it sits above — the replacement has to go
   // first, or the insertion shifts the text out from under its end offset and
   // the rewrite lands in the middle of what was just inserted.
   for (const edit of [...edits].sort(
      (a, b) => b.start - a.start || b.end - a.end,
   ))
      out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
   return out;
}

/** The `#` tags a tile's presentation implies, in the order they are written. */
function tagsFor(tile: DashboardTile): string[] {
   const tags: string[] = [];
   if (tile.colspan !== undefined) tags.push(`# colspan=${tile.colspan}`);
   if (tile.break) tags.push("# break");
   if (tile.borderless) tags.push("# borderless");
   if (tile.label !== undefined) tags.push(`# label=${quoted(tile.label)}`);
   if (tile.subtitle !== undefined)
      tags.push(`# subtitle=${quoted(tile.subtitle)}`);
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
 * Without this rule, unticking one filter on the storefront overview's KPI
 * strip deleted its `# big_value` and the strip came back as a one-row table;
 * the round-trip gate cannot catch that, because the projection never held
 * the tag it lost.
 */
const MODELLED_TAG_KEYS: ReadonlySet<string> = new Set([
   "colspan",
   "break",
   "borderless",
   "label",
   "subtitle",
]);

/**
 * The same rule for a given: the keys {@link givenTagLine} writes are the only
 * ones the writer may rewrite or remove. A routed annotation such as
 * `#(secure)` has no key at all, so it is never ours, and a relabel that took
 * it with the control contract would strip an access marker the projection
 * cannot see it lost.
 */
const MODELLED_GIVEN_TAG_KEYS: ReadonlySet<string> = new Set([
   "label",
   "description",
   "control",
   "suggest",
   "range_min",
   "range_max",
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
   if (given.label !== undefined) parts.push(`label=${quoted(given.label)}`);
   if (given.description !== undefined)
      parts.push(`description=${quoted(given.description)}`);
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

/** A name as a regex literal: view and source names are identifiers, but the
 * pattern is built from document data and should not be able to mean anything
 * else. */
const literal = (name: string) => name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * The line where `<source>` declares `view: <view>`, or -1.
 *
 * Scoped to that source's own `extend { … }` block, because a view name is
 * only unique WITHIN a source: two sources in one dashboard may each declare
 * `by_month`, and a file-wide search finds whichever comes first and then
 * rewrites its tags — silently retagging a tile the author never touched.
 *
 * A source this file does not extend has nothing here to patch, so -1 is the
 * honest answer; the caller refuses rather than reaching for another source's
 * view. Tiles whose view lives on the model are `inherited` and never get
 * this far.
 */
function viewDeclarationLine(
   lines: string[],
   sourceName: string,
   viewName: string,
): number {
   const sourceLine = lines.findIndex((line) =>
      new RegExp(`\\bsource:\\s*${literal(sourceName)}\\s+is\\b`).test(line),
   );
   if (sourceLine < 0) return -1;
   const extent = declarationExtent(lines, sourceLine);
   // A source whose own extent cannot be trusted has nothing here to search;
   // the caller already produces a specific refusal for "not found".
   if ("unreadable" in extent) return -1;
   const wanted = new RegExp(`\\bview:\\s*${literal(viewName)}\\s+is\\b`);
   for (let i = sourceLine; i <= extent.end; i++)
      if (wanted.test(lines[i])) return i;
   return -1;
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

/** A tag string value, with the characters the tag parser unescapes escaped. */
const quoted = (text: string) =>
   `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;

/**
 * One tile's identity TO THE FILE, ignoring presentation and position — but
 * not its declaration: a tile redeclared from another view is a different tile
 * to the file, removed and added, which `document.tileKey` does not say.
 * `document.tileKey` is the grid's identity, which a drag names and React keys
 * on; this one decides what gets written. Anything asking "will this save
 * rewrite declarations?" wants THIS key.
 */
export const tileFileKey = (t: DashboardTile) =>
   canonical([t.name, t.source, t.declaration]);
const tileKey = tileFileKey;

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
         setProperty("title", next.title ? quoted(next.title) : undefined);
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
               .map(([k, v]) => `${k}=${quoted(v)}`)
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
      // Only the keys this document models are the writer's to rewrite. A
      // removed declaration is the exception and takes every tag with it: a
      // `#` line left behind does not lapse, it attaches to whatever is
      // declared next, so an orphaned `#(secure)` would silently move.
      const { tags } = blockAbove(lines, at.line);
      const owned =
         want === undefined
            ? tags
            : tags.filter((tag) => {
                 const key = tagKey(tag.text);
                 return key !== undefined && MODELLED_GIVEN_TAG_KEYS.has(key);
              });
      for (const tag of owned) {
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
      const extent = declarationExtent(lines, declLine);
      if ("unreadable" in extent) {
         return {
            ok: false,
            reason: `Could not tell where \`${tile.name}\` ends: ${extent.unreadable}.`,
         };
      }
      const endLine = extent.end;
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
      if (open < 0) continue;
      const extent = declarationExtent(lines, open);
      // An anchor for a NEW extension only; a source this scan cannot read is
      // simply not counted, rather than failing a splice that never touches it.
      if ("unreadable" in extent) continue;
      lastExtensionEnd = Math.max(lastExtensionEnd, extent.end);
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
         const extent = declarationExtent(lines, open);
         if ("unreadable" in extent) {
            return {
               ok: false,
               reason:
                  `Could not find where \`${sourceName}\`'s extension ends: ` +
                  `${extent.unreadable}.`,
            };
         }
         if (!extent.opened) {
            return {
               ok: false,
               reason:
                  `\`${sourceName}\` never opens an \`extend { … }\` block, so a ` +
                  `new tile cannot be added inside it.`,
            };
         }
         const close = extent.end;
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

      const declLine = viewDeclarationLine(lines, tile.source, tile.name);
      if (declLine < 0) {
         return {
            ok: false,
            reason:
               `Could not find where \`${tile.name}\` is declared inside ` +
               `\`${tile.source}\`.`,
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

      // The filter bindings live in the declaration itself either way. Only
      // the BINDING clauses are ours to rewrite: a `limit:`, an `order_by:`
      // or a `where:` on a literal someone wrote is unmodeled Malloy and
      // stays exactly as written, in both shapes below.
      if (tile.declaration.kind === "reference") {
         // A `+ { where: … }` refinement on the view reference. A trailing
         // `//` comment is set aside first and put back after, because the
         // refinement goes at the END of the code and a comment there would
         // swallow it.
         const { code, comment } = splitTrailingComment(lines[declLine]);
         const existing = /\+\s*\{([\s\S]*)\}\s*$/.exec(code)?.[1] ?? "";
         // Removed by SPAN, never by a global regex over the free text: the
         // regex matches the `where: a ~ $A` prefix of `where: a ~ $A and c
         // = 1` too, and cutting that out strands `and c = 1` as a statement
         // of its own. `cleanBindingClauses` reports only clauses it is safe
         // to excise whole.
         const cleanExisting = cleanBindingClauses(existing);
         let kept = existing;
         for (let i = cleanExisting.length - 1; i >= 0; i--)
            kept =
               kept.slice(0, cleanExisting[i].start) +
               kept.slice(cleanExisting[i].end);
         kept = kept
            .replace(/\s*,\s*,\s*/g, ", ")
            .replace(/^[\s,;]+|[\s,;]+$/g, "");
         const clash = givenCollision(tile.filters, kept);
         if (clash) return collisionRefusal(tile.name, clash.given);
         const bindings = (tile.filters ?? []).map(
            (f) => `where: ${f.field} ${f.op ?? "~"} $${f.given}`,
         );
         // Bindings are comma-joined to each other, but only SPACED from
         // whatever was already there: Malloy rejects a comma after a
         // `limit:`, while a space parses after every statement form.
         const clauses = [
            ...(kept ? [kept] : []),
            ...(bindings.length > 0 ? [bindings.join(", ")] : []),
         ];
         const withoutRefinement = code.replace(/\s*\+\s*\{[\s\S]*\}\s*$/, "");
         const body =
            clauses.length === 0
               ? withoutRefinement
               : `${withoutRefinement.trimEnd()} + { ${clauses.join(" ")} }`;
         const rewritten =
            comment === "" ? body : `${body.trimEnd()} ${comment}`;
         if (rewritten !== lines[declLine])
            edits.push({ ...wholeLine(declLine), text: `${rewritten}\n` });
      } else if (canonical(was.filters) !== canonical(tile.filters)) {
         // An inline tile whose filters actually changed — nothing to do
         // otherwise, and nothing safe to do for a body shape
         // planInlineFilters refuses (see its doc comment).
         const failure = planInlineFilters(ctx, tile, declLine);
         if (failure) return failure;
      }
   }
   return undefined;
}

/**
 * Rewrite an inline tile's `where:` bindings: depth-1 statements in the
 * body's own first stage (see {@link viewBodyStage1}), one per line for a
 * multi-line body or comma-joined inside the braces for a one-line one.
 * Existing binding lines are patched by GIVEN NAME, the same way
 * {@link planTilePresentation}'s tags are: rewritten in place if changed,
 * dropped if no longer bound, and a survivor carrying anything unmodeled (a
 * compound predicate, say) is left untouched because it was never a binding
 * to begin with. New bindings go after the last existing binding line, or
 * before the first stage's closing brace when there is none.
 *
 * Refuses a body whose first stage this scan cannot pin down — a `->` second
 * stage, or a `{ … } + { … }` compound refinement — because there would be no
 * single place to put the binding. The reader still opens such a file; this
 * only blocks WRITING a filter change onto it.
 */
/**
 * The requested binding whose given already appears in `text`, if any. `text`
 * is whatever the builder does NOT own, with the managed clauses removed, and
 * quoted literals are masked so a given's name inside a string is not mistaken
 * for a use of it.
 */
function givenCollision(
   filters: Array<{ field: string; given: string; op?: string }> | undefined,
   text: string,
): { given: string } | undefined {
   const scanned = maskQuoted(text);
   return (filters ?? []).find((f) => {
      const name = f.given.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return new RegExp(`\\$${name}(?![A-Za-z0-9_])`).test(scanned);
   });
}

function collisionRefusal(tileName: string, given: string): SpliceFailure {
   return {
      ok: false,
      reason:
         `\`${tileName}\` already filters on \`$${given}\` in a \`where:\` ` +
         `the builder does not manage, so binding it again would filter on ` +
         `it twice. Edit that \`where:\` in the file instead.`,
   };
}

function planInlineFilters(
   ctx: SpliceContext,
   tile: DashboardTile,
   declLine: number,
): SpliceFailure | undefined {
   const { lines, starts, wholeLine, indentOf, edits } = ctx;
   const extent = declarationExtent(lines, declLine);
   if ("unreadable" in extent) {
      return {
         ok: false,
         reason: `Could not tell where \`${tile.name}\`'s body ends: ${extent.unreadable}.`,
      };
   }
   const stage = viewBodyStage1(lines, declLine, extent.end);
   if (stage.more) {
      return {
         ok: false,
         reason:
            `\`${tile.name}\`'s body is a multi-stage \`->\` pipeline or a ` +
            `\`{ … } + { … }\` compound refinement, so there is no single ` +
            `first stage to write its filter into.`,
      };
   }

   const bindingText = (f: { field: string; given: string; op?: string }) =>
      `where: ${f.field} ${f.op ?? "~"} $${f.given}`;

   if (stage.oneLiner !== undefined) {
      const { openCol, closeCol, content } = stage.oneLiner;
      const clean = cleanBindingClauses(content);
      let kept = content;
      for (let i = clean.length - 1; i >= 0; i--)
         kept = kept.slice(0, clean[i].start) + kept.slice(clean[i].end);
      kept = kept.replace(/\s*,\s*,\s*/g, ", ").replace(/^[\s,]+|[\s,]+$/g, "");
      const clash = givenCollision(tile.filters, kept);
      if (clash) return collisionRefusal(tile.name, clash.given);
      const bindings = (tile.filters ?? []).map(bindingText);
      const rebuilt = [...(kept ? [kept] : []), ...bindings].join(", ");
      const raw = lines[declLine];
      const before = raw.slice(0, openCol + 1);
      const after = raw.slice(closeCol);
      const rewritten = rebuilt
         ? `${before} ${rebuilt} ${after}`
         : `${before} ${after}`;
      if (rewritten !== raw)
         edits.push({ ...wholeLine(declLine), text: `${rewritten}\n` });
      return undefined;
   }

   const wantedByGiven = new Map((tile.filters ?? []).map((f) => [f.given, f]));
   const existing: Array<{
      line: number;
      startCol: number;
      endCol: number;
      givens: string[];
   }> = [];
   // Text in the first stage that this writer does NOT own: a compound
   // predicate, or a clause list running onto the next line. It is left
   // exactly as written -- but a given it mentions cannot also be bound as a
   // managed clause, because the two would filter on the same control while
   // only one of them is the builder's to remove again.
   for (const wl of stage.whereLines) {
      const clean = wl.continued ? undefined : isBindingOnly(wl.code);
      if (!clean) continue; // not ours: a compound predicate or the like
      existing.push({
         line: wl.line,
         startCol: wl.startCol,
         endCol: wl.endCol,
         givens: clean.map((c) => c.given),
      });
   }

   // Scanned over the whole first stage with the managed clauses blanked out,
   // rather than over the `where:` LINES this scan recognized: a statement
   // running onto a second line is exactly the shape that is only half-visible
   // here, so collecting per-line would miss the half that matters.
   const remainder = lines
      .slice(declLine, stage.end + 1)
      .map((line, offset) => {
         const at = declLine + offset;
         let out = line;
         for (const ex of existing)
            if (ex.line === at)
               out =
                  out.slice(0, ex.startCol) +
                  " ".repeat(ex.endCol - ex.startCol) +
                  out.slice(ex.endCol);
         return out;
      })
      .join("\n");
   const clash = givenCollision(tile.filters, remainder);
   if (clash) return collisionRefusal(tile.name, clash.given);

   const seenGivens = new Set<string>();
   let lastBindingLine = -1;
   for (const ex of existing) {
      lastBindingLine = ex.line;
      for (const given of ex.givens) seenGivens.add(given);
      const stillWanted = ex.givens
         .filter((given) => wantedByGiven.has(given))
         .map(
            (given) =>
               wantedByGiven.get(given) as {
                  field: string;
                  given: string;
                  op?: string;
               },
         );
      const raw = lines[ex.line];
      const lineStart = starts[ex.line];
      if (stillWanted.length === 0) {
         // A clause alone on its own line — nothing but indentation before
         // it, nothing after — is dropped whole line and all, so no blank
         // line is left behind. One sharing its line with the body's `{` or
         // `}` (or another statement) has only its own span removed, so that
         // brace survives; `before` keeps its original indent when there is
         // nothing structural in it to trim, which is what a bare `}` left
         // behind reuses as its own.
         const beforeAll = raw.slice(0, ex.startCol);
         const afterAll = raw.slice(ex.endCol);
         const hasBefore = beforeAll.trim() !== "";
         const hasAfter = afterAll.trim() !== "";
         if (!hasBefore && !hasAfter) {
            edits.push({ ...wholeLine(ex.line), text: "" });
            continue;
         }
         const before = hasBefore ? beforeAll.trimEnd() : beforeAll;
         const after = afterAll.trimStart();
         const joined =
            hasBefore && hasAfter ? `${before} ${after}` : `${before}${after}`;
         edits.push({
            start: lineStart,
            end: wholeLine(ex.line).end,
            text: `${joined}\n`,
         });
         continue;
      }
      const rebuiltLine = stillWanted.map(bindingText).join(", ");
      // A span replace of just the clause's own columns, not the whole
      // line — a `{` or `}` sharing the line, or a trailing comment, sits
      // outside [startCol, endCol) and is carried over untouched.
      if (rebuiltLine !== raw.slice(ex.startCol, ex.endCol))
         edits.push({
            start: lineStart + ex.startCol,
            end: lineStart + ex.endCol,
            text: rebuiltLine,
         });
   }

   const added = (tile.filters ?? []).filter((f) => !seenGivens.has(f.given));
   if (added.length > 0) {
      const bodyIndent =
         existing.length > 0
            ? indentOf(existing[0].line)
            : firstBodyIndent(lines, declLine, stage.end, indentOf);
      // A surviving binding on the body's own closing line: appending after
      // the WHOLE line would land past the `}`, so the new binding is
      // spliced in right after that clause's own span instead — still
      // inside the body, and still after what was already there, which
      // whole-line insertion could not be, either way.
      const closingLineExisting = existing.find((e) => e.line === stage.end);
      const closingLineSurvives =
         closingLineExisting !== undefined &&
         closingLineExisting.givens.some((g) => wantedByGiven.has(g));
      if (closingLineExisting && closingLineSurvives) {
         const at =
            starts[closingLineExisting.line] + closingLineExisting.endCol;
         const text = added
            .map((f) => `\n${bodyIndent}${bindingText(f)}`)
            .join("");
         edits.push({ start: at, end: at, text });
      } else {
         const text = added
            .map((f) => `${bodyIndent}${bindingText(f)}\n`)
            .join("");
         const at =
            lastBindingLine >= 0 && lastBindingLine !== stage.end
               ? wholeLine(lastBindingLine).end
               : wholeLine(stage.end).start;
         edits.push({ start: at, end: at, text });
      }
   }
   return undefined;
}

/** The indent of the first statement inside a body with no binding line yet. */
function firstBodyIndent(
   lines: string[],
   declLine: number,
   stageEnd: number,
   indentOf: (line: number) => string,
): string {
   for (let i = declLine + 1; i < stageEnd; i++) {
      const trimmed = lines[i].trim();
      if (trimmed === "" || trimmed.startsWith("#") || trimmed.startsWith("//"))
         continue;
      return indentOf(i);
   }
   // A body with no statement to copy an indent from. Two spaces past the
   // declaration's own, which is what the rest of this writer assumes when it
   // has nothing else to go on.
   return `${indentOf(declLine)}  `;
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

   // Drills are compared as a set: their order is the file's, and a caller
   // appending one to the array has no way to know where its dimension sits.
   //
   // An optional collection the reader omits and a caller materialises as `[]`
   // are the same document, so they compare equal. Without that, a host that
   // normalises its shape asks for no change, gets no edits, and would be told
   // its save did not produce what it asked for.
   const comparable = (document: DashboardDocument): DashboardDocument => {
      const { drills, localGivens, ...rest } = document;
      const tiles = document.tiles.map((tile) => {
         if (tile.filters?.length) return tile;
         const { filters: _filters, ...tileRest } = tile;
         return tileRest as DashboardTile;
      });
      return {
         ...rest,
         tiles,
         ...(drills?.length
            ? {
                 drills: [...drills].sort((a, b) =>
                    drillKey(a).localeCompare(drillKey(b)),
                 ),
              }
            : {}),
         ...(localGivens?.length ? { localGivens } : {}),
      } as DashboardDocument;
   };

   // No edit means no planner found anything to place. That is only correct if
   // the document asked for is the one already on disk -- otherwise the ask
   // fell through every planner and returning `ok` would report a write that
   // never happened.
   if (edits.length === 0) {
      if (canonical(comparable(current)) !== canonical(comparable(next))) {
         return {
            ok: false,
            reason:
               "No part of this edit was recognized, so nothing was written. " +
               "Your changes are still here.",
         };
      }
      return { ok: true, source: sourceText };
   }

   const spliced = applyEdits(sourceText, edits);

   // The first gate, and the only one that can see damage OUTSIDE the part of
   // the file the builder models. The readback below compares the projection
   // -- tiles, tags, filters -- so text this writer strands beside a binding
   // it did rewrite is invisible to it: the orphan is not a filter, so the
   // comparison it would have to fail never looks at it. Malloy's parser is
   // the only reader here that judges the whole file.
   //
   // What this promises is narrow on purpose: a file that parsed before still
   // parses after. It says nothing about one that was already broken -- an
   // editor can open a file the compiler rejects, and refusing every save on
   // it would trap the user with no way out.
   //
   // Comparing the two error lists instead, and refusing what looks new, is
   // what this replaced: the parser's messages quote the tokens around the
   // error, so inserting an unrelated line rewrites the message of a fault
   // that was already there and it reads as one we just caused.
   if ((await syntaxErrors(sourceText)).length === 0) {
      const broke = await syntaxErrors(spliced);
      if (broke.length > 0) {
         return {
            ok: false,
            reason:
               "The edit would have produced a file Malloy cannot parse, so " +
               `it was not written (${broke[0]}). Your changes are still here.`,
         };
      }
   }

   // The second gate. Read back what was actually written and compare it
   // against what was asked for. Comments survived because they were never
   // rewritten; correctness is established here rather than assumed.
   const after = await readDashboardDocument(spliced);
   if (readFailed(after)) {
      return {
         ok: false,
         reason: `The edit produced a file that cannot be read back: ${after.reason}`,
      };
   }
   if (canonical(comparable(after.document)) !== canonical(comparable(next))) {
      return {
         ok: false,
         reason:
            "What was written did not read back as what was asked for, so it " +
            "was not written. Your changes are still here.",
      };
   }

   return { ok: true, source: spliced };
}

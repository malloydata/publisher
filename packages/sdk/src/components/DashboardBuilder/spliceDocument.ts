// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   DashboardDocument,
   DashboardDrill,
   DashboardSource,
   DashboardTile,
} from "./document";
import type { LocalGiven } from "./document";
import { artifactLine } from "./malloyText";
import {
   parseMalloy,
   parseRefused,
   type ParsedMalloy,
   type Span,
   type TreeGiven,
   type TreeStage,
   type TreeView,
} from "./malloyTree";
import { blockAbove, readDashboardDocument, readFailed } from "./readDocument";

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
   parsed: ParsedMalloy,
   sourceName: string,
   viewName: string,
): number {
   const view = parsed.sources
      .find((source) => source.name === sourceName)
      ?.views.find((v) => v.name === viewName);
   return view ? view.line : -1;
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
   /** Where everything is, from Malloy's own parser. */
   parsed: ParsedMalloy;
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
   const { lines, wholeLine, indentOf, current, next, edits, parsed } = ctx;
   // THE DASHBOARD'S OWN GIVENS. Added, removed, or retagged — by name, since
   // a given's name is its identity in every `where:` that reads it.
   const givensBefore = new Map(
      (current.localGivens ?? []).map((g) => [g.name, g]),
   );
   const givensAfter = new Map(
      (next.localGivens ?? []).map((g) => [g.name, g]),
   );
   const declared = new Map<string, TreeGiven>(
      parsed.givens.map((g) => [g.name, g]),
   );
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
   const { lines, wholeLine, indentOf, current, next, edits, parsed } = ctx;
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
      const at = parsed.sources
         .find((source) => source.name === drill.source)
         ?.dimensions.find((d) => d.name === drill.name);
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
   const { lines, wholeLine, removedTiles, edits, parsed } = ctx;
   // REMOVED TILES: the declaration and its `#` tags. An inline view's body
   // runs to its closing brace; a reference is one line. An inherited tile has
   // nothing here to remove — its entry left the artifact list above.
   for (const tile of removedTiles) {
      if (tile.declaration.kind === "inherited") continue;
      const view = viewOf(ctx, tile);
      if (!view) {
         return {
            ok: false,
            reason: `Could not find where \`${tile.name}\` is declared.`,
         };
      }
      if (view.siblings > 1) {
         return {
            ok: false,
            reason:
               `\`${tile.name}\` is one of several views declared by a single ` +
               `\`view:\` statement, which the builder does not restructure.`,
         };
      }
      // The declaration and its `#` tags, which is what the statement's own
      // span covers. A `//` comment above is NOT taken: it is left where the
      // author put it rather than assumed to belong to the tile.
      const first = lineOf(parsed, view.statement.start);
      const endLine = lineOf(parsed, view.statement.end - 1);
      edits.push({
         start: parsed.lineStarts[first],
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
      addedTiles,
      newSources,
      currentSources,
      edits,
      parsed,
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
   // An anchor for a NEW extension: the last line any source's declaration
   // reaches, so one added after it cannot land inside another.
   let lastExtensionEnd = -1;
   for (const source of parsed.sources)
      lastExtensionEnd = Math.max(
         lastExtensionEnd,
         lineOf(parsed, source.span.end - 1),
      );
   for (const [sourceName, tiles] of byExtension) {
      if (currentSources.has(sourceName)) {
         const source = parsed.sources.find((s) => s.name === sourceName);
         if (!source) {
            return {
               ok: false,
               reason: `Could not find where the source \`${sourceName}\` is declared.`,
            };
         }
         if (!source.properties) {
            return {
               ok: false,
               reason:
                  `\`${sourceName}\` never opens an \`extend { … }\` block, so a ` +
                  `new tile cannot be added inside it.`,
            };
         }
         const open = source.line;
         const close = lineOf(parsed, source.properties.closeStart);
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
   const { lines, starts, wholeLine, indentOf, current, next, edits, parsed } =
      ctx;
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

      // Declared in the model, not here, so there is nothing in this file to
      // patch -- saying so beats writing a tag that would land on the wrong
      // object.
      if (tile.declaration.kind === "inherited") {
         return {
            ok: false,
            reason:
               `\`${tile.source} -> ${tile.name}\` is declared on its source, ` +
               `not in this dashboard, so its presentation cannot be changed here.`,
         };
      }

      // An `opaque` tile falls through on purpose: its TAGS are ordinary `#`
      // lines in this file, so a label or colspan change is safe. Only a filter
      // has nowhere to go, and the filter pass refuses that on its own.

      const declLine = viewDeclarationLine(parsed, tile.source, tile.name);
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

      // The filter bindings live in the declaration itself either way, and
      // only the BINDING clauses are ours: a `limit:`, an `order_by:` or a
      // `where:` on a literal someone wrote is unmodeled Malloy and stays
      // exactly as written, in both shapes.
      if (canonical(was.filters) === canonical(tile.filters)) continue;
      const view = viewOf(ctx, tile);
      if (!view) {
         return {
            ok: false,
            reason: `Could not find \`${tile.name}\`'s declaration to filter it.`,
         };
      }
      if (view.body.kind === "reference") {
         const failure = planReferenceFilters(ctx, tile, view);
         if (failure) return failure;
         continue;
      }
      if (view.body.kind !== "inline") {
         return {
            ok: false,
            reason:
               `\`${tile.name}\`'s body is ${view.body.why}, so there is no ` +
               `single first stage to write its filter into.`,
         };
      }
      const clash = givenCollision(tile.filters, view.body.stage);
      if (clash) return collisionRefusal(tile.name, clash.given);
      const failure = planStageFilters(ctx, tile, view.body.stage);
      if (failure) return failure;
   }
   return undefined;
}

/** The 0-based line an offset falls on. */
const lineOf = (parsed: ParsedMalloy, offset: number): number =>
   parsed.lineStarts.findLastIndex((at) => at <= offset);

/** The parsed declaration a tile names, if this file declares it. */
function viewOf(ctx: SpliceContext, tile: DashboardTile): TreeView | undefined {
   return ctx.parsed.sources
      .find((source) => source.name === tile.source)
      ?.views.find((view) => view.name === tile.name);
}

/**
 * The requested binding whose given is ALREADY filtered on by a clause the
 * builder does not own.
 *
 * Binding it again would filter the tile on one control twice while only one
 * of the two could ever be unbound, so the edit is refused instead. The test
 * is structural: a clause the tree says is not a `field <op> $GIVEN` binding,
 * which nonetheless references that given. The scan this replaces looked for
 * `$NAME` in text and could be disarmed by an apostrophe in a comment.
 */
function givenCollision(
   filters: Array<{ field: string; given: string; op?: string }> | undefined,
   stage: TreeStage | undefined,
): { given: string } | undefined {
   if (!stage) return undefined;
   const unmanaged = new Set<string>();
   for (const where of stage.wheres)
      for (const clause of where.clauses)
         if (!clause.binding) for (const g of clause.givens) unmanaged.add(g);
   return (filters ?? []).find((f) => unmanaged.has(f.given));
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

const bindingText = (f: { field: string; given: string; op?: string }) =>
   `where: ${f.field} ${f.op ?? "~"} $${f.given}`;

/**
 * The span to delete to remove ONE clause of a `where:` list, separator and
 * all: up to the next clause when there is one, back to the previous when it
 * is the last. Taking only the clause's own span would leave the comma beside
 * it, which is a syntax error.
 */
function clauseCut(clauses: Array<{ span: Span }>, index: number): Span {
   const self = clauses[index].span;
   if (index + 1 < clauses.length)
      return { start: self.start, end: clauses[index + 1].span.start };
   if (index > 0) return { start: clauses[index - 1].span.end, end: self.end };
   return self;
}

/**
 * The span to delete to remove a whole statement. A statement alone on its
 * line takes the line with it, so no blank one is left behind; one sharing a
 * line with a brace or another statement gives up only its own span and the
 * space before it, so the brace survives.
 */
function statementCut(parsed: ParsedMalloy, span: Span): Span {
   const line =
      parsed.lineStarts[
         parsed.lineStarts.findLastIndex((at) => at <= span.start)
      ];
   const before = parsed.text.slice(line, span.start);
   const lineEnd = parsed.text.indexOf("\n", span.end);
   const after = parsed.text.slice(span.end, lineEnd < 0 ? undefined : lineEnd);
   if (before.trim() === "" && after.trim() === "")
      return {
         start: line,
         end: lineEnd < 0 ? parsed.text.length : lineEnd + 1,
      };
   if (before.trim() === "") {
      // Only indentation before it, and something after -- a closing brace,
      // say. The indent stays and belongs to whatever follows, so the space
      // between goes instead.
      const gap = /^\s*/.exec(after)?.[0].length ?? 0;
      return { start: span.start, end: span.end + gap };
   }
   // A separator belonging to the statement BEFORE this one goes with it:
   // leaving the comma behind turns the previous statement into a list whose
   // last entry has just been deleted.
   const trimmed = before.replace(/[\s,]*$/, "");
   return { start: line + trimmed.length, end: span.end };
}

/**
 * Rewrite a tile's builder-managed `where:` bindings inside `stage`.
 *
 * One implementation for both tile shapes, because a reference tile's
 * `+ { … }` refinement and an inline tile's own first stage are the same
 * thing to the parser. Every position comes from the tree, so a clause is
 * replaced, dropped or appended by SPAN: a `limit:`, a compound predicate, a
 * brace sharing the line or a trailing comment all sit outside those spans
 * and are carried over untouched.
 */
function planStageFilters(
   ctx: SpliceContext,
   tile: DashboardTile,
   stage: TreeStage,
): SpliceFailure | undefined {
   const { parsed, edits } = ctx;
   const wanted = new Map((tile.filters ?? []).map((f) => [f.given, f]));
   const seen = new Set<string>();
   let lastManaged: Span | undefined;

   for (const where of stage.wheres) {
      const managed = where.clauses
         .map((clause, index) => ({ clause, index }))
         .filter(({ clause }) => clause.binding !== undefined);
      if (managed.length === 0) continue;
      const surviving = managed.filter(({ clause }) =>
         wanted.has(clause.binding!.given),
      );
      for (const { clause } of managed) seen.add(clause.binding!.given);

      // Every clause of this `where:` was ours and none survives: the
      // statement itself goes, rather than being left as a bare `where:`.
      if (surviving.length === 0 && managed.length === where.clauses.length) {
         edits.push({ ...statementCut(parsed, where.span), text: "" });
         continue;
      }
      if (surviving.length > 0) lastManaged = where.span;
      // Highest index first, so an earlier cut cannot move a later span.
      for (let i = managed.length - 1; i >= 0; i--) {
         const { clause, index } = managed[i];
         const want = wanted.get(clause.binding!.given);
         if (!want) {
            edits.push({ ...clauseCut(where.clauses, index), text: "" });
            continue;
         }
         // The clause text without its `where:` keyword, which the statement
         // already carries.
         const text = bindingText(want).replace(/^where:\s*/, "");
         if (text !== parsed.text.slice(clause.span.start, clause.span.end))
            edits.push({ ...clause.span, text });
      }
   }

   const added = (tile.filters ?? []).filter((f) => !seen.has(f.given));
   if (added.length === 0) return undefined;

   // After the last statement already inside the block, so a new binding
   // follows what was there; inside the opening brace when there is none.
   const anchor =
      lastManaged?.end ??
      (stage.statements.length > 0
         ? stage.statements[stage.statements.length - 1].span.end
         : stage.openEnd);
   // A comma only ever follows another `where:`. Malloy rejects one after a
   // `limit:` -- the writer used to emit `{ limit: 5, where: … }` and that is
   // a parse error -- while a space parses after every statement form.
   const lead = lastManaged !== undefined ? ", " : " ";
   const text = stage.oneLine
      ? `${lead}${added.map(bindingText).join(", ")}`
      : added.map((f) => `\n${stage.indent}${bindingText(f)}`).join("");
   edits.push({ start: anchor, end: anchor, text });
   return undefined;
}

/**
 * A reference tile's filters, which live in a `+ { … }` refinement that may
 * have to be created or removed outright.
 */
function planReferenceFilters(
   ctx: SpliceContext,
   tile: DashboardTile,
   view: TreeView,
): SpliceFailure | undefined {
   if (view.body.kind !== "reference") return undefined;
   const { edits } = ctx;
   const { fromSpan, refinement } = view.body;

   const clash = givenCollision(tile.filters, refinement);
   if (clash) return collisionRefusal(tile.name, clash.given);

   const filters = tile.filters ?? [];
   if (!refinement) {
      if (filters.length === 0) return undefined;
      // Appended at the END of the base expression, which is before any
      // trailing `//` comment because the comment is not part of it.
      const text = ` + { ${filters.map(bindingText).join(", ")} }`;
      edits.push({ start: fromSpan.end, end: fromSpan.end, text });
      return undefined;
   }

   // Everything in the refinement that is not a managed binding. If nothing
   // is left and nothing is wanted, the whole `+ { … }` goes with it.
   const unmanaged = refinement.wheres.some((w) =>
      w.clauses.some((c) => !c.binding),
   );
   const otherStatements = refinement.statements.some(
      (st) => !refinement.wheres.some((w) => w.span.start === st.span.start),
   );
   if (filters.length === 0 && !unmanaged && !otherStatements) {
      edits.push({ start: fromSpan.end, end: refinement.span.end, text: "" });
      return undefined;
   }
   return planStageFilters(ctx, tile, refinement);
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

   const parse = await parseMalloy(sourceText);
   if (parseRefused(parse))
      return {
         ok: false,
         reason: `Cannot edit a file that will not open: ${parse.reason}`,
      };

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
      parsed: parse.parsed,
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

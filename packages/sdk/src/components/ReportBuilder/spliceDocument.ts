// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { annotationTextProblem } from "./annotationText";
import { MODELLED_TAG_KEYS, tagKey } from "./tileTags";
import {
   chartLineText,
   isChartPick,
   mentionsChartTag,
   parseChartLine,
   seriesColorLine,
   type ChartPick,
} from "./chartLine";
import type {
   DashboardDocument,
   DashboardDrill,
   DashboardTile,
   QueryTile,
   TextTile,
} from "./document";
import {
   isInlineStage,
   isQueryTile,
   isReadOnlyTile,
   isTextTile,
   namesQuery,
   tileExpression,
} from "./document";
import type { LocalGiven } from "./document";
import type { EntryDrill } from "../drill/resolveDrill";
import {
   artifactLine,
   artifactTag,
   type ArtifactTag,
   descriptionNotes,
   isIdentifier,
   isBareName,
   isStrictName,
   malloyPath,
} from "../../utils/malloyText";
import {
   parseMalloy,
   parseRefused,
   readTileList,
   translate,
   type ParsedMalloy,
   type Span,
   type TreeGiven,
   type TreeQuery,
   type TreeStage,
   type TreeView,
} from "./malloyTree";
import { blockAbove, readDashboardDocument, readFailed } from "./readDocument";
import {
   spliceFailed,
   type SpliceFailure,
   type SpliceResult,
} from "./spliceResult";

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
   const { problems } = await translate(text, "file://splice-check.malloy");
   return problems
      .filter((p) => p.code === "syntax-error")
      .map((p) => p.message ?? "")
      .sort();
}

/**
 * Write a change back into a report file by PATCHING it.
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
 * ADDED or REMOVED: a new tile is an entry naming a served view, or a
 * `query:` of the report's own over one when it has settings of its own; a
 * removed one goes with its `query:` or `view:` and their `#` tags. The writer
 * never declares, changes or removes a source. Those moves are the one place the file cannot
 * say who owns the comment beside a declaration, so the `//` comments are left
 * where they were and the author can read the change back with View change.
 *
 * A given is different from a tile in exactly the way that matters there: the
 * `#` tags above its declaration are its control contract and have no other
 * owner, so removing the declaration can take them with it, and a `//` comment
 * in the same block is left where it is.
 */

export { spliceFailed, type SpliceFailure, type SpliceResult };

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

/** A description without blank lines at either end, each of which would write a bare `##"`. */
function trimDescription(text: string): string {
   return text.replace(/^(?:[ \t]*\r?\n)+/, "").trimEnd();
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
   //
   // Two insertions at one point keep the order they were planned in: the
   // later one is applied first, so the earlier lands in front of it, and an
   // import added at a file's end stays ABOVE a source extension written at
   // the same point.
   const planned = edits.map((edit, index) => ({ edit, index }));
   for (const { edit } of planned.sort(
      (a, b) =>
         b.edit.start - a.edit.start ||
         b.edit.end - a.edit.end ||
         b.index - a.index,
   ))
      out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
   return out;
}

/** The chart a tile's `chart` asks the writer to put on its wrapper; `default` and `custom` ask for no line. */
const writableChart = (
   chart: QueryTile["chart"],
): ChartPick | "none" | undefined =>
   chart === "none" || isChartPick(chart) ? chart : undefined;

/**
 * The `#` tags a tile's presentation implies, in the order they are written.
 * Never its layout: that is written on its entry in `tiles=[…]`.
 */
function tagsFor(tile: QueryTile, withChart = true): string[] {
   const tags: string[] = [];
   if (tile.borderless) tags.push("# borderless");
   if (tile.label !== undefined) tags.push(`# label=${quoted(tile.label)}`);
   if (tile.subtitle !== undefined)
      tags.push(`# subtitle=${quoted(tile.subtitle)}`);
   const chart = withChart ? writableChart(tile.chart) : undefined;
   if (chart !== undefined) tags.push(chartLineText(chart, tile.chartReset));
   if (tile.color !== undefined) tags.push(seriesColorLine(tile.color));
   return tags;
}

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
              ? `query=${quoted(given.suggest.query)}`
              : undefined;
      parts.push(
         `suggest { ${by ? `${by} ` : ""}dimension=${quoted(given.suggest.dimension)} }`,
      );
   }
   if (given.rangeMin !== undefined) parts.push(`range_min=${given.rangeMin}`);
   if (given.rangeMax !== undefined) parts.push(`range_max=${given.rangeMax}`);
   return parts.length > 0 ? `# ${parts.join(" ")}` : undefined;
}

/** `given: NAME :: type is default`, the one-line spelling the builder writes. */
const givenDeclaration = (given: LocalGiven) =>
   `given: ${given.name} :: ${given.type} is ${given.default}`;

/** `# drill { to=… given=… }`, one line, as the reader spells it back. */
function drillTagLine(drill: DashboardDrill): string {
   // A path target (`sales/overview`) is not a bare tag value, so it is
   // quoted, with `"` and `\` escaped as the tag parser reads them; a bare
   // name or `self` stays as written.
   const one = (d: string) =>
      /^[A-Za-z_][A-Za-z0-9_]*$/.test(d) ? d : quoted(d);
   const to =
      drill.to.length === 1
         ? one(drill.to[0])
         : `[${drill.to.map(quoted).join(", ")}]`;
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
   isTextTile(t)
      ? canonical(["text", t.name])
      : canonical([t.name, t.source, t.declaration]);
const tileKey = tileFileKey;

/** The tile list as identities, IN ORDER. Differs under a reorder. */
const tileIdentity = (document: DashboardDocument) =>
   canonical(document.tiles.map(tileKey));

interface TileMembership {
   currentKeys: Set<string>;
   nextKeys: Set<string>;
   removedTiles: DashboardTile[];
   addedTiles: DashboardTile[];
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
   visibleSources: readonly string[] | undefined,
): SpliceFailure | TileMembership {
   // Tiles ADDED and REMOVED, by identity. A removed tile's declaration goes,
   // with its `#` tags; a `//` comment above it stays, because the file cannot
   // say whether it belonged to the tile, the row or the page, and a comment
   // left behind is a smaller wrong than one destroyed. An added tile is a `view:` in the extension of
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
   // A tile the builder adds names a served view, or is a query of the
   // report's own over one; it never declares a source to hold it. The view's
   // source has to be one the file can see: imported by name, extended here,
   // or brought in by a whole-file import, or the run model's for a document
   // held as text.
   const visible = new Set([
      ...next.imports.flatMap((i) => (i.kind === "names" ? i.names : [])),
      ...current.sources.map((s) => s.name),
      ...(visibleSources ?? []),
   ]);
   const wholeFile = next.imports.some((i) => i.kind === "all");
   for (const tile of addedTiles) {
      if (!isQueryTile(tile)) continue;
      const kind = tile.declaration.kind;
      if (kind !== "inherited" && kind !== "named") {
         return {
            ok: false,
            reason:
               `A new tile names a view of a source; \`${tile.name}\` is ` +
               `${kind === "inline" ? "an inline query" : `a ${kind} declaration`}, ` +
               `which the builder cannot write.`,
         };
      }
      if (
         tile.source === undefined ||
         (!visible.has(tile.source) && !wholeFile)
      ) {
         return {
            ok: false,
            reason: `\`${tile.source}\` is not imported in this file, so a tile cannot be put on it.`,
         };
      }
   }
   // The builder never adds, renames or removes a source: a report it writes
   // declares none, and one it reads keeps those it has.
   const currentSources = new Map(current.sources.map((s) => [s.name, s]));
   if (
      next.sources.length !== current.sources.length ||
      next.sources.some((s) => currentSources.get(s.name)?.base !== s.base)
   ) {
      return {
         ok: false,
         reason:
            "The builder does not add, change or remove a source; a report's tiles name served views.",
      };
   }
   return {
      currentKeys,
      nextKeys,
      removedTiles,
      addedTiles,
      reordered,
   };
}

/** The tag keys of a tile's layout, which a view declared here carries until the builder moves them to its entry. */
const LAYOUT_TAG_KEYS: ReadonlySet<string> = new Set(["colspan", "break"]);

/** A tile's entry drill, or none: text tiles carry none. */
const drillOf = (tile: DashboardTile) =>
   isTextTile(tile) ? undefined : tile.drill;

/** Whether what a tile's entry says changed: its width, row break, rowspan, row, column or click actions. */
const layoutChanged = (was: DashboardTile, now: DashboardTile) =>
   was.colspan !== now.colspan ||
   Boolean(was.break) !== Boolean(now.break) ||
   was.rowspan !== now.rowspan ||
   was.row !== now.row ||
   was.col !== now.col ||
   drillText(drillOf(was)) !== drillText(drillOf(now));

/** A tile with its layout taken off, for asking whether anything ELSE about it changed. */
const withoutLayout = <T extends DashboardTile>(tile: T): T => {
   const {
      colspan: _c,
      break: _b,
      rowspan: _r,
      row: _row,
      col: _col,
      ...rest
   } = tile;
   // The entry's click actions are the entry's too, like its layout.
   delete (rest as { drill?: unknown }).drill;
   return rest as T;
};

/** A drill destination as the entry writes it: `self` bare, a report path quoted. */
const destinationText = (to: string) =>
   to === "self" ? "self" : JSON.stringify(to);

/**
 * A tile's click actions as its entry writes them, `drill { cat { to=self
 * given=CATEGORY } }`, or "" when it has none. Dimensions in name order, so
 * the same settings always write the same text.
 */
function drillText(drill: Record<string, EntryDrill> | undefined): string {
   const dimensions = Object.keys(drill ?? {}).sort();
   if (!drill || dimensions.length === 0) return "";
   const specs = dimensions.map((dimension) => {
      const { to, given } = drill[dimension];
      const target =
         to.length === 1
            ? destinationText(to[0])
            : `[${to.map(destinationText).join(", ")}]`;
      return `${dimension} { to=${target}${given ? ` given=${given}` : ""} }`;
   });
   return `drill { ${specs.join(" ")} }`;
}

/** The properties a tile's entry is written with: its layout, then its click actions. */
const layoutProperties = (tile: DashboardTile): string[] => [
   ...(tile.row === undefined ? [] : [`row=${tile.row}`]),
   ...(tile.col === undefined ? [] : [`col=${tile.col}`]),
   ...(tile.colspan === undefined ? [] : [`colspan=${tile.colspan}`]),
   ...(tile.break ? ["break"] : []),
   ...(tile.rowspan === undefined ? [] : [`rowspan=${tile.rowspan}`]),
   ...(drillText(drillOf(tile)) ? [drillText(drillOf(tile))] : []),
];

/** `text` with every quoted string blanked to `x`s of the same length, so a scan for syntax cannot land inside one. */
function maskStrings(text: string): string {
   let out = "";
   for (let i = 0; i < text.length; i++) {
      if (text[i] !== '"') {
         out += text[i];
         continue;
      }
      const end = endOfString(text, i);
      out += "x".repeat(Math.min(end, text.length - 1) - i + 1);
      i = end;
   }
   return out;
}

/**
 * A tile's entry in `tiles=[…]` with its layout set as `tile` has it, and
 * every other property as written: `name { kind=text … }` for prose, and
 * `"source -> view"` for a query, with a `{ … }` only when it has layout.
 */
function tileEntryText(tile: DashboardTile, written?: string): string {
   const layout = layoutProperties(tile);
   if (written === undefined) {
      const head = isTextTile(tile) ? tile.name : `"${tileExpression(tile)}"`;
      const props = [...(isTextTile(tile) ? ["kind=text"] : []), ...layout];
      return props.length > 0 ? `${head} { ${props.join(" ")} }` : head;
   }
   const masked = maskStrings(written);
   const open = masked.indexOf("{");
   if (open === -1)
      return layout.length > 0 ? `${written} { ${layout.join(" ")} }` : written;
   const close = masked.lastIndexOf("}");
   let inner = written.slice(open + 1, close);
   let maskedInner = masked.slice(open + 1, close);
   const drop = (re: RegExp) => {
      const hit = re.exec(maskedInner);
      if (!hit) return;
      inner =
         inner.slice(0, hit.index) + inner.slice(hit.index + hit[0].length);
      maskedInner =
         maskedInner.slice(0, hit.index) +
         maskedInner.slice(hit.index + hit[0].length);
   };
   drop(/\s*\bcolspan\s*=\s*[^\s}]+/);
   drop(/\s*\bbreak\b(?:\s*=\s*[^\s}]+)?/);
   drop(/\s*\browspan\s*=\s*[^\s}]+/);
   drop(/\s*\brow\s*=\s*[^\s}]+/);
   drop(/\s*\bcol\s*=\s*[^\s}]+/);
   // `drill { … }` holds braces of its own: drop it through its matching close.
   const drillAt = /\s*\bdrill\s*\{/.exec(maskedInner);
   if (drillAt) {
      let depth = 0;
      let end = drillAt.index + drillAt[0].length - 1;
      for (; end < maskedInner.length; end++) {
         if (maskedInner[end] === "{") depth++;
         else if (maskedInner[end] === "}" && --depth === 0) break;
      }
      inner = inner.slice(0, drillAt.index) + inner.slice(end + 1);
      maskedInner =
         maskedInner.slice(0, drillAt.index) + maskedInner.slice(end + 1);
   }
   const props = [inner.trim(), ...layout].filter(Boolean);
   // A query entry left with nothing in its braces is its expression alone.
   if (props.length === 0) return written.slice(0, open).trimEnd();
   return `${written.slice(0, open)}{ ${props.join(" ")} }${written.slice(close + 1)}`;
}

function planOrder(ctx: SpliceContext): SpliceFailure | undefined {
   const {
      lines,
      wholeLine,
      current,
      next,
      reordered,
      currentKeys,
      nextKeys,
      edits,
   } = ctx;
   const wasByKey = new Map(current.tiles.map((tile) => [tileKey(tile), tile]));
   const resized = next.tiles.some((tile) => {
      const was = wasByKey.get(tileKey(tile));
      return was !== undefined && layoutChanged(was, tile);
   });
   // A reorder, and any tile's layout, are one rewritten array on the
   // `## artifact` line. Each entry is re-emitted AS IT WAS WRITTEN rather than
   // rebuilt from `source` and `name`, so a file that spells a tile
   // `overview->kpis` keeps its spelling and the diff is the change and nothing else.
   if (!reordered && !resized) return undefined;
   const tag = artifactTag(lines);
   if (!tag) {
      return {
         ok: false,
         reason: "Could not find the `## artifact` tag to reorder.",
      };
   }
   const line = tag.text;
   const list = readTileList(line);
   if (!list) {
      return {
         ok: false,
         reason: "Could not read the `tiles=[…]` list to rewrite it.",
      };
   }
   if (nextKeys.size === 0 && currentKeys.size > 0) {
      return {
         ok: false,
         reason:
            "A grid report with no tiles is not served, so the last tile cannot be removed. Add another tile first.",
      };
   }
   const byKey = new Map<string, string>();
   for (const { text } of list.entries) {
      const quoted = /^"((?:[^"\\]|\\.)+)"\s*(?:\{[\s\S]*\})?$/.exec(text);
      if (quoted) {
         const parts = quoted[1].split("->").map((part) => part.trim());
         if (parts.length === 2) byKey.set(`${parts[0]}->${parts[1]}`, text);
         // A named query: kept as written, never resolved to what it reads.
         else if (parts.length === 1) byKey.set(`query.${parts[0]}`, text);
         continue;
      }
      // A bare name is a named query too (`tiles=[by_carrier]`), kept bare.
      if (isIdentifier(text)) {
         byKey.set(`query.${text}`, text);
         continue;
      }
      // `name { … }`: a text tile, or a bare named query carrying its layout.
      const name = /^([A-Za-z_][A-Za-z0-9_]*)\s*\{/.exec(text)?.[1];
      if (name !== undefined)
         byKey.set(
            /\bkind\s*=\s*text\b/.test(maskStrings(text))
               ? `text.${name}`
               : `query.${name}`,
            text,
         );
   }
   // An existing tile keeps its spelling; a new one is written canonically.
   const nextEntries = next.tiles.map((tile) => {
      const isNew = !currentKeys.has(tileKey(tile));
      const written = byKey.get(
         isTextTile(tile)
            ? `text.${tile.name}`
            : namesQuery(tile)
              ? `query.${tile.name}`
              : `${tile.source}->${tile.name}`,
      );
      if (written === undefined) return isNew ? tileEntryText(tile) : undefined;
      const was = wasByKey.get(tileKey(tile));
      return was !== undefined && !layoutChanged(was, tile)
         ? written
         : tileEntryText(tile, written);
   });
   if (nextEntries.some((entry) => entry === undefined)) {
      return {
         ok: false,
         reason:
            "A tile in the new order is not one the `## artifact` tag names.",
      };
   }
   // A `## artifact` line must stay on ONE line or the package fails to
   // compile, so the array is replaced in place rather than reformatted; a
   // `##|` block keeps whatever line layout its array was written in.
   const keyAt = line.slice(0, list.open).search(/tiles\s*=\s*$/);
   const rewritten = `${line.slice(0, keyAt)}tiles=[${arrayBody(line, list, nextEntries)}]${line.slice(list.close + 1)}`;
   if (rewritten !== line)
      edits.push({ ...tagExtent(wholeLine, tag), text: `${rewritten}\n` });
   return undefined;
}

/** The whole lines a tag's text occupies; a block's closer is not part of its text. */
function tagExtent(
   wholeLine: (line: number) => { start: number; end: number },
   tag: ArtifactTag,
) {
   return {
      start: wholeLine(tag.from).start,
      end: wholeLine(tag.block ? tag.to - 1 : tag.from).end,
   };
}

/** The text between a list's brackets for `entries`, spaced as the list was: a multi-line list keeps its lead, separator and tail. */
function arrayBody(
   line: string,
   list: NonNullable<ReturnType<typeof readTileList>>,
   entries: string[],
): string {
   const first = list.entries[0];
   const last = list.entries[list.entries.length - 1];
   if (!first || !/\n/.test(line.slice(list.open, list.close)))
      return entries.join(", ");
   const lead = line.slice(list.open + 1, first.span.start);
   const tail = line.slice(last.span.end, list.close);
   const indent = lead.slice(lead.lastIndexOf("\n"));
   const separator =
      list.entries.length > 1
         ? line.slice(first.span.end, list.entries[1].span.start)
         : `,${indent.startsWith("\n") ? indent : " "}`;
   return `${lead}${entries.join(separator)}${tail}`;
}

/** `line` without its `dashboard { … }` block: outside the artifact braces and never inside a string. */
function removeDashboardBlock(line: string): string {
   let depth = 0;
   for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') i = endOfString(line, i);
      else if (c === "{" || c === "[") depth++;
      else if (c === "}" || c === "]") depth--;
      else if (
         depth === 0 &&
         line.startsWith("dashboard", i) &&
         !/[A-Za-z0-9_]/.test(line[i - 1] ?? " ")
      ) {
         const open = /^dashboard\s*\{/.exec(line.slice(i));
         if (!open) continue;
         let inner = 1;
         let j = i + open[0].length;
         for (; j < line.length && inner > 0; j++) {
            if (line[j] === '"') j = endOfString(line, j);
            else if (line[j] === "{") inner++;
            else if (line[j] === "}") inner--;
         }
         const from = i - (/\s*$/.exec(line.slice(0, i))?.[0].length ?? 0);
         return line.slice(0, from) + line.slice(j);
      }
   }
   return line;
}

/** The index of the quote closing the string that opens at `open`. */
function endOfString(line: string, open: number): number {
   for (let i = open + 1; i < line.length; i++) {
      if (line[i] === "\\") i++;
      else if (line[i] === '"') return i;
   }
   return line.length;
}

function planSettings(ctx: SpliceContext): SpliceFailure | undefined {
   const { lines, wholeLine, current, next, edits } = ctx;
   // THE PAGE'S OWN SETTINGS. Title, autorun and starting values are
   // properties on the `## artifact { … }` tag (one line, or a `##|` block);
   // the grid width is the `dashboard { columns=N }` beside it; the description is the run of `##"`
   // lines above. Each is patched in place, so a one-line tag stays one line
   // — the package fails to compile otherwise — and a property
   // the file spells its own way keeps that spelling when it did not change.
   if (
      current.layout !== next.layout ||
      current.title !== next.title ||
      current.autorun !== next.autorun ||
      current.columns !== next.columns ||
      canonical(current.startingGivens) !== canonical(next.startingGivens)
   ) {
      const tag = artifactTag(lines);
      if (!tag) {
         return {
            ok: false,
            reason:
               "Could not find the `## artifact` tag to change the page's settings.",
         };
      }
      const extent = tagExtent(wholeLine, tag);
      // Whatever the reorder wrote to this tag is the text to patch further.
      const already = edits.find(
         (edit) => edit.start === extent.start && edit.end === extent.end,
      );
      let line = already ? already.text.replace(/\n$/, "") : tag.text;
      // A layout switch rewrites `layout=` and nothing else.
      const writeLayout = current.layout !== next.layout;
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
      // A block puts each property on a line of its own, as its array is.
      const lineBreak = tag.block
         ? `\n${/\n([ \t]+)\S/.exec(inner)?.[1] ?? "  "}`
         : " ";
      const append = (property: string) =>
         tag.block
            ? `${inner.trimEnd()}${lineBreak}${property}\n`
            : `${inner.trimEnd()} ${property} `;
      const setProperty = (key: string, value: string | undefined) => {
         const re = new RegExp(
            `${tag.block ? "[ \\t]*" : "\\s*"}\\b${key}=(?:"(?:[^"\\\\]|\\\\.)*"|[^\\s}]+)`,
         );
         if (value === undefined) {
            const own = new RegExp(`\n${re.source}[ \\t]*(?=\n)`);
            inner = inner.replace(tag.block && own.test(inner) ? own : re, "");
         } else if (re.test(inner))
            inner = inner.replace(
               re,
               (hit) =>
                  `${tag.block ? hit.slice(0, hit.length - hit.trimStart().length) : " "}${key}=${value}`,
            );
         else inner = append(`${key}=${value}`);
      };
      if (writeLayout) setProperty("layout", next.layout);
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
            inner = append(
               `givens { ${entries.map(([k, v]) => `${k}=${quoted(v)}`).join(" ")} }`,
            );
      }
      if (tag.block) {
         line = `${line.slice(0, braceOpen + 1)}${inner}${line.slice(braceClose)}`;
      } else {
         inner = inner.replace(/\s{2,}/g, " ");
         line = `${line.slice(0, braceOpen + 1)}${inner.startsWith(" ") ? inner : ` ${inner}`}${inner.endsWith(" ") ? "" : " "}${line.slice(braceClose)}`;
      }
      if (current.columns !== next.columns) {
         line = removeDashboardBlock(line);
         if (next.columns !== undefined)
            line = `${line.trimEnd()} dashboard { columns=${next.columns} }`;
      }
      if (already) already.text = `${line}\n`;
      else edits.push({ ...extent, text: `${line}\n` });
   }
   if (current.description !== next.description) {
      // Written above the tag, where the server reads it. A description read
      // from below (the legacy spot) moves there, and the lines it came from go.
      const { read, blankAbove, below, belowBlock, inBlock } =
         descriptionNotes(lines);
      if (inBlock || (next.description === undefined && belowBlock)) {
         return {
            ok: false,
            reason:
               "The description is written in a `##|\"` block, which the builder cannot edit in place. Change it in the file's text.",
         };
      }
      const text = trimDescription(next.description ?? "")
         .split("\n")
         .map((para) => (para.trim() === "" ? '##"' : `##" ${para.trim()}`))
         .join("\n");
      const above = read.length > 0 && read[0] < artifactLine(lines);
      // Clearing it must not let the server fall back to notes left below the tag.
      const clearing = next.description === undefined;
      const removed = above
         ? [...read.slice(1), ...(clearing ? below : [])]
         : [...read, ...blankAbove];
      for (const at of removed) edits.push({ ...wholeLine(at), text: "" });
      if (above) {
         edits.push({
            ...wholeLine(read[0]),
            text: next.description === undefined ? "" : `${text}\n`,
         });
      } else if (next.description !== undefined) {
         const at = wholeLine(artifactLine(lines)).start;
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
      const { tags, prose } = blockAbove(parsed, lines, at.line);
      const owned =
         want === undefined
            ? tags
            : tags.filter((tag) => {
                 const key = tagKey(tag.text);
                 return key !== undefined && MODELLED_GIVEN_TAG_KEYS.has(key);
              });
      // Prose leaves with a removed declaration for the same reason.
      const ownedLines = [
         ...owned.map((tag) => tag.line),
         ...(want === undefined ? prose : []),
      ];
      for (const line of ownedLines) {
         edits.push({ ...wholeLine(line), text: "" });
         removedLines.add(line);
      }
      // A declaration can run past its first line -- `NAME :: string is` with
      // its default below it is ordinary Malloy -- so the lines to take come
      // from the span, not from the line the name sits on. Taking only the
      // first left the continuation behind as a statement of its own.
      const lastLine = lineOf(parsed, at.span.end - 1);
      if (want === undefined) {
         edits.push({
            start: wholeLine(at.line).start,
            end: wholeLine(lastLine).end,
            text: "",
         });
         for (let l = at.line; l <= lastLine; l++) removedLines.add(l);
         // A declaration set off by blank lines takes one of them with it, or
         // the two separators meet and the file gains an empty line per edit.
         const first = Math.min(at.line, ...ownedLines);
         const above = at.blockHeader ?? first;
         const belowIsBlank = (lines[lastLine + 1] ?? "x").trim() === "";
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
            !removedLines.has(lastLine + 1)
         ) {
            edits.push({ ...wholeLine(lastLine + 1), text: "" });
            removedLines.add(lastLine + 1);
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
         : lines
              .slice(at.line, lastLine + 1)
              .join("\n")
              .trim();
      edits.push({
         start: wholeLine(at.line).start,
         end: wholeLine(lastLine).end,
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

/** `import { a, b } from "../model.malloy"`, the one-line spelling the builder writes. */
const importStatement = (names: string[], from: string) =>
   `import { ${names.join(", ")} } from ${quoted(from)}`;

/**
 * IMPORTS. Only a `{ names }` import is the builder's to edit: a name is added
 * to the statement for its file (or a new statement follows the last import),
 * a name taken off rewrites it, and the last name takes the statement. A bare
 * `import "…"` brings in a whole file's declarations and is never touched, and
 * a name a source or tile still reads cannot go.
 */
function planImports(ctx: SpliceContext): SpliceFailure | undefined {
   const { parsed, current, next, edits, wholeLine } = ctx;
   const bare = (document: DashboardDocument) =>
      canonical(
         document.imports
            .flatMap((i) => (i.kind === "all" ? [i.from] : []))
            .sort(),
      );
   if (bare(current) !== bare(next)) {
      return {
         ok: false,
         reason: "A whole-file `import` cannot be changed here.",
      };
   }
   if (canonical(current.imports) === canonical(next.imports)) return undefined;

   const wanted = next.imports.flatMap((i) =>
      i.kind === "names" ? [{ ...i, claimed: false }] : [],
   );
   const inUse = new Set([
      ...next.sources.map((s) => s.base),
      ...next.tiles.filter(isQueryTile).map((t) => t.source),
   ]);
   const written = parsed.imports.map((statement, index) => ({
      statement,
      entry: current.imports[index],
   }));
   for (const { statement, entry } of written) {
      if (!entry || entry.kind !== "names") continue;
      const keep = wanted.find((w) => !w.claimed && w.from === entry.from);
      if (keep) keep.claimed = true;
      const names = keep?.names ?? [];
      const dropped = entry.names.filter((n) => !names.includes(n));
      const stuck = dropped.find((n) => inUse.has(n));
      if (stuck !== undefined) {
         return {
            ok: false,
            reason: `\`${stuck}\` is still read by a tile, so its import cannot be removed.`,
         };
      }
      if (canonical(names) === canonical(entry.names)) continue;
      const cut =
         names.length === 0
            ? statementCut(parsed, statement.statement)
            : statement.statement;
      const lost = parsed.commentIn(cut);
      if (lost) {
         return {
            ok: false,
            reason:
               `Changing this import would also remove the comment inside it ` +
               `(\`${parsed.text.slice(lost.start, lost.end).trim()}\`), so ` +
               `nothing was written. Delete it in the file first.`,
         };
      }
      edits.push({
         ...cut,
         text: names.length === 0 ? "" : importStatement(names, entry.from),
      });
   }

   const added = wanted.filter((w) => !w.claimed);
   if (added.length > 0) {
      const last = parsed.imports.at(-1);
      if (!last) {
         return {
            ok: false,
            reason:
               "This file has no `import` to put a source beside. Import its first source in the file.",
         };
      }
      const at = wholeLine(lineOf(parsed, last.statement.end - 1)).end;
      edits.push({
         start: at,
         end: at,
         text: added
            .map((w) => `${importStatement(w.names, w.from)}\n`)
            .join(""),
      });
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
      const { tags } = blockAbove(parsed, lines, at.line);
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
   // REMOVED TILES: the declaration and its `#` tags — a view in an
   // extension, or the report's own `query:`. An inline view's body runs to
   // its closing brace; a reference is one line. A tile that names a served
   // view has nothing here to remove — its entry left the artifact list above.
   // A `query:` goes with its tile unless something else in the file still
   // names it (another tile, another query built on it): then it stays.
   const stillNamed = (name: string, own: Span) => {
      const word = new RegExp(`(^|[^A-Za-z0-9_])${name}([^A-Za-z0-9_]|$)`);
      return parsed.text.split("\n").some((line, at) => {
         const start = parsed.lineStarts[at];
         const end = start + line.length;
         // Its own statement, and the report's `##` tag (whose list the edit
         // has already taken it out of), do not count.
         if (start < own.end && end > own.start) return false;
         if (line.trimStart().startsWith("##")) return false;
         return word.test(line.replace(/\/\/.*$/, ""));
      });
   };
   for (const tile of removedTiles) {
      if (isTextTile(tile) || tile.declaration.kind === "inherited") continue;
      const readOnlyQuery =
         isReadOnlyTile(tile) && tile.declaration.kind === "query";
      if (isReadOnlyTile(tile) && !readOnlyQuery) continue;
      const view = readOnlyQuery
         ? parsed.queries.find((query) => query.name === tile.name)
         : declarationOf(ctx, tile);
      if (readOnlyQuery && !view) continue;
      if (
         view &&
         (readOnlyQuery || tile.declaration.kind === "named") &&
         (stillNamed(tile.name, view.statement) ||
            ctx.next.tiles.some(
               (t) => isQueryTile(t) && namesQuery(t) && t.name === tile.name,
            ))
      )
         continue;
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
      // Taking whole lines is right only when this statement is the only thing
      // on them. Malloy lets a second declaration share a line, and one that is
      // not a tile of this dashboard is invisible to the readback gate -- so
      // widening to the line would delete it and report success.
      const trailing = parsed.trailingComment(endLine);
      const tail =
         trailing && trailing.start >= view.statement.end
            ? trailing.end
            : view.statement.end;
      const sharesLine =
         parsed.text
            .slice(parsed.lineStarts[first], view.statement.start)
            .trim() !== "" ||
         parsed.text.slice(tail, wholeLine(endLine).end).trim() !== "";
      if (sharesLine) {
         edits.push({ ...statementCut(parsed, view.statement), text: "" });
         continue;
      }
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
      // A query of the report's own sits at the top level, set off by the
      // blank line the writer put above it, and that line goes with it. Taking
      // the one ABOVE means two removed in a row never claim the same line.
      if (
         (tile.declaration.kind === "named" || readOnlyQuery) &&
         first > 0 &&
         before === ""
      ) {
         edits.push({ ...wholeLine(first - 1), text: "" });
         continue;
      }
      if (
         (lines[endLine + 1] ?? "x").trim() === "" &&
         (first === 0 || before === "" || before.endsWith("{"))
      )
         edits.push({ ...wholeLine(endLine + 1), text: "" });
   }
   return undefined;
}

/**
 * `query: name is source -> view [+ { where: … }]`, with the tile's tags above it.
 *
 * The filters are a refinement of the NAMED view, which Malloy applies to the
 * view's first stage, so a field the view does not output, a joined field, or
 * one a second stage drops still filters (measured, with exact rows, in
 * reportForms.compile.spec.ts). `source extend { where: … } -> view` gives the
 * same rows, with the source's `#(access_filter)` applied the same way
 * (report_tile_access.spec.ts in the server), but one form is enough to write
 * and this one keeps the tile on the named view. The failure
 * malloydata/publisher#723 reported for a trailing refinement of an inline
 * multi-stage query does not reproduce on the Malloy this package pins
 * (reportForms.compile.spec.ts), and the builder writes only named views. A
 * query an author filtered on its source is read, and its filters edited where
 * they are ({@link planNamedFilters}).
 */
function queryStatement(tile: QueryTile & { declaration: { from: string } }) {
   const bindings = (tile.filters ?? []).map(bindingText).join(", ");
   return [
      ...tagsFor(tile),
      `query: ${tile.name} is ${tile.source} -> ${tile.declaration.from}${
         bindings ? ` + { ${bindings} }` : ""
      }`,
   ].join("\n");
}

/**
 * Filters on a query's source, ` extend { where: a ~ $A, b ~ $B }`, or nothing
 * when it has none: one `where:` holding every clause, since an `extend { … }`
 * block takes no comma between statements.
 */
function scopeText(filters: QueryTile["filters"]): string {
   const clauses = (filters ?? []).map((f) =>
      bindingText(f).replace(/^where:\s*/, ""),
   );
   return clauses.length > 0 ? ` extend { where: ${clauses.join(", ")} }` : "";
}

/**
 * A `named` tile's filters. Written as a `+ { where: … }` refinement of its
 * view ({@link queryStatement}). A query whose author filters its source
 * instead, `source extend { where: … } -> view`, keeps them there: the block
 * is rewritten or taken away whole. A block holding anything but bindings is
 * the author's, so a filter edit there is refused rather than risk it; so is a
 * query that binds in both places.
 */
function planNamedFilters(
   ctx: SpliceContext,
   tile: QueryTile,
   written: TreeQuery,
): SpliceFailure | undefined {
   const { parsed, edits } = ctx;
   // `src -> { … }` with no refinement yet: its stage is the base a
   // `+ { where: … }` is appended to, exactly as a view reference's name is.
   const query: TreeQuery =
      written.body.kind === "inline" && !written.body.pipeline
         ? {
              ...written,
              body: {
                 kind: "reference",
                 from: parsed.text.slice(
                    written.body.stage.span.start,
                    written.body.stage.span.end,
                 ),
                 fromSpan: written.body.stage.span,
              },
           }
         : written;
   const refinement =
      query.body.kind === "reference" ? query.body.refinement : undefined;
   const binds = (stage: TreeStage | undefined) =>
      stage?.wheres.some((w) => w.clauses.some((c) => c.binding)) ?? false;
   const block = query.scope?.block;
   if (binds(refinement) && binds(block)) {
      return {
         ok: false,
         reason: `\`${tile.name}\` binds its filters in two places, so they cannot be changed here. Edit them in the file.`,
      };
   }
   if (!binds(block)) return planReferenceFilters(ctx, tile, query);
   if (
      block &&
      (block.statements.some(
         (st) => !block.wheres.some((w) => w.span.start === st.span.start),
      ) ||
         block.wheres.some((w) => w.clauses.some((c) => !c.binding)))
   ) {
      return {
         ok: false,
         reason: `\`${tile.name}\`'s source is extended with more than the builder writes, so its filters cannot be changed here. Edit them in the file.`,
      };
   }
   const start = query.scope?.sourceEnd ?? query.sourceEnd;
   if (start === undefined) {
      return {
         ok: false,
         reason: `Could not find \`${tile.name}\`'s source to filter it.`,
      };
   }
   const cut = { start, end: query.scope?.end ?? start };
   const comment = parsed.commentIn(cut);
   if (comment) return commentRefusal(tile.name, parsed, comment);
   edits.push({ ...cut, text: scopeText(tile.filters) });
   return undefined;
}

function planAddedTiles(ctx: SpliceContext): SpliceFailure | undefined {
   const { sourceText, lines, wholeLine, addedTiles, edits, parsed } = ctx;
   // ADDED TILES. One that names a served view is only its entry in the list,
   // which `planOrder` writes. One with settings of its own is a `query:` of
   // the report's own, written after the file's last query, else after its
   // last source, else after its imports and givens.
   const queries = addedTiles.filter(
      (
         tile,
      ): tile is QueryTile & { declaration: { kind: "named"; from: string } } =>
         isQueryTile(tile) && tile.declaration.kind === "named",
   );
   if (queries.length === 0) return undefined;
   // A `query:` shares the file's top-level namespace with every source,
   // query and given it declares or imports by name.
   const taken = new Set<string>([
      ...parsed.queries.map((q) => q.name),
      ...parsed.sources.map((s) => s.name),
      ...parsed.givens.map((g) => g.name),
      ...parsed.imports.flatMap((i) => i.names ?? []),
   ]);
   for (const tile of queries) {
      const problem = nameProblem(tile.name);
      if (problem) return { ok: false, reason: problem };
      if (taken.has(tile.name)) {
         return {
            ok: false,
            reason: `This report already declares or imports \`${tile.name}\`, so a tile's query cannot take that name.`,
         };
      }
   }
   let anchor = -1;
   for (const declared of [...parsed.queries, ...parsed.sources])
      anchor = Math.max(anchor, lineOf(parsed, declared.statement.end - 1));
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
   // A file that ends without a newline needs one before the first statement.
   const separator =
      at === sourceText.length && !sourceText.endsWith("\n") ? "\n" : "";
   edits.push({
      start: at,
      end: at,
      text:
         separator +
         queries.map((tile) => `\n${queryStatement(tile)}\n`).join(""),
   });
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
      // Written whole above, tags and all; a text tile has no declaration to retag.
      if (was === undefined || isTextTile(tile) || isTextTile(was)) continue;
      // A layout change is written on the entry (`planOrder`); here it only
      // takes the view's own `# colspan` and `# break` off, so the entry is
      // the one place the layout lives.
      const moved = layoutChanged(was, tile);
      const presented =
         canonical(withoutLayout(withoutDefaultChart(was))) !==
         canonical(withoutLayout(withoutDefaultChart(tile)));
      if (!presented && !moved) continue;

      // A served view has nothing in this file to patch: a setting on one
      // makes it a query of the report's own (`normalizeTileForms`), which
      // reaches here as a new tile. Saying so beats writing a tag that would
      // land on the wrong object. Its layout is its entry's, which is this
      // file's.
      if (tile.declaration.kind === "inherited") {
         if (!presented) continue;
         return {
            ok: false,
            reason:
               `\`${tileExpression(tile)}\` names a served view, so a setting ` +
               `on it needs a query of the report's own.`,
         };
      }
      // Its layout is its entry's too, which wins over the query's own tags.
      if (tile.declaration.kind === "query") {
         if (!presented) continue;
         return {
            ok: false,
            reason:
               `\`${tile.name}\` names a query, whose presentation is the ` +
               `query's own, so it cannot be changed here.`,
         };
      }

      // An `opaque` tile falls through on purpose: its TAGS are ordinary `#`
      // lines in this file, so a label or colspan change is safe. Only a filter
      // has nowhere to go, and the filter pass refuses that on its own.

      const declared = declarationOf(ctx, tile);
      if (!declared) {
         return {
            ok: false,
            reason: `Could not find where \`${tile.name}\` is declared.`,
         };
      }
      const declLine = declared.line;

      const { tags } = blockAbove(parsed, lines, declLine);
      const indent = indentOf(declLine);
      const wanted = tagsFor(tile, false);
      const wantedByKey = new Map(
         wanted.map((text) => [tagKey(text) ?? text, text]),
      );
      const seen = new Set<string>();

      // Only a changed chart touches a chart line, so a bare `# bar_chart` survives every other edit.
      const wantChart = writableChart(tile.chart);
      const chartChanged =
         chartOf(was) !== chartOf(tile) ||
         canonical(was.chartReset ?? []) !== canonical(tile.chartReset ?? []);
      if (chartChanged) {
         const blockers = tags.filter((tag) => mentionsChartTag(tag.text));
         const ours = tags.filter((tag) => parseChartLine(tag.text));
         const stuck =
            was.chart === "custom"
               ? tags.filter(
                    (tag) =>
                       parseChartLine(tag.text) || mentionsChartTag(tag.text),
                 )
               : blockers.length > 0
                 ? blockers
                 : ours.length > 1
                   ? ours
                   : [];
         if (stuck.length > 0) {
            return {
               ok: false,
               reason:
                  `\`${tile.source} -> ${tile.name}\` has a chart line the builder does not ` +
                  `model (${stuck.map((tag) => tag.text).join(" and ")}), so its chart ` +
                  `cannot be changed here.`,
            };
         }
      }
      let chartPlaced = false;

      // Existing tag lines are patched or removed IN PLACE, so anything else in
      // the block — a comment explaining the tile — keeps its position.
      for (const tag of tags) {
         if (chartChanged && parseChartLine(tag.text)) {
            if (wantChart === undefined)
               edits.push({ ...wholeLine(tag.line), text: "" });
            else if (tag.text !== chartLineText(wantChart, tile.chartReset))
               edits.push({
                  ...wholeLine(tag.line),
                  text: `${indent}${chartLineText(wantChart, tile.chartReset)}\n`,
               });
            chartPlaced = true;
            continue;
         }
         const key = tagKey(tag.text);
         // Not a property this document models: not ours to touch.
         if (key === undefined || !MODELLED_TAG_KEYS.has(key)) continue;
         // The view's layout stays put until the tile's layout changes, and
         // then it moves to the entry.
         if (LAYOUT_TAG_KEYS.has(key)) {
            if (moved) edits.push({ ...wholeLine(tag.line), text: "" });
            continue;
         }
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
      if (chartChanged && wantChart !== undefined && !chartPlaced)
         added.push(chartLineText(wantChart, tile.chartReset));
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
      const view = declarationOf(ctx, tile);
      if (!view) {
         return {
            ok: false,
            reason: `Could not find \`${tile.name}\`'s declaration to filter it.`,
         };
      }
      if (tile.declaration.kind === "named") {
         const failure = planNamedFilters(ctx, tile, view as TreeQuery);
         if (failure) return failure;
         continue;
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

/** A tile's chart as the writer compares it: no line (absent or `default`) is one state. */
const chartOf = (tile: QueryTile) =>
   tile.chart === "default" ? undefined : tile.chart;

/** The tile as the writer compares it: "default" is no line, and `chartLines` is not part of what is asked for. */
const withoutDefaultChart = (tile: QueryTile): QueryTile => {
   const { chartLines: _lines, ...withReset } = tile;
   const rest =
      withReset.chart === "default" || withReset.chart === undefined
         ? (({ chartReset: _reset, ...bare }) => bare)(withReset)
         : withReset;
   if (rest.chart !== "default") return rest;
   const { chart: _chart, ...bare } = rest;
   return bare;
};

/** A matched tile with no `chart` stated is unchanged, so only an explicit "default" removes a line. */
function keepUnstatedCharts(
   current: DashboardDocument,
   next: DashboardDocument,
): DashboardDocument {
   const was = new Map(current.tiles.map((t) => [tileKey(t), t]));
   return {
      ...next,
      tiles: next.tiles.map((tile) => {
         const before = was.get(tileKey(tile));
         return isQueryTile(tile) &&
            tile.chart === undefined &&
            before &&
            isQueryTile(before) &&
            before.chart !== undefined
            ? {
                 ...tile,
                 chart: before.chart,
                 ...(before.chartReset
                    ? { chartReset: before.chartReset }
                    : {}),
              }
            : tile;
      }),
   };
}

/** Why a text tile's markdown cannot be written into a block, or undefined when it can: the writer's refusals, for an editor to show live. */
export function markdownProblem(markdown: string): string | undefined {
   if (markdown.includes("\r"))
      return "Text uses plain line breaks; a carriage return cannot be written.";
   if (/^\|##/m.test(markdown))
      return "A line starting with `|##` would close the text early. Indent that line or reword it.";
   return undefined;
}

/** Why `tile` cannot be written, or undefined; only what changed from `before` is judged, so an existing odd value does not block unrelated edits. */
function textTileProblem(
   tile: TextTile,
   before: TextTile | undefined,
   seen: Set<string>,
): string | undefined {
   if (seen.has(tile.name))
      return `Two text tiles are named \`${tile.name}\`, so they could not be told apart.`;
   seen.add(tile.name);
   if (before === undefined && !isIdentifier(tile.name))
      return `The text tile name ${JSON.stringify(tile.name)} must be a bare word.`;
   if (before === undefined || tile.markdown !== before.markdown) {
      const problem = markdownProblem(tile.markdown);
      if (problem) return problem;
   }
   if (
      tile.colspan !== undefined &&
      tile.colspan !== before?.colspan &&
      !(Number.isInteger(tile.colspan) && tile.colspan >= 1)
   )
      return "A text tile's width is a whole number of columns, 1 or more.";
   return undefined;
}

/** The block a text tile's prose is written in. */
const textBlockText = (tile: TextTile) =>
   `##|(markdown) ${tile.name}\n${tile.markdown === "" ? "" : `${tile.markdown}\n`}|##\n`;

function planTextBlocks(ctx: SpliceContext): SpliceFailure | undefined {
   const { sourceText, lines, wholeLine, current, next, parsed, edits } = ctx;
   // TEXT TILES: the `##|(markdown) name` block is the tile's body, so adding
   // one writes a block, editing one replaces its block, and removing one takes
   // the block out; the `tiles=[…]` entry was already handled with the order.
   const wasText = new Map(
      current.tiles.filter(isTextTile).map((tile) => [tile.name, tile]),
   );
   const nowText = next.tiles.filter(isTextTile);
   const blockOf = (name: string) =>
      parsed.textBlocks.find((block) => block.name === name);
   const nowNames = new Set(nowText.map((tile) => tile.name));

   for (const name of wasText.keys()) {
      const block = nowNames.has(name) ? undefined : blockOf(name);
      if (!block) continue;
      const last = lineOf(parsed, block.span.end - 1);
      edits.push({ ...block.span, text: "" });
      const after = wholeLine(last + 1);
      const blankBefore =
         block.line === 0 || lines[block.line - 1].trim() === "";
      // The blank line after goes too when one already precedes, or two separators meet.
      if (
         after.end > after.start &&
         lines[last + 1].trim() === "" &&
         blankBefore
      )
         edits.push({ ...after, text: "" });
      // A last block takes the blank line that set it off.
      else if (
         block.line > 0 &&
         blankBefore &&
         sourceText.slice(block.span.end).trim() === ""
      )
         edits.push({ ...wholeLine(block.line - 1), text: "" });
   }

   const fresh: string[] = [];
   for (const tile of nowText) {
      const was = wasText.get(tile.name);
      const block = blockOf(tile.name);
      if (was === undefined) {
         if (block) {
            return {
               ok: false,
               reason: `A block named \`${tile.name}\` is already in this file, so a new text tile cannot take that name.`,
            };
         }
         fresh.push(textBlockText(tile));
      } else if (was.markdown !== tile.markdown) {
         if (block) edits.push({ ...block.span, text: textBlockText(tile) });
         else fresh.push(textBlockText(tile));
      }
   }
   if (fresh.length > 0) {
      // Prose blocks group together ahead of the `<source>_tiles` extension, in tile order.
      const kept = parsed.textBlocks.filter(
         (block) => block.name === undefined || nowNames.has(block.name),
      );
      const lastKept = kept[kept.length - 1];
      const extension = parsed.sources.find((s) => s.name.endsWith("_tiles"));
      if (lastKept) {
         const at = lastKept.span.end;
         const after = sourceText.slice(at);
         edits.push({
            start: at,
            end: at,
            text: `\n${fresh.join("\n")}${after === "" || after.startsWith("\n") ? "" : "\n"}`,
         });
         return undefined;
      }
      if (extension) {
         const at = ctx.starts[parsed.blockStart(extension.line)];
         const blankBefore =
            at === 0 || sourceText.slice(0, at).endsWith("\n\n");
         edits.push({
            start: at,
            end: at,
            text: `${blankBefore ? "" : "\n"}${fresh.join("\n")}\n`,
         });
         return undefined;
      }
      const at = sourceText.length;
      const lead =
         sourceText === "" || sourceText.endsWith("\n\n")
            ? ""
            : sourceText.endsWith("\n")
              ? "\n"
              : "\n\n";
      edits.push({ start: at, end: at, text: lead + fresh.join("\n") });
   }
   return undefined;
}

/** A name the writer emits unquoted, or why it cannot be written. */
const nameProblem = (name: string, bare = isBareName) =>
   bare(name)
      ? undefined
      : `The name ${JSON.stringify(name)} cannot be written as a Malloy name.`;

/** Why `next` holds a chart, name or annotation text the writer must not emit; only what changed is judged, so an existing odd string does not block unrelated edits. */
function unwritable(
   current: DashboardDocument,
   next: DashboardDocument,
): string | undefined {
   const was = new Map(current.tiles.map((t) => [tileKey(t), t]));
   // A linear report is one column, so the builder never SETS a width on one.
   // A width the file already states is kept as written, a layout switch
   // included: the switch rewrites `layout=` and nothing else.
   if (
      next.layout === "linear" &&
      next.columns !== undefined &&
      next.columns !== current.columns
   )
      return "A linear report is one column, so its width is not written.";
   const textNames = new Set<string>();
   for (const tile of next.tiles) {
      const before = was.get(tileKey(tile));
      if (isTextTile(tile)) {
         const problem = textTileProblem(
            tile,
            before && isTextTile(before) ? before : undefined,
            textNames,
         );
         if (problem) return problem;
         continue;
      }
      const prior = before && isQueryTile(before) ? before : undefined;
      if (before && !prior) continue;
      const chart = tile.chart;
      if (
         chart !== undefined &&
         chart !== "default" &&
         chart !== "none" &&
         !isChartPick(chart) &&
         !(chart === "custom" && prior?.chart === "custom")
      )
         return `"${String(chart)}" is not a chart this editor writes.`;
      const filtersBefore = new Set(prior?.filters?.map((f) => f.field));
      for (const f of tile.filters ?? []) {
         if (!filtersBefore.has(f.field) && /[\r\n]/.test(f.field))
            return "A filter field is one line.";
      }
      if (before === undefined) {
         const names = [tile.name, tile.source];
         if (
            (tile.declaration.kind === "reference" ||
               tile.declaration.kind === "named") &&
            // An inline stage is the query's own text, carried as read.
            !isInlineStage(tile.declaration.from)
         )
            names.push(tile.declaration.from);
         for (const name of names) {
            const problem = nameProblem(name);
            if (problem) return problem;
         }
      }
      for (const key of ["label", "subtitle"] as const) {
         const text = tile[key];
         if (text === undefined || text === prior?.[key]) continue;
         const problem = annotationTextProblem(`tile ${key}`, text);
         if (problem) return problem;
      }
   }
   const knownSources = new Set(current.sources.map((s) => s.name));
   for (const source of next.sources) {
      if (knownSources.has(source.name)) continue;
      for (const name of [source.name, source.base]) {
         const problem = nameProblem(name);
         if (problem) return problem;
      }
   }
   if (next.description !== current.description) {
      for (const line of (next.description ?? "").split(/\r?\n/)) {
         const problem = annotationTextProblem("description line", line);
         if (problem) return problem;
      }
   }
   if (next.title !== current.title) {
      const problem = annotationTextProblem("title", next.title);
      if (problem) return problem;
   }
   const givens = new Map((current.localGivens ?? []).map((g) => [g.name, g]));
   for (const given of next.localGivens ?? []) {
      const was = givens.get(given.name);
      if (was === undefined) {
         const problem = nameProblem(given.name, isStrictName);
         if (problem) return problem;
      }
      const source = given.suggest?.source;
      if (source !== undefined && source !== was?.suggest?.source) {
         const problem = nameProblem(source);
         if (problem) return `The suggest source is not a name. ${problem}`;
      }
      for (const key of ["query", "dimension"] as const) {
         const text = given.suggest?.[key];
         if (text === undefined || text === was?.suggest?.[key]) continue;
         const problem = annotationTextProblem(`suggest ${key}`, text);
         if (problem) return problem;
      }
      for (const key of ["label", "description"] as const) {
         const text = given[key];
         if (text === undefined || text === givens.get(given.name)?.[key])
            continue;
         const problem = annotationTextProblem(`given ${key}`, text);
         if (problem) return problem;
      }
   }
   for (const [name, value] of Object.entries(next.startingGivens ?? {})) {
      if (value === current.startingGivens?.[name]) continue;
      const problem = annotationTextProblem(
         `starting value for ${name}`,
         value,
      );
      if (problem) return problem;
   }
   return undefined;
}

/** The 0-based line an offset falls on. */
const lineOf = (parsed: ParsedMalloy, offset: number): number =>
   parsed.lineStarts.findLastIndex((at) => at <= offset);

/**
 * Where this file declares a tile: the report's own `query:` for a `named`
 * tile, else its `view:` inside the extension it names. Scoped to that
 * extension, because a view name is only unique WITHIN a source: two sources
 * may each declare `by_month`, and a file-wide search would retag a tile the
 * author never touched. Undefined when the file does not declare it.
 */
function declarationOf(
   ctx: SpliceContext,
   tile: QueryTile,
): TreeView | undefined {
   if (tile.declaration.kind === "named")
      return ctx.parsed.queries.find((query) => query.name === tile.name);
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
   `where: ${malloyPath(f.field)} ${f.op ?? "~"} $${f.given}`;

/**
 * THE RULE the three removal paths obey: a comment goes only with a
 * declaration the author asked to delete outright. A filter edit is a REWRITE
 * of a declaration that stays, so a comment in the range it would clear is
 * nobody's to destroy — and one it would leave standing over the closing brace
 * is nobody's to strand either. Both refuse, and say which comment.
 *
 * Removing a TILE is the other case, and the one exception: the declaration
 * itself is what was asked for, so its own comments go with it.
 */
function commentRefusal(
   tileName: string,
   parsed: ParsedMalloy,
   comment: Span,
): SpliceFailure {
   return {
      ok: false,
      reason:
         `Removing a filter from \`${tileName}\` would also remove the comment ` +
         `written beside it (\`${parsed.text
            .slice(comment.start, comment.end)
            .trim()}\`), so nothing was written. Delete it in the file first.`,
   };
}

/**
 * The comment a whole-statement cut would take with it, or strand behind.
 *
 * `commentIn` covers what the cut spans. The second half covers what it does
 * not: a comment TRAILING the statement's last line survives the cut and is
 * left explaining the closing brace, which says something its author never
 * wrote. Only a comment that directly follows the statement counts — a second
 * statement sharing the line owns its own trailing comment, and that one the
 * cut never reaches.
 */
function commentLostBy(
   parsed: ParsedMalloy,
   statement: Span,
   cut: Span,
): Span | undefined {
   const inside = parsed.commentIn(cut);
   if (inside) return inside;
   const trailing = parsed.trailingComment(lineOf(parsed, statement.end - 1));
   return trailing &&
      trailing.start >= statement.end &&
      parsed.text.slice(statement.end, trailing.start).trim() === ""
      ? trailing
      : undefined;
}

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
   tile: QueryTile,
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
         const cut = statementCut(parsed, where.span);
         const lost = commentLostBy(parsed, where.span, cut);
         if (lost) return commentRefusal(tile.name, parsed, lost);
         edits.push({ ...cut, text: "" });
         continue;
      }
      if (surviving.length > 0) lastManaged = where.span;
      // Highest index first, so an earlier cut cannot move a later span.
      for (let i = managed.length - 1; i >= 0; i--) {
         const { clause, index } = managed[i];
         const want = wanted.get(clause.binding!.given);
         if (!want) {
            const cut = clauseCut(where.clauses, index);
            // The separator between two clauses is the builder's to take; a
            // comment written in that gap is not, and nothing downstream can
            // see it go.
            const comment = parsed.commentIn(cut);
            if (comment) return commentRefusal(tile.name, parsed, comment);
            edits.push({ ...cut, text: "" });
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
   let anchor =
      lastManaged?.end ??
      (stage.statements.length > 0
         ? stage.statements[stage.statements.length - 1].span.end
         : stage.openEnd);
   // A `//` comment is trivia, so it sits outside every span and the anchor
   // lands BEFORE one written after the statement it explains. Inserting a
   // line there would slide that comment onto the new `where:`, where it says
   // something the author never wrote -- and nothing downstream can tell,
   // because the comment is still in the file.
   if (!stage.oneLine) {
      const trailing = parsed.trailingComment(lineOf(parsed, anchor));
      if (trailing && trailing.start >= anchor) anchor = trailing.end;
   }
   // A comma only ever follows another `where:`. Malloy rejects one after a
   // `limit:` (`{ limit: 5, where: … }` is a parse error), while a space
   // parses after every statement form. A
   // one-line `where:` whose own span already ends at the separator needs none
   // added, or the file gains a `,,`.
   const lead =
      lastManaged !== undefined && !/,\s*$/.test(parsed.text.slice(0, anchor))
         ? ", "
         : " ";
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
   tile: QueryTile,
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
      const cut = { start: fromSpan.end, end: refinement.span.end };
      // The `+` and the block are the builder's; a comment written between or
      // inside them is not, and it is trivia to the parser, so deleting it
      // leaves a file that parses and reads back exactly as asked for.
      const comment = ctx.parsed.commentIn(cut);
      if (comment) {
         return {
            ok: false,
            reason:
               `Removing the last filter from \`${tile.name}\` would also ` +
               `remove the comment beside its refinement (\`${ctx.parsed.text
                  .slice(comment.start, comment.end)
                  .trim()}\`), so nothing was written. Delete it in the file ` +
               `first, or edit the refinement there.`,
         };
      }
      edits.push({ ...cut, text: "" });
      return undefined;
   }
   return planStageFilters(ctx, tile, refinement);
}

export interface SpliceOptions {
   /** The sources a document held as text sees without an `import`: its run model's. */
   visibleSources?: readonly string[];
}

export async function spliceDashboardDocument(
   sourceText: string,
   requested: DashboardDocument,
   options: SpliceOptions = {},
): Promise<SpliceResult> {
   // The planners split and join on `\n`, so a wholly CRLF file is edited as LF and written back as CRLF.
   const crlf = sourceText.includes("\r\n") && !/(?<!\r)\n/.test(sourceText);
   if (!crlf) return spliceLines(sourceText, requested, options);
   const result = await spliceLines(
      sourceText.replace(/\r\n/g, "\n"),
      requested,
      options,
   );
   return spliceFailed(result)
      ? result
      : { ...result, source: result.source.replace(/\n/g, "\r\n") };
}

async function spliceLines(
   sourceText: string,
   requested: DashboardDocument,
   options: SpliceOptions,
): Promise<SpliceResult> {
   const before = await readDashboardDocument(sourceText);
   if (readFailed(before)) {
      return {
         ok: false,
         reason: `Cannot edit a file that will not open: ${before.reason}`,
      };
   }
   const current = before.document;
   const next = keepUnstatedCharts(current, requested);
   const shape = checkShape(current, next, options.visibleSources);
   if ("reason" in shape) return shape;
   const problem = unwritable(current, next);
   if (problem) return { ok: false, reason: problem };

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
      planImports,
      planGivens,
      planDrills,
      planRemovedTiles,
      planAddedTiles,
      planTilePresentation,
      planTextBlocks,
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
      const { drills, localGivens, description, ...rest } = document;
      const tiles = document.tiles.map((tile) => {
         if (isTextTile(tile)) return tile;
         if (tile.filters?.length) return withoutDefaultChart(tile);
         const { filters: _filters, ...tileRest } = tile;
         return withoutDefaultChart(tileRest as QueryTile);
      });
      return {
         ...rest,
         ...(description === undefined
            ? {}
            : { description: trimDescription(description) }),
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
   // A file with a syntax error never reaches here -- the parse above refuses
   // it, and the editor refuses to open it at all -- so the guard below is
   // belt and braces against those two ever disagreeing.
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

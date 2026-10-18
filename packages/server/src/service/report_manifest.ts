// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Report manifests: the Malloyyo `# artifact` grammar, read out of a
 * package's reports (any tagged `.malloy`, wherever it lives; see `report.ts`)
 * into the manifest the REST surface serves.
 *
 * Ported from Malloyyo's `@malloyyo/mcp-engine` (`artifacts.ts`,
 * `given-specs.ts` — MIT), a private package. The *grammar* is the contract,
 * so one model repo largely works unchanged in both products. The grid width
 * deliberately diverges; see `docs/malloyyo-dashboards-design.md`
 * §Where Publisher diverges.
 *
 * A report written as tiles is `## artifact { tiles=[…] }` at model level:
 * named views that run separately and combine into one page Publisher lays
 * out, the only form that can span sources (a nest's pipeline starts from its
 * own query's source, so no single Malloy result spans two). Only the
 * model-level tag makes a report; a `# artifact` on a query or a view is not
 * read.
 *
 * A `.malloy` with no model-level tag is an ordinary model, which a report may
 * import: skipped, not an error.
 */

import { isJoined, isSourceDef } from "@malloydata/malloy";
import type {
   ModelDef,
   NamedModelObject,
   NamedQueryDef,
   TurtleDef,
} from "@malloydata/malloy";
import type { Tag } from "@malloydata/malloy-tag";
import { ownModelNoteObjects } from "./annotations";
import { referencedGivenNames } from "./authorize";
import {
   dashboardDescriptionNotes,
   isMarkdownNote,
   parseMarkdownOpener,
} from "./notebook";
import {
   readReportTag,
   reportName,
   type ReportLayout,
   type ReportTag,
} from "./report";
import {
   readGivenControlSpec,
   type GivenControlKind,
   type GivenSuggestSpec,
   type MalloyGivenApi,
   collectGivenRefs,
   suggestGivenLookup,
   suggestGivenNames,
   type SuggestGivenLookup,
} from "./given";
import {
   docCommentText,
   docCommentTitleAndDescription,
   motlyAnnotations,
   motlyParseErrors,
   motlyTag,
   quoteFilterLiterals,
   readAutorun,
   readStartingGivens,
   tagNumeric,
   tagText,
   unwrapFilterLiteral,
   ENV_REFERENCE_DROPPED,
   ANNOTATION_TOO_LONG,
} from "./motly";

// Re-exported: callers read these MOTLY primitives as part of the dashboard grammar.
export {
   docCommentText,
   docCommentTitleAndDescription,
   motlyAnnotations,
   quoteFilterLiterals,
   unwrapFilterLiteral,
};

/** The widest grid the Console builder offers; matches `MAX_COLUMNS` in the SDK. */
const MAX_BUILDER_COLUMNS = 24;

/**
 * The control contract is a property of the `given:` declaration, not of the
 * report that filters by it, so these are the shared types (`given.ts`).
 */
export type ReportControlKind = GivenControlKind;
export type ReportSuggestSpec = GivenSuggestSpec;

/**
 * One filter control on a dashboard: the ordinary API `Given`, derived from the
 * declaration, never authored per dashboard. Its `default` is the
 * declaration's; a dashboard's own starting values are
 * {@link ServedReportManifest.startingGivens}, already in run shape.
 */
export type ReportGivenSpec = MalloyGivenApi;

/** A tile that runs a query: what to run, and how to present it. */
export interface ReportQueryTileSpec extends ReportTileLayout {
   kind: "query";
   /** The run expression exactly as written in `tiles=[…]` (`"orders -> by_month"`). */
   query: string;
   /**
    * Given names this tile's compiled query reads (gates included) and the
    * entry file can bind, or undefined when discovery could not resolve the
    * tile (see {@link resolveTileGivens}). Scopes which tiles a changed control
    * re-runs; running with the whole control row also works, since an
    * unreferenced given is ignored.
    */
   givenNames?: string[];
   /**
    * `rowspan=N` on the tile's entry in `tiles=[…]`: the grid rows it is tall.
    * Absent, the tile is as tall as its result, or on a placed report
    * {@link PLACED_DEFAULT_ROWSPAN} rows. Only the entry sets it; a view's own
    * tags never do.
    */
   rowspan?: number;
   /**
    * `row=N` and `col=N` on the tile's entry: the grid row and column its top
    * left corner sits in, counted from 1. A report with any `row` is placed:
    * its rows are fixed tracks and its tiles sit where they say rather than
    * flowing. Only the entry sets them.
    */
   row?: number;
   col?: number;
   /**
    * `drill { <dimension> { to=… given=… } }` on the tile's entry: the click
    * actions this tile offers on a value of that dimension. An entry's drill
    * replaces the model's `# drill` tag for the dimension it names, on this
    * tile only; a dimension the entry does not name keeps the model's tag.
    * Only the entry sets it.
    */
   drill?: Record<string, TileDrill>;
   /**
    * The caller may not read this tile's source. Set only by a compile of
    * caller-submitted text; carries nothing else of the tile, so the answer
    * discloses neither the gated view's layout tags nor the givens it reads.
    */
   restricted?: boolean;
}

/** A tile that shows markdown: its `##|(markdown) name` block, laid out by its own `tiles=[…]` entry. */
export interface ReportTextTileSpec {
   kind: "text";
   name: string;
   /** The block's body; empty when the file has no block of that name. */
   markdown: string;
   colspan?: number;
   break?: boolean;
   /** `rowspan=N` on its entry: the grid rows it is tall. */
   rowspan?: number;
   /** `row=N` and `col=N` on its entry: where it sits on a placed report. */
   row?: number;
   col?: number;
}

export type ReportTileSpec = ReportQueryTileSpec | ReportTextTileSpec;

/** The tiles that run a query, for every reader that has no use for prose. */
export function queryTiles(
   tiles: readonly ReportTileSpec[] | undefined,
): ReportQueryTileSpec[] {
   return (tiles ?? []).filter(
      (tile): tile is ReportQueryTileSpec => tile.kind === "query",
   );
}

/**
 * Per-tile presentation, read off the view or query a tile names rather than off
 * the tile entry in `tiles=[…]`.
 *
 * Exactly @malloydata/render's per-child dashboard tags (its
 * `DashboardChildConfig`) plus `# label`, so one view presents the same as a
 * tile or nested under a `# dashboard` query. Keep it the whole set: a tag
 * honored on one form and dropped on the other is a silent divergence.
 */
export interface ReportTileLayout {
   /** `# label`: the tile's heading. */
   label?: string;
   /** `# subtitle`: a second line under the heading. */
   subtitle?: string;
   /** `# colspan=N`: grid columns this tile spans. */
   colspan?: number;
   /**
    * `# break`: start a new grid row at this tile. Named for the tag, not a
    * language: generated clients escape the reserved word (Python `break_`)
    * while the wire key stays `break`.
    */
   break?: boolean;
   /** `# borderless`: draw the result with no card around it. */
   borderless?: boolean;
}

export interface ServedReportManifest {
   /** The filename basename: a display label, and the deprecated `…/dashboards/{name}` segment. Never the identity, which is `entryFile`. */
   name: string;
   /** How the tiles are placed (`report.ts`, §2); a linear report lays out as one column. */
   layout: ReportLayout;
   title: string;
   description?: string;
   /** The tiles, in order; absent only on a grid report listed with its compile error. */
   tiles?: ReportTileSpec[];
   /** Grid width: `# dashboard {columns=N}`, beside the artifact tag. */
   dashboardColumns?: number;
   /**
    * Per-dashboard starting values for controls, in the shape the query
    * endpoint accepts (a `filter<…>` value is the filter body, not the `f'…'`
    * literal — see {@link unwrapFilterLiteral}). Overridden by URL params.
    */
   startingGivens?: Record<string, string>;
   /** False batches control changes behind an Apply button. Defaults true. */
   autorun: boolean;
   /** Package-relative path of the file the tag was read from. */
   entryFile: string;
   /**
    * The control row: the given declarations the dashboard runs with, the union
    * over its tiles, widened to the entry file's surfaced set when a tile cannot
    * be resolved. Values keyed by name are {@link startingGivens}.
    */
   givens: ReportGivenSpec[];
}

/**
 * MOTLY tags the dashboard grammar owns. They share the `#` namespace with
 * `@malloydata/render`, which reports every tag it did not consume as
 * `Unknown render tag '<name>' on field '<field>'`; without this list every
 * dashboard query would carry a spurious warning. The renderer's own
 * suppression is internal to it, so the log is filtered afterwards
 * (`docs/malloyyo-dashboards-design.md` §Follow-ups).
 */
//
// `theme.palette.series` is the per-chart series list the SDK reads off the
// result; the report builder writes a one-colour line of it for a tile's
// colour, so without this every coloured tile warned. Only that path: a
// `theme.*` typo (`# theme.palete.series`) is the author's mistake and stays.
const PUBLISHER_OWNED_TAGS = ["artifact", "drill"];
const PUBLISHER_OWNED_PATHS = ["theme.palette.series"];

/** A tag Publisher owns: a whole root (`artifact`, `drill`) or one exact path. */
function isPublisherOwnedTag(tag: string): boolean {
   if (PUBLISHER_OWNED_TAGS.includes(tag.split(".")[0])) return true;
   return PUBLISHER_OWNED_PATHS.some(
      (path) => tag === path || tag.startsWith(`${path}.`),
   );
}

/**
 * The renderer's own per-child dashboard tags, which Publisher also reads for a
 * tile ({@link ReportTileLayout}).
 *
 * The renderer resolves these only for direct children of a `# dashboard` nest,
 * and a tile is a standalone query, so every tile would answer
 * `Unknown render tag 'colspan'` for a tag Publisher did consume.
 *
 * Dropped only ON THE TOP-LEVEL RESULT, which is the shape a tile has. A
 * `# colspan` on a nest that is not a dashboard child is still a real mistake
 * nothing else reads, and it reports against that nest's field name, so it
 * survives. Nor is the renderer's own `Invalid # colspan` / `Ignored # colspan`
 * touched: this matches only the unknown-tag line.
 *
 * Two scopes, one per route. A query is dropped when it ran against a report
 * file ({@link filterPublisherOwnedRenderLogs}), since that is where a tile
 * runs. The load-time check knows more, so it drops the finding only on a view
 * or query a grid report names as a tile ({@link dropGridTileLayoutFindings}),
 * wherever that view is defined. Suppressing by shape alone would take the
 * warning away from an ordinary view that puts one of these at top level, where
 * there is no grid and the tag really does nothing.
 */
const RENDERER_CHILD_TAGS = ["colspan", "break", "subtitle", "borderless"];

const UNKNOWN_RENDER_TAG = /^Unknown render tag '([^']+)' on field '([^']*)'/;

/** The renderer calling a tile layout tag (`# colspan`, `# break`, …) unknown on a result's top level. */
function isTileLayoutTagOnRoot(message: string | undefined): boolean {
   const match = UNKNOWN_RENDER_TAG.exec(message ?? "");
   if (!match) return false;
   const [, tag, field] = match;
   return RENDERER_CHILD_TAGS.includes(tag.split(".")[0]) && field === "root";
}

/**
 * What a run expression names, for `Model.definitionIdentity`: a query's
 * name, or a source and one of its views. Undefined for anything else (a
 * refinement, source arguments, a further stage), which names no single
 * declaration a tile's layout is read from (`tileAnnotations` keys on the
 * same two forms).
 */
export function tileTarget(
   expression: string,
): { name: string; view?: string } | undefined {
   const parts = normalizeTileExpression(expression).split(" -> ");
   if (parts.length === 1) return { name: parts[0] };
   if (parts.length === 2) return { name: parts[0], view: parts[1] };
   return undefined;
}

/**
 * Drop the renderer's "unknown tag" finding for a tile layout tag on a view or
 * query a grid report names as a tile, and keep it everywhere else.
 *
 * A grid report reads `# colspan`, `# break`, `# subtitle` and `# borderless`
 * off the view each tile names ({@link ReportTileLayout}), with or without a
 * `dashboard { columns=N }` (without one the report lint says the tile is
 * clamped to the viewer's default). The renderer validates the view on its own
 * and calls the tag unknown, so without this the load reports a tag the grid
 * reads. A view no grid report names keeps the finding, including one in a
 * report file and a tile of a linear report, which is one column.
 *
 * `isGridTile` says whether a finding's subject is defined where a grid tile
 * is (`Model.definitionIdentity` over {@link tileTarget}), so a view a report
 * imports from another file counts, and so does a copy of it inherited by an
 * extending source.
 */
export function dropGridTileLayoutFindings<T extends { message?: string }>(
   warnings: T[],
   isGridTile: (warning: T) => boolean,
   /**
    * Whether a finding's subject is a tile of any report, linear included: a
    * linear report's tile reads `# subtitle` and `# borderless` as a grid's
    * does, so those two are dropped there too. Absent, grid tiles only.
    */
   isAnyTile: (warning: T) => boolean = isGridTile,
): T[] {
   return warnings.filter((warning) => {
      if (!isTileLayoutTagOnRoot(warning.message)) return true;
      const tag = UNKNOWN_RENDER_TAG.exec(warning.message ?? "")?.[1] ?? "";
      return !(
         isGridTile(warning) ||
         (TILE_PRESENTATION_TAGS.includes(tag.split(".")[0]) &&
            isAnyTile(warning))
      );
   });
}

/** The per-tile tags read in either layout; `colspan` and `break` are a grid's only. */
const TILE_PRESENTATION_TAGS = ["subtitle", "borderless"];

/**
 * Drop "unknown render tag" complaints about tags Publisher owns, leaving every
 * other render finding intact. `artifact` and `drill` match on the tag's root,
 * so nested paths (`artifact.title`) are covered too. `theme` matches only
 * `theme.palette.series`, so a typo such as `theme.palete.series` is kept.
 */
export function filterPublisherOwnedRenderLogs<T extends { message?: string }>(
   logs: T[],
   /** Whether the model this query ran against carries a report tag
    * (`Model.carriesReportTag`). Required so a new call site cannot silently
    * widen the suppression to every query. */
   couldBeATile: boolean,
   /** The Malloy result the logs are about, whose own tags say what a bare `theme` finding refers to. */
   result: unknown,
): T[] {
   const themeOwned = themeIsPublisherOwned(result);
   return logs.filter((log) => {
      const match = UNKNOWN_RENDER_TAG.exec(log.message ?? "");
      if (!match) return true;
      if (isPublisherOwnedTag(match[1])) return false;
      // The renderer names only the root of a path it does not read, so the
      // colour line Publisher writes (`# theme.palette.series=[…]`) comes back
      // as `theme`. It is Publisher's when every `theme.` path in the result's
      // own tags is that one; a typo beside it (`theme.palete.series`) keeps
      // the finding.
      if (match[1] === "theme" && themeOwned) return false;
      return !(couldBeATile && isTileLayoutTagOnRoot(log.message));
   });
}

/**
 * Whether every `theme.` path in a result's own tags is the one Publisher
 * reads (`theme.palette.series`), and there is at least one.
 */
function themeIsPublisherOwned(result: unknown): boolean {
   const notes = (
      (result as { annotations?: Array<{ value?: string }> } | undefined)
         ?.annotations ?? []
   ).map((note) => note.value ?? "");
   const paths: string[] = [];
   for (const note of notes) {
      if (
         !note.trimStart().startsWith("#") ||
         note.trimStart().startsWith("#(")
      )
         continue;
      for (const hit of note.matchAll(
         /(?<![\w.])theme((?:\.[A-Za-z_][\w]*)*)/g,
      ))
         paths.push(`theme${hit[1]}`);
   }
   return (
      paths.length > 0 &&
      paths.every((path) =>
         PUBLISHER_OWNED_PATHS.some(
            (owned) => path === owned || path.startsWith(`${owned}.`),
         ),
      )
   );
}

/**
 * Whether a name matches the naming convention `api-doc.yaml` describes in
 * prose. Advisory only: every tagged dashboard is served whatever its name
 * (the name is percent-encoded into its URL, and the spec deliberately has no
 * pattern because the name comes from a filename). Worth reporting because a
 * name outside it must be percent-encoded by a caller building the URL by hand.
 *
 * Not a gate: an empty name cannot occur (`listPackageFiles` ignores
 * dotfiles), a dot mid-name does not break routing, and the lookup is a `Map`,
 * so there is no traversal risk.
 */
export function matchesDocumentedReportName(slug: string): boolean {
   return /^[a-zA-Z0-9_-]+$/.test(slug);
}

/**
 * Artifact-tag properties {@link readArtifactTag} reads. The lint enumerates
 * against these, so anything added to the reader belongs here too.
 */
const ARTIFACT_PROPERTIES: readonly string[] = [
   "title",
   "tiles",
   "givens",
   "autorun",
   // Not read here: notebook_lint.ts judges `layout`; this lint only stays quiet about it.
   "layout",
];

/**
 * Properties that reach the artifact tag by mistake, and what to write instead.
 * The grid width is a sibling tag, so `## artifact { … dashboard { columns=12 } }`
 * reads naturally and puts it somewhere nothing looks.
 */
const ARTIFACT_PROPERTY_REPLACEMENTS: Record<string, string> = {
   dashboard:
      "The grid width is a sibling of the artifact tag, not a property of it: " +
      "close the artifact braces first, as `## artifact { … } dashboard { columns=N }`.",
   dashboard_columns:
      "The grid width is written beside the artifact tag: " +
      "`## artifact { … } dashboard { columns=N }`.",
};

/**
 * The layout a `tiles=[…]` entry carries, read the same on either kind of
 * entry. On a query entry each property wins over the view's own tag, so
 * `break=false` is kept: it is how an entry turns off a view's `# break`.
 */
interface EntryLayout {
   colspan?: number;
   break?: boolean;
   rowspan?: number;
   row?: number;
   col?: number;
   drill?: Record<string, TileDrill & { missingTo?: true }>;
}

/** One dimension's click actions on a tile's entry: `<dimension> { to=… given=… }` under `drill`. */
export interface TileDrill {
   /**
    * Destinations: a report path each, or the literal `self`. Empty when the
    * entry writes `to=[]`, which turns the model's tag off on this tile: the
    * value then offers its rows alone.
    */
   to: string[];
   /** The given a click seeds; absent means the dimension's own name. */
   given?: string;
}

/** The properties an entry's layout is written with, and so the only ones the lint lets an entry carry. */
const ENTRY_LAYOUT_PROPERTIES = ["row", "col", "colspan", "rowspan", "break"];
/** What a query tile's entry may carry besides its layout. */
const QUERY_ENTRY_PROPERTIES = [...ENTRY_LAYOUT_PROPERTIES, "drill"];

/**
 * Rows a tile with no `rowspan` takes on a placed report, whose rows are fixed
 * tracks: about the height an unplaced tile is drawn at. Kept equal to the
 * SDK's `PLACED_DEFAULT_ROWSPAN`, since the lint's overlap check must count
 * the rows the viewer draws.
 */
export const PLACED_DEFAULT_ROWSPAN = { query: 5, text: 2 } as const;

/** One entry of `tiles=[…]`: a run expression or a named text block, each with its own layout. */
type ArtifactTileEntry =
   | { kind: "query"; expression: string; layout: EntryLayout }
   | { kind: "text"; name: string; layout: EntryLayout };

function readEntryLayout(entry: Tag): EntryLayout {
   const colspan = positiveInteger(tagNumeric(entry, "colspan"));
   const rowspan = positiveInteger(tagNumeric(entry, "rowspan"));
   const row = positiveInteger(tagNumeric(entry, "row"));
   const col = positiveInteger(tagNumeric(entry, "col"));
   const drill = readEntryDrill(entry);
   return {
      ...(colspan !== undefined && { colspan }),
      ...(entry.has("break") && { break: tagText(entry, "break") !== "false" }),
      ...(rowspan !== undefined && { rowspan }),
      ...(row !== undefined && { row }),
      ...(col !== undefined && { col }),
      ...(drill !== undefined && { drill }),
   };
}

/**
 * `drill { region { to=self given=REGION } product { to="sales/detail" } }` on
 * an entry, by dimension. Read like a model's `# drill` tag ({@link readDrillTag}):
 * one destination may be written unbracketed, and `to=[]` turns the model's tag
 * off on this tile. A dimension with no `to` at all is marked `missingTo`, so
 * the lint can report it and the manifest can leave it out.
 */
function readEntryDrill(
   entry: Tag,
): Record<string, TileDrill & { missingTo?: true }> | undefined {
   const drill = entry.tag("drill");
   if (!drill) return undefined;
   const out: Record<string, TileDrill & { missingTo?: true }> = {};
   for (const dimension of Object.keys(drill.dict ?? {})) {
      const spec = drill.tag(dimension);
      if (!spec) continue;
      const missingTo = !spec.has("to");
      const to =
         tagTextArray(spec, "to") ?? [tagText(spec, "to")].filter(isString);
      const given = tagText(spec, "given");
      out[dimension] = {
         to,
         ...(given !== undefined && { given }),
         ...(missingTo && { missingTo: true as const }),
      };
   }
   return Object.keys(out).length > 0 ? out : undefined;
}

/** The layout fields a tile spec carries from its entry: a `break` that is off is no field at all. */
function entryLayoutFields(layout: EntryLayout): {
   colspan?: number;
   break?: true;
   rowspan?: number;
   row?: number;
   col?: number;
} {
   return {
      ...(layout.colspan !== undefined && { colspan: layout.colspan }),
      ...(layout.break === true && { break: true as const }),
      ...(layout.rowspan !== undefined && { rowspan: layout.rowspan }),
      ...(layout.row !== undefined && { row: layout.row }),
      ...(layout.col !== undefined && { col: layout.col }),
   };
}

/** A query tile's click actions from its entry; a dimension written with no `to` is the lint's and left off. */
function entryDrillField(layout: EntryLayout): {
   drill?: Record<string, TileDrill>;
} {
   const usable = Object.entries(layout.drill ?? {}).flatMap(
      ([dimension, { missingTo, ...spec }]) =>
         missingTo ? [] : [[dimension, spec] as const],
   );
   return usable.length > 0 ? { drill: Object.fromEntries(usable) } : {};
}

/** The `artifact` sub-tag read into the manifest fields it contributes. */
interface ArtifactTagData {
   title?: string;
   kind?: string;
   tiles?: ArtifactTileEntry[];
   dashboardColumns?: number;
   givens?: Record<string, string>;
   autorun: boolean;
}

function readTileEntries(artifact: Tag): ArtifactTileEntry[] | undefined {
   return artifact.array("tiles")?.flatMap((entry): ArtifactTileEntry[] => {
      const text = tagText(entry);
      if (text === undefined) return [];
      const layout = readEntryLayout(entry);
      return tagText(entry, "kind") === "text"
         ? [{ kind: "text", name: text, layout }]
         : [{ kind: "query", expression: text, layout }];
   });
}

function readArtifactTag(
   tag: Tag,
   declaredType: (name: string) => string | undefined,
): ArtifactTagData | undefined {
   const artifact = tag.tag("artifact");
   if (!artifact) return undefined;

   const givens = readStartingGivens(artifact, declaredType);
   const autorun = readAutorun(artifact);

   return {
      title: tagText(artifact, "title"),
      kind: tagText(artifact, "kind"),
      tiles: readTileEntries(artifact),
      // The `# dashboard { columns=N }` tag beside the artifact tag, the same
      // spelling the renderer reads on a single query.
      //
      // Read through the lint's strict reader and required to be a positive
      // integer: `Tag.numeric()` is parseFloat, which takes `12abc` as 12 and
      // `1e999` as Infinity (JSON null on an integer field), so the manifest
      // would serve a value the lint reports as dropped.
      dashboardColumns: tag.tag("dashboard")?.has("columns")
         ? positiveInteger(tagNumeric(tag.tag("dashboard"), "columns"))
         : undefined,
      givens,
      autorun,
   };
}

/** The run expressions of a composite artifact tag's query tiles, read off the file's own `##` notes. */
export function readDocumentTileExpressions(
   notes: readonly string[],
): string[] {
   const tag = motlyTag(notes);
   const composite = tag ? readArtifactTag(tag, () => undefined) : undefined;
   return (composite?.tiles ?? []).flatMap((entry) =>
      entry.kind === "text" ? [] : [entry.expression],
   );
}

/** The `##|(markdown) name` blocks among a file's own model notes, in file order. */
export function readTextBlocks(
   notes: readonly string[],
): { name: string; body: string }[] {
   return notes.flatMap((note) => {
      const newline = note.indexOf("\n");
      const opener = parseMarkdownOpener(
         newline === -1 ? note : note.slice(0, newline),
      );
      if (opener?.level !== 2 || !opener.name) return [];
      const body = newline === -1 ? "" : note.slice(newline + 1);
      return [{ name: opener.name, body: body.replace(/\r\n?/g, "\n") }];
   });
}

/**
 * A grid width or a colspan, or undefined when the value is not one. Shared
 * with the lint so the manifest and the finding cannot disagree about a tag.
 */
function positiveInteger(value: number | undefined): number | undefined {
   return value !== undefined && Number.isInteger(value) && value >= 1
      ? value
      : undefined;
}

/** One given declaration, as discovery needs it. */
export interface ReportGivenDeclaration {
   name: string;
   type: string;
   default?: string;
   /** It declares no default, so a query that reads it fails until a value is supplied. */
   required?: boolean;
   annotations: string[];
}

/**
 * The facts one compiled report file contributes, lifted off its `ModelDef`
 * so the manifest build is a pure function, testable without a compile.
 */
export interface ReportModelFacts {
   modelPath: string;
   /** Model-level (`##`) annotation texts, folded across the import lineage. */
   modelAnnotations: string[];
   /**
    * The own notes that describe the dashboard, of every route; the doc-comment
    * reader keeps the `"` ones. Absent means all of `modelAnnotations`.
    */
   descriptionNotes?: string[];
   /**
    * Every named query in the file, uncurated: curation shapes the discovery
    * surface, and a report is a separate artifact.
    */
   queries: {
      name: string;
      annotations: string[];
      /** Names of the givens this query references. */
      givens: string[];
      /**
       * The source this query reads, when it reads one BY NAME. A query imported
       * without its source keeps the name and resolves as a query, so nothing
       * upstream reports it; running it is what fails. `undefined` when the
       * source is inlined in the query's own definition, which cannot dangle.
       */
      sourceName?: string;
   }[];
   /**
    * Given declarations this file can actually *bind*, keyed by name: the
    * model's surfaced givens, not every declaration in the compile closure.
    * Malloy's given namespace is per-file, so binding a given the entry file
    * does not import fails with "unknown given", and a control the server would
    * reject is worse than no control. A tile reading such a given is a lint
    * finding.
    */
   givens: Map<string, ReportGivenDeclaration>;
   /**
    * Given names each source view references, keyed by the run expression that
    * names it (`"orders -> by_month"`, normalized), the form `tiles=[…]`
    * entries are written in. Reference, not bindability: intersect with
    * {@link ReportModelFacts.givens} before promising a control. Its key set
    * doubles as "every view this file can run as a tile" for the lint.
    */
   viewGivens: Map<string, string[]>;
   /**
    * What each tile's COMPILED query reads, keyed by normalized tile
    * expression; preferred over {@link viewGivens} wherever present. Absent for
    * a tile that did not compile, and entirely when the caller compiled
    * nothing (a test, a sync reader). See `Model.getCompiledReportModelFacts`.
    */
   compiledTileGivens?: Map<string, CompiledTileGivens>;
   /**
    * Annotation texts on each source view, keyed like
    * {@link ReportModelFacts.viewGivens}: the per-tile layout tags. Raw rather
    * than a {@link ReportTileLayout}, because the lint reports values the
    * reader drops.
    */
   viewAnnotations: Map<string, string[]>;
   /**
    * Field names per source, for validating `suggest { source= dimension= }`.
    * Fields of a direct join appear as `join.field`.
    */
   sourceFields: Map<string, Set<string>>;
   /**
    * Every `# drill` tag reachable in this file, one per tagged dimension, then
    * every `drill { … }` on this file's own tile entries (those carry `tile`).
    */
   drills: ReportDrill[];
   /**
    * The output field names of each one-stage source view, nests included,
    * keyed like {@link ReportModelFacts.viewGivens}: what a tile entry's
    * `drill { <dimension> … }` can name on a plain `source -> view` tile.
    * Absent for a multi-stage view, whose names the lint does not check.
    */
   viewOutputs?: Map<string, Set<string>>;
   /**
    * Which givens a `suggest` needs in its request: the source's own `where:`
    * and effective `#(authorize)` references, or a named query's plus its
    * source's, narrowed to what this file can bind. See `suggestGivenLookup`.
    */
   suggestGivens: SuggestGivenLookup;
}

/** The givens one compiled tile query depends on. */
export interface CompiledTileGivens {
   /** Malloy's `givenUsage`: every given the query reads, joins and dimensions included. */
   reads: string[];
   /**
    * Givens the run target's `#(authorize)`/`#(access_filter)` gates read.
    * Kept apart from `reads` because an unimported gate given is accepted at
    * query time, so the import lint must not name it.
    */
   gateReads: string[];
}

/** A `# drill { to=[…] given=… }` tag on a source dimension. */
export interface ReportDrill {
   source: string;
   dimension: string;
   /**
    * Set when the drill is a tile entry's (`drill { … }` in `tiles=[…]`)
    * rather than a dimension's `# drill` tag: the tile's run expression. Its
    * `source` is then that expression too.
    */
   tile?: string;
   /** Destinations: a dashboard slug each, or the literal `self`. */
   to: string[];
   /** Overrides the given name, which is otherwise the dimension name
    *  verbatim (see `resolveDrill.ts`, which owns that rule). */
   given?: string;
}

/**
 * The run expression form a tile is written in, normalized so `"orders->by_x"`
 * and `"orders  ->  by_x"` are the same key.
 */
export function normalizeTileExpression(tile: string): string {
   // Must stay linear: never `/\s*->\s*/g`, which backtracks quadratically over
   // a run of whitespace (2.34s at 80k spaces). This runs on an author's
   // `tiles=[…]` string in the main process on every load and every reload,
   // which any caller can trigger, so a slow pattern blocks the shared event
   // loop for every tenant. A fuzz test holds it equal to the regex form; the
   // trailing collapse keeps `a->->b` exact.
   return tile
      .split("->")
      .map((part) => part.trim().replace(/\s+/g, " "))
      .join(" -> ")
      .replace(/\s+/g, " ");
}

/**
 * Read the report-relevant facts off a compiled model: given declarations from
 * `ModelDef.givens` and each named query's `givenUsage`. Source views get the
 * same answer by walking (`collectGivenRefs`), since a `TurtleDef` carries no
 * `givenUsage`.
 *
 * `surfacedGivenNames` narrows the registry to what this file can be run with
 * ({@link ReportModelFacts.givens}).
 */
export function readReportModelFacts(
   modelPath: string,
   modelDef: ModelDef,
   surfacedGivenNames: string[],
   /**
    * The effective gate expressions per source on both routes, so a suggest
    * over a gated source knows which givens its gates read. Absent means no
    * source is gated.
    */
   gatesBySource?: ReadonlyMap<string, readonly string[]>,
): ReportModelFacts {
   const registry = modelDef.givens ?? {};
   const surfaced = new Set(surfacedGivenNames);
   const givens = new Map<string, ReportGivenDeclaration>();
   for (const given of Object.values(registry)) {
      if (!surfaced.has(given.name)) continue;
      const type = given.type;
      const isFilter = type.type === "filter expression";
      givens.set(given.name, {
         name: given.name,
         type: isFilter ? `filter<${type.filterType}>` : type.type,
         // Raw as declared; `givenSpec` unwraps a filter literal when it publishes.
         default: given.defaultText,
         // Both forms absent, not either: the rendered text alone can go missing
         // without the given losing its default.
         ...(given.default === undefined && given.defaultText === undefined
            ? { required: true }
            : {}),
         annotations: (given.annotations?.blockNotes ?? [])
            .concat(given.annotations?.notes ?? [])
            .map((note) => note.text),
      });
   }

   const isNamedQuery = (obj: NamedModelObject): obj is NamedQueryDef =>
      obj.type === "query";
   const queries = Object.values(modelDef.contents)
      .filter(isNamedQuery)
      .map((query) => ({
         name: query.as || query.name,
         annotations: [
            ...(query.annotations?.blockNotes ?? []),
            ...(query.annotations?.notes ?? []),
         ].map((note) => note.text),
         givens: (query.givenUsage ?? [])
            .map((usage) => registry[usage.id]?.name)
            .filter((name): name is string => name !== undefined),
         sourceName:
            typeof query.structRef === "string" ? query.structRef : undefined,
      }));

   const viewGivens = new Map<string, string[]>();
   const viewAnnotations = new Map<string, string[]>();
   const sourceFields = new Map<string, Set<string>>();
   const viewOutputs = new Map<string, Set<string>>();
   const drills: ReportDrill[] = [];
   for (const obj of Object.values(modelDef.contents)) {
      if (!isSourceDef(obj)) continue;
      const sourceName = obj.as || obj.name;
      const fieldNames = new Set<string>();
      sourceFields.set(sourceName, fieldNames);

      // A source-level `where:` reading a given scopes every view of the source
      // but sits outside each view's pipeline; without unioning it in, a tile
      // would show no control while silently filtered by the given's default.
      const sourceGivens = new Set<string>();
      collectGivenRefs(obj.filterList, sourceGivens);

      for (const field of obj.fields) {
         const fieldName = field.as || field.name;
         fieldNames.add(fieldName);
         // One join level, as the SDK catalog lists it: `products.category` is a
         // valid `suggest` dimension, `products.maker.name` is not.
         if (isJoined(field)) {
            for (const joined of field.fields) {
               fieldNames.add(`${fieldName}.${joined.as || joined.name}`);
            }
         }
         if (field.type === "turtle") {
            const refs = new Set(sourceGivens);
            collectGivenRefs((field as TurtleDef).pipeline, refs);
            const key = normalizeTileExpression(
               `${sourceName} -> ${fieldName}`,
            );
            viewGivens.set(key, Array.from(refs));
            const outputs = viewOutputNames(field as TurtleDef);
            if (outputs) viewOutputs.set(key, outputs);
            viewAnnotations.set(
               key,
               [
                  ...(field.annotations?.blockNotes ?? []),
                  ...(field.annotations?.notes ?? []),
               ].map((note) => note.text),
            );
            continue;
         }
         const drill = readDrillTag(
            [
               ...(field.annotations?.blockNotes ?? []),
               ...(field.annotations?.notes ?? []),
            ].map((note) => note.text),
         );
         if (drill) {
            drills.push({ ...drill, source: sourceName, dimension: fieldName });
         }
      }
   }

   const modelNotes = ownModelNoteObjects(modelDef);
   // A tile entry's own click actions, beside the model's tags: the drill
   // lints check both the same way.
   const ownArtifact = motlyTag(modelNotes.map((note) => note.text))?.tag(
      "artifact",
   );
   for (const entry of ownArtifact
      ? (readTileEntries(ownArtifact) ?? [])
      : []) {
      if (entry.kind !== "query") continue;
      for (const [dimension, { missingTo, ...spec }] of Object.entries(
         entry.layout.drill ?? {},
      )) {
         // `to=[]` turns the model's tag off: nothing to check. No `to` at all
         // is a broken entry, which the target lint reports as one.
         if (spec.to.length === 0 && !missingTo) continue;
         drills.push({
            ...spec,
            source: entry.expression,
            dimension,
            tile: entry.expression,
         });
      }
   }
   return {
      modelPath,
      // This file's own `##` only. An `## artifact` in a shared include
      // describes that include, not everything importing it.
      modelAnnotations: modelNotes.map((note) => note.text),
      descriptionNotes: dashboardDescriptionNotes(modelNotes),
      queries,
      givens,
      viewGivens,
      viewAnnotations,
      sourceFields,
      drills,
      viewOutputs,
      suggestGivens: suggestGivenLookup(
         modelDef,
         (source) => gatesBySource?.get(source),
         surfaced,
      ),
   };
}

/**
 * Read the per-tile presentation tags off one view's annotations, or undefined
 * when it carries none of them.
 *
 * Validated as @malloydata/render validates them on a `# dashboard` child:
 * colspan a positive integer, break and borderless by presence. A bad colspan
 * is dropped, not clamped (the lint says so); clamping to the grid width
 * happens at render time, where the width is known.
 */
function readTileLayout(annotations: string[]): ReportTileLayout | undefined {
   const tag = motlyTag(annotations);
   if (!tag) return undefined;
   const layout: ReportTileLayout = {};
   const label = tagText(tag, "label");
   if (label !== undefined) layout.label = label;
   const subtitle = tagText(tag, "subtitle");
   if (subtitle !== undefined) layout.subtitle = subtitle;
   const colspan = positiveInteger(tagNumeric(tag, "colspan"));
   if (colspan !== undefined) layout.colspan = colspan;
   if (tag.has("break")) layout.break = true;
   if (tag.has("borderless")) layout.borderless = true;
   return Object.keys(layout).length > 0 ? layout : undefined;
}

/**
 * Read a `# drill { to=[…] given=… }` tag off a dimension's annotations, or
 * undefined when it carries none. A bare `# drill` with no `to=` is returned
 * with an empty `to`, so `lintDrillTargets` can tell a broken drill from none.
 */
function readDrillTag(
   annotations: string[],
): Omit<ReportDrill, "source" | "dimension"> | undefined {
   const drill = motlyTag(annotations)?.tag("drill");
   if (!drill) return undefined;
   // Malloyyo accepts a single destination unbracketed (`to=overview`) as well
   // as the list form, so fall back to reading `to` as text.
   const to =
      tagTextArray(drill, "to") ?? [tagText(drill, "to")].filter(isString);
   const given = tagText(drill, "given");
   return given === undefined ? { to } : { to, given };
}

/**
 * A one-stage view's output names, nests' own included, or undefined for a
 * view that pipes into a later stage. A `fieldref` counts by its last path
 * segment, as its output column is named.
 */
function viewOutputNames(view: TurtleDef): Set<string> | undefined {
   if (view.pipeline.length !== 1) return undefined;
   const names = new Set<string>();
   const walk = (fields: readonly unknown[]) => {
      for (const field of fields) {
         const f = field as {
            type?: string;
            path?: string[];
            name?: string;
            as?: string;
            pipeline?: Array<{ queryFields?: unknown[] }>;
         };
         if (f.type === "fieldref" && f.path?.length) {
            names.add(f.path[f.path.length - 1]);
         } else if (f.type === "turtle") {
            if (f.as || f.name) names.add((f.as || f.name) as string);
            for (const stage of f.pipeline ?? []) walk(stage.queryFields ?? []);
         } else if (f.as || f.name) {
            names.add((f.as || f.name) as string);
         }
      }
   };
   walk((view.pipeline[0] as { queryFields?: unknown[] }).queryFields ?? []);
   return names;
}

function isString(value: string | undefined): value is string {
   return value !== undefined;
}

/**
 * `Tag.textArray()` throws on a bad literal exactly as `text()` does, so read
 * the list with `array()` (which does not) and take each element through
 * {@link tagText}. One unreadable entry is dropped rather than costing the
 * whole list.
 */
function tagTextArray(
   tag: Tag | undefined,
   ...path: string[]
): string[] | undefined {
   const items = tag?.array(...path);
   return items?.map((item) => tagText(item)).filter(isString);
}

/**
 * The givens a composite tile reads, or undefined when discovery could not
 * resolve the tile.
 *
 * Its compiled query answers first ({@link CompiledTileGivens}). Without one,
 * the static walk covers `source -> view` and a bare named query, and leaves
 * anything else absent rather than guessing; a consumer then sends the whole
 * control row, which is safe since an unreferenced given is ignored.
 */
function resolveTileGivens(
   tile: string,
   facts: ReportModelFacts,
): string[] | undefined {
   const reads = referencedTileGivens(tile, facts);
   if (reads === undefined) return undefined;
   const gateReads =
      facts.compiledTileGivens?.get(normalizeTileExpression(tile))?.gateReads ??
      [];
   // Narrowed to what the entry file can bind: a given reachable only through
   // an import chain would fail with "unknown given".
   return Array.from(new Set([...reads, ...gateReads])).filter((name) =>
      facts.givens.has(name),
   );
}

/**
 * The layout a tile inherits from the view or query it names, or undefined when
 * that view declares none. Resolved like {@link referencedTileGivens}, so an
 * unresolvable tile gets no layout: there is no single view to have tagged one.
 */
function resolveTileLayout(
   tile: string,
   facts: ReportModelFacts,
): ReportTileLayout | undefined {
   const annotations = tileAnnotations(tile, facts);
   return annotations && readTileLayout(annotations);
}

/** The annotations on whatever a tile expression names, for the two readers of them. */
function tileAnnotations(
   tile: string,
   facts: ReportModelFacts,
): string[] | undefined {
   const normalized = normalizeTileExpression(tile);
   return (
      facts.viewAnnotations.get(normalized) ??
      facts.queries.find((query) => query.name === normalized)?.annotations
   );
}

/** {@link resolveTileGivens} before the bindability filter; the lint wants both. */
function referencedTileGivens(
   tile: string,
   facts: ReportModelFacts,
): string[] | undefined {
   const normalized = normalizeTileExpression(tile);
   const statically =
      facts.viewGivens.get(normalized) ??
      facts.queries.find((query) => query.name === normalized)?.givens;
   const compiled = facts.compiledTileGivens?.get(normalized)?.reads;
   if (!compiled) return statically;
   // Ordered as the static walk was, so the control row a tile feeds keeps its order.
   const rank = (name: string) => {
      const at = statically?.indexOf(name) ?? -1;
      return at === -1 ? Number.MAX_SAFE_INTEGER : at;
   };
   return [...compiled].sort((a, b) => rank(a) - rank(b));
}

/**
 * Build the control row for a set of given names, in the order the names
 * arrive. It imposes no order of its own; each caller supplies one.
 */
function buildGivenSpecs(
   names: readonly string[],
   facts: ReportModelFacts,
): ReportGivenSpec[] {
   const declarations = facts.givens;
   const specs: ReportGivenSpec[] = [];
   for (const name of new Set(names)) {
      const declaration = declarations.get(name);
      // Referenced but not imported by the entry file: not bindable, so no
      // control. `lintReport` names it.
      if (!declaration) continue;
      specs.push(givenSpec(declaration, facts));
   }
   return specs;
}

function givenSpec(
   declaration: ReportGivenDeclaration,
   facts: ReportModelFacts,
): ReportGivenSpec {
   const control = readGivenControlSpec(declaration.annotations);
   // Which givens the suggest query must carry to run (a gated or scoped
   // source's). An unresolvable target is left without names; the lint reports it.
   if (control.suggest) {
      const names = suggestGivenNames(control.suggest, facts.suggestGivens);
      if (names) control.suggest.givenNames = names;
   }
   return {
      name: declaration.name,
      type: declaration.type,
      // The body the query endpoint takes, not the `f'…'` literal a filter
      // given is declared as: sent back verbatim, the literal matches zero rows
      // with no error. Keyed on the declared type, since a plain string default
      // that reads `f'x'` must survive untouched. Done here, the one place the
      // published field is produced.
      default:
         declaration.type.startsWith("filter<") &&
         declaration.default !== undefined
            ? unwrapFilterLiteral(declaration.default)
            : declaration.default,
      ...(declaration.required ? { required: true } : {}),
      // The `#(…)` app route only, deliberately narrower than the model surface
      // (`malloyGivenToApi` also carries `#<doc>`, `#[doc]`, `#{doc}`). `#(` is
      // what `GivenInput` reads, and the narrowing is permanent: Credible's app
      // consumes this field from another repo, so widening it reintroduces a
      // bug this repo cannot see. `(markdown)` is cell prose, not control help.
      annotations: declaration.annotations.filter(
         (text) => /^##?\(/.test(text) && !isMarkdownNote(text),
      ),
      ...(declaration.annotations.some((text) => /^#\(secure\)/.test(text)) && {
         secure: true,
      }),
      ...control,
   };
}

/** A compiled file's report tag, by the one reader (`report.ts`) over its own model-level notes. */
export function factsReportTag(facts: ReportModelFacts): ReportTag {
   return readReportTag(facts.modelAnnotations);
}

/**
 * Build the manifest of a report, or undefined when the file is no report
 * (`report.ts`) or a grid tag whose tile list is missing or empty. A linear
 * report always lists its tiles, so it always has one.
 */
export function buildReportManifest(
   facts: ReportModelFacts,
   options?: {
      /** Normalized tile expressions the caller may not read; they come back bare. */
      restrictedTiles?: ReadonlySet<string>;
   },
): ServedReportManifest | undefined {
   const name = reportName(facts.modelPath);
   const base = {
      name,
      entryFile: facts.modelPath,
   };

   const tag = factsReportTag(facts);
   if (tag.status !== "report") return undefined;
   const { layout } = tag;
   const modelTag = motlyTag(facts.modelAnnotations);
   const declaredType = (name: string) => facts.givens.get(name)?.type;
   const composite = modelTag
      ? readArtifactTag(modelTag, declaredType)
      : undefined;
   // A linear report with no tiles yet is still a layout, so the editor can open it.
   if (
      composite?.tiles &&
      (composite.tiles.length > 0 || layout === "linear")
   ) {
      const blocks = readTextBlocks(facts.modelAnnotations);
      const tiles = composite.tiles.map((entry): ReportTileSpec => {
         if (entry.kind === "text") {
            return {
               kind: "text",
               name: entry.name,
               markdown:
                  blocks.find((block) => block.name === entry.name)?.body ?? "",
               ...entryLayoutFields(entry.layout),
            };
         }
         const query = entry.expression;
         // The entry's layout is the report's own, not the gated view's, so a
         // restricted tile keeps its place on the grid.
         if (options?.restrictedTiles?.has(normalizeTileExpression(query))) {
            return {
               kind: "query",
               query,
               ...entryLayoutFields(entry.layout),
               restricted: true,
            };
         }
         const givenNames = resolveTileGivens(query, facts);
         // The view's tags, then the entry's layout over them, property by
         // property: the entry is where this report places the tile, and the
         // view's tags are the default for every report that names it.
         const { break: viewBreak, ...viewLayout } =
            resolveTileLayout(query, facts) ?? {};
         return {
            kind: "query",
            query,
            ...(givenNames ? { givenNames } : {}),
            ...viewLayout,
            ...entryLayoutFields({
               break: viewBreak,
               ...entry.layout,
            }),
            ...entryDrillField(entry.layout),
         };
      });
      const queries = queryTiles(tiles).filter((tile) => !tile.restricted);
      const doc = docCommentTitleAndDescription(
         facts.descriptionNotes ?? facts.modelAnnotations,
         composite.title,
      );
      return {
         ...base,
         layout,
         title: doc.title ?? name,
         description: doc.description,
         tiles,
         dashboardColumns: layout === "linear" ? 1 : composite.dashboardColumns,
         startingGivens: composite.givens,
         autorun: composite.autorun,
         // The union across the tiles. One unresolvable tile (no `givenNames`)
         // widens the row to the file's whole surfaced set: otherwise its givens
         // would have no control and silently filter at their defaults, and a
         // client told to "send the whole control row" needs them in it. Side
         // effect: `lintReport` walks `manifest.givens`, so a widened row can
         // yield a suggest finding for a given no tile references.
         givens: buildGivenSpecs(
            queries.some((tile) => tile.givenNames === undefined)
               ? Array.from(facts.givens.keys())
               : // A given only a gate reads is the host's to inject, not a viewer control.
                 queries.flatMap((tile) =>
                    (referencedTileGivens(tile.query, facts) ?? []).filter(
                       (name) => facts.givens.has(name),
                    ),
                 ),
            facts,
         ),
      };
   }

   return undefined;
}

/**
 * One load-time lint finding, in the shape the package-warnings surface serves.
 * `model` is filled in by the caller, which knows the file.
 */
export interface ReportLintFinding {
   /**
    * What the finding is about: a dashboard slug, a tile, a dimension. For a
    * `# drill` finding, where the tag is declared (not `target`, which here
    * means where a drill points).
    */
   subject: string;
   message: string;
   severity: "error" | "warn";
}

/** A tile written as a plain `source -> view`, or a bare name. */
function plainTileParts(
   tile: string,
): { source: string; view: string } | { name: string } | undefined {
   const piped = /^(\w+)\s*->\s*(\w+)$/.exec(tile.trim());
   if (piped) return { source: piped[1], view: piped[2] };
   const bare = /^\w+$/.exec(tile.trim());
   return bare ? { name: tile.trim() } : undefined;
}

/**
 * Lint one report file, as load-time package warnings: loud where authors see
 * it, instead of dead-ending a viewer at click time.
 *
 * Conservative: a tile can be any runnable Malloy, so only plain
 * `source -> view` and bare-name tiles, which can be proven wrong, are checked.
 * Compile failures surface through the ordinary package load; drill targets
 * are package-wide ({@link lintDrillTargets}).
 */
export function lintReport(
   facts: ReportModelFacts,
   manifest: ServedReportManifest,
): ReportLintFinding[] {
   const findings: ReportLintFinding[] = [];
   const slug = manifest.name;
   const add = (message: string, severity: "error" | "warn" = "warn") =>
      findings.push({ subject: slug, message, severity });

   // The file's own model-level notes: the tag and every line beside it.
   const ownTag = motlyTag(facts.modelAnnotations);
   const artifactTag = ownTag?.tag("artifact");

   // A dropped or refused annotation on a dashboard that still built: every
   // `motlyParseErrors` reason is per line, so a well-formed `## artifact`
   // beside `## note=@env.X` yields a working dashboard missing one line.
   // package.ts runs only one of the two lints, and `lintUndiscoveredReport`
   // covers only files that produced no dashboard, so this path must say it.
   for (const message of new Set(motlyParseErrors(facts.modelAnnotations))) {
      add(
         `Annotation ${describeParseFailure(message)}, so that line had no ` +
            `effect on this dashboard.`,
      );
   }

   // Every property `readArtifactTag` does not read: the reader looks up by
   // name, so a misspelling is silently never asked for. Top level only;
   // `givens` keys are the author's own given names.
   for (const property of Object.keys(artifactTag?.dict ?? {})) {
      if (ARTIFACT_PROPERTIES.includes(property)) continue;
      add(
         `\`${property}\` in the artifact tag does nothing in Publisher` +
            (ARTIFACT_PROPERTY_REPLACEMENTS[property]
               ? `. ${ARTIFACT_PROPERTY_REPLACEMENTS[property]}`
               : `.`),
      );
   }

   // An entry reads its layout and nothing else: `readArtifactTag` drops
   // anything else hung off it, so a property there is silently inert. A
   // tile's label, subtitle, chart and card belong to the view it names.
   const textBlocks = readTextBlocks(facts.modelAnnotations);
   const grid = manifest.layout !== "linear";
   for (const entry of artifactTag?.array("tiles") ?? []) {
      const kind = tagText(entry, "kind");
      const label = tagText(entry) ?? "a tile";
      const readable =
         kind === "text" ? ENTRY_LAYOUT_PROPERTIES : QUERY_ENTRY_PROPERTIES;
      const carried = Object.keys(entry.dict ?? {}).filter(
         (property) =>
            !readable.includes(property) &&
            !(property === "kind" && (kind === "text" || kind === "query")),
      );
      if (carried.length > 0) {
         const named = carried.map((property) => `\`${property}\``).join(", ");
         add(
            `\`${label}\` in \`tiles=[…]\` carries ${named}, which Publisher does not read: ` +
               (kind === "text"
                  ? `a text tile's entry reads only \`row\`, \`col\`, \`colspan\`, \`rowspan\` and \`break\`.`
                  : `a tile's entry reads only \`row\`, \`col\`, \`colspan\`, \`rowspan\`, \`break\` and \`drill\`.`) +
               (kind === "text"
                  ? ""
                  : ` The rest of its presentation goes on the view the tile names ` +
                    `(\`# label\`, \`# subtitle\`, \`# borderless\`).`),
         );
      }
      const tile = kind === "text" ? `text tile \`${label}\`` : `\`${label}\``;
      // A dimension the tile's view does not output never gets a click, so its
      // drill is inert. Checked only on a plain one-stage `source -> view`,
      // whose outputs the model states; any other tile is the reader's to judge.
      if (kind !== "text") {
         const outputs = facts.viewOutputs?.get(normalizeTileExpression(label));
         const named = Object.keys(entry.tag("drill")?.dict ?? {});
         for (const dimension of outputs ? named : []) {
            if (outputs?.has(dimension)) continue;
            add(
               `\`drill\` on ${tile} names \`${dimension}\`, which that view does not output, so no click offers it. ` +
                  `Fix: name one of its fields (${Array.from(outputs ?? []).join(", ")}).`,
            );
         }
      }
      for (const property of ["row", "col", "colspan", "rowspan"]) {
         if (
            entry.has(property) &&
            positiveInteger(tagNumeric(entry, property)) === undefined
         ) {
            add(
               `${property} on ${tile} in \`tiles=[…]\` must be a positive integer, so it is ignored.`,
            );
         }
      }
      // A linear report's entry layout is `notebook_lint`'s to report.
      const colspan = positiveInteger(tagNumeric(entry, "colspan"));
      if (grid && colspan !== undefined) {
         const columns = manifest.dashboardColumns;
         if (columns === undefined) {
            add(
               `colspan=${colspan} on ${tile} needs a grid to span. This ` +
                  `report declares no # dashboard { columns=N }, so it lays out ` +
                  `at the viewer's default width and every colspan is clamped to it.`,
            );
         } else if (colspan > columns) {
            add(
               `colspan=${colspan} on ${tile} exceeds the ${columns} columns ` +
                  `of this report and is clamped to ${columns}. Widen ` +
                  `# dashboard { columns=… } or lower the colspan.`,
            );
         }
      }
      if (kind !== "text") continue;
      if (!textBlocks.some((block) => block.name === label)) {
         add(
            `text tile \`${label}\` has no \`##|(markdown) ${label}\` block, so it shows nothing. ` +
               `Fix: write that block, with the text on the lines below its opener and \`|##\` after.`,
         );
      }
   }
   // A placed tile that runs past the grid's right edge, or covers another:
   // the viewer draws both anyway, the first spilling into an extra column
   // and the second stacked on its neighbour, so neither is what was meant.
   if (grid) {
      const columns = manifest.dashboardColumns;
      const placed = (manifest.tiles ?? []).flatMap((tile) => {
         if (tile.row === undefined || tile.col === undefined) return [];
         const name = tile.kind === "text" ? tile.name : tile.query;
         const colspan = Math.min(tile.colspan ?? 1, columns ?? Infinity);
         const rowspan =
            tile.rowspan ??
            PLACED_DEFAULT_ROWSPAN[tile.kind === "text" ? "text" : "query"];
         return [{ name, row: tile.row, col: tile.col, colspan, rowspan }];
      });
      for (const tile of placed) {
         if (columns !== undefined && tile.col + tile.colspan - 1 > columns) {
            add(
               `\`${tile.name}\` at col=${tile.col} with colspan=${tile.colspan} runs past the ` +
                  `${columns} columns of this report. Move it left or narrow it.`,
            );
         }
      }
      for (let a = 0; a < placed.length; a++) {
         for (let b = a + 1; b < placed.length; b++) {
            const [x, y] = [placed[a], placed[b]];
            const overlaps =
               x.col < y.col + y.colspan &&
               y.col < x.col + x.colspan &&
               x.row < y.row + y.rowspan &&
               y.row < x.row + x.rowspan;
            if (overlaps) {
               add(
                  `\`${x.name}\` and \`${y.name}\` are placed over each other, so one is ` +
                     `drawn on top of the other. Move one, or change a colspan or rowspan.`,
               );
            }
         }
      }
   }

   // A tile whose entry sets its width is not spanned by its view's
   // `# colspan`, so the view's tag is not checked against this grid.
   const entrySpanned = new Set(
      (artifactTag?.array("tiles") ?? []).flatMap((entry) => {
         const text = tagText(entry);
         return text !== undefined &&
            tagText(entry, "kind") !== "text" &&
            entry.has("colspan")
            ? [normalizeTileExpression(text)]
            : [];
      }),
   );
   const seenBlocks = new Set<string>();
   for (const { name } of textBlocks) {
      if (seenBlocks.has(name)) {
         add(
            `the \`##|(markdown) ${name}\` block is written twice, and only the first is shown. Fix: rename or delete the second.`,
         );
      }
      seenBlocks.add(name);
   }

   // Per-tile colspan, from the view a tile names: a value the reader dropped,
   // or one wider than the grid (rendered clamped). A notebook is one column,
   // so no span means anything on it.
   for (const { query: tile } of manifest.layout === "linear"
      ? []
      : queryTiles(manifest.tiles)) {
      if (entrySpanned.has(normalizeTileExpression(tile))) continue;
      const tag = motlyTag(tileAnnotations(tile, facts) ?? []);
      if (!tag?.has("colspan")) continue;
      const colspan = positiveInteger(tagNumeric(tag, "colspan"));
      if (colspan === undefined) {
         const raw = tagText(tag, "colspan");
         add(
            `# colspan on "${tile}" must be a positive integer, got ` +
               `${raw === undefined ? "a value that could not be read" : JSON.stringify(raw)}. ` +
               `The tile spans one column.`,
         );
         continue;
      }
      const columns = manifest.dashboardColumns;
      // Undeclared is the likelier loss: the viewer clamps every colspan to its
      // narrow default. The default is the viewer's, so it is not named.
      if (columns === undefined) {
         add(
            `# colspan=${colspan} on "${tile}" needs a grid to span. This ` +
               `dashboard declares no # dashboard { columns=N }, so it lays out ` +
               `at the viewer's default width and every colspan is clamped to it.`,
         );
      } else if (colspan > columns) {
         add(
            `# colspan=${colspan} on "${tile}" exceeds the ${columns} columns ` +
               `of this dashboard and is clamped to ${columns}. Widen ` +
               `# dashboard { columns=… } or lower the colspan.`,
         );
      }
   }

   // A bad grid width is invisible in the manifest (`positiveInteger` drops it
   // there too), so it needs saying aloud.
   const columnsTag = ownTag?.tag("dashboard");
   if (columnsTag?.has("columns")) {
      const width = positiveInteger(tagNumeric(columnsTag, "columns"));
      if (width === undefined) {
         // `tagText` is undefined for a bad literal; never print "undefined".
         const raw = tagText(columnsTag, "columns");
         add(
            `# dashboard { columns=… } must be a positive integer, got ` +
               `${raw === undefined ? "a value that could not be read" : JSON.stringify(raw)}. ` +
               `The grid falls back to the default.`,
         );
      } else if (width > MAX_BUILDER_COLUMNS) {
         add(
            `# dashboard { columns=${width} } is wider than the builder offers (${MAX_BUILDER_COLUMNS}); the reader still renders it.`,
         );
      }
   }

   for (const { query: tile } of queryTiles(manifest.tiles)) {
      const parts = plainTileParts(tile);
      if (!parts) continue;
      const resolved =
         referencedTileGivens(tile, facts) !== undefined ||
         ("name" in parts &&
            facts.queries.some((query) => query.name === parts.name));
      if (
         !resolved &&
         "name" in parts &&
         textBlocks.some((block) => block.name === parts.name)
      ) {
         add(
            `tile "${tile}" names the ##|(markdown) ${tile} block but has no kind=text, so it is read as a query. Fix: write ${tile} { kind=text }`,
            "error",
         );
         continue;
      }
      if (!resolved) {
         const detail =
            "source" in parts
               ? facts.sourceFields.has(parts.source)
                  ? `source "${parts.source}" has no view "${parts.view}"`
                  : `no source "${parts.source}" in this file`
               : `no query named "${parts.name}" in this file`;
         add(`tile "${tile}" does not resolve: ${detail}.`, "error");
      }

      // A given reached only through an import chain has no control and stays
      // at its default; bindability is per-file. (A compiled query can read one
      // given twice, hence the Set.)
      const unbindable = Array.from(
         new Set(referencedTileGivens(tile, facts) ?? []),
      ).filter((name) => !facts.givens.has(name));
      for (const name of unbindable) {
         add(
            `tile "${tile}" filters by given "${name}", which this file does ` +
               `not import, so no control is shown for it and it stays at its ` +
               `default. Add it to an import in ${facts.modelPath}.`,
         );
      }
   }

   // A starting value (`## artifact { givens { … } }`) for a given this file
   // cannot bind: `filterGivensToModelSurface` drops it at query time and no
   // control shows, silently. Reported but kept in the manifest, so a consumer
   // can still see what the author wrote.
   for (const name of Object.keys(manifest.startingGivens ?? {})) {
      if (facts.givens.has(name)) continue;
      add(
         `'artifact { givens { ${name}=… } }' sets a starting value for given ` +
            `"${name}", which this file does not import, so the value is ` +
            `dropped at query time and the dashboard opens at the ` +
            `declaration's default. Add it to an import in ${facts.modelPath}.`,
      );
   }

   for (const spec of manifest.givens) {
      const suggest = spec.suggest;
      if (!suggest) {
         // `readGivenControlSpec` silently drops a `suggest` that cannot fetch
         // options, so read the declaration back to say what was dropped.
         const declared = motlyTag(
            facts.givens.get(spec.name)?.annotations ?? [],
         )?.tag("suggest");
         if (declared) {
            const has = (key: string) => tagText(declared, key) !== undefined;
            const missing = has("dimension")
               ? `it names a dimension but no query= or source= to read it from`
               : has("source")
                 ? `source= alone names no column to read options from`
                 : `it names neither a query= nor a source=`;
            add(
               `given "${spec.name}" declares a suggest that cannot fetch ` +
                  `options: ${missing}. Use query=, source= with dimension=, ` +
                  `or query= with dimension=. No control options are offered ` +
                  `for it.`,
               "error",
            );
         }
         continue;
      }
      if (suggest.query !== undefined) {
         const named = facts.queries.find(
            (query) => query.name === suggest.query,
         );
         if (!named) {
            add(
               `given "${spec.name}" suggests options from query ` +
                  `"${suggest.query}", which this file does not define.`,
               "error",
            );
         } else if (
            named.sourceName !== undefined &&
            !facts.sourceFields.has(named.sourceName)
         ) {
            // Imports are not transitive: importing the suggest query alone
            // compiles and loads clean, but the picker answers "Undefined
            // source" the first time a reader opens it.
            add(
               `given "${spec.name}" suggests options from query ` +
                  `"${suggest.query}", which reads source ` +
                  `"${named.sourceName}" that this file does not import. The ` +
                  `query resolves by name, so the package loads, but the ` +
                  `control answers "Undefined source '${named.sourceName}'" ` +
                  `and offers no options. Import that source here too.`,
               "error",
            );
         }
      }
      if (suggest.source !== undefined) {
         const fields = facts.sourceFields.get(suggest.source);
         if (!fields) {
            add(
               `given "${spec.name}" suggests options from source ` +
                  `"${suggest.source}", which this file does not define.`,
               "error",
            );
         } else if (
            suggest.dimension !== undefined &&
            !fields.has(suggest.dimension)
         ) {
            add(
               `given "${spec.name}" suggests options from ` +
                  `"${suggest.source} -> ${suggest.dimension}", but that ` +
                  `source has no field "${suggest.dimension}".`,
               "error",
            );
         }
      }
   }

   findings.push(...lintMissingSharedFilter(facts, manifest));
   return findings;
}

/**
 * A filter most of a report's tiles apply, missing from one of them.
 *
 * A report's filter is a given its tiles bind one by one, each in its own
 * `+ { where: … }`. A tile that leaves it out is not filtered by the control
 * while the rest of the page is, and nothing on the page says so. Narrow on
 * purpose: tiles are compared only with tiles reading the same source (a tile
 * over another source may have no field the filter could compare), only for a
 * given the report can bind, read by at least two of them and by more than
 * half, and only where every tile's reads are known.
 */
export function lintMissingSharedFilter(
   facts: ReportModelFacts,
   manifest: ServedReportManifest,
): ReportLintFinding[] {
   const sourceOf = (query: string) => {
      const [left, right] = normalizeTileExpression(query).split(" -> ");
      return right !== undefined
         ? left
         : facts.queries.find((q) => q.name === left)?.sourceName;
   };
   const bySource = new Map<
      string,
      Array<{ query: string; reads: string[] }>
   >();
   for (const { query } of queryTiles(manifest.tiles)) {
      const source = sourceOf(query);
      const reads = referencedTileGivens(query, facts);
      if (source === undefined || reads === undefined) continue;
      bySource.set(source, [...(bySource.get(source) ?? []), { query, reads }]);
   }
   const findings: ReportLintFinding[] = [];
   for (const [source, tiles] of bySource) {
      const counts = new Map<string, number>();
      for (const { reads } of tiles)
         for (const name of new Set(reads))
            if (facts.givens.has(name))
               counts.set(name, (counts.get(name) ?? 0) + 1);
      for (const [given, count] of counts) {
         if (count < 2 || count * 2 <= tiles.length) continue;
         for (const { query, reads } of tiles) {
            if (reads.includes(given)) continue;
            findings.push({
               subject: manifest.name,
               severity: "warn",
               message:
                  `"${query}" does not filter on $${given}, which ${count} of ` +
                  `this report's ${tiles.length} tiles over "${source}" do, so ` +
                  `that control leaves it unfiltered. If it should follow the ` +
                  `control, bind it in its query: ` +
                  `\`+ { where: <field> ~ $${given} }\`.`,
            });
         }
      }
   }
   return findings;
}

/**
 * How a `motlyParseErrors` message should be introduced to an author. Two of
 * them are policy refusals of a well-formed line, and "does not parse" would
 * send the author hunting a syntax error that is not there. Every call site
 * goes through here so none is left behind when the set grows.
 */
function describeParseFailure(message: string): string {
   if (message === ENV_REFERENCE_DROPPED) {
      return `was dropped rather than parsed (${message}), so nothing on that line took effect`;
   }
   if (message === ANNOTATION_TOO_LONG) {
      return `was refused rather than parsed (${message})`;
   }
   return `does not parse (${message})`;
}

const TAG_DISCARDED =
   "so the whole tag is discarded and this file is treated as a shared " +
   "include rather than a dashboard.";

const TAG_UNPARSED_PREFIX = "Tag ";

/** The one place {@link lintUndiscoveredReport}'s unparsed-tag message is
 *  built, so {@link isUnparsedReportTagFinding} reads the same pieces. */
function unparsedTagMessage(parseError: string): string {
   return `${TAG_UNPARSED_PREFIX}${describeParseFailure(parseError)}, ${TAG_DISCARDED}`;
}

/**
 * Whether a finding is {@link lintUndiscoveredReport}'s report of a tag that
 * does not parse, which the notebook lint also reports. Read from the message
 * because package warnings carry no code; it matches the constants
 * {@link unparsedTagMessage} builds from.
 */
export function isUnparsedReportTagFinding(message: string): boolean {
   return (
      message.startsWith(TAG_UNPARSED_PREFIX) && message.endsWith(TAG_DISCARDED)
   );
}

/**
 * Explain a tagged `.malloy` that produced no report. A file with no artifact
 * tag is a shared include and silent, but MOTLY discards a whole tag on a
 * syntax error, so a report would otherwise vanish from every listing with
 * nothing said.
 */
export function lintUndiscoveredReport(
   facts: ReportModelFacts,
): ReportLintFinding[] {
   const subject = reportName(facts.modelPath);
   // Deduplicated on the final message: every finding shares one subject, so
   // two tags failing the same way would be byte-identical.
   const messages = motlyParseErrors(facts.modelAnnotations).map(
      unparsedTagMessage,
   );
   const findings: ReportLintFinding[] = Array.from(
      new Set(messages),
      (message) => ({ subject, message, severity: "error" as const }),
   );
   if (findings.length > 0) return findings;

   // A tag that parses but describes no dashboard vanishes just as completely:
   // a grid `## artifact` whose `tiles=` is missing or empty. (A removed
   // `kind=` is the text lint's to report.)
   const modelArtifact = motlyTag(facts.modelAnnotations)?.tag("artifact");
   if (modelArtifact && factsReportTag(facts).status === "report") {
      findings.push({
         subject,
         message:
            `${describeTilelessModelArtifact(modelArtifact)}, so this file ` +
            `produces no dashboard. ${NAME_THE_TILES}`,
         severity: "error" as const,
      });
   }
   findings.push(...lintArtifactOnView(facts));
   return findings;
}

/**
 * Report an `# artifact` tag on a source view, which Malloyyo accepts and
 * Publisher does not read (model level only), so the file would vanish as a
 * shared include with nothing said. Only for a file that produced no
 * dashboard. `viewAnnotations` spans the compile closure, so an include that
 * imports such a view is told too; one finding per importer beats none.
 */
function lintArtifactOnView(facts: ReportModelFacts): ReportLintFinding[] {
   const subject = reportName(facts.modelPath);
   const findings: ReportLintFinding[] = [];
   for (const [view, annotations] of facts.viewAnnotations) {
      if (!motlyTag(annotations)?.tag("artifact")) continue;
      findings.push({
         subject,
         message:
            `'# artifact' on view '${view}' is not read: Publisher reads the ` +
            `tag at model level only, so this file produces no report. Name ` +
            `the view as a tile: ` +
            `## artifact { layout=grid tiles=["${view}"] }.`,
         severity: "error" as const,
      });
   }
   return findings;
}

/**
/** What is wrong with a model-level `## artifact` that builds no report. */
function describeTilelessModelArtifact(modelArtifact: Tag): string {
   return modelArtifact.array("tiles") === undefined
      ? `Model-level '## artifact' declares no tiles=`
      : `Model-level '## artifact' declares an empty tiles=`;
}

const NAME_THE_TILES = `Name the views to show: tiles=["source -> view", …].`;

/**
 * Report a given whose own annotations do not parse as MOTLY: the only signal
 * for it, since a failed parse yields an empty tag and `readGivenControlSpec`
 * silently returns `{}` (no label, control, range or suggest).
 *
 * Package-wide and deduplicated on name and message: a given is reached
 * through imports, so per-report would repeat it per importer, or miss it.
 * Syntax only ({@link motlyParseErrors}); no finding is not proof of a
 * well-formed tag.
 */
export function lintGivenTags(
   facts: readonly ReportModelFacts[],
): ReportLintFinding[] {
   const findings = new Map<string, ReportLintFinding>();
   for (const file of facts) {
      for (const [name, declaration] of file.givens) {
         for (const message of motlyParseErrors(declaration.annotations)) {
            findings.set(`${name}|${message}`, {
               subject: name,
               message:
                  `given "${name}" has an annotation that ` +
                  `${describeParseFailure(message)}, so the whole line is ` +
                  `discarded and the given ` +
                  `loses any label, control, range or suggest it declared. ` +
                  `It still accepts values; only its presentation is lost.`,
               severity: "error",
            });
         }
      }
   }
   return Array.from(findings.values());
}

/**
 * How a `# drill { to=… }` destination resolves against the package's reports
 * (`docs/reports-design.md` §3). A report is identified by its package path,
 * and `to=` names that path from the package root, with or without `.malloy`:
 * `to="sales/overview"` is `sales/overview.malloy`, of either layout, and a
 * bare `to="overview"` is the root file `overview.malloy`. A basename is never
 * looked up elsewhere.
 *
 * A target that is not a plain package path ({@link isDrillTargetPath}) is
 * unresolved before any lookup: the Console would otherwise build a route that
 * leaves the package (`../../x`). The SDK's `resolveDrillTarget` refuses the
 * same shapes.
 */
export type DrillResolution =
   | { kind: "self" }
   | { kind: "path"; path: string }
   | { kind: "unresolved"; malformed?: true };

/**
 * Whether `to=` is a plain package path: non-empty segments, none `.` or `..`,
 * and no leading `/`. Anything else could name a route outside the package.
 */
export function isDrillTargetPath(target: string): boolean {
   if (target === "" || target.startsWith("/")) return false;
   return target
      .split("/")
      .every(
         (segment) => segment !== "" && segment !== "." && segment !== "..",
      );
}

export function resolveDrillDestination(
   destination: string,
   reportPaths: ReadonlySet<string>,
): DrillResolution {
   if (destination === "self") return { kind: "self" };
   if (!isDrillTargetPath(destination))
      return { kind: "unresolved", malformed: true };
   for (const candidate of [`${destination}.malloy`, destination]) {
      if (reportPaths.has(candidate)) return { kind: "path", path: candidate };
   }
   return { kind: "unresolved" };
}

/**
 * Validate every `# drill { to=… }` in the package against the reports that
 * exist, so a dead-ended click is caught at load. Package-wide and keyed on
 * dimension and destination, since a tagged dimension is reachable from every
 * importer. `reportPaths` includes reports that could not be served (their
 * own finding states why).
 */
export function lintDrillTargets(
   facts: readonly ReportModelFacts[],
   reportPaths: ReadonlySet<string>,
): ReportLintFinding[] {
   const findings = new Map<string, ReportLintFinding>();
   for (const file of facts) {
      for (const drill of file.drills) {
         const where = drillSubject(drill);
         const what = drillPhrase(drill);
         if (drill.to.length === 0) {
            findings.set(where, {
               subject: where,
               message: `${what} names no destination (to=…).`,
               severity: "error",
            });
            continue;
         }
         for (const destination of drill.to) {
            const resolved = resolveDrillDestination(destination, reportPaths);
            if (resolved.kind !== "unresolved") continue;
            findings.set(`${where}|${destination}`, {
               subject: where,
               message: resolved.malformed
                  ? `${what} targets "${destination}", which is not ` +
                    `a package path (a path from the package root with no "." ` +
                    `or ".." segment, no empty segment and no leading "/"), so ` +
                    `it resolves to no report. Fix: write the report's path ` +
                    `from the package root, such as to="sales/overview".`
                  : `${what} targets "${destination}", which is not ` +
                    `a report in this package.`,
               severity: "error",
            });
         }
      }
   }
   return Array.from(findings.values());
}

/**
 * The given a `# drill` seeds, by the same rule as the SDK runtime
 * (`packages/sdk/src/components/drill/resolveDrill.ts`): the `given=` or the
 * field name verbatim. A copy only because the server cannot import the SDK;
 * if they disagree the resolver wins. A wrong copy is worse than no lint: a
 * green lint tells the author the drill is wired.
 */
function drillGivenName(drill: ReportDrill): string {
   return drill.given ?? drill.dimension;
}

/** Where a drill is declared, as a finding's subject: `source.dimension`, or the dimension on a tile. */
function drillSubject(drill: ReportDrill): string {
   return drill.tile === undefined
      ? `${drill.source}.${drill.dimension}`
      : `${drill.dimension} on ${drill.tile}`;
}

/** The drill as a finding's sentence names it. */
function drillPhrase(drill: ReportDrill): string {
   return drill.tile === undefined
      ? `# drill on ${drill.source}.${drill.dimension}`
      : `The \`drill\` for \`${drill.dimension}\` on tile \`${drill.tile}\``;
}

/**
 * Validate every `# drill { to=self }` against the givens the package's
 * documents actually surface.
 *
 * `to=self` needs a control in the clicked document, which is per-document: a
 * given one report surfaces another may not, by choice. So only the provable
 * half is reported, a given no model in the package declares, which leaves the
 * tag dead everywhere. The per-document case is a click-time console warning.
 */
export function lintSelfDrills(
   facts: readonly ReportModelFacts[],
): ReportLintFinding[] {
   // Case-folded, because the SDK viewers resolve drill givens against a
   // folded map (`Dashboard.tsx`, `givenNamesByFold`): `region` against a
   // declared `REGION` works and must not be reported. If the viewers stop
   // folding, this goes back with them.
   const surfaced = new Set<string>();
   for (const file of facts) {
      for (const name of file.givens.keys()) surfaced.add(name.toLowerCase());
   }

   const findings = new Map<string, ReportLintFinding>();
   for (const file of facts) {
      for (const drill of file.drills) {
         if (!drill.to.includes("self")) continue;
         const given = drillGivenName(drill);
         if (surfaced.has(given.toLowerCase())) continue;
         const where = drillSubject(drill);
         findings.set(where, {
            subject: where,
            message:
               `${drillPhrase(drill)} has to=self, but no model in this ` +
               `package declares a given "${given}" for the clicked value to ` +
               `filter on, so the drill cannot fire anywhere. Declare it, or ` +
               `point the tag at a given that exists with given=.`,
            severity: "error",
         });
      }
   }
   return Array.from(findings.values());
}

/**
 * What each tile's COMPILED query reads, keyed by normalized tile expression.
 * A tile that does not compile is left out and keeps the static answer; one in
 * `skip` is never compiled, so a caller who may not read it gets no diagnostic.
 * `onPrepared` sees each compiled tile, for a caller that gates on it.
 */
export async function compileTileGivens(
   tiles: readonly string[],
   materializer: {
      loadQuery(text: string): { getPreparedQuery(): Promise<unknown> };
   },
   fallbackRegistry: ModelDef["givens"] | undefined,
   gatesOf: (sourceName: string) => readonly string[] | undefined,
   skip?: ReadonlySet<string>,
   onPrepared?: (tile: string, prepared: unknown) => Promise<void>,
   onCompileError?: (tile: string, error: unknown) => void,
): Promise<Map<string, CompiledTileGivens>> {
   const compiled = new Map<string, CompiledTileGivens>();
   for (const tile of tiles) {
      const key = normalizeTileExpression(tile);
      if (compiled.has(key) || skip?.has(key)) continue;
      let prepared: {
         _query?: { givenUsage?: { id: string }[]; structRef?: unknown };
         _modelDef?: ModelDef;
      };
      try {
         prepared = (await materializer
            .loadQuery(`run: ${tile}`)
            .getPreparedQuery()) as typeof prepared;
      } catch (error) {
         onCompileError?.(tile, error);
         continue;
      }
      await onPrepared?.(tile, prepared);
      const usage = prepared._query?.givenUsage;
      if (!usage) continue;
      const registry = prepared._modelDef?.givens ?? fallbackRegistry ?? {};
      const reads = usage
         .map((given) => registry[given.id]?.name)
         .filter((name): name is string => name !== undefined);
      const target = prepared._query?.structRef;
      const sourceName =
         typeof target === "string"
            ? target
            : (target as { as?: string; name?: string } | undefined)?.as ||
              (target as { name?: string } | undefined)?.name;
      const gateReads = new Set<string>();
      for (const expr of sourceName ? (gatesOf(sourceName) ?? []) : []) {
         for (const name of referencedGivenNames(expr)) gateReads.add(name);
      }
      compiled.set(key, { reads, gateReads: Array.from(gateReads) });
   }
   return compiled;
}

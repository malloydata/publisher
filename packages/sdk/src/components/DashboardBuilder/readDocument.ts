// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   chartLinesOf,
   chartStateOfTagLines,
   type ChartState,
} from "./chartLine";
import type {
   DashboardDocument,
   DashboardDrill,
   DashboardImport,
   DashboardSource,
   DashboardTile,
   LocalGiven,
} from "./document";
import { isTextTile } from "./document";
import { parseTagLines } from "./tagParse";
import {
   parseMalloy,
   parseRefused,
   readTileList,
   type ParsedMalloy,
   type TreeStage,
   type TreeView,
} from "./malloyTree";
import {
   ARTIFACT_NOT_FIRST,
   artifactLeads,
   artifactTag as locateArtifactTag,
   descriptionNotes,
   readPath,
   splitSourceLines,
   tagAnnotation,
   tileSteps,
} from "./malloyText";

/**
 * Read a `dashboards/*.malloy` file into a {@link DashboardDocument}.
 *
 * PURE: source text in, document out. No server, no schema, no connection.
 * `Malloy.parse` needs none of those — only `Malloy.compile` does — so this runs
 * in the browser and in a unit test alike, which is what lets the suite open
 * every dashboard in the repository as a regression gate.
 *
 * Structure comes from Malloy's own parse tree, through `malloyTree`: imports,
 * sources, views, dimensions, givens and every `where:` clause, each with an
 * exact span. Tags come from `parseAnnotation` over the `#` block above a
 * declaration, which is `malloy-tag`'s grammar rather than Malloy's.
 *
 * Reading tags from the text rather than from the server, which serves them
 * already attached, is deliberate. The splice writer has to LOCATE a tag in
 * order to edit it, so the scan exists either way; taking the values from the
 * same scan keeps one source of truth for them and keeps this function pure.
 *
 * ALL-OR-NOTHING: a file this cannot fully represent is refused rather than
 * half-opened. The bar is lower than it sounds because the document is an
 * editing projection — unmodelled Malloy survives a splice untouched, so it only
 * blocks when it would make an edit unsafe.
 */

/** Why a file could not be opened, in terms its author can act on. */
export interface ReadFailure {
   ok: false;
   reason: string;
   /** 1-based, for a message that can point at the line. */
   line?: number;
   /** The file is a `kind=notebook` in the run-cell format, which `convertLegacyNotebook` turns into a layout notebook. */
   legacyNotebook?: true;
}

export type ReadResult =
   | { ok: true; document: DashboardDocument }
   | ReadFailure;

/**
 * Narrow a result to its failure arm.
 *
 * A guard rather than `if (!result.ok)`, because this package compiles with
 * `strict: false` — the tsconfig says why, and it is not ours to change — and
 * without `strictNullChecks` TypeScript does not narrow a discriminated union
 * through a negated discriminant. The failure is quiet: `result.reason` simply
 * does not typecheck, in a codebase where most code never notices.
 */
export const readFailed = (result: ReadResult): result is ReadFailure =>
   result.ok === false;

/** A `#`/comment block above a declaration, and where it sits. */
export interface Block {
   /** 0-based line of the first line in the block. */
   start: number;
   /**
    * The `#`-prefixed lines only, in order, WITH their line numbers. The writer
    * needs the numbers to patch a tag in place; the reader only needs the text.
    */
   tags: Array<{ line: number; text: string }>;
   /** The `(markdown)` annotation lines in the block, which are not tags but leave with the declaration they describe. */
   prose: number[];
}

/**
 * The comment-and-tag block immediately above `declLine`, up to the blank line
 * that is the author's own separator. Validated against the bundled dashboard,
 * where it collects `revenue_trend`'s three tags and the fourteen-line comment
 * explaining its colspan, then stops at the blank line above — which is the
 * right unit to carry when that tile moves.
 *
 * WHERE the block starts comes from `parsed`, which takes its comments from the
 * lexer, and not from a scan for `//` here. Malloy spells a comment three ways
 * — `//`, `--` and `/* … *\/` — and this walked upward looking only for the
 * first. The other two stopped it early while the parser read straight past
 * them, so a `#` tag above one was visible to the reader and invisible to the
 * writer, which then wrote a SECOND copy of the tag below the comment. The
 * reader picks the lower one up and the read-back gate is satisfied, so the
 * file quietly ends up carrying two.
 */
export function blockAbove(
   parsed: ParsedMalloy,
   lines: string[],
   declLine: number,
): Block {
   const start = parsed.blockStart(declLine);
   const tags: Array<{ line: number; text: string }> = [];
   const prose: number[] = [];
   for (let i = start; i < declLine; i++) {
      // Inside a `/* … */`, where a line beginning `#` is prose. Rewriting one
      // would put an edit inside a comment, and `(markdown)` text there is no annotation.
      if (parsed.commentLine(i)) continue;
      if (parsed.proseLine(i)) {
         prose.push(i);
         continue;
      }
      const text = lines[i].trim();
      // `##` at this indent level is a MODEL annotation and never belongs to a
      // declaration; only single-`#` object tags do.
      if (text.startsWith("#") && !text.startsWith("##"))
         tags.push({ line: i, text });
   }
   return { start, tags, prose };
}

/** Just the text of a block's tags, which is what `parseAnnotation` takes. */
const tagText = (tags: Array<{ text: string }>) => tags.map((t) => t.text);

/** The tile's `chart` property, absent when its wrapper carries no chart line; a custom one also carries the lines. */
function chartField(tags: Array<{ text: string }>): {
   chart?: ChartState;
   chartLines?: string[];
} {
   const lines = tagText(tags);
   const chart = chartStateOfTagLines(lines);
   if (chart === undefined) return {};
   return chart === "custom"
      ? { chart, chartLines: chartLinesOf(lines) }
      : { chart };
}

/**
 * The builder-managed filters of a `{ … }` block: every clause the tree says
 * is exactly `field <op> $GIVEN`.
 *
 * A clause that is anything else — a compound predicate, a literal comparison
 * — is not a binding and is left exactly as written. That is a STRUCTURAL
 * test now, so the isolation heuristics this used to need are gone along with
 * the shapes that defeated them.
 */
function filtersOf(
   stage: TreeStage | undefined,
): Array<{ field: string; given: string; op?: string }> | undefined {
   if (!stage) return undefined;
   const out: Array<{ field: string; given: string; op?: string }> = [];
   for (const where of stage.wheres)
      for (const clause of where.clauses) {
         if (!clause.binding) continue;
         const { field, op, given } = clause.binding;
         out.push({
            field: readPath(field),
            given,
            ...(op === "~" ? {} : { op }),
         });
      }
   return out.length > 0 ? out : undefined;
}

/**
 * A tag object, as `parseAnnotation` returns it. Only the reads this file makes.
 */
type TagLike = {
   text: (key: string) => string | undefined;
   numeric: (key: string) => number | undefined;
   tag: (key: string) => TagLike | undefined;
};
type ParseTags = (lines: string[]) => { tag?: TagLike | null };

/**
 * `given:` declarations, in BOTH spellings Malloy accepts:
 *
 *     given: CATEGORY :: filter<string> is f''      // one per line
 *
 *     given:                                        // a block
 *       CATEGORY :: filter<string> is f''
 *       SINCE :: date is @2023-01-01
 *
 * Either way the `#` tags above a declaration are its control contract.
 */
export function localGivens(
   parsed: ParsedMalloy,
   lines: string[],
   parse: ParseTags,
): LocalGiven[] | undefined {
   const out: LocalGiven[] = [];
   for (const given of parsed.givens) {
      const m = /^([A-Za-z_][A-Za-z0-9_]*)\s*::\s*(\S+)\s+is\s+([\s\S]+)$/.exec(
         given.declaration,
      );
      if (!m) continue;
      out.push({
         name: m[1],
         type: m[2],
         default: m[3].trim(),
         ...readControlTags(
            parse(tagText(blockAbove(parsed, lines, given.line).tags)).tag,
         ),
      });
   }
   return out.length > 0 ? out : undefined;
}

/** The control contract off a given's tags; see {@link LocalGiven}. */
function readControlTags(tag: TagLike | null | undefined): Partial<LocalGiven> {
   if (!tag) return {};
   const suggest = tag.tag("suggest");
   const dimension = suggest?.text("dimension");
   const rangeMin = tag.numeric("range_min");
   const rangeMax = tag.numeric("range_max");
   return {
      ...(tag.text("label") === undefined ? {} : { label: tag.text("label") }),
      ...(tag.text("description") === undefined
         ? {}
         : { description: tag.text("description") }),
      ...(tag.text("control") === undefined
         ? {}
         : { control: tag.text("control") }),
      ...(suggest && dimension
         ? {
              suggest: {
                 ...(suggest.text("source") === undefined
                    ? {}
                    : { source: suggest.text("source") }),
                 ...(suggest.text("query") === undefined
                    ? {}
                    : { query: suggest.text("query") }),
                 dimension,
              },
           }
         : {}),
      ...(rangeMin === undefined ? {} : { rangeMin }),
      ...(rangeMax === undefined ? {} : { rangeMax }),
   };
}

/** A grid width: a plain decimal positive integer, else undefined. */
function gridWidth(
   tag: { text(key: string): string | undefined } | undefined,
   key: string,
): number | undefined {
   const raw = tag?.text(key)?.trim();
   if (
      raw === undefined ||
      !/^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$/.test(raw)
   )
      return undefined;
   const value = Number(raw);
   return Number.isInteger(value) && value >= 1 ? value : undefined;
}

const QUOTED_ENTRY = /^"((?:[^"\\]|\\.)+)"$/;
const TEXT_ENTRY = /^([A-Za-z_][A-Za-z0-9_]*)\s*\{/;

/** Refuses rather than throws: `Tag.text()` throws on a malformed date literal such as `@2024-13-01`, and dropping the value would lose it on the next save. */
export async function readDashboardDocument(
   sourceText: string,
   modelPath?: string,
): Promise<ReadResult> {
   try {
      return await readDocumentText(sourceText, modelPath);
   } catch (error) {
      return {
         ok: false,
         reason: `A value in this file's tags cannot be read (${error instanceof Error ? error.message : String(error)}), so it cannot be opened in the builder.`,
      };
   }
}

async function readDocumentText(
   sourceText: string,
   modelPath: string | undefined,
): Promise<ReadResult> {
   const { parseAnnotation } = await import("@malloydata/malloy-tag");
   const lines = splitSourceLines(sourceText);

   const parse = await parseMalloy(sourceText);
   if (parseRefused(parse))
      return {
         ok: false,
         reason: parse.reason,
         ...(parse.line ? { line: parse.line } : {}),
      };
   const parsed = parse.parsed;

   const description = descriptionNotes(lines).text;
   const artifactAt = locateArtifactTag(lines);
   if (artifactAt === undefined) {
      return {
         ok: false,
         reason:
            "No `## artifact { … }` tag, so this file is not a composite dashboard.",
      };
   }
   if (!artifactLeads(artifactAt.text))
      return {
         ok: false,
         reason: ARTIFACT_NOT_FIRST,
         line: artifactAt.from + 1,
      };

   const { tag, errors: tagErrors } = parseTagLines(parseAnnotation, [
      tagAnnotation(artifactAt.text),
   ]);
   const artifactTag = tag?.tag("artifact");
   const tagKind = artifactTag?.text("kind");
   // The server's rule: a tag that names no kind takes the folder's.
   const kind =
      tagKind === "notebook" ||
      (tagKind !== "dashboard" && modelPath?.startsWith("notebooks/"))
         ? ("notebook" as const)
         : undefined;
   const list = readTileList(artifactAt.text);
   if (list === undefined) {
      if (tagErrors.length > 0)
         return {
            ok: false,
            reason: `The \`## artifact\` tag does not parse: ${tagErrors[0]}`,
         };
      // A notebook is told from a layout one by whether it lists tiles at all.
      if (kind === "notebook")
         return {
            ok: false,
            legacyNotebook: true,
            reason:
               "This notebook is in the cell format, with no `tiles=[…]` list.",
         };
      return {
         ok: false,
         reason:
            "The `## artifact` tag has no `tiles=[…]` list the builder can read.",
      };
   }

   const imports: DashboardImport[] = parsed.imports.map((i) =>
      i.names
         ? { kind: "names", names: i.names, from: i.from }
         : { kind: "all", from: i.from },
   );
   const sources: DashboardSource[] = [];
   const drills: DashboardDrill[] = [];
   const viewsBySource = new Map<string, Map<string, TreeView>>();

   for (const source of parsed.sources) {
      const entry: DashboardSource = { name: source.name, base: source.base };
      if (source.dimensions.length > 0)
         entry.dimensions = source.dimensions.map((d) => ({
            name: d.name,
            expression: d.expression,
         }));
      const scopedBy = new Set(
         source.wheres.flatMap((where) =>
            where.clauses.flatMap((clause) => clause.givens),
         ),
      );
      if (scopedBy.size > 0) entry.scopedBy = Array.from(scopedBy);
      sources.push(entry);

      // A `# drill` is a tag on a dimension's declaration, so the dimensions
      // this file declares are exactly where one can be authored.
      for (const dimension of source.dimensions) {
         const drillTag = parseAnnotation(
            tagText(blockAbove(parsed, lines, dimension.line).tags),
         ).tag?.tag("drill");
         if (!drillTag) continue;
         const to = drillTag.textArray("to") ?? [drillTag.text("to") ?? ""];
         drills.push({
            source: source.name,
            name: dimension.name,
            expression: dimension.expression,
            to: to.filter(Boolean),
            ...(drillTag.text("given")
               ? { given: drillTag.text("given") as string }
               : {}),
         });
      }

      viewsBySource.set(
         source.name,
         new Map(source.views.map((v) => [v.name, v])),
      );
   }

   const tiles: DashboardTile[] = [];
   for (const { text: entryText } of list.entries) {
      const quoted = QUOTED_ENTRY.exec(entryText);
      const textName = quoted ? undefined : TEXT_ENTRY.exec(entryText)?.[1];
      if (textName !== undefined) {
         const textTag = parseAnnotation([`# ${entryText}`]).tag?.tag(textName);
         if (textTag?.text("kind") !== "text") {
            return {
               ok: false,
               reason:
                  `The tile \`${entryText}\` is neither a quoted \`source -> view\` ` +
                  `expression nor a \`${textName} { kind=text }\` entry, which are ` +
                  `the only forms the builder can lay out.`,
            };
         }
         if (tiles.some((tile) => isTextTile(tile) && tile.name === textName)) {
            return {
               ok: false,
               reason: `Two text tiles are named \`${textName}\`, so the builder cannot tell them apart.`,
            };
         }
         const colspan = gridWidth(textTag, "colspan");
         tiles.push({
            kind: "text",
            name: textName,
            markdown:
               parsed.textBlocks.find((block) => block.name === textName)
                  ?.body ?? "",
            ...(colspan === undefined ? {} : { colspan }),
            ...(textTag.has("break") ? { break: true } : {}),
         });
         continue;
      }
      const entry = quoted?.[1] ?? entryText;
      const steps = quoted ? tileSteps(entry) : undefined;
      if (!steps) {
         return {
            ok: false,
            reason:
               `The tile \`${entry}\` is not a \`source -> view\` expression, ` +
               `which is the only form the builder can lay out.`,
         };
      }
      const { source: sourceName, view: viewName } = steps;
      const view = viewsBySource.get(sourceName)?.get(viewName);

      // Not declared here: the view belongs to an imported source, which is a
      // complete dashboard in itself — `tiles=["orders -> by_brand"]` over an
      // imported `orders` needs nothing else in the file. Shown, not editable:
      // its tags live on the model's view, and the builder does not write model
      // files.
      if (view === undefined) {
         tiles.push({
            name: viewName,
            source: sourceName,
            declaration: { kind: "inherited" },
         });
         continue;
      }

      const t = parseAnnotation(tagText(view.tags)).tag;
      const declaration:
         | { kind: "reference"; from: string }
         | { kind: "inline" }
         | undefined =
         view.body.kind === "reference"
            ? { kind: "reference" as const, from: view.body.from }
            : view.body.kind === "inline"
              ? { kind: "inline" as const }
              : undefined;
      if (declaration === undefined) {
         // Declared here, in a body the builder does not rewrite. Its tags are
         // still `#` lines in this file, so they come with it: only a filter
         // has nowhere to go.
         tiles.push({
            name: viewName,
            source: sourceName,
            declaration: {
               kind: "opaque",
               why:
                  view.body.kind === "unsupported"
                     ? view.body.why
                     : "unreadable",
            },
            ...(t?.text("label") ? { label: t.text("label") as string } : {}),
            ...(t?.text("subtitle")
               ? { subtitle: t.text("subtitle") as string }
               : {}),
            ...(t?.numeric("colspan") !== undefined
               ? { colspan: t.numeric("colspan") as number }
               : {}),
            ...chartField(view.tags),
            ...(t?.has("break") ? { break: true } : {}),
            ...(t?.has("borderless") ? { borderless: true } : {}),
         });
         continue;
      }
      // A refinement's filters, or an inline body's own — the same clauses
      // either way, located by the tree rather than by depth in the text.
      const filters = filtersOf(
         view.body.kind === "reference"
            ? view.body.refinement
            : view.body.kind === "inline"
              ? view.body.stage
              : undefined,
      );
      tiles.push({
         name: viewName,
         source: sourceName,
         declaration,
         ...(filters ? { filters } : {}),
         ...(t?.text("label") ? { label: t.text("label") as string } : {}),
         ...(t?.text("subtitle")
            ? { subtitle: t.text("subtitle") as string }
            : {}),
         ...(t?.numeric("colspan") !== undefined
            ? { colspan: t.numeric("colspan") as number }
            : {}),
         ...chartField(view.tags),
         ...(t?.has("break") ? { break: true } : {}),
         ...(t?.has("borderless") ? { borderless: true } : {}),
      });
   }

   const givens = localGivens(parsed, lines, parseAnnotation as ParseTags);

   const startingGivens: Record<string, string> = {};
   const givensTag = artifactTag?.tag("givens");
   for (const key of Object.keys(givensTag?.dict ?? {})) {
      const value = givensTag?.text(key);
      if (value !== undefined) startingGivens[key] = value;
   }

   // The server's rule: a written `dashboard { columns }` wins even when it is
   // not a width (the default width then applies), and the alias only counts
   // beside `tiles`.
   const columnsTag = tag?.tag("dashboard")?.has("columns")
      ? gridWidth(tag.tag("dashboard"), "columns")
      : artifactTag?.array("tiles")
        ? gridWidth(artifactTag, "dashboard_columns")
        : undefined;

   return {
      ok: true,
      document: {
         ...(kind ? { kind } : {}),
         title: artifactTag?.text("title") ?? "",
         ...(description ? { description } : {}),
         ...(columnsTag === undefined ? {} : { columns: columnsTag }),
         ...(artifactTag?.has("autorun")
            ? { autorun: artifactTag.isTrue("autorun") }
            : {}),
         imports,
         sources,
         ...(givens ? { localGivens: givens } : {}),
         ...(Object.keys(startingGivens).length > 0 ? { startingGivens } : {}),
         ...(drills.length > 0 ? { drills } : {}),
         tiles,
      },
   };
}

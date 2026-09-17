// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   DashboardDocument,
   DashboardDrill,
   DashboardImport,
   DashboardSource,
   DashboardTile,
   LocalGiven,
} from "./document";
import {
   parseMalloy,
   parseRefused,
   type ParsedMalloy,
   type TreeStage,
   type TreeView,
} from "./malloyTree";
import { tileSteps } from "./malloyText";

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

/** A `#`/`//` block above a declaration, and where it sits. */
export interface Block {
   /** 0-based line of the first line in the block. */
   start: number;
   /**
    * The `#`-prefixed lines only, in order, WITH their line numbers. The writer
    * needs the numbers to patch a tag in place; the reader only needs the text.
    */
   tags: Array<{ line: number; text: string }>;
}

/**
 * The comment-and-tag block immediately above `declLine`.
 *
 * Scans upward while lines are `#` tags or `//` comments and STOPS AT A BLANK
 * LINE. Validated against the bundled dashboard, where it collects
 * `revenue_trend`'s three tags and the fourteen-line comment explaining its
 * colspan, then stops at the blank line above — which is the right unit to carry
 * when that tile moves.
 *
 * A blank line is the author's own separator, which is why it is the boundary
 * rather than a count or a heuristic about comment content.
 */
export function blockAbove(lines: string[], declLine: number): Block {
   let start = declLine;
   for (let i = declLine - 1; i >= 0; i--) {
      const text = lines[i].trim();
      if (text === "") break;
      if (text.startsWith("#") || text.startsWith("//")) start = i;
      else break;
   }
   const tags: Array<{ line: number; text: string }> = [];
   for (let i = start; i < declLine; i++) {
      const text = lines[i].trim();
      // `##` at this indent level is a MODEL annotation and never belongs to a
      // declaration; only single-`#` object tags do.
      if (text.startsWith("#") && !text.startsWith("##"))
         tags.push({ line: i, text });
   }
   return { start, tags };
}

/** Just the text of a block's tags, which is what `parseAnnotation` takes. */
const tagText = (tags: Array<{ text: string }>) => tags.map((t) => t.text);

/** The model-level `##` lines, which are not symbols and must be read as text. */
function modelLines(lines: string[]): {
   description?: string;
   artifact: string[];
} {
   const doc: string[] = [];
   const artifact: string[] = [];
   for (const raw of lines) {
      const text = raw.trim();
      if (text.startsWith('##"')) doc.push(text.slice(3).trim());
      else if (text.startsWith("##!")) continue;
      else if (text.startsWith("##")) artifact.push(text);
   }
   return {
      description: doc.length > 0 ? doc.join("\n") : undefined,
      artifact,
   };
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
         out.push({ field, given, ...(op === "~" ? {} : { op }) });
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
            parse(tagText(blockAbove(lines, given.line).tags)).tag,
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

/** The `tiles=[…]` entries, in order, as written. */
function tileEntries(artifactLine: string): string[] {
   const key = artifactLine.search(/tiles\s*=\s*\[/);
   if (key < 0) return [];
   const open = artifactLine.indexOf("[", key);
   const close = artifactLine.indexOf("]", open);
   if (close < 0) return [];
   const list = artifactLine.slice(open + 1, close);
   return [...list.matchAll(/"([^"]+)"/g)].map((m) => m[1]);
}

export async function readDashboardDocument(
   sourceText: string,
): Promise<ReadResult> {
   const { parseAnnotation } = await import("@malloydata/malloy-tag");
   const lines = sourceText.split("\n");

   const parse = await parseMalloy(sourceText);
   if (parseRefused(parse))
      return {
         ok: false,
         reason: parse.reason,
         ...(parse.line ? { line: parse.line } : {}),
      };
   const parsed = parse.parsed;

   const { description, artifact } = modelLines(lines);
   const artifactLine = artifact.find((l) => l.includes("artifact"));
   if (artifactLine === undefined) {
      return {
         ok: false,
         reason:
            "No `## artifact { … }` tag, so this file is not a composite dashboard.",
      };
   }

   const tag = parseAnnotation([artifactLine.replace(/^##\s*/, "# ")]).tag;
   const artifactTag = tag?.tag("artifact");
   const entries = tileEntries(artifactLine);
   if (entries.length === 0) {
      return {
         ok: false,
         reason:
            "The `## artifact` tag names no tiles, so there is nothing to lay out.",
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
      sources.push(entry);

      // A `# drill` is a tag on a dimension's declaration, so the dimensions
      // this file declares are exactly where one can be authored.
      for (const dimension of source.dimensions) {
         const drillTag = parseAnnotation(
            tagText(blockAbove(lines, dimension.line).tags),
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
   for (const entry of entries) {
      const steps = tileSteps(entry);
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

   const columnsTag = tag?.tag("dashboard")?.numeric("columns");

   return {
      ok: true,
      document: {
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

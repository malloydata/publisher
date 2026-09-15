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

/**
 * Read a `dashboards/*.malloy` file into a {@link DashboardDocument}.
 *
 * PURE: source text in, document out. No server, no schema, no connection.
 * `Malloy.parse` needs none of those — only `Malloy.compile` does — so this runs
 * in the browser and in a unit test alike, which is what lets the suite open
 * every dashboard in the repository as a regression gate.
 *
 * Structure comes from the parser's symbol tree, which covers imports (including
 * the items of a named list), sources, views and dimensions, each with a source
 * RANGE. Content comes from the text inside those ranges, and tags from
 * `parseAnnotation` over the block above each declaration.
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
export const tagText = (tags: Array<{ text: string }>) =>
   tags.map((t) => t.text);

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

/** `source: overview is scoped_orders extend {` -> `scoped_orders`. */
function sourceBase(text: string, name: string): string | undefined {
   const m = new RegExp(
      `source:\\s*${name}\\s+is\\s+([A-Za-z_][A-Za-z0-9_.]*)`,
   ).exec(text);
   return m?.[1];
}

/**
 * `view: revenue_trend is sales_by_month + { where: … }` -> base and filters,
 * or `inline` for `view: order_tile is { aggregate: … }`, which is a query body
 * rather than a reference and is read but never rewritten.
 */
function viewBody(
   text: string,
   name: string,
):
   | { kind: "reference"; from: string; refinement?: string }
   | { kind: "inline" }
   | undefined {
   const ref = new RegExp(
      `view:\\s*${name}\\s+is\\s+([A-Za-z_][A-Za-z0-9_.]*)\\s*(\\+\\s*\\{[\\s\\S]*\\})?`,
   ).exec(text);
   if (ref)
      return { kind: "reference", from: ref[1], refinement: ref[2]?.trim() };
   if (new RegExp(`view:\\s*${name}\\s+is\\s*\\{`).test(text))
      return { kind: "inline" };
   return undefined;
}

/**
 * One `where: <field> <op> $<GIVEN>` clause. Exported for the writer, which
 * has to find the same clauses in order to replace them and nothing else.
 */
export const BINDING_CLAUSE =
   /where:\s*([A-Za-z_][A-Za-z0-9_.]*)\s*(~|>=|<=|!=|=|>|<)\s*\$([A-Za-z_][A-Za-z0-9_]*)/g;

/** `where: products.brand ~ $BRAND`, `where: created_at >= $SINCE` … */
function filtersOf(refinement: string | undefined) {
   if (!refinement) return undefined;
   const out: Array<{ field: string; given: string; op?: string }> = [];
   for (const m of refinement.matchAll(BINDING_CLAUSE))
      out.push({
         field: m[1],
         given: m[3],
         ...(m[2] === "~" ? {} : { op: m[2] }),
      });
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
 * `given:` declarations, which the parser's symbol tree does not cover at all —
 * its types are query, unnamed_query, explore, field, join, import and
 * import_item. So these are read as text, in BOTH spellings Malloy accepts and
 * this repository uses:
 *
 *     given: CATEGORY :: filter<string> is f''      // one per line
 *
 *     given:                                        // a block
 *       CATEGORY :: filter<string> is f''
 *       SINCE :: date is @2023-01-01
 *
 * The one-line form is what `givens.malloy` and the docs write and what the
 * builder emits; the block form is read so a file written the other way still
 * opens. Either way the tags above a declaration are its control contract.
 */
export function localGivens(
   lines: string[],
   parse: ParseTags,
): LocalGiven[] | undefined {
   const out: LocalGiven[] = [];
   const push = (line: number, declaration: string) => {
      const m = /^([A-Z_][A-Z0-9_]*)\s*::\s*(\S+)\s+is\s+(.+)$/.exec(
         declaration.trim(),
      );
      if (!m) return;
      out.push({
         name: m[1],
         type: m[2],
         default: m[3].trim(),
         ...readControlTags(parse(tagText(blockAbove(lines, line).tags)).tag),
      });
   };
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
            push(j, inner);
         }
      } else if (text.startsWith("given:")) {
         push(i, text.slice("given:".length));
      }
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

/**
 * Every `view: <name> is` declared under the top-level `source: <owner> is`
 * line, by name -> 0-based line. See the note at the call site for why this is
 * textual rather than read off the symbol tree.
 */
function viewsDeclaredUnder(
   lines: string[],
   owner: string,
): Map<string, number> {
   const views = new Map<string, number>();
   let current: string | undefined;
   for (let line = 0; line < lines.length; line++) {
      const text = lines[line];
      const source = /^source:\s*([A-Za-z_][A-Za-z0-9_]*)\s+is\b/.exec(text);
      if (source) {
         current = source[1];
         continue;
      }
      // Any other top-level declaration ends the source's body.
      if (/^(query|run|import|given)\b/.test(text) || text.startsWith("##"))
         current = undefined;
      if (current !== owner) continue;
      const view = /^\s*view:\s*([A-Za-z_][A-Za-z0-9_]*)\s+is\b/.exec(text);
      if (view) views.set(view[1], line);
   }
   return views;
}

/** The `tiles=[…]` entries, in order, as written. */
function tileEntries(artifactLine: string): string[] {
   const list = /tiles\s*=\s*\[([\s\S]*?)\]/.exec(artifactLine)?.[1];
   if (list === undefined) return [];
   return [...list.matchAll(/"([^"]+)"/g)].map((m) => m[1]);
}

export async function readDashboardDocument(
   sourceText: string,
): Promise<ReadResult> {
   const { Malloy } = await import("@malloydata/malloy");
   const { parseAnnotation } = await import("@malloydata/malloy-tag");
   const lines = sourceText.split("\n");

   let symbols;
   try {
      symbols = Malloy.parse({ source: sourceText }).symbols;
   } catch (error) {
      return { ok: false, reason: `This file is not valid Malloy: ${error}` };
   }

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

   const imports: DashboardImport[] = [];
   const sources: DashboardSource[] = [];
   const drills: DashboardDrill[] = [];
   const viewsBySource = new Map<string, Map<string, number>>();

   for (const symbol of symbols) {
      if (symbol.type === "import") {
         const names = (symbol.children ?? [])
            .filter((c) => c.type === "import_item")
            .map((c) => String(c.name));
         imports.push(
            names.length > 0
               ? { kind: "names", names, from: String(symbol.name) }
               : { kind: "all", from: String(symbol.name) },
         );
         continue;
      }
      if (symbol.type !== "explore") continue;

      const name = String(symbol.name);
      const head = lines[symbol.range.start.line] ?? "";
      const base = sourceBase(head, name);
      if (base === undefined) {
         return {
            ok: false,
            reason: `Could not read what source \`${name}\` extends.`,
            line: symbol.range.start.line + 1,
         };
      }
      sources.push({ name, base });

      // The VIEWS are found in the TEXT, attributed to the nearest `source:`
      // above them, and not taken from the symbol tree. The tree is reliable
      // about which sources and imports exist and unreliable about what is
      // inside a source: measured, a refinement spelled `+ { limit: 5, where: … }`
      // — which compiles — makes it report the refined view's BASE as a child
      // view, end the source early, and drop the next declaration altogether,
      // so the tile that named it read back as "inherited" and lost its tags.
      // A `view: <name> is` line under a `source: <name> is` line is
      // unambiguous, and Malloy has no nested sources to confuse it.
      const views = viewsDeclaredUnder(lines, name);
      for (const child of symbol.children ?? []) {
         const childLine = child.range.start.line;
         if (child.type === "field") {
            const { tags } = blockAbove(lines, childLine);
            const drillTag = parseAnnotation(tagText(tags)).tag?.tag("drill");
            if (!drillTag) continue;
            const to = drillTag.textArray("to") ?? [drillTag.text("to") ?? ""];
            const expression = /is\s+(.+)$/
               .exec(lines[childLine].trim())?.[1]
               ?.trim();
            drills.push({
               source: name,
               name: String(child.name),
               expression: expression ?? "",
               to: to.filter(Boolean),
               ...(drillTag.text("given")
                  ? { given: drillTag.text("given") as string }
                  : {}),
            });
         }
      }
      viewsBySource.set(name, views);
   }

   const tiles: DashboardTile[] = [];
   for (const entry of entries) {
      const parts = entry.split("->").map((p) => p.trim());
      if (parts.length !== 2) {
         return {
            ok: false,
            reason:
               `The tile \`${entry}\` is not a \`source -> view\` expression, ` +
               `which is the only form the builder can lay out.`,
         };
      }
      const [sourceName, viewExpr] = parts;
      // A tile may carry its own refinement: `orders -> by_brand + { limit: 2 }`.
      const viewName = viewExpr.split("+")[0].trim();
      const declLine = viewsBySource.get(sourceName)?.get(viewName);

      // Not declared here: the view belongs to an imported source, which is a
      // complete dashboard in itself — `tiles=["orders -> by_brand"]` over an
      // imported `orders` needs nothing else in the file. Shown, not editable:
      // its tags live on the model's view, and the builder does not write model
      // files.
      if (declLine === undefined) {
         tiles.push({
            name: viewName,
            source: sourceName,
            declaration: { kind: "inherited" },
         });
         continue;
      }

      const body = viewBody(lines[declLine], viewName);
      if (body === undefined) {
         return {
            ok: false,
            reason: `Could not read what view \`${viewName}\` is declared from.`,
            line: declLine + 1,
         };
      }
      const { tags } = blockAbove(lines, declLine);
      const t = parseAnnotation(tagText(tags)).tag;
      const filters =
         body.kind === "reference" ? filtersOf(body.refinement) : undefined;
      tiles.push({
         name: viewName,
         source: sourceName,
         declaration:
            body.kind === "reference"
               ? { kind: "reference", from: body.from }
               : { kind: "inline" },
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

   const givens = localGivens(lines, parseAnnotation as ParseTags);

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

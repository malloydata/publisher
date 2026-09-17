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
   type DeclarationAt,
   declarationExtent,
   declarationsUnder,
   givenDeclarations,
   splitTrailingComment,
   tileSteps,
   viewBodyStage1,
} from "./malloyText";

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

/**
 * `where: products.brand ~ $BRAND`, `where: created_at >= $SINCE` … from a
 * `+ { … }` refinement. Only ISOLATED clauses count: the writer removes what
 * it reads here by span, so a clause it reads out of `where: a ~ $A and c =
 * 1` would take the predicate's other half with it and leave `and c = 1`
 * behind as invalid Malloy. A compound predicate reads as no binding and is
 * left exactly as written.
 */
function filtersOf(refinement: string | undefined) {
   if (!refinement) return undefined;
   const clean = cleanBindingClauses(refinement);
   if (clean.length === 0) return undefined;
   return clean.map((c) => ({
      field: c.field,
      given: c.given,
      ...(c.op ? { op: c.op } : {}),
   }));
}

/**
 * The `where:` binding clauses in `content` that are ISOLATED — the text
 * between one clause's end and whatever follows is nothing but a separator
 * (a comma, or nothing at all) before the next binding clause, a top-level
 * statement keyword, a closing brace, or the end of `content`. The brace
 * counts because a refinement arrives here still wrapped in its own `{ … }`,
 * and a clause that ends the block is as isolated as one that ends the text. `end` reaches through that
 * separator only — never into a following statement's own text — so a caller
 * stripping a clean clause out never leaves a dangling comma behind, and
 * never deletes the statement beside it.
 *
 * `where: a ~ $A and c = 1` matches BINDING_CLAUSE once, for `a ~ $A`, and
 * fails this isolation check because ` and c = 1` follows it — a compound
 * predicate, unmodeled Malloy, left exactly as written: `and` has no `:`
 * after it, so it does not read as the next statement. `where: a ~ $A, where:
 * b ~ $B` passes twice: the gap between them is a bare comma. `where: a ~
 * $A, aggregate: n is count()` — a one-line body's binding sharing a line
 * with its query — passes too: `aggregate:` is a statement keyword, not a
 * continuation of the predicate.
 */
export function cleanBindingClauses(content: string): Array<{
   start: number;
   end: number;
   field: string;
   given: string;
   op?: string;
}> {
   const matches = [...content.matchAll(BINDING_CLAUSE)];
   const out: Array<{
      start: number;
      end: number;
      field: string;
      given: string;
      op?: string;
   }> = [];
   for (let i = 0; i < matches.length; i++) {
      const m = matches[i];
      const start = m.index as number;
      const clauseEnd = start + m[0].length;
      const boundary =
         i + 1 < matches.length
            ? (matches[i + 1].index as number)
            : content.length;
      const gap = content.slice(clauseEnd, boundary);
      const separator = /^[\s,]*/.exec(gap)?.[0] ?? "";
      const rest = gap.slice(separator.length);
      if (
         rest !== "" &&
         !rest.startsWith("}") &&
         !/^[A-Za-z_][A-Za-z0-9_]*\s*:/.test(rest)
      )
         continue;
      out.push({
         start,
         end: clauseEnd + separator.length,
         field: m[1],
         given: m[3],
         ...(m[2] === "~" ? {} : { op: m[2] }),
      });
   }
   return out;
}

/**
 * Whether `code`'s entire text (once trimmed) is TILED by binding clauses —
 * the rule a depth-1 `where:` line inside an inline body's first stage must
 * meet to be a builder-managed binding. Nothing before the first clause,
 * nothing after the last, and nothing but a separator BETWEEN clauses:
 * `cleanBindingClauses` accepts a following statement keyword as a clause's
 * own boundary (that is what lets a one-liner's binding share a line with its
 * query), so two clauses each passing in isolation can still leave a whole
 * statement sitting unnoticed in the gap between them — a `where:` either
 * side of an `aggregate:` reads as two clean clauses this way, and skipping
 * the gap check would call the line binding-only anyway, dropping the
 * aggregate along with the bindings on a splice. `undefined` for anything
 * that fails any of the three checks — a compound predicate is one such case,
 * an untiled statement between two clauses is another.
 */
export function isBindingOnly(
   code: string,
): ReturnType<typeof cleanBindingClauses> | undefined {
   const trimmed = code.trim();
   const clean = cleanBindingClauses(trimmed);
   if (clean.length === 0) return undefined;
   if (clean[0].start !== 0) return undefined;
   if (clean[clean.length - 1].end !== trimmed.length) return undefined;
   for (let i = 1; i < clean.length; i++)
      if (!/^[\s,]*$/.test(trimmed.slice(clean[i - 1].end, clean[i].start)))
         return undefined;
   return clean;
}

/**
 * An inline body's filters from its multi-line first stage: one depth-1
 * `where:` LINE per binding, or several clauses on one line, each checked
 * whole via {@link isBindingOnly}. A line that fails — a compound predicate,
 * say — is skipped, not reported: it is unmodeled Malloy, not a binding.
 */
function lineFilters(
   whereLines: Array<{ line: number; code: string; continued: boolean }>,
): Array<{ field: string; given: string; op?: string }> | undefined {
   const out: Array<{ field: string; given: string; op?: string }> = [];
   for (const { code, continued } of whereLines) {
      if (continued) continue; // only half of it was read; not a binding
      const clean = isBindingOnly(code);
      if (!clean) continue;
      for (const c of clean)
         out.push({
            field: c.field,
            given: c.given,
            ...(c.op ? { op: c.op } : {}),
         });
   }
   return out.length > 0 ? out : undefined;
}

/**
 * A one-line body's filters: every ISOLATED binding clause anywhere in its
 * braces, ignoring whatever query content sits alongside it — unlike
 * {@link lineFilters}, the whole content is not required to be binding-only,
 * because a one-liner's query and its bindings necessarily share the line.
 */
function oneLinerFilters(
   content: string,
): Array<{ field: string; given: string; op?: string }> | undefined {
   const clean = cleanBindingClauses(content);
   if (clean.length === 0) return undefined;
   return clean.map((c) => ({
      field: c.field,
      given: c.given,
      ...(c.op ? { op: c.op } : {}),
   }));
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
   for (const at of givenDeclarations(lines).values()) {
      const m = /^([A-Z_][A-Z0-9_]*)\s*::\s*(\S+)\s+is\s+(.+)$/.exec(
         at.declaration,
      );
      if (!m) continue;
      out.push({
         name: m[1],
         type: m[2],
         default: m[3].trim(),
         ...readControlTags(
            parse(tagText(blockAbove(lines, at.line).tags)).tag,
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
   const viewsBySource = new Map<string, Map<string, DeclarationAt>>();

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
      const views = declarationsUnder(lines, name, "view");
      // Dimensions the same way, and drills off THEIR tag blocks: a `# drill`
      // is a tag on a dimension's declaration, so the dimensions this file
      // declares are exactly where one can be authored.
      const dimensions = declarationsUnder(lines, name, "dimension");
      if (dimensions.size > 0) {
         sources[sources.length - 1].dimensions = [...dimensions].map(
            ([dimensionName, at]) => ({
               name: dimensionName,
               expression: at.rest,
            }),
         );
      }
      for (const [dimensionName, at] of dimensions) {
         const { tags } = blockAbove(lines, at.line);
         const drillTag = parseAnnotation(tagText(tags)).tag?.tag("drill");
         if (!drillTag) continue;
         const to = drillTag.textArray("to") ?? [drillTag.text("to") ?? ""];
         drills.push({
            source: name,
            name: dimensionName,
            expression: at.rest,
            to: to.filter(Boolean),
            ...(drillTag.text("given")
               ? { given: drillTag.text("given") as string }
               : {}),
         });
      }
      viewsBySource.set(name, views);
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
      const declLine = viewsBySource.get(sourceName)?.get(viewName)?.line;

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

      // Without the comment split, a greedy read of
      // `view: x is y + { limit: 5 } // note + { where: c ~ $C }` finds the
      // binding inside the comment and reports a filter Malloy never applies.
      const body = viewBody(
         splitTrailingComment(lines[declLine]).code,
         viewName,
      );
      if (body === undefined) {
         return {
            ok: false,
            reason: `Could not read what view \`${viewName}\` is declared from.`,
            line: declLine + 1,
         };
      }
      const { tags } = blockAbove(lines, declLine);
      const t = parseAnnotation(tagText(tags)).tag;
      let filters:
         | Array<{ field: string; given: string; op?: string }>
         | undefined;
      if (body.kind === "reference") {
         filters = filtersOf(body.refinement);
      } else {
         // Filters live as depth-1 `where:` statements in the body's own
         // first stage rather than a `+ { … }` refinement — see
         // BINDING_CLAUSE and viewBodyStage1 for the shape this scan trusts.
         const extent = declarationExtent(lines, declLine);
         if ("unreadable" in extent) {
            return {
               ok: false,
               reason: `Could not read \`${viewName}\`'s body: ${extent.unreadable}.`,
               line: declLine + 1,
            };
         }
         const stage = viewBodyStage1(lines, declLine, extent.end);
         filters =
            stage.oneLiner !== undefined
               ? oneLinerFilters(stage.oneLiner.content)
               : lineFilters(stage.whereLines);
      }
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

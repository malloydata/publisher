// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { malloyLiteral } from "../../utils/malloyLiteral";
import {
   chartLineText,
   chartResetLine,
   isChartPick,
   seriesColorLine,
   seriesColorResetLine,
} from "./chartLine";
import {
   isQueryTile,
   isReadOnlyTile,
   tileExpression,
   type DashboardDocument,
   type QueryTile,
} from "./document";
import { malloyPath } from "../../utils/malloyText";
import { localGivenSpec } from "./localGiven";
import { reaches, resolvedOf } from "./imports";

/**
 * What a reader would see if the DOCUMENT were the file: the controls, and
 * each tile's query — so the builder's live view follows an edit the moment it
 * is made, rather than the file on the server's disk. Run from the file, an
 * unbound tile keeps filtering and a removed control stays in the row until a
 * save lands, so the edit looks like it did nothing.
 *
 * Two functions, one per half of what a reader sees. Both are pure, so the
 * host can hold the live document and derive its preview from it. A given the
 * document declares but the server's model has not compiled cannot be SENT to
 * it — the server refuses a given it does not know — so a new control's value
 * is written into the query as a literal instead, and the new filter works the
 * moment it is added rather than after a save.
 */

/**
 * The controls this document shows, in the order the reader shows them: every
 * given some tile binds in this file or reads through the model (`modelReads`,
 * from {@link givensReadThroughModel}).
 *
 * That is the reader's rule — only a given some tile reads becomes a control
 * — applied to the document instead of the manifest, and so is the order. The
 * server builds the row tile by tile, in tile order, each tile's givens as its
 * compiled query reads them: what the tile's source reads first, then the
 * tile's own `where:` bindings. So here too: per tile, the givens its served
 * self read (`servedReads`), less the bindings since removed, then the tile's
 * current bindings not already listed — a new filter lands after the tile's
 * existing ones, where the save will put it. A model read no tile position
 * accounts for (no `servedReads` given) goes first, in the manifest's order,
 * since a model read is a source's and the reader lists a source's first.
 *
 * A local declaration is built into a `Given` from its control contract, and
 * wins over a model given of the same name; a model given is the manifest's
 * own spec, which carries what the file cannot say (`givenNames` a suggest is
 * gated by, for one).
 */
export function previewGivens(
   document: DashboardDocument,
   modelSpecs: readonly Given[],
   modelReads: ReadonlySet<string> = new Set(),
   servedReads: ReadonlyMap<string, readonly string[]> = new Map(),
): Given[] {
   const order: string[] = [];
   for (const tile of document.tiles) {
      if (!isQueryTile(tile)) continue;
      const binds = (tile.filters ?? []).map((filter) => filter.given);
      const served =
         servedReads.get(tileExpressionKey(tileExpression(tile))) ?? [];
      for (const name of served)
         if (modelReads.has(name) || binds.includes(name)) order.push(name);
      order.push(...binds);
   }
   const placed = new Set(order);
   const unplaced = modelSpecs.flatMap((spec) =>
      spec.name !== undefined &&
      modelReads.has(spec.name) &&
      !placed.has(spec.name)
         ? [spec.name]
         : [],
   );

   const locals = new Map(
      (document.localGivens ?? []).map((local) => [local.name, local]),
   );
   const models = new Map(
      modelSpecs.flatMap((spec) =>
         spec.name === undefined ? [] : [[spec.name, spec] as const],
      ),
   );
   const out: Given[] = [];
   for (const name of new Set([...unplaced, ...order])) {
      const local = locals.get(name);
      const spec = local ? localGivenSpec(local) : models.get(name);
      if (spec) out.push(spec);
   }
   return out;
}

/**
 * The givens the document's tiles read through the model rather than through
 * a binding in this file: a source whose own `where:` reads a given, say.
 *
 * The server says what each served tile reads (`servedReads`, by tile
 * expression), bindings in the file included. What the file bound when it
 * was opened is taken back out, so what is left is what the model reads; a
 * binding the author removes then takes its control with it. A tile the
 * server has not compiled (new, or under a new name) is read by the served
 * tile with the same expression, if there is one, and otherwise reads
 * nothing until the next save.
 */
export function givensReadThroughModel(
   document: DashboardDocument,
   opened: DashboardDocument,
   servedReads: ReadonlyMap<string, readonly string[]>,
): Set<string> {
   const boundWhenOpened = new Map<string, Set<string>>();
   for (const tile of opened.tiles) {
      if (!isQueryTile(tile)) continue;
      boundWhenOpened.set(
         tileExpressionKey(tileExpression(tile)),
         new Set((tile.filters ?? []).map((filter) => filter.given)),
      );
   }
   const reads = new Set<string>();
   for (const tile of document.tiles) {
      if (!isQueryTile(tile)) continue;
      const key = tileExpressionKey(tileExpression(tile));
      const bound = boundWhenOpened.get(key);
      for (const name of servedReads.get(key) ?? [])
         if (!bound?.has(name)) reads.add(name);
   }
   return reads;
}

/**
 * A declaration's default as a clause can use it: none when it filters
 * nothing (an empty `f''`), so an unset filter adds no clause, as before.
 */
function defaultLiteral(spelled: string | undefined): string | undefined {
   const text = spelled?.trim();
   if (!text || /^f(['"])\1$/.test(text)) return undefined;
   return text;
}

/**
 * Whether dropping the wrapper's chart line leaves a view with no chart of its own: an inline
 * body, a `{ … } + { … }` compound or a `->` pipeline. Any other opaque body starts from a
 * named view and inherits that view's chart, which a preview must not hide.
 */
function dropsBaseChart(declaration: QueryTile["declaration"]): boolean {
   if (declaration.kind === "inline") return true;
   return (
      declaration.kind === "opaque" &&
      (declaration.why.includes("compound") ||
         declaration.why.includes("pipeline"))
   );
}

/** A tile expression as the server keys it, so `a->b` and `a -> b` match. */
export function tileExpressionKey(expression: string): string {
   // Split rather than `/\s*->\s*/g`, which backtracks quadratically on long whitespace.
   return expression
      .split("->")
      .map((part) => part.trim().replace(/\s+/g, " "))
      .join(" -> ")
      .replace(/\s+/g, " ");
}

/** A tile's query as the document has it, and the givens it sends. */
export interface PreviewTileQuery {
   /** A run expression, without `run:` — what `DashboardTile.tile` takes. */
   expression: string;
   /**
    * The chart line the tile's wrapper carries, to sit above the `run:`. A
    * reference tile runs its base view, which has never seen the wrapper, so
    * without it the tile would draw the view's own chart whatever the picker
    * says. An inline or opaque tile runs the SAVED view, wrapper line and all,
    * so Default there carries the all-negating line: the saved line is what
    * Default removes. Absent when a reference tile's wrapper adds none.
    */
   annotation?: string;
   /**
    * The givens this tile binds, its extension reads, or its served tile
    * reads that the server can take, for narrowing the request. Undefined for a tile whose bindings live in the model, where the
    * document cannot know them: send the whole row, as the reader does.
    */
   givenNames: string[] | undefined;
   /**
    * Every given this tile's preview answers to, runnable or not, for saying
    * which controls it ignores. Undefined when nothing can say: a tile the
    * served file did not resolve (inherited, or not saved yet) whose document
    * side reads no given either.
    */
   reads: string[] | undefined;
   /**
    * The package model to run on instead of the report's own. Set only where
    * the tile's source comes in through an import the served report does not
    * have yet; see {@link UnsavedImports}.
    */
   modelPath?: string;
}

/**
 * Where to run a tile whose source came in with an import this session added:
 * the model that import names, since the report the server serves cannot see
 * that source until the file is saved. Undefined for a source the served
 * report already sees.
 */
export type UnsavedImports = (source: string) => string | undefined;

/**
 * The {@link UnsavedImports} of `document` against the document as it was
 * opened, which is what the served report was built from. Undefined when it
 * imports nothing the opened one did not, and for a report held as text,
 * which has no imports: its sources are all the run model's.
 */
export function unsavedImports(
   document: DashboardDocument,
   opened: DashboardDocument,
   documentPath: string,
   textHeld: boolean,
): UnsavedImports | undefined {
   if (textHeld) return undefined;
   if (JSON.stringify(document.imports) === JSON.stringify(opened.imports))
      return undefined;
   return (source: string) => {
      const by = document.imports.find(
         (i) => i.kind === "names" && i.names.includes(source),
      );
      if (by === undefined) return undefined;
      const model = resolvedOf(by.from, documentPath);
      return reaches(opened, source, model, documentPath) ? undefined : model;
   };
}

/**
 * A tile's query with the DOCUMENT's bindings and chart.
 *
 * A tile that names a served view, or is a query of the report's own over one,
 * runs that view with the document's settings: `flights -> by_carrier + {
 * where: origin_code ~ $ORIGIN }` under the chart and colour lines its query
 * carries. That is the query the writer is about to produce, run before it is
 * written, and it needs nothing from the saved file. The rest of this says how
 * a tile in a file written before the builder wrote queries runs, on the file's
 * own extension.
 *
 * `overview -> revenue_trend` would run the view AS SAVED, bindings and all.
 * `overview -> sales_by_month + { where: category ~ $CATEGORY }` is the base
 * view the tile names, on the same extension, refined by what the document
 * binds now — which is exactly the declaration the writer would produce, run
 * before it is written. Every reference tile has such a base view, and the
 * extension inherits it: `view: x is base_view + …` can only name a view of the
 * source the extension extends.
 *
 * `runnable` is the set of givens the server's model declares; a binding to
 * one of them is written as `$NAME` and the given is sent with the request. A
 * binding to a given outside it — one this document just declared, which the
 * server would refuse by name — is written with its VALUE as a literal,
 * `where: brand ~ f'Nike'`, from `values` and the declaration's type; with no
 * value yet it is left out, which is what an empty filter means. A binding to
 * a given neither side knows is left out too.
 *
 * An inline tile takes the SAME path as a reference: `view: x is { … }` names
 * a view on the extension exactly as `view: x is base_view` does, so
 * `source -> x + { where: … }` runs it refined. Without this, a bound control
 * on an inline tile — most tiles in practice — looked like it did nothing in
 * the live editor, and sent no given at all once saved.
 *
 * ADDITIVE, though, where a reference is exact. A reference tile refines its
 * BASE view, which carries no bindings, so the preview is what the writer is
 * about to produce. An inline tile has no base to name: `x` is the saved view,
 * whose body already holds whatever bindings were saved into it, and this runs
 * against the package's own model rather than the edited text. So adding a
 * binding previews correctly, while REMOVING one leaves the saved `where:`
 * filtering and CHANGING one applies the old and the new together, until the
 * file is saved. Previewing those exactly would need the body re-emitted
 * without its bindings, which this builder deliberately does not do: it
 * splices, and never writes a view's body out from the document.
 */
export function previewTileQuery(
   document: DashboardDocument,
   tile: QueryTile,
   runnable: ReadonlySet<string>,
   values: ReadonlyMap<string, GivenValue> = new Map(),
   /** What the served file's compiled tile reads (`DashboardTile.givenNames`), when it has this tile. */
   served?: readonly string[],
   /** Where a source imported since the file was opened runs; see {@link UnsavedImports}. */
   unsaved?: UnsavedImports,
): PreviewTileQuery {
   if (isReadOnlyTile(tile)) {
      // Declared in the model, or a named query; bindings live there too, out
      // of this document's reach, so the server is sent the whole row as it does.
      return {
         expression: tileExpression(tile),
         givenNames: undefined,
         reads: served && [...served],
      };
   }
   // Run on the dashboard's OWN extension, not the model source it extends.
   // An extension inherits every view of its base, so `overview -> sales_by_month`
   // resolves; and it carries whatever else the extension declares — a
   // source-level `where:`, a `# drill` dimension — which the base does not.
   // Running on the base showed unscoped numbers for a dashboard that scopes
   // its source, and the saved page then differed from what the author watched.
   const on = tile.source;
   const { kind } = tile.declaration;
   const baseView =
      tile.declaration.kind === "reference" || tile.declaration.kind === "named"
         ? tile.declaration.from
         : tile.name;
   // A source this session imported runs on its own model until the save.
   const modelPath =
      kind === "inherited" || kind === "named"
         ? unsaved?.(on as string)
         : undefined;
   // A model other than the report's declares none of the report's givens, so
   // every binding is written there as a literal.
   const sendable = modelPath !== undefined ? new Set<string>() : runnable;
   const localTypes = new Map(
      (document.localGivens ?? []).map((local) => [local.name, local.type]),
   );
   // Each declaration's default, already a Malloy literal (`f'Jeans'`,
   // `@2023-01-01`): what applies when no value is set, as the server applies
   // a given's default.
   const localDefaults = new Map(
      (document.localGivens ?? []).map((local) => [local.name, local.default]),
   );
   // The extension's own `where:` filters every tile on it, as the served manifest counts.
   const scopedBy =
      document.sources.find((source) => source.name === on)?.scopedBy ?? [];
   // What the served tile reads moves it in the reader, so the preview sends it too.
   const sent = new Set(
      [...scopedBy, ...(served ?? [])].filter((name) => sendable.has(name)),
   );
   // A binding removed but not yet saved still counts here until the save.
   const reads = new Set<string>([...scopedBy, ...(served ?? [])]);
   const clauses: string[] = [];
   for (const filter of tile.filters ?? []) {
      const comparison = `where: ${malloyPath(filter.field)} ${filter.op ?? "~"}`;
      if (sendable.has(filter.given)) {
         sent.add(filter.given);
         reads.add(filter.given);
         clauses.push(`${comparison} $${filter.given}`);
         continue;
      }
      if (!localTypes.has(filter.given)) continue;
      // Applied as a literal, so the control still changes this tile.
      reads.add(filter.given);
      const literal =
         malloyLiteral(
            values.get(filter.given),
            localTypes.get(filter.given),
         ) ?? defaultLiteral(localDefaults.get(filter.given));
      if (literal !== undefined) clauses.push(`${comparison} ${literal}`);
   }
   const refinement = clauses.join(", ");
   const chart = tile.chart;
   const chartAnnotation =
      chart === "none" || isChartPick(chart)
         ? chartLineText(chart, tile.chartReset)
         : chart === "custom" && tile.chartLines?.length
           ? tile.chartLines.join("\n")
           : chart === "default" && dropsBaseChart(tile.declaration)
             ? chartResetLine()
             : undefined;
   // The tile's colour, under its chart line: the base view never saw the
   // wrapper's line, so the preview carries it as it carries the chart. With
   // no colour (Theme), an inline or opaque tile runs its own SAVED view, whose
   // last-saved colour line would still apply, so the preview takes it off.
   // Not where the tile's own tags carry a palette the builder did not write:
   // that line is the author's, it stays in the file, and the page draws it.
   const colorAnnotation =
      tile.color !== undefined
         ? seriesColorLine(tile.color)
         : (kind === "inline" || kind === "opaque") && !tile.ownPalette
           ? seriesColorResetLine
           : undefined;
   const annotation =
      [chartAnnotation, colorAnnotation].filter(Boolean).join("\n") ||
      undefined;
   return {
      ...(annotation ? { annotation } : {}),
      ...(modelPath !== undefined ? { modelPath } : {}),
      expression:
         `${on} -> ${baseView}` + (refinement ? ` + { ${refinement} }` : ""),
      // A served view the server has not resolved as a tile of this report
      // may read the model's givens (its source's own `where:`), which the
      // document cannot see: it is sent the whole row, as the reader does.
      givenNames:
         (kind === "inherited" || kind === "named") &&
         served === undefined &&
         modelPath === undefined
            ? undefined
            : Array.from(sent),
      // No served entry and nothing from the document: the model may still scope it, so say nothing.
      reads: served || reads.size > 0 ? Array.from(reads) : undefined,
   };
}

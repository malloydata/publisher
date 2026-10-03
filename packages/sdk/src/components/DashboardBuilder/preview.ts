// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { malloyLiteral } from "../../utils/malloyLiteral";
import { chartLineText, isChartPick } from "./chartLine";
import {
   isQueryTile,
   type DashboardDocument,
   type LocalGiven,
   type QueryTile,
} from "./document";
import { malloyPath } from "./malloyText";

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
 * The controls this document shows: every given some tile binds, this file's
 * own declarations first in declaration order, then the model's.
 *
 * That is the reader's rule — only a given some tile reads becomes a control
 * — applied to the document instead of the manifest. A local declaration is
 * built into a `Given` from its control contract; a model given is the
 * manifest's own spec, which carries what the file cannot say (`givenNames` a
 * suggest is gated by, for one).
 */
export function previewGivens(
   document: DashboardDocument,
   modelSpecs: readonly Given[],
): Given[] {
   const bound = new Set<string>();
   for (const tile of document.tiles)
      for (const filter of isQueryTile(tile) ? (tile.filters ?? []) : [])
         bound.add(filter.given);

   const out: Given[] = [];
   const seen = new Set<string>();
   for (const local of document.localGivens ?? []) {
      if (!bound.has(local.name)) continue;
      seen.add(local.name);
      out.push(givenFromLocal(local));
   }
   for (const spec of modelSpecs) {
      if (
         spec.name === undefined ||
         seen.has(spec.name) ||
         !bound.has(spec.name)
      )
         continue;
      seen.add(spec.name);
      out.push(spec);
   }
   return out;
}

/** A `given:` this file declares, as the `Given` the control row renders. */
function givenFromLocal(local: LocalGiven): Given {
   return {
      name: local.name,
      type: local.type,
      default: local.default,
      ...(local.label === undefined ? {} : { label: local.label }),
      ...(local.description === undefined
         ? {}
         : { description: local.description }),
      ...(local.control === undefined
         ? {}
         : { control: local.control as Given["control"] }),
      ...(local.suggest
         ? {
              suggest: {
                 ...(local.suggest.source === undefined
                    ? {}
                    : { source: local.suggest.source }),
                 ...(local.suggest.query === undefined
                    ? {}
                    : { query: local.suggest.query }),
                 dimension: local.suggest.dimension,
              },
           }
         : {}),
      ...(local.rangeMin === undefined ? {} : { rangeMin: local.rangeMin }),
      ...(local.rangeMax === undefined ? {} : { rangeMax: local.rangeMax }),
   };
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
    * The givens this tile binds or its extension reads that the server can
    * take, for narrowing the request. Undefined for a tile whose bindings live in the model, where the
    * document cannot know them: send the whole row, as the reader does.
    */
   givenNames: string[] | undefined;
   /**
    * Every given this tile's preview answers to, runnable or not, for saying
    * which controls it ignores. Undefined when nothing can say: an inherited
    * tile the served file did not resolve.
    */
   reads: string[] | undefined;
}

/**
 * A tile's query with the DOCUMENT's bindings, run on the dashboard's own
 * extension.
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
): PreviewTileQuery {
   if (tile.declaration.kind === "inherited") {
      // Declared in the model; bindings live there too, out of this
      // document's reach, so the server is sent the whole row as it does.
      return {
         expression: `${tile.source} -> ${tile.name}`,
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
   const baseView =
      tile.declaration.kind === "reference" ? tile.declaration.from : tile.name;
   const localTypes = new Map(
      (document.localGivens ?? []).map((local) => [local.name, local.type]),
   );
   // The extension's own `where:` filters every tile on it, as the served manifest counts.
   const scopedBy =
      document.sources.find((source) => source.name === on)?.scopedBy ?? [];
   const sent = new Set(scopedBy.filter((name) => runnable.has(name)));
   // A binding removed but not yet saved still counts here until the save; it errs toward no warning.
   const reads = new Set<string>([...scopedBy, ...(served ?? [])]);
   const clauses: string[] = [];
   for (const filter of tile.filters ?? []) {
      const comparison = `where: ${malloyPath(filter.field)} ${filter.op ?? "~"}`;
      if (runnable.has(filter.given)) {
         sent.add(filter.given);
         reads.add(filter.given);
         clauses.push(`${comparison} $${filter.given}`);
         continue;
      }
      if (!localTypes.has(filter.given)) continue;
      // Applied as a literal, so the control still changes this tile.
      reads.add(filter.given);
      const literal = malloyLiteral(
         values.get(filter.given),
         localTypes.get(filter.given),
      );
      if (literal !== undefined) clauses.push(`${comparison} ${literal}`);
   }
   const refinement = clauses.join(", ");
   const chart = tile.chart;
   const annotation =
      chart === "none" || isChartPick(chart)
         ? chartLineText(chart, tile.chartCarried)
         : chart === "custom" && tile.chartLines?.length
           ? tile.chartLines.join("\n")
           : chart === "default" && dropsBaseChart(tile.declaration)
             ? chartLineText("none")
             : undefined;
   return {
      ...(annotation ? { annotation } : {}),
      expression:
         `${on} -> ${baseView}` + (refinement ? ` + { ${refinement} }` : ""),
      givenNames: Array.from(sent),
      reads: Array.from(reads),
   };
}

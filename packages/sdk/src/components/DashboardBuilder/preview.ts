// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { malloyLiteral } from "../../utils/malloyLiteral";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";

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
      for (const filter of tile.filters ?? []) bound.add(filter.given);

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

/** A tile's query as the document has it, and the givens it sends. */
export interface PreviewTileQuery {
   /** A run expression, without `run:` — what `DashboardTile.tile` takes. */
   expression: string;
   /**
    * The givens this tile binds and the server can take, for narrowing the
    * request. Undefined for a tile whose bindings live in the model, where the
    * document cannot know them: send the whole row, as the reader does.
    */
   givenNames: string[] | undefined;
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
 */
export function previewTileQuery(
   document: DashboardDocument,
   tile: DashboardTile,
   runnable: ReadonlySet<string>,
   values: ReadonlyMap<string, GivenValue> = new Map(),
): PreviewTileQuery {
   if (tile.declaration.kind !== "reference") {
      // Inherited: the model's view, bindings in the model. Inline: this file's
      // query body, which the document does not model and cannot rebind.
      return {
         expression: `${tile.source} -> ${tile.name}`,
         givenNames: tile.declaration.kind === "inherited" ? undefined : [],
      };
   }
   // Run on the dashboard's OWN extension, not the model source it extends.
   // An extension inherits every view of its base, so `overview -> sales_by_month`
   // resolves; and it carries whatever else the extension declares — a
   // source-level `where:`, a `# drill` dimension — which the base does not.
   // Running on the base showed unscoped numbers for a dashboard that scopes
   // its source, and the saved page then differed from what the author watched.
   const on = tile.source;
   const localTypes = new Map(
      (document.localGivens ?? []).map((local) => [local.name, local.type]),
   );
   const sent: string[] = [];
   const clauses: string[] = [];
   for (const filter of tile.filters ?? []) {
      const comparison = `where: ${filter.field} ${filter.op ?? "~"}`;
      if (runnable.has(filter.given)) {
         sent.push(filter.given);
         clauses.push(`${comparison} $${filter.given}`);
         continue;
      }
      if (!localTypes.has(filter.given)) continue;
      const literal = malloyLiteral(
         values.get(filter.given),
         localTypes.get(filter.given),
      );
      if (literal !== undefined) clauses.push(`${comparison} ${literal}`);
   }
   const refinement = clauses.join(", ");
   return {
      expression:
         `${on} -> ${tile.declaration.from}` +
         (refinement ? ` + { ${refinement} }` : ""),
      givenNames: sent,
   };
}

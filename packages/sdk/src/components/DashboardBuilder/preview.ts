// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";

/**
 * What a reader would see if the DOCUMENT were the file: the controls, and
 * each tile's query — so the builder's live view follows an edit the moment it
 * is made, rather than the file on the server's disk.
 *
 * The live view used to run from that file. The control row was the server's
 * manifest and each tile ran `overview -> revenue_trend`, the view as saved,
 * with whatever bindings it had when the package loaded. So unbinding a tile
 * changed the document and, after a save, the text — and the live tile kept
 * its disk bindings and kept filtering; removing a control edited the
 * document, and the live filter box stayed the server's list. The edit looked
 * like it did nothing, because on screen it did nothing.
 *
 * Two functions, one per half of what a reader sees. Both are pure, so the
 * host can hold the live document and derive its preview from it, and both
 * are honest about the one thing a preview cannot do: a given the document
 * declares but the server's model does not yet know cannot be sent to it, so
 * a NEW control shows in the row and moves nothing until the file is saved.
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
 * A tile's query with the DOCUMENT's bindings, run against the model source
 * the dashboard's extension is built on.
 *
 * `overview -> revenue_trend` would run the view AS SAVED, bindings and all.
 * `order_items -> sales_by_month + { where: category ~ $CATEGORY }` is the same
 * view on the source it extends, refined by what the document binds now —
 * which is exactly the declaration the writer would produce, run before it is
 * written. Every reference tile has such a base: `view: x is base_view + …` can
 * only name a view of the source the extension extends.
 *
 * `runnable` is the set of givens the server's model declares. A binding to a
 * given outside it — one this document just declared — is left out of the
 * refinement and the request, because the server would refuse a given it does
 * not know; the control still shows, and takes effect once the file is saved.
 */
export function previewTileQuery(
   document: DashboardDocument,
   tile: DashboardTile,
   runnable: ReadonlySet<string>,
): PreviewTileQuery {
   if (tile.declaration.kind !== "reference") {
      // Inherited: the model's view, bindings in the model. Inline: this file's
      // query body, which the document does not model and cannot rebind.
      return {
         expression: `${tile.source} -> ${tile.name}`,
         givenNames: tile.declaration.kind === "inherited" ? undefined : [],
      };
   }
   const base =
      document.sources.find((source) => source.name === tile.source)?.base ??
      tile.source;
   const bindings = (tile.filters ?? []).filter((filter) =>
      runnable.has(filter.given),
   );
   const refinement = bindings
      .map(
         (filter) =>
            `where: ${filter.field} ${filter.op ?? "~"} $${filter.given}`,
      )
      .join(", ");
   return {
      expression:
         `${base} -> ${tile.declaration.from}` +
         (refinement ? ` + { ${refinement} }` : ""),
      givenNames: bindings.map((filter) => filter.given),
   };
}

/**
 * The controls the document shows that the server's model does not declare
 * yet: they render, and move nothing until the file is saved. For the host to
 * say so.
 */
export function unsavedControls(
   document: DashboardDocument,
   runnable: ReadonlySet<string>,
): string[] {
   return previewGivens(document, [])
      .map((given) => given.name as string)
      .filter((name) => !runnable.has(name));
}

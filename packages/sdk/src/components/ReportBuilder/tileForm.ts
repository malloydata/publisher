// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   isInlineStage,
   isQueryTile,
   tileKey,
   type DashboardDocument,
   type QueryTile,
} from "./document";

/**
 * Which of its two forms a tile the builder writes takes.
 *
 * A report the builder writes declares no source. A tile names a served view
 * directly, `"flights -> by_carrier"` (`inherited`), until it needs something
 * the view does not have: a chart, a colour, a title or subtitle, no border,
 * or a filter. Then it is a query of the report's own (`named`), whose `#`
 * tags carry those settings over the view's:
 *
 *     # viz=line
 *     query: by_carrier_tile is flights -> by_carrier + { where: origin_code ~ $ORIGIN }
 *
 * A tile's layout (width, row break, height, place) is on its entry in
 * `tiles=[…]` in either form, so a resize or a move never makes a query.
 *
 * When every setting is the view's again, it goes back to naming the view, and
 * the writer removes the query. Applied after every edit (the editor's
 * `normalize`), so the document is always in one of the two forms and the file
 * it saves reads back as the same document.
 */

/** Whether a tile asks for something its view does not have, which only a query of its own can say. */
export function needsQuery(tile: QueryTile): boolean {
   return (
      tile.label !== undefined ||
      tile.subtitle !== undefined ||
      tile.borderless === true ||
      (tile.chart !== undefined && tile.chart !== "default") ||
      tile.color !== undefined ||
      (tile.filters?.length ?? 0) > 0
   );
}

/**
 * A name for a new query of the report's own: `<view>_tile`, numbered past any
 * name a tile, source or given already has, since a `query:` shares the file's
 * top-level namespace with all of them.
 */
export function freeQueryName(
   document: DashboardDocument,
   view: string,
): string {
   const taken = new Set<string>([
      ...document.tiles.filter(isQueryTile).map((t) => t.name),
      ...document.sources.map((s) => s.name),
      ...(document.localGivens ?? []).map((g) => g.name),
   ]);
   let name = `${view}_tile`;
   for (let n = 2; taken.has(name); n++) name = `${view}_tile_${n}`;
   return name;
}

/**
 * Whether a query's name is one {@link freeQueryName} gives: `<view>_tile`, or
 * numbered. A query the author named is theirs, and may be read by more than
 * this report's list, so it is never taken out of the file; it only loses the
 * settings the builder wrote on it.
 */
const namedByBuilder = (name: string, view: string) =>
   name === `${view}_tile` ||
   (name.startsWith(`${view}_tile_`) &&
      /^\d+$/.test(name.slice(`${view}_tile_`.length)));

/** The `inherited` form of a `named` tile: the view it reads, with no setting of its own, laid out where the tile was. */
const viewTile = (tile: QueryTile & { source: string }, from: string) =>
   ({
      name: from,
      source: tile.source,
      declaration: { kind: "inherited" },
      ...(tile.colspan === undefined ? {} : { colspan: tile.colspan }),
      ...(tile.break ? { break: true } : {}),
      ...(tile.rowspan === undefined ? {} : { rowspan: tile.rowspan }),
      ...(tile.row === undefined ? {} : { row: tile.row }),
      ...(tile.col === undefined ? {} : { col: tile.col }),
   }) satisfies QueryTile;

/**
 * Put every tile this edit touched into the form its settings call for. A tile
 * the edit did not touch is left as the file has it, so opening and saving a
 * report never rewrites a tile nobody changed.
 *
 * A tile is promoted to a query when it needs one ({@link needsQuery}). Two
 * entries may name the same view: each carries its own layout, so they are
 * told apart by where they sit. A query goes back to naming its view only when
 * the builder named it, nothing else in it is the author's (`unmodelled`), and
 * it is not an inline stage.
 */
export function normalizeTileForms(
   draft: DashboardDocument,
   before: DashboardDocument,
): void {
   // Each tile as it was, by key, as a list: a tile added beside another that
   // shares its key (the same view, added twice) is new, not the first again.
   const was = new Map<string, string[]>();
   for (const tile of before.tiles) {
      const key = tileKey(tile);
      was.set(key, [...(was.get(key) ?? []), JSON.stringify(tile)]);
   }
   const touched = (tile: QueryTile) => {
      const prior = was.get(tileKey(tile)) ?? [];
      const at = prior.indexOf(JSON.stringify(tile));
      if (at < 0) return true;
      prior.splice(at, 1);
      return false;
   };
   draft.tiles.forEach((tile, index) => {
      if (!isQueryTile(tile) || !touched(tile) || tile.source === undefined)
         return;
      if (tile.declaration.kind === "inherited") {
         if (!needsQuery(tile)) return;
         draft.tiles[index] = {
            ...tile,
            name: freeQueryName(draft, tile.name),
            declaration: { kind: "named", from: tile.name },
         };
         return;
      }
      if (tile.declaration.kind === "named") {
         const from = tile.declaration.from;
         if (
            tile.unmodelled ||
            // An inline stage has no served view to go back to.
            isInlineStage(from) ||
            !namedByBuilder(tile.name, from) ||
            needsQuery(tile)
         )
            return;
         draft.tiles[index] = viewTile(
            tile as QueryTile & { source: string },
            from,
         );
      }
   });
}

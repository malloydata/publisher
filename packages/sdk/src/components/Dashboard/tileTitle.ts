// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { humanizeSlug } from "../drill";

/**
 * A composite tile's heading when the view it names carries no `# label`: the
 * last step's name as a sentence, so `scoped_sales -> sales_by_month` is titled
 * "Sales by month".
 *
 * The label itself is the same `# label` a nest gets in the single-query form,
 * read off the view server-side, so one view is titled identically whichever way
 * it is consumed. This derivation is the fallback for an untitled view, so a
 * tile shows a heading rather than code.
 */
export function tileTitle(tile: string): string {
   // A refinement (`by_carrier + { where: … }`) filters the view; the title is the view's.
   const lastStep = (tile.split("->").at(-1)?.trim() ?? tile)
      .replace(/\s*\+\s*\{[\s\S]*\}\s*$/, "")
      .trim();
   // Anything that is not a bare name (an inline `{ … }` stage, say) has no
   // sensible title to derive, so it keeps the expression as written.
   return /^[A-Za-z_][\w-]*$/.test(lastStep) ? humanizeSlug(lastStep) : tile;
}

/**
 * Whether `name` is the one the builder gives a view's tile when it turns the
 * tile into a query to carry a filter: `<view>_tile`, or `<view>_tile_N` when
 * that is taken. A query the author named is theirs, whatever it reads.
 */
export const isBuilderTileName = (name: string, view: string): boolean =>
   name === `${view}_tile` ||
   (name.startsWith(`${view}_tile_`) &&
      /^\d+$/.test(name.slice(`${view}_tile_`.length)));

/**
 * The heading of a tile with no `# label`. `view` is the view a named query
 * reads (`source -> view`), when that is known. A query the builder named for
 * that view is titled as the view is, so adding a filter to "Top brands" does
 * not retitle it "Top brands tile". Any other tile is titled from what its
 * entry names.
 */
export function fallbackTileTitle(entry: string, view?: string): string {
   const viewName = view?.split("->").at(-1)?.trim();
   return viewName !== undefined && isBuilderTileName(entry.trim(), viewName)
      ? tileTitle(view as string)
      : tileTitle(entry);
}

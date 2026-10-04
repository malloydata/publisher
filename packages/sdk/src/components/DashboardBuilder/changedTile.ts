// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { tileKey, type DashboardDocument } from "./document";

/**
 * The key of the first tile that differs between two states of a document: new
 * in `after`, edited, or moved. Undefined when no tile that still exists
 * changed (a title edit, or a tile that is gone), so there is nowhere to point.
 */
export function changedTileKey(
   before: DashboardDocument,
   after: DashboardDocument,
): string | undefined {
   const was = new Map(
      before.tiles.map((tile, index) => [tileKey(tile), { tile, index }]),
   );
   for (const [index, tile] of after.tiles.entries()) {
      const prior = was.get(tileKey(tile));
      if (
         !prior ||
         prior.index !== index ||
         JSON.stringify(prior.tile) !== JSON.stringify(tile)
      )
         return tileKey(tile);
   }
   return undefined;
}

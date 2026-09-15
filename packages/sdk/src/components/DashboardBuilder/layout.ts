// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GridTile } from "../Dashboard/DashboardGrid";
import { tileKey, type DashboardTile } from "./document";

/**
 * How the builder's grid is arranged: what a reorder does to the rows, and
 * where the empty end of a row is. Pure, so the gestures that call it can be
 * tested without a pointer.
 */
/**
 * Reordering keeps the ROW STRUCTURE and moves only the tiles through it.
 *
 * `# break` starts a fresh row. It is written as a tag on a tile, so the
 * obvious reading is that it belongs to the tile and should travel with it —
 * and that is what this did at first. The result was wrong in practice: drag
 * one of four half-width tiles and its `break` lands mid-row, forcing a new
 * row and leaving the half beside it empty. Four `colspan=6` tiles stopped
 * being a 2x2 the moment you moved one.
 *
 * The tag is positional in MEANING even though it is stored per tile: it
 * says "a row starts here", which is a fact about the grid, not about the
 * view that happens to sit there. So a move re-applies the break pattern by
 * POSITION, and the surrounding tiles close up behind the one that left.
 * Widths still travel with their tiles, because a width really is the tile's.
 */
export const keepRowStructure = (
   reordered: DashboardTile[],
   pattern: readonly boolean[],
) =>
   reordered.map((tile, index) => {
      const starts = pattern[index] ?? false;
      if (starts === (tile.break ?? false)) return tile;
      const next = { ...tile };
      if (starts) next.break = true;
      else delete next.break;
      return next;
   });

/**
 * Moving a tile INTO A GAP — the empty end of a row — is the one move that
 * must change the row structure, because the gap IS the row structure.
 *
 * A gap exists for one reason: the tile after the row's last tile carries a
 * `# break`, so it starts a fresh row instead of filling the space. Dropping a
 * tile into that space says, as plainly as a gesture can, "this belongs up
 * here" — and {@link keepRowStructure} would refuse it: re-applying the break
 * by position hands the break to the moved tile, which then starts its own row
 * and leaves the gap exactly where it was. Measured on the storefront overview
 * with the trend narrowed to half width: the map could not be dragged up
 * beside it at all.
 *
 * So a gap drop lets the break TRAVEL instead: the moved tile arrives with
 * none, so it flows into the row; the tile that started the next row keeps its
 * break and still does. And the row the tile LEFT closes up — if it was that
 * row's first tile, the tile after it starts the row now, which is what the
 * positional rule would have done there too.
 *
 * `to` is where the tile lands in the array it is spliced back into, after
 * `from` has been removed — the same convention the tile-target move uses.
 */
export const moveIntoGap = (
   tiles: readonly DashboardTile[],
   from: number,
   to: number,
) => {
   const next = [...tiles];
   const [moved] = next.splice(from, 1);
   if (!moved) return [...tiles];
   // The tile that followed the one leaving, now at `from`, inherits a row
   // start it did not have — the row must still begin somewhere.
   const follower = next[from];
   if (moved.break && follower && !follower.break)
      next[from] = { ...follower, break: true };
   const placed = { ...moved };
   delete placed.break;
   next.splice(to, 0, placed);
   return next;
};

/** One thing the grid lays out: a tile, or the empty end of a row. */
export type GridEntry = GridTile &
   (
      | {
           kind: "tile";
           tile: DashboardTile;
           /** Its place among the TILES — what a sortable item is told. */
           index: number;
        }
      | { kind: "gap"; after: string }
   );

export const tileEntry = (tile: DashboardTile, index: number): GridEntry => ({
   kind: "tile",
   tile,
   index,
   colspan: tile.colspan,
   break: tile.break,
});

/** A gap's id: its drop target's, and the grid's key for it. */
export const gapId = (after: string) => `gap.${after}`;

/**
 * The grid's entries with a drop target in every gap.
 *
 * A flow grid has no element where a row runs out; the space is simply not
 * painted, so there is nothing for a drag to be "over". This walks the tiles
 * as the grid will place them — `# break` starts a row, and so does a tile too
 * wide for what is left of the current one — and wherever a row ends short of
 * the last column, lays a gap entry spanning exactly the columns left over. It
 * fills space that was already empty, so adding it moves nothing; and it
 * carries the key of the tile it follows, which is what a drop on it needs.
 */
export const withGaps = (
   tiles: readonly DashboardTile[],
   columns: number,
): GridEntry[] => {
   const entries: GridEntry[] = [];
   let used = 0;
   tiles.forEach((tile, index) => {
      const span = Math.min(tile.colspan ?? 1, columns);
      const startsRow =
         index === 0 || tile.break === true || used + span > columns;
      if (startsRow && index > 0 && used < columns)
         entries.push({
            kind: "gap",
            after: tileKey(tiles[index - 1]),
            colspan: columns - used,
         });
      if (startsRow) used = 0;
      entries.push(tileEntry(tile, index));
      used += span;
   });
   const last = tiles.at(-1);
   if (last && used < columns)
      entries.push({
         kind: "gap",
         after: tileKey(last),
         colspan: columns - used,
      });
   return entries;
};

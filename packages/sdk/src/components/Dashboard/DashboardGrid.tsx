// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { ReactNode } from "react";

/** Grid width when the dashboard declares no `# dashboard { columns=N }`. */
export const DEFAULT_COLUMNS = 2;

/**
 * The gutter between tiles, in px.
 *
 * A number rather than a `gap: 2` spacing unit because the BUILDER has to do
 * arithmetic with it: turning a dragged edge into a column count means solving
 * for the track width, and that needs the gutter in the same units as a
 * `getBoundingClientRect`. Exported so the value the grid paints and the value
 * the drag solves with cannot drift apart.
 */
export const GRID_GAP_PX = 16;

/**
 * The `grid-column` one tile occupies: its `# colspan`, and a `# break` forcing
 * it to start a fresh row.
 *
 * Clamped to the grid width the same way @malloydata/render clamps it, so one
 * view laid out as a composite tile and as a `nest:` under `# dashboard` lands
 * in the same place. A break is `1 / span N` — an explicit start line, which is
 * what pushes the tile down to the next row; the renderer's grid does the same.
 */
export function tileGridColumn(
   tile: { colspan?: number; break?: boolean },
   columns: number,
): string {
   const span = Math.min(tile.colspan ?? 1, columns);
   return tile.break ? `1 / span ${span}` : `span ${span}`;
}

/** The layout a tile carries, whatever else its own shape holds. */
export interface GridTile {
   colspan?: number;
   break?: boolean;
}

/**
 * The composite dashboard's grid: the column track, and each tile's place on
 * it.
 *
 * Shared by the READER ({@link Dashboard}) and the BUILDER, which is the whole
 * point of it being a component rather than a rule each surface applies. An
 * author arranging a dashboard is arranging the thing a reader will open, so
 * the two cannot be allowed to lay tiles out even slightly differently; a
 * restated track without the grid item's own `display: grid` ends rows at
 * ragged heights on one surface and level on the other.
 *
 * **Why the item is a grid and not a block.** A grid item stretches to its row,
 * but a block CHILD of one does not — it keeps its content height. Results
 * divide on exactly that line (see `resultSizing`): a table, a map or a
 * `# big_value` sizes to its content, while a plotted chart fills whatever box
 * it is handed. So a block item put a short table beside a tall chart and let
 * the row end twice. Making the item a grid passes the row's height down to the
 * tile, and every card in a row ends level.
 *
 * The tile itself is the caller's: the reader renders a `DashboardTile`, the
 * builder wraps one in its selection outline. Anything this component rendered
 * on their behalf would be a third opinion about what a tile looks like.
 */
export function DashboardGrid<T extends GridTile>({
   tiles,
   columns,
   keyOf,
   renderTile,
}: {
   tiles: readonly T[];
   /** Track count — `# dashboard { columns=N }`, or {@link DEFAULT_COLUMNS}. */
   columns: number;
   /**
    * This tile's React key. Taken from the caller because the two surfaces
    * identify a tile differently and both are right: the manifest's `tiles=[…]`
    * can repeat one expression (a typo, not a request for two identical
    * panels), so the reader keys on position as well.
    */
   keyOf: (tile: T, index: number) => string;
   renderTile: (tile: T, index: number) => ReactNode;
}) {
   return (
      <Box
         sx={{
            display: "grid",
            gridTemplateColumns: {
               xs: "1fr",
               md: `repeat(${columns}, minmax(0, 1fr))`,
            },
            gap: `${GRID_GAP_PX}px`,
         }}
      >
         {tiles.map((tile, index) => (
            <Box
               key={keyOf(tile, index)}
               sx={{
                  // Load-bearing — see the note above.
                  display: "grid",
                  // A tile follows its TRACK, whatever it holds. A grid item's
                  // minimum width is `auto` — its content's minimum — and a
                  // chart's content is an SVG drawn at the width the tile had
                  // when it rendered. So without this, narrowing a tile did not
                  // narrow it: the item stayed as wide as the old chart, ran
                  // under its neighbour, and the renderer's own size observer
                  // never saw a change to redraw for. Widening worked, which is
                  // what made it look like the chart filled in one direction
                  // only. With the minimum at zero the item takes the track's
                  // width, the chart's box shrinks with it, and the renderer
                  // redraws to fit — the same way it already did on growth.
                  minWidth: 0,
                  // Only above `md`: the narrow breakpoint is one column, where
                  // a span would overflow the grid rather than widen anything.
                  gridColumn: { md: tileGridColumn(tile, columns) },
               }}
            >
               {renderTile(tile, index)}
            </Box>
         ))}
      </Box>
   );
}

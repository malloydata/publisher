// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportLayout } from "../Package/documentLocation";
import { DEFAULT_COLUMNS } from "../Dashboard/DashboardGrid";
import {
   LINEAR_TABLE_MAX_HEIGHT,
   TILE_MAX_HEIGHT,
} from "../RenderedResult/resultSizing";

/**
 * What a report's layout decides, and nothing else.
 *
 * A linear report and a grid draw the same page (header, filter row, cards,
 * prose) through the same components; the layout only constrains how tiles
 * are arranged on it. Every surface that arranges tiles (the reader's grid,
 * the builder's grid, adding a tile) asks this one function rather than
 * branching on the layout itself, so a new constraint is added here once and
 * the two layouts cannot drift apart in how they look.
 */
export interface LayoutRules {
   layout: ReportLayout;
   /** Columns the grid lays tiles across: one for linear, whatever it says, as the server reads it. */
   columns: number;
   /**
    * Whether a tile has a width of its own: dragged at its edge, stepped with
    * the arrow keys, written as `# colspan`. Off in one column, where every
    * tile is the column's width and a width in the file is not the builder's
    * to rewrite.
    */
   widths: boolean;
   /**
    * Whether tiles are added between others: a "+" under each tile and an
    * "Add tile" after the last. A top-to-bottom report is written by
    * inserting; a grid is arranged by dragging, so it adds at the end.
    */
   insertBetween: boolean;
   /**
    * The cap on a query tile's result, or undefined for none. A grid keeps its
    * rows even, so a tile is one panel among several: it stops at
    * {@link TILE_MAX_HEIGHT} and scrolls past it. A linear report is read top
    * to bottom and nothing beside a tile sets its height, so a tile grows to
    * fit what it holds, the way the single-query form already does: a row of
    * KPI cards that wraps one per line in a narrow column shows every card. A
    * table is the exception, held by {@link tableMaxHeight}. A
    * chart, which has no height of its own, still draws at the uncapped
    * default (see `resolveResultHeight`).
    */
   tileMaxHeight: number | undefined;
   /**
    * A further cap on a tile whose result is a table, or undefined for none
    * beyond {@link tileMaxHeight}. A linear report's tiles grow to fit, but a
    * long table at full length pushes the rest of the report a screen or more
    * down, so a table stops at {@link LINEAR_TABLE_MAX_HEIGHT} and scrolls
    * inside its tile under its pinned header row. A grid needs none: its tile
    * cap is lower. See `resultCap`.
    */
   tableMaxHeight: number | undefined;
}

/**
 * The `# colspan` to write for a tile `span` columns wide, or undefined to
 * write none because the tile draws the same without one.
 *
 * A tile with no colspan is one column, except that the reader widens it until
 * it is at least `MIN_TILE_PX` wide (`defaultTileSpan`). In a grid of one or two
 * columns, one column is already that wide wherever the grid is laid out in
 * columns (two columns of 240px need a box of 496px). So there a written
 * `colspan=1` only repeats the default. In a wider grid an unset tile can be
 * drawn wider than one column, and an explicit 1 is the author's choice to keep.
 *
 * `drawnWithout` is the span the caller draws an unset tile at now, when it
 * measures one (the builder does): a width the author just set to 1 is still
 * written while the tile would draw wider without it.
 */
export function writtenSpan(
   span: number,
   columns: number,
   drawnWithout = 1,
): number | undefined {
   return span === 1 && columns <= 2 && drawnWithout === 1 ? undefined : span;
}

/** Set a tile's width as {@link writtenSpan} would write it: a `colspan`, or none. */
export function setSpan(
   tile: { colspan?: number },
   span: number,
   columns: number,
   drawnWithout = 1,
): void {
   const colspan = writtenSpan(span, columns, drawnWithout);
   if (colspan === undefined) delete tile.colspan;
   else tile.colspan = colspan;
}

export function layoutRules(
   layout: ReportLayout | undefined,
   /** `# dashboard { columns=N }` as the file has it. */
   declaredColumns?: number,
): LayoutRules {
   if (layout === "linear")
      return {
         layout: "linear",
         columns: 1,
         widths: false,
         insertBetween: true,
         tileMaxHeight: undefined,
         tableMaxHeight: LINEAR_TABLE_MAX_HEIGHT,
      };
   const columns = declaredColumns ?? DEFAULT_COLUMNS;
   return {
      layout: "grid",
      columns,
      widths: columns > 1,
      insertBetween: false,
      tileMaxHeight: TILE_MAX_HEIGHT,
      tableMaxHeight: undefined,
   };
}

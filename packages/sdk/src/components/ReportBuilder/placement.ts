// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   GRID_GAP_PX,
   MIN_TEXT_TILE_ROWS,
   MIN_TILE_ROWS,
   PLACED_DEFAULT_ROWSPAN,
   TILE_ROW_PX,
} from "../Dashboard/DashboardGrid";
import { isTextTile, type DashboardTile } from "./document";

/**
 * Free placement on a grid report: each tile at its own `row` and `col`, so
 * empty space anywhere on the grid is somewhere a tile can go.
 *
 * Pure: what a move or a resize does to the tiles, never how the gesture
 * feels. A move or resize keeps the tile it changed where it was put and
 * pushes whatever it now covers straight down, the way the grid dashboards
 * people already know behave. Nothing is pulled up to close a gap, since an
 * empty cell is a choice the author made.
 */

/** A tile's rectangle on the grid, in rows and columns counted from 1. */
export interface Cell {
   row: number;
   col: number;
   colspan: number;
   rowspan: number;
}

/** The fewest rows the builder offers a tile: a chart needs room to draw, prose does not. */
export const minRowsOf = (tile: DashboardTile) =>
   isTextTile(tile) ? MIN_TEXT_TILE_ROWS : MIN_TILE_ROWS;

/** Rows a tile takes on a placed report: its own, or its kind's default. */
export const rowsOf = (tile: DashboardTile) =>
   tile.rowspan ?? PLACED_DEFAULT_ROWSPAN[isTextTile(tile) ? "text" : "query"];

/** A placed tile's rectangle, held to the grid; undefined for one not placed yet. */
export function cellOf(tile: DashboardTile, columns: number): Cell | undefined {
   if (tile.row === undefined || tile.col === undefined) return undefined;
   const colspan = Math.min(tile.colspan ?? 1, columns);
   return {
      row: tile.row,
      col: Math.min(tile.col, columns - colspan + 1),
      colspan,
      rowspan: rowsOf(tile),
   };
}

const overlaps = (a: Cell, b: Cell) =>
   a.col < b.col + b.colspan &&
   b.col < a.col + a.colspan &&
   a.row < b.row + b.rowspan &&
   b.row < a.row + a.rowspan;

/** Top to bottom, then left to right: the order a placed report is read in, and written. */
export function inReadingOrder(
   tiles: readonly DashboardTile[],
): DashboardTile[] {
   return tiles
      .map((tile, index) => ({ tile, index }))
      .sort(
         (a, b) =>
            (a.tile.row ?? Infinity) - (b.tile.row ?? Infinity) ||
            (a.tile.col ?? Infinity) - (b.tile.col ?? Infinity) ||
            a.index - b.index,
      )
      .map(({ tile }) => tile);
}

/**
 * The tiles with nothing covering anything: `fixed` (the tile just moved or
 * resized) stays exactly where it is, and every placed tile it, or a tile it
 * pushed, now covers moves straight down to just below what it hit. Tiles
 * settle top to bottom, so a pushed tile pushes the ones under it in turn.
 * Without `fixed`, the first tile in reading order holds its place.
 */
export function settle(
   tiles: readonly DashboardTile[],
   columns: number,
   fixed?: number,
): DashboardTile[] {
   const out = tiles.map((tile) => ({ ...tile }));
   const held: Cell[] = [];
   const hold = (index: number) => {
      const cell = cellOf(out[index], columns);
      if (!cell) return;
      // Pulled onto the grid if it was hanging off its right edge.
      out[index].col = cell.col;
      held.push(cell);
   };
   if (fixed !== undefined) hold(fixed);
   const rest = out
      .map((tile, index) => ({ tile, index }))
      .filter(({ index, tile }) => index !== fixed && tile.row !== undefined)
      .sort(
         (a, b) =>
            (a.tile.row ?? 0) - (b.tile.row ?? 0) ||
            (a.tile.col ?? 0) - (b.tile.col ?? 0) ||
            a.index - b.index,
      );
   for (const { index } of rest) {
      const cell = cellOf(out[index], columns);
      if (!cell) continue;
      for (;;) {
         const hit = held.filter((other) => overlaps(cell, other));
         if (hit.length === 0) break;
         cell.row = Math.max(...hit.map((other) => other.row + other.rowspan));
      }
      out[index].row = cell.row;
      hold(index);
   }
   return out;
}

/**
 * The tiles with the one at `index` dropped at `at`. A drop onto the lower
 * part of a tile that starts higher up lands just under that tile, rather
 * than shoving it beneath the dropped one; a drop at or above a tile's top
 * pushes that tile down. Then everything settles around where it landed.
 */
export function dropAt(
   tiles: readonly DashboardTile[],
   index: number,
   at: { row: number; col: number },
   columns: number,
): DashboardTile[] {
   const others = tiles.flatMap((tile, i) => {
      const cell = i === index ? undefined : cellOf(tile, columns);
      return cell ? [cell] : [];
   });
   // Never lower than right under the lowest other tile: space below
   // everything holds nothing, and a drag held at the window's bottom edge
   // would otherwise grow the page, and scroll it, without end.
   const lowest = Math.max(1, ...others.map((cell) => cell.row + cell.rowspan));
   const moved = { ...tiles[index], ...at, row: Math.min(at.row, lowest) };
   const cell = cellOf(moved, columns);
   if (cell) {
      for (;;) {
         const above = others.filter(
            (other) => other.row < cell.row && overlaps(cell, other),
         );
         if (above.length === 0) break;
         cell.row = Math.max(
            ...above.map((other) => other.row + other.rowspan),
         );
      }
      moved.row = cell.row;
   }
   return settle(
      tiles.map((tile, i) => (i === index ? moved : tile)),
      columns,
      index,
   );
}

/** A tile's box against the grid's top left corner, in px. */
export interface Box {
   top: number;
   left: number;
   width: number;
   height: number;
}

/** One column track's width in a grid `gridWidth` px wide. */
export const trackWidth = (gridWidth: number, columns: number) =>
   (gridWidth - GRID_GAP_PX * (columns - 1)) / columns;

const ROW_STEP = TILE_ROW_PX + GRID_GAP_PX;

/**
 * The cell a tile of `colspan` columns lands in when its top left corner is
 * at (`left`, `top`) px from the grid's: the nearest row and column, held on
 * the grid.
 */
export function cellAt(
   left: number,
   top: number,
   colspan: number,
   columns: number,
   track: number,
): { row: number; col: number } {
   const span = Math.min(colspan, columns);
   const col = Math.round(left / (track + GRID_GAP_PX)) + 1;
   return {
      row: Math.max(1, Math.round(top / ROW_STEP) + 1),
      col: Math.min(Math.max(col, 1), columns - span + 1),
   };
}

/**
 * The tiles placed where they are drawn now, the first time a report is
 * placed: each tile's row, column, width and height read off its box, so
 * turning a flowing report into a placed one moves nothing. A tile with no
 * box (not drawn) is left for the grid to place.
 */
export function placeWhereDrawn(
   tiles: readonly DashboardTile[],
   boxes: ReadonlyArray<Box | undefined>,
   columns: number,
   track: number,
): DashboardTile[] {
   const placed = tiles.map((tile, index) => {
      if (tile.row !== undefined && tile.col !== undefined) return tile;
      const box = boxes[index];
      if (!box) return tile;
      const { break: _break, ...rest } = tile;
      const colspan =
         tile.colspan ??
         Math.max(
            1,
            Math.round((box.width + GRID_GAP_PX) / (track + GRID_GAP_PX)),
         );
      return {
         ...rest,
         ...cellAt(box.left, box.top, colspan, columns, track),
         colspan,
         rowspan:
            tile.rowspan ??
            Math.max(
               minRowsOf(tile),
               // Up, never down: a tile is never placed shorter than it is
               // drawn, so nothing it shows is cut or starts to scroll. A
               // hair of slack keeps a tile already on whole rows where it is.
               Math.ceil((box.height + GRID_GAP_PX) / ROW_STEP - 0.02),
            ),
      };
   });
   return settle(placed, columns);
}

/**
 * The tiles with the one at `index` placed under everything else, at the
 * left, when the report is placed: where a new tile goes, so it never drops
 * into a gap an author left on purpose. A report that flows is left as it is.
 */
export function placeAtEnd(
   tiles: readonly DashboardTile[],
   index: number,
   columns: number,
): DashboardTile[] {
   const others = tiles.flatMap((tile, i) => {
      const cell = i === index ? undefined : cellOf(tile, columns);
      return cell ? [cell] : [];
   });
   if (others.length === 0) return [...tiles];
   const bottom = Math.max(...others.map((cell) => cell.row + cell.rowspan));
   return tiles.map((tile, i) =>
      i === index ? { ...tile, row: bottom, col: 1 } : tile,
   );
}

/**
 * The tiles slid up as far as each will go, top to bottom and then left to
 * right, so every gap above a tile closes; nothing moves sideways. A tile
 * with no place is left for the grid to place.
 */
export function pack(
   tiles: readonly DashboardTile[],
   columns: number,
): DashboardTile[] {
   const out = tiles.map((tile) => ({ ...tile }));
   const held: Cell[] = [];
   const order = out
      .map((tile, index) => ({ tile, index }))
      .filter(({ tile }) => tile.row !== undefined && tile.col !== undefined)
      .sort(
         (a, b) =>
            (a.tile.row ?? 0) - (b.tile.row ?? 0) ||
            (a.tile.col ?? 0) - (b.tile.col ?? 0) ||
            a.index - b.index,
      );
   for (const { index } of order) {
      const cell = cellOf(out[index], columns);
      if (!cell) continue;
      while (
         cell.row > 1 &&
         !held.some((other) => overlaps({ ...cell, row: cell.row - 1 }, other))
      )
         cell.row -= 1;
      out[index].row = cell.row;
      out[index].col = cell.col;
      held.push(cell);
   }
   return out;
}

/** Whether packing would move anything: the action is offered only then. */
export const hasGaps = (tiles: readonly DashboardTile[], columns: number) =>
   pack(tiles, columns).some((tile, index) => tile.row !== tiles[index].row);

/**
 * The tiles with their places taken off, so the report flows again in the
 * order it is read. Widths and heights stay; a row break means nothing to a
 * placed report, so none is set.
 */
export function flowTiles(tiles: readonly DashboardTile[]): DashboardTile[] {
   return inReadingOrder(tiles).map((tile) => {
      const { row: _row, col: _col, ...rest } = tile;
      return rest as DashboardTile;
   });
}

/** The tiles with the one at `index` set to `at`, nothing else changed: a keyboard step, before it settles. */
export const stepTo = (
   tiles: readonly DashboardTile[],
   index: number,
   at: { row: number; col: number },
): DashboardTile[] =>
   tiles.map((tile, i) => (i === index ? { ...tile, ...at } : tile));

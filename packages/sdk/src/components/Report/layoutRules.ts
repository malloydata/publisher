// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportLayout } from "../Package/documentLocation";
import { DEFAULT_COLUMNS } from "../Dashboard/DashboardGrid";

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
      };
   const columns = declaredColumns ?? DEFAULT_COLUMNS;
   return {
      layout: "grid",
      columns,
      widths: columns > 1,
      insertBetween: false,
   };
}

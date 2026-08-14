import { describe, expect, it } from "bun:test";
import { DRILL_CELL_CLASS, markDrillableCells } from "./markDrillableCells";

/**
 * The DOM half of the marking, which the sibling spec deliberately does not
 * cover. Added because a guard written against rendered TEXT missed the case it
 * existed for: the renderer draws a null cell as the glyph `∅` inside a
 * `.value-null` span, so its `textContent` is not empty and every null cell
 * stayed marked as a link whose click `resolveDrill` then refused.
 */
describe("markDrillableCells and cells with no value", () => {
   /** The renderer's own shape: a grid table with an inline grid-column per cell. */
   const table = (cells: string[]) => {
      const root = document.createElement("div");
      root.innerHTML = `
         <div class="malloy-table">
            <div class="column-cell th" style="grid-column: 1 / 2;">region</div>
            ${cells.join("")}
         </div>`;
      return root;
   };
   const cell = (inner: string) =>
      `<div class="column-cell td" style="grid-column: 1 / 2;">${inner}</div>`;

   it("marks a cell with a value and skips a null one", () => {
      const root = table([
         cell("West"),
         cell('<span class="value-null">∅</span>'),
         cell(""),
         cell("0"),
      ]);
      markDrillableCells(root, new Set(["region"]));

      const marked = Array.from(
         root.querySelectorAll<HTMLElement>(".column-cell.td"),
      ).map((node) => node.classList.contains(DRILL_CELL_CLASS));
      // West and 0 are values; the null glyph and the empty cell are not.
      expect(marked).toEqual([true, false, false, true]);
   });
});

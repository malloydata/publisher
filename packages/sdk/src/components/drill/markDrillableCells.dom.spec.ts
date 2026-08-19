import { describe, expect, it } from "bun:test";
import {
   DRILL_CELL_CLASS,
   drillColumnSiblings,
   markDrillableCells,
} from "./markDrillableCells";

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

describe("markDrillableCells and the tab order", () => {
   const table = (rows: number) => {
      const root = document.createElement("div");
      const cells = Array.from(
         { length: rows },
         (_unused, index) =>
            `<div class="column-cell td" style="grid-column: 1 / 2;">v${index}</div>` +
            `<div class="column-cell td" style="grid-column: 2 / 3;">n${index}</div>`,
      ).join("");
      root.innerHTML = `
         <div class="malloy-table">
            <div class="column-cell th" style="grid-column: 1 / 2;">region</div>
            <div class="column-cell th" style="grid-column: 2 / 3;">sales</div>
            ${cells}
         </div>`;
      return root;
   };
   const stops = (root: HTMLElement) =>
      Array.from(root.querySelectorAll<HTMLElement>(`.${DRILL_CELL_CLASS}`))
         .filter((node) => node.getAttribute("tabindex") === "0")
         .map((node) => node.textContent);

   // Marking every cell tabbable put a whole column in the tab order, so a
   // result at the row cap cost a keyboard reader hundreds of presses to get
   // past it. One stop per column, arrow keys within.
   it("gives a drillable column one tab stop, not one per cell", () => {
      const root = table(12);
      markDrillableCells(root, new Set(["region"]));

      const marked = root.querySelectorAll(`.${DRILL_CELL_CLASS}`);
      expect(marked.length).toBe(12);
      expect(stops(root)).toEqual(["v0"]);
      // The rest are reachable by arrow key, so they are focusable but skipped.
      const rest = Array.from(marked).slice(1);
      expect(rest.every((n) => n.getAttribute("tabindex") === "-1")).toBe(true);
      // The non-drillable column is left alone entirely.
      expect(
         root.querySelectorAll('[style*="grid-column: 2"][tabindex]').length,
      ).toBe(0);
   });

   // A `# dashboard` settles over several frames, so the caller re-runs the
   // marking. Rebuilding the stop from scratch each pass would snap it back to
   // the first row under a reader who had arrowed down.
   it("keeps the roving position across a re-run", () => {
      const root = table(5);
      markDrillableCells(root, new Set(["region"]));

      const cells = Array.from(
         root.querySelectorAll<HTMLElement>(`.${DRILL_CELL_CLASS}`),
      );
      // Move the stop the way the arrow handler does.
      cells[0].tabIndex = -1;
      cells[3].tabIndex = 0;

      markDrillableCells(root, new Set(["region"]));
      expect(stops(root)).toEqual(["v3"]);
   });

   it("reports a column's marked cells in row order", () => {
      const root = table(4);
      markDrillableCells(root, new Set(["region"]));
      const first = root.querySelector<HTMLElement>(`.${DRILL_CELL_CLASS}`);
      expect(first).not.toBeNull();
      expect(
         drillColumnSiblings(first as HTMLElement).map((n) => n.textContent),
      ).toEqual(["v0", "v1", "v2", "v3"]);
   });

   it("reports nothing for a cell that is not in a table", () => {
      const orphan = document.createElement("div");
      orphan.className = DRILL_CELL_CLASS;
      expect(drillColumnSiblings(orphan)).toEqual([]);
   });
});

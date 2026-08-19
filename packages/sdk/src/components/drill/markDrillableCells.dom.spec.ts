import { describe, expect, it } from "bun:test";
import {
   DRILL_CELL_CLASS,
   drillColumnSiblings,
   moveDrillStop,
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

describe("markDrillableCells across two drillable columns", () => {
   // The stop is per drillable COLUMN, scoped to its table, not per table. The
   // guide said "one stop per TABLE" for two rounds, which is wrong the moment a
   // dashboard groups by two drilled dimensions.
   it("gives each drillable column its own stop", () => {
      const root = document.createElement("div");
      root.innerHTML = `
         <div class="malloy-table">
            <div class="column-cell th" style="grid-column: 1 / 2;">category</div>
            <div class="column-cell th" style="grid-column: 2 / 3;">brand</div>
            <div class="column-cell th" style="grid-column: 3 / 4;">sales</div>
            <div class="column-cell td" style="grid-column: 1 / 2;">c0</div>
            <div class="column-cell td" style="grid-column: 2 / 3;">b0</div>
            <div class="column-cell td" style="grid-column: 3 / 4;">1</div>
            <div class="column-cell td" style="grid-column: 1 / 2;">c1</div>
            <div class="column-cell td" style="grid-column: 2 / 3;">b1</div>
            <div class="column-cell td" style="grid-column: 3 / 4;">2</div>
         </div>`;
      markDrillableCells(root, new Set(["category", "brand"]));

      const stops = Array.from(
         root.querySelectorAll<HTMLElement>(`.${DRILL_CELL_CLASS}`),
      )
         .filter((n) => n.getAttribute("tabindex") === "0")
         .map((n) => n.textContent);
      // One per column, both on the first row, and the undrilled column
      // contributes none.
      expect(stops).toEqual(["c0", "b0"]);
      expect(root.querySelectorAll(`.${DRILL_CELL_CLASS}`).length).toBe(4);
   });
});

describe("moveDrillStop keeps a column to exactly one tab stop", () => {
   const table = (rows: number) => {
      const root = document.createElement("div");
      const cells = Array.from(
         { length: rows },
         (_unused, index) =>
            `<div class="column-cell td" style="grid-column: 1 / 2;">v${index}</div>`,
      ).join("");
      root.innerHTML = `
         <div class="malloy-table">
            <div class="column-cell th" style="grid-column: 1 / 2;">region</div>
            ${cells}
         </div>`;
      markDrillableCells(root, new Set(["region"]));
      return root;
   };
   const marked = (root: HTMLElement) =>
      Array.from(root.querySelectorAll<HTMLElement>(`.${DRILL_CELL_CLASS}`));
   const stops = (root: HTMLElement) =>
      marked(root)
         .filter((n) => n.getAttribute("tabindex") === "0")
         .map((n) => n.textContent);

   // The case both earlier tests missed, because each simulated the move by
   // hand-setting tabIndex on the cell that already held the stop. A cell with
   // `tabindex="-1"` is still CLICK-focusable, so the cell an arrow moves from
   // is frequently not the one holding the stop.
   it("moves from a click-focused cell without leaving a second stop", () => {
      const root = table(6);
      const cells = marked(root);
      expect(stops(root)).toEqual(["v0"]);

      // The reader clicks row 3. Focus is there; the stop is still on row 0.
      const next = moveDrillStop(cells[3], 1);

      expect(next?.textContent).toBe("v4");
      expect(stops(root)).toEqual(["v4"]);
   });

   it("repeating that gesture cannot accumulate stops", () => {
      const root = table(6);
      const cells = marked(root);
      moveDrillStop(cells[3], 1);
      moveDrillStop(cells[1], 1);
      moveDrillStop(cells[5], -1);
      expect(stops(root)).toEqual(["v4"]);
   });

   it("a clamped move at the end repairs a column rather than doing nothing", () => {
      const root = table(4);
      const cells = marked(root);
      // Drift the column into two stops by hand, the state the old move left.
      cells[2].tabIndex = 0;
      expect(stops(root)).toEqual(["v0", "v2"]);

      moveDrillStop(cells[3], 1); // already last, so it clamps to itself
      expect(stops(root)).toEqual(["v3"]);
   });

   it("reports nothing for a cell outside a table", () => {
      const orphan = document.createElement("div");
      orphan.className = DRILL_CELL_CLASS;
      expect(moveDrillStop(orphan, 1)).toBeUndefined();
   });

   // Belt and braces: even if some other path drifts a column, the next mark
   // pass demotes the extra rather than preserving both for good.
   it("a re-mark demotes a duplicate stop instead of keeping it", () => {
      const root = table(4);
      const cells = marked(root);
      cells[2].tabIndex = 0;
      expect(stops(root)).toEqual(["v0", "v2"]);

      markDrillableCells(root, new Set(["region"]));
      expect(stops(root)).toEqual(["v0"]);
   });
});

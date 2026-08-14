/**
 * Making a drillable cell *look* drillable.
 *
 * Resolution (`resolveDrill.ts`) answers what a click means; this answers the
 * question that comes first, from the reader's side, which cells can be clicked
 * at all. Without it a `# drill` is a hidden feature: the cells that navigate
 * and the cells that do nothing render identically, and the only way to find one
 * is to click at random.
 *
 * Ported from Malloyyo's `frame-runtime/drill.ts`, class name aside, so the same
 * tagged model reads the same way in both: a drillable cell is ordinary text
 * with a pointer cursor until hovered, at which point it reads as a link. The
 * styling lives with Publisher's other renderer overrides in
 * `RenderedResult.tsx`; this module only marks the DOM.
 *
 * Browser-only, and deliberately outside React: the cells belong to
 * `@malloydata/render`, which offers no per-cell hook.
 */

import type { DrillField } from "./resolveDrill";

/**
 * Marks a leaf value cell whose column declares an honorable `# drill`.
 * Malloyyo's counterpart is `dash-drill`; the name is local to each app's
 * stylesheet, and only the tag grammar is a compatibility surface.
 */
export const DRILL_CELL_CLASS = "publisher-drill";

/** As much of the renderer's viz handle as the affordance reads. */
export interface DrillMetadataSource {
   getMetadata: () => { getAllFields: () => DrillField[] } | null | undefined;
}

/**
 * The column names to mark: every field whose `# drill` this surface can honor.
 *
 * Both the field's own name and its `# label` go in, because the header cell
 * this is matched against renders `getLabel()`: the label when the author set
 * one, the name otherwise, and the caller cannot tell which from the DOM.
 *
 * `canDrill` is the host's capability filter (see `useDrill`), so a destination
 * the surface cannot reach never becomes a link: a dead link is worse than none.
 */
export function drillableFieldNames(
   viz: DrillMetadataSource,
   canDrill: (field: DrillField) => boolean,
): Set<string> {
   const names = new Set<string>();
   let fields: DrillField[];
   try {
      fields = viz.getMetadata()?.getAllFields() ?? [];
   } catch {
      // Metadata unavailable: no affordance, but clicks still resolve.
      return names;
   }
   // Header text that a NON-drillable field renders. A header shows the `# label`
   // when the author set one and the field name otherwise, so that is the one
   // spelling each of these can put on screen.
   const taken = new Set<string>();
   for (const field of fields) {
      if (!field || canDrill(field)) continue;
      taken.add(field.tag?.text("label") ?? field.name);
   }

   for (const field of fields) {
      if (!field || !canDrill(field)) continue;
      // The ONE spelling this field can render, by the same rule `taken` uses
      // above. Adding the raw `name` as well whenever a field had a label put a
      // key in here that this field can never put on screen, so its only
      // possible effect was to match some OTHER column whose header happened to
      // equal this field's name, and paint that column as a link that does
      // nothing. The two loops now agree about what a header shows.
      //
      // A spelling some non-drillable field also renders is dropped rather than
      // marked. Matching on text cannot tell those two columns apart, and this
      // file would rather lose an affordance than paint a dead link, which is
      // the same reason `canDrill` filters at all.
      const rendered = field.tag?.text("label") ?? field.name;
      if (!taken.has(rendered)) names.add(rendered);
      // KNOWN LIMITATION that remains: `markDrillableCells` walks every
      // `.malloy-table` in the result, nested ones included, and matches this
      // one flat set against all of them. Two fields that render the same text
      // are handled above, but only because `getAllFields` reports both; a
      // column this set never saw can still collide. The honest fix is to match
      // on field identity rather than rendered text, which is a rewrite of the
      // DOM walk rather than a patch to it.
   }
   return names;
}

/**
 * The renderer gives a cell no field identity, but its normal and pivot tables
 * lay out on a CSS grid with an inline `grid-column: N / …` per cell, so a
 * header cell naming a drillable field identifies that field's column by
 * number, and the body cells sharing the number are its values.
 *
 * NOT every table. A `# transpose` result sets `grid-template-columns` on the
 * container and no `grid-column` on any cell (checked against the renderer's
 * own bundle: zero such calls on that path against nine elsewhere), so this
 * returns undefined for every cell and a transposed table is left unmarked.
 * Its clicks still resolve, because the renderer hands them to `onClick`
 * regardless, so the drill WORKS there and is simply undiscoverable. Marking it
 * needs a second strategy keyed on the transpose layout rather than a patch to
 * this one.
 */
function gridColumnStart(element: HTMLElement): string | undefined {
   const match = /^\s*(\d+)/.exec(element.style?.gridColumn ?? "");
   return match ? match[1] : undefined;
}

/**
 * Add {@link DRILL_CELL_CLASS} to the cells `names` makes clickable.
 *
 * Per table rather than per container: a nested table has its own grid and its
 * own column numbering, so matching across them would mark whichever column of
 * the child happened to share a number with the parent's.
 *
 * Idempotent, and safe to call repeatedly: a `# dashboard` result builds its
 * cards over several frames, so the caller re-runs this as the DOM settles.
 */
export function markDrillableCells(
   container: HTMLElement,
   names: Set<string>,
): void {
   if (names.size === 0) return;
   for (const table of container.querySelectorAll<HTMLElement>(
      ".malloy-table",
   )) {
      const ownedByTable = (element: Element) =>
         element.closest(".malloy-table") === table;

      const columns = new Set<string>();
      for (const header of table.querySelectorAll<HTMLElement>(
         ".column-cell.th",
      )) {
         if (!ownedByTable(header)) continue;
         // The renderer inserts a zero-width space after each underscore in a
         // header so long field names wrap; strip it before matching.
         const text = (header.textContent ?? "").replace(/\u200b/g, "").trim();
         const column = gridColumnStart(header);
         if (column && names.has(text)) columns.add(column);
      }
      if (columns.size === 0) continue;

      for (const cell of table.querySelectorAll<HTMLElement>(
         ".column-cell.td",
      )) {
         if (!ownedByTable(cell)) continue;
         const column = gridColumnStart(cell);
         // A cell with nothing in it is not marked. `resolveDrill` refuses a
         // null or empty value (clicking a blank cell is far likelier a misclick
         // than a request for the rows with no value), so painting one as a link
         // promised a click that was then dropped in silence.
         const hasValue =
            (cell.textContent ?? "").replace(/\u200b/g, "").trim() !== "";
         // Leaf value cells only: a cell wrapping a nested table is structure,
         // and its click lands on the inner cell anyway.
         if (
            column &&
            hasValue &&
            columns.has(column) &&
            !cell.querySelector(".malloy-table")
         ) {
            cell.classList.add(DRILL_CELL_CLASS);
         }
      }
   }
}

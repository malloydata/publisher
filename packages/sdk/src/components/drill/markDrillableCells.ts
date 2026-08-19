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
      // A `# hidden` field renders no header, so it cannot be confused with
      // anything and must not suppress a drillable field's spelling. Counting
      // it gave up the affordance on a column nothing else could collide with.
      if (field.tag?.tag("hidden") !== undefined) continue;
      taken.add(field.tag?.text("label") ?? field.name);
   }
   // STILL over-broad for nests: `getAllFields` is flat, so a field inside a
   // nested view that renders no header of its own in THIS table is counted
   // here too. Narrowing that needs the nesting structure, which this flat list
   // does not carry, and erring toward suppression costs an affordance rather
   // than painting a dead link.

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

      // Which columns already own their single tab stop. Read off the DOM first
      // so the roving position survives the re-runs a settling `# dashboard`
      // triggers; rebuilding it from scratch each pass would snap the stop back
      // to the first row under a reader who had arrowed down.
      const hasStop = new Set<string>();
      for (const marked of table.querySelectorAll<HTMLElement>(
         `.${DRILL_CELL_CLASS}[tabindex="0"]`,
      )) {
         // LOAD-BEARING, and a previous comment here claimed the opposite on
         // the strength of a measurement that missed the case. This query is
         // scoped to `table`, whose subtree contains any nested table, so
         // without this line an outer pass sees a nested table's stop and
         // demotes it as a duplicate of its own column. The nested pass then
         // re-promotes, but it promotes the FIRST row, not the one the reader
         // had roved to, so the restoration is not position-preserving.
         //
         // Measured both ways. Move the stop to the nested table's second row
         // and re-mark: with this line, the stop stays there; without it, the
         // stop is back on the nested table's first row. The earlier
         // measurement moved the stop in the OUTER table, where the fallback
         // lands on the same cell and the two are indistinguishable.
         if (!ownedByTable(marked)) continue;
         const column = gridColumnStart(marked);
         if (!column) continue;
         // The first one keeps the stop and a duplicate is DEMOTED, not left
         // alone. Preserving every `tabindex="0"` it found meant a column that
         // had somehow gained a second stop kept both through every re-mark,
         // which is the accumulation this whole scheme exists to prevent.
         if (hasStop.has(column)) marked.tabIndex = -1;
         else hasStop.add(column);
      }

      for (const cell of table.querySelectorAll<HTMLElement>(
         ".column-cell.td",
      )) {
         if (!ownedByTable(cell)) continue;
         const column = gridColumnStart(cell);
         // A cell with no value is not marked. `resolveDrill` refuses a null or
         // empty value (clicking a blank cell is far likelier a misclick than a
         // request for the rows with no value), so painting one as a link
         // promised a click that was then dropped in silence.
         //
         // Structure first, TEXT second. Testing the text alone did not work:
         // the renderer draws a null cell as the glyph `∅` inside a
         // `.value-null` span, so its `textContent` is not empty and every null
         // cell stayed marked, which is the exact case this guard was added for.
         // The U+200B strip is for the renderer's own soft-wrap hints inside
         // long HEADER names, not for values, and stripping it here made this
         // disagree with the resolver in the other direction: a cell whose
         // value IS a zero-width space is refused an affordance while
         // `drillValueToFilter` accepts it and drills fine. `trim()` already
         // leaves U+200B alone, which is the boundary `filterValue` documents.
         const hasValue =
            !cell.querySelector(".value-null") &&
            (cell.textContent ?? "").trim() !== "";
         // Leaf value cells only: a cell wrapping a nested table is structure,
         // and its click lands on the inner cell anyway.
         if (
            column &&
            hasValue &&
            columns.has(column) &&
            !cell.querySelector(".malloy-table")
         ) {
            cell.classList.add(DRILL_CELL_CLASS);
            // Reachable without a mouse. The class alone gave a pointer cursor
            // and a hover colour, both of which a keyboard user never sees and
            // a touch user cannot produce, so the drill was discoverable only
            // by clicking at random. `tabindex` puts the cell in the tab order
            // and `role` tells a screen reader it does something; the cell's
            // own text is its accessible name, which is the value that gets
            // drilled on, so no `aria-label` is invented here.
            //
            // `button` rather than `link`: a `to=self` drill filters in place
            // and does not navigate, and one tag can offer both, so the honest
            // role is the one that promises less. Set alongside the class and
            // never separately, so the three can never disagree about which
            // cells are actionable.
            // ONE tab stop per drillable column, not one per cell. Making every
            // cell tabbable put the whole column in the tab order, so a result at
            // the server's row cap cost a reader hundreds of presses to reach the
            // next tile, and a composite grid multiplied that by its tiles. That
            // traded one accessibility problem for another. Arrow keys move
            // within the column instead (see `drillColumnSiblings`, driven from
            // RenderedResult), which is the ordinary pattern for a grid.
            //
            // A cell already holding the stop keeps it, so a re-run does not move
            // focus out from under the reader.
            if (cell.tabIndex === 0) {
               hasStop.add(column);
            } else {
               const owned = hasStop.has(column);
               if (!owned) hasStop.add(column);
               cell.tabIndex = owned ? -1 : 0;
            }
            cell.setAttribute("role", "button");
         }
      }
   }
}

/**
 * The drillable cells sharing `cell`'s column, in row order, or an empty array
 * when `cell` is not a marked cell.
 *
 * Lives here rather than in the keyboard handler that calls it because the
 * column identity is this module's rule: a cell's column is its inline
 * `grid-column` start, scoped to its own `.malloy-table` so a nested table's
 * numbering never matches the parent's. Two sites deciding that separately is
 * how they come to disagree, so the handler asks this one.
 */
export function drillColumnSiblings(cell: HTMLElement): HTMLElement[] {
   const table = cell.closest<HTMLElement>(".malloy-table");
   const column = gridColumnStart(cell);
   if (!table || !column) return [];
   return Array.from(
      table.querySelectorAll<HTMLElement>(`.${DRILL_CELL_CLASS}`),
   ).filter(
      (sibling) =>
         sibling.closest(".malloy-table") === table &&
         gridColumnStart(sibling) === column,
   );
}

/**
 * Move a drillable column's single tab stop `to` rows from `from`, or to the
 * first or last cell, and return the cell that should now take focus.
 *
 * Lives beside {@link drillColumnSiblings} because it enforces the same
 * invariant the marking does: exactly one cell per column carries
 * `tabindex="0"`.
 */
export function moveDrillStop(
   from: HTMLElement,
   to: number | "first" | "last",
): HTMLElement | undefined {
   const siblings = drillColumnSiblings(from);
   const at = siblings.indexOf(from);
   if (at === -1) return undefined;
   const next =
      to === "first"
         ? siblings[0]
         : to === "last"
           ? siblings[siblings.length - 1]
           : siblings[Math.min(Math.max(at + to, 0), siblings.length - 1)];
   if (!next) return undefined;
   // EVERY sibling is rewritten, not just `from`. A cell with `tabindex="-1"`
   // is still click-focusable, so `from` is frequently NOT the cell holding the
   // stop: clicking a cell mid-column and then pressing an arrow used to clear
   // `from` (already -1, a no-op) and promote its neighbour, leaving the column
   // with two stops. The marking pass then preserved both, so it stuck, and
   // repeating the gesture accumulated more.
   //
   // Rewriting all of them also means a clamped move at either end, where
   // `next === from`, repairs a column that had drifted rather than doing
   // nothing.
   for (const sibling of siblings) sibling.tabIndex = sibling === next ? 0 : -1;
   return next;
}

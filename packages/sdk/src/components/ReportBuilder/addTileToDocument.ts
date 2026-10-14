// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NewTile } from "./AddTileDialog";
import {
   isQueryTile,
   type DashboardDocument,
   type DashboardSource,
   type QueryTile,
} from "./document";
import { withSource } from "./imports";

/** Where a tile is added and what the document is: the parts of an add that are not the tile. */
export interface AddTileContext {
   /** The document's file within the package, which a tile's import is relative to. */
   modelPath: string | undefined;
   /** The document is held as text: it has no `import`, so a tile's source is the run model's and nothing is imported. */
   textHeld: boolean;
   /** Whether tiles carry a width (`LayoutRules.widths`): off, an added tile takes none. */
   widths: boolean;
   /** Where the tile lands; undefined appends. */
   insertAt?: number;
}

/** A tile from the picker, added to a draft: on the extension of its source, or a new one. */
export function addTileToDocument(
   draft: DashboardDocument,
   tile: NewTile,
   { modelPath, textHeld, widths, insertAt }: AddTileContext,
): void {
   // A source the file cannot see yet comes in by name, with the tile.
   if (tile.modelPath && !textHeld)
      draft.imports = withSource(
         draft,
         tile.base,
         tile.modelPath,
         modelPath,
         tile.exporters ?? [tile.modelPath],
      );
   const extension = extensionOf(draft, tile.base);
   const name = freeViewName(draft, extension.name, tile.view);
   draft.tiles.splice(insertAt ?? draft.tiles.length, 0, {
      name,
      source: extension.name,
      declaration: { kind: "reference", from: tile.view },
      ...(widths ? { colspan: tile.colspan } : {}),
      ...(tile.label ? { label: tile.label } : {}),
      ...(tile.chart ? { chart: tile.chart } : {}),
      ...(tile.chartCarried ? { chartCarried: tile.chartCarried } : {}),
   });
}

/** The file's extension of `base`, or a new one under a name of the file's own. */
function extensionOf(draft: DashboardDocument, base: string): DashboardSource {
   const found = draft.sources.find((s) => s.base === base);
   if (found) return found;
   // A name of the file's own: the base's, suffixed, since an extension cannot
   // share its base's name.
   const taken = new Set(draft.sources.map((s) => s.name));
   let name = `${base}_tiles`;
   for (let n = 2; taken.has(name); n++) name = `${base}_tiles_${n}`;
   const extension: DashboardSource = { name, base };
   draft.sources.push(extension);
   return extension;
}

/**
 * A tile's view name in `extension`: the view it shows, suffixed, because an
 * extension inherits its base's views and cannot redeclare one under the same
 * name; then kept distinct from its siblings.
 */
function freeViewName(
   draft: DashboardDocument,
   extension: string,
   view: string,
): string {
   const used = new Set(
      draft.tiles
         .filter(isQueryTile)
         .filter((t) => t.source === extension)
         .map((t) => t.name),
   );
   let name = `${view}_tile`;
   for (let n = 2; used.has(name); n++) name = `${view}_tile_${n}`;
   return name;
}

/**
 * Why a tile cannot be duplicated, or undefined when it can. A copy is a new
 * view in this file, and the writer adds a view only as `view: x is y`, so a
 * tile whose view the file writes some other way has no copy it could write.
 */
export function duplicateBlocked(tile: QueryTile): string | undefined {
   switch (tile.declaration.kind) {
      case "inline":
         return "Its query is written in this file, and the builder does not copy a query's body.";
      case "opaque":
         return `Its query is ${tile.declaration.why}, which the builder does not copy.`;
      default:
         return tile.chart === "custom"
            ? "Its chart line is one the builder does not write, so a copy would lose it."
            : undefined;
   }
}

/**
 * A copy of the query tile at `index`, placed right after it, as a view of its
 * own so its title, chart and width can differ from the original's. Returns
 * where the copy landed, or undefined when it cannot be copied
 * ({@link duplicateBlocked}).
 *
 * A copy of a view this file declares shows the same view the original does
 * (`is` the original's `from`, never the original's own name, which would carry
 * the original's tags and break when the original is removed), with every
 * setting the original has. A copy of a view the model declares starts as any
 * added tile does: the model's view under a name of the file's own, at the
 * original's width.
 */
export function duplicateTile(
   draft: DashboardDocument,
   index: number,
): number | undefined {
   const original = draft.tiles[index];
   if (!original || !isQueryTile(original)) return undefined;
   if (duplicateBlocked(original) !== undefined) return undefined;
   let copy: QueryTile;
   if (original.declaration.kind === "reference") {
      const { break: _break, ...settings } = structuredClone(original);
      copy = {
         ...settings,
         name: freeViewName(draft, original.source, original.declaration.from),
      };
   } else {
      // Inherited: a view of the source the tile names. When the file
      // declares that source, the copy goes on it; otherwise on the file's
      // extension of it, as an added tile would.
      const own = draft.sources.find((s) => s.name === original.source);
      const extension = own ?? extensionOf(draft, original.source);
      copy = {
         name: freeViewName(draft, extension.name, original.name),
         source: extension.name,
         declaration: { kind: "reference", from: original.name },
         ...(original.colspan !== undefined
            ? { colspan: original.colspan }
            : {}),
      };
   }
   draft.tiles.splice(index + 1, 0, copy);
   return index + 1;
}

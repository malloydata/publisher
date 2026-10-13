// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NewTile } from "./AddTileDialog";
import { isQueryTile, type DashboardDocument } from "./document";
import { withSource } from "./imports";

/** Where a tile is added and what the document is: the parts of an add that are not the tile. */
export interface AddTileContext {
   /** The document's file within the package, which a tile's import is relative to. */
   modelPath: string | undefined;
   /** The document is held as text: it has no `import`, so a tile's source is the run model's and nothing is imported. */
   textHeld: boolean;
   notebook: boolean;
   /** Where the tile lands; undefined appends. */
   insertAt?: number;
}

/** A tile from the picker, added to a draft: on the extension of its source, or a new one. */
export function addTileToDocument(
   draft: DashboardDocument,
   tile: NewTile,
   { modelPath, textHeld, notebook, insertAt }: AddTileContext,
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
   let extension = draft.sources.find((s) => s.base === tile.base);
   if (!extension) {
      // A name of the file's own: the base's, suffixed, since an
      // extension cannot share its base's name.
      const taken = new Set(draft.sources.map((s) => s.name));
      let name = `${tile.base}_tiles`;
      for (let n = 2; taken.has(name); n++) name = `${tile.base}_tiles_${n}`;
      extension = { name, base: tile.base };
      draft.sources.push(extension);
   }
   // The view's name in the extension: the base view's, suffixed,
   // because an extension inherits its base's views and cannot redeclare
   // one under the same name; then kept distinct from its siblings.
   const used = new Set(
      draft.tiles
         .filter(isQueryTile)
         .filter((t) => t.source === extension!.name)
         .map((t) => t.name),
   );
   let name = `${tile.view}_tile`;
   for (let n = 2; used.has(name); n++) name = `${tile.view}_tile_${n}`;
   draft.tiles.splice(insertAt ?? draft.tiles.length, 0, {
      name,
      source: extension.name,
      declaration: { kind: "reference", from: tile.view },
      ...(notebook ? {} : { colspan: tile.colspan }),
      ...(tile.label ? { label: tile.label } : {}),
      ...(tile.chart ? { chart: tile.chart } : {}),
      ...(tile.chartCarried ? { chartCarried: tile.chartCarried } : {}),
   });
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useState } from "react";
import type { NewTile } from "./AddTileDialog";
import {
   isQueryTile,
   isTextTile,
   tileKey,
   type DashboardTile,
} from "./document";
import { withSource } from "./imports";
import type { DashboardEditor } from "./useDashboardEditor";

/**
 * Adding, changing and removing tiles, with the add-tile picker and a tile's
 * menu that they are reached from.
 */
export function useTileEditing({
   editor,
   notebook,
   columns,
   modelPath,
   selectTile,
   deselectTile,
}: {
   editor: DashboardEditor;
   notebook: boolean;
   columns: number;
   /** The document's file within the package, which a tile's import is relative to. */
   modelPath: string | undefined;
   selectTile: (index: number) => void;
   deselectTile: () => void;
}) {
   // A tile's menu, anchored to the button that opened it.
   const [menu, setMenu] = useState<
      { anchor: HTMLElement; index: number } | undefined
   >(undefined);
   // The add-tile picker.
   const [addingTile, setAddingTile] = useState(false);
   // Where the next added tile lands; undefined appends.
   const [insertAt, setInsertAt] = useState<number | undefined>(undefined);
   const openAdd = (at?: number) => {
      setInsertAt(at);
      setAddingTile(true);
   };
   const closeAdd = () => setAddingTile(false);

   /** A tile from the picker: on the extension of its source, or a new one. */
   const addTile = (tile: NewTile) => {
      setAddingTile(false);
      editor.update((draft) => {
         // A source the file cannot see yet comes in by name, with the tile.
         if (tile.modelPath)
            draft.imports = withSource(
               draft,
               tile.base,
               tile.modelPath,
               ...(modelPath ? [modelPath] : []),
            );
         let extension = draft.sources.find((s) => s.base === tile.base);
         if (!extension) {
            // A name of the file's own: the base's, suffixed, since an
            // extension cannot share its base's name.
            const taken = new Set(draft.sources.map((s) => s.name));
            let name = `${tile.base}_tiles`;
            for (let n = 2; taken.has(name); n++)
               name = `${tile.base}_tiles_${n}`;
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
      });
      selectTile(insertAt ?? editor.document.tiles.length);
   };

   /** An empty text tile at the end, named for the first free `text_N`. */
   const addText = () => {
      setAddingTile(false);
      editor.update((draft) => {
         const taken = new Set(
            draft.tiles.filter(isTextTile).map((t) => t.name),
         );
         let n = 1;
         while (taken.has(`text_${n}`)) n++;
         draft.tiles.splice(insertAt ?? draft.tiles.length, 0, {
            kind: "text",
            name: `text_${n}`,
            markdown: "",
            ...(notebook ? {} : { colspan: columns }),
         });
      });
      selectTile(insertAt ?? editor.document.tiles.length);
   };

   /** Change one tile where it stands, found by key so a preview order cannot misdirect it. */
   const editTile = (key: string, change: (tile: DashboardTile) => void) =>
      editor.update((draft) => {
         const tile = draft.tiles.find((each) => tileKey(each) === key);
         if (tile) change(tile);
      });

   const removeTile = (index: number) => {
      setMenu(undefined);
      deselectTile();
      editor.update((draft) => {
         draft.tiles.splice(index, 1);
      });
   };

   return {
      menu,
      setMenu,
      addingTile,
      openAdd,
      closeAdd,
      addTile,
      addText,
      editTile,
      removeTile,
   };
}

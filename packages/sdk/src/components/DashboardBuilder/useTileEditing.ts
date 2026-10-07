// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useState } from "react";
import type { NewTile } from "./AddTileDialog";
import { isTextTile, tileKey, type DashboardTile } from "./document";
import { addTileToDocument } from "./addTileToDocument";
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
   textHeld,
   selectTile,
   deselectTile,
}: {
   editor: DashboardEditor;
   notebook: boolean;
   columns: number;
   /** The document's file within the package, which a tile's import is relative to. */
   modelPath: string | undefined;
   /** The document is held as text: it has no `import`, so a tile's source is the run model's and nothing is imported. */
   textHeld: boolean;
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
      editor.update((draft) =>
         addTileToDocument(draft, tile, {
            modelPath,
            textHeld,
            notebook,
            insertAt,
         }),
      );
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

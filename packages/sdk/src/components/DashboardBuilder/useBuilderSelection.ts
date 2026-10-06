// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   useCallback,
   useEffect,
   useMemo,
   useRef,
   useState,
   type RefObject,
} from "react";
import { scrollBehavior } from "../../theme/motion";
import { changedTileKey } from "./changedTile";
import type { DashboardDocument } from "./document";
import type { DashboardEditor } from "./useDashboardEditor";

/**
 * What is selected on the page: one tile, by index, or the description.
 *
 * ONE value, because only one thing is ever selected: selecting a tile
 * deselects the description, and selecting the description deselects the
 * tile. Held as two flags, the pair could disagree for a render.
 */
type Selection = { tile: number } | "description" | undefined;

/**
 * The builder's selection, and the flash that points at what undo and redo
 * changed.
 *
 * `stepEditor` is the editor with its undo and redo marked as STEPS, so the
 * document they produce is diffed against the one before it and the tile that
 * changed is lit briefly and scrolled to. An ordinary edit is not a step: the
 * author is already looking at what they changed.
 */
export function useBuilderSelection({
   editor,
   gridBox,
}: {
   editor: DashboardEditor;
   /** The grid's box, searched for the changed tile to scroll to. */
   gridBox: RefObject<HTMLDivElement | null>;
}) {
   const [selection, setSelection] = useState<Selection>(undefined);
   const selected = typeof selection === "object" ? selection.tile : undefined;
   // The description is selected the way a tile is, and only one thing is:
   // selecting a tile deselects it, and selecting it deselects the tile.
   const descriptionSelected = selection === "description";
   const selectTile = useCallback(
      (index: number) =>
         setSelection((previous) =>
            typeof previous === "object" && previous.tile === index
               ? previous
               : { tile: index },
         ),
      [],
   );
   // Drops a selected TILE only: the description, if that is what is
   // selected, stays selected.
   const deselectTile = useCallback(
      () =>
         setSelection((previous) =>
            previous === "description" ? previous : undefined,
         ),
      [],
   );
   const selectDescription = useCallback(() => setSelection("description"), []);
   const clearSelection = useCallback(() => setSelection(undefined), []);

   // Undo and redo point at the tile they changed: lit briefly, and scrolled to.
   const [flash, setFlash] = useState<string | undefined>(undefined);
   const stepping = useRef(false);
   const lastDocument = useRef<DashboardDocument>(editor.document);
   useEffect(() => {
      const before = lastDocument.current;
      lastDocument.current = editor.document;
      if (!stepping.current) return;
      stepping.current = false;
      const key = changedTileKey(before, editor.document);
      if (key === undefined) return;
      setFlash(key);
      const target = Array.from(
         gridBox.current?.querySelectorAll<HTMLElement>("[data-tile-key]") ??
            [],
      ).find((element) => element.dataset.tileKey === key);
      // jsdom has no layout, so no scrollIntoView.
      target?.scrollIntoView?.({
         block: "nearest",
         behavior: scrollBehavior(),
      });
   }, [editor.document, gridBox]);
   useEffect(() => {
      if (flash === undefined) return;
      const timer = setTimeout(() => setFlash(undefined), 1500);
      return () => clearTimeout(timer);
   }, [flash]);
   const stepEditor = useMemo(
      () => ({
         ...editor,
         undo: () => {
            stepping.current = true;
            editor.undo();
         },
         redo: () => {
            stepping.current = true;
            editor.redo();
         },
      }),
      [editor],
   );

   return {
      selected,
      descriptionSelected,
      selectTile,
      deselectTile,
      selectDescription,
      clearSelection,
      flash,
      stepEditor,
   };
}

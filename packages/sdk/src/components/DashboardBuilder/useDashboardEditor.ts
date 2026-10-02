// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback } from "react";
import { type DashboardDocument } from "./document";
import { spliceDashboardDocument, tileFileKey } from "./spliceDocument";
import {
   type DocumentEditor,
   type SaveHandler,
   type SaveOutcome,
   useDocumentEditor,
} from "./useDocumentEditor";

export type DashboardEditor = DocumentEditor<DashboardDocument>;
export type { SaveOutcome };

/**
 * Whether the unsaved change adds or removes a tile.
 *
 * The FILE's identity for a tile, not the grid's: `document.tileKey` is
 * `source.name`, so a tile redeclared from another view keeps its key and a
 * save that rewrites its declaration would not be reported as structural — the
 * one case where the author most wants the diff.
 */
const tilesChanged = (saved: DashboardDocument, next: DashboardDocument) => {
   const before = new Set(saved.tiles.map(tileFileKey));
   const after = new Set(next.tiles.map(tileFileKey));
   return (
      [...before].some((k) => !after.has(k)) ||
      [...after].some((k) => !before.has(k))
   );
};

export function useDashboardEditor(options: {
   /** The file as read from storage. */
   source: string;
   /** The document that file produced. */
   document: DashboardDocument;
   /** Persist the patched file. Rejecting leaves the editor dirty. */
   onSave?: SaveHandler<DashboardDocument>;
   /**
    * A cell-format notebook the document is the conversion of: while `source`
    * is `from` the edits are spliced into `to`, the layout text it converts to,
    * and the open is unsaved ({@link DocumentEditorOptions.opensDirty}).
    */
   conversion?: { from: string; to: string };
   /** The file's path within the package. */
   modelPath?: string;
}): DashboardEditor {
   const { conversion, modelPath, ...rest } = options;
   const from = conversion?.from;
   const to = conversion?.to;
   const splice = useCallback(
      (text: string, document: DashboardDocument) =>
         // The kind is only ever changed by the settings toggle, so the writer is always allowed to follow it.
         spliceDashboardDocument(
            from !== undefined && text === from ? (to as string) : text,
            document,
            {
               changeKind: true,
               ...(modelPath !== undefined ? { modelPath } : {}),
            },
         ),
      [from, to, modelPath],
   );
   return useDocumentEditor({
      ...rest,
      splice,
      structural: tilesChanged,
      opensDirty: conversion !== undefined,
   });
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type DashboardDocument } from "./document";
import { spliceDashboardDocument, tileFileKey } from "./spliceDocument";
import {
   type DocumentEditor,
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
   onSave?: (source: string) => Promise<void> | void;
}): DashboardEditor {
   return useDocumentEditor({
      ...options,
      splice: spliceDashboardDocument,
      structural: tilesChanged,
   });
}

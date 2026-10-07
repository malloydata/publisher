// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { SavesTo } from "./documentSession";
import type { BuilderEvent } from "./telemetry";
import type { SessionReport } from "./useBuilderSession";

/**
 * What the builder's session says about a save, in the event names the
 * document's kind reports under.
 */
export const builderReport = ({
   size,
   notebook,
   savesTo,
   onEvent,
}: {
   size: number;
   notebook: boolean;
   savesTo: SavesTo;
   onEvent: ((event: BuilderEvent) => void) | undefined;
}): SessionReport => ({
   size,
   // A notebook-kind document keeps the `notebook.*` names hosts already count.
   saved: ({ size, structural, durationMs, fromOpen }) =>
      onEvent?.(
         notebook
            ? {
                 type: "notebook.saved",
                 cells: size,
                 structural,
                 converted: fromOpen,
                 where: savesTo,
                 durationMs,
              }
            : {
                 type: "dashboard.saved",
                 tiles: size,
                 structural,
                 where: savesTo,
                 durationMs,
              },
      ),
   refused: (reason) =>
      onEvent?.({
         type: notebook ? "notebook.save_refused" : "dashboard.save_refused",
         reason,
      }),
   undone: ({ size, structural, durationMs }) =>
      onEvent?.(
         notebook
            ? {
                 type: "notebook.save_undone",
                 cells: size,
                 structural,
                 where: savesTo,
                 durationMs,
              }
            : {
                 type: "dashboard.save_undone",
                 tiles: size,
                 structural,
                 where: savesTo,
                 durationMs,
              },
      ),
   undoRefused: (reason) =>
      onEvent?.({
         type: notebook
            ? "notebook.save_undo_refused"
            : "dashboard.save_undo_refused",
         reason,
      }),
});

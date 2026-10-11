// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportLayout } from "../Package/documentLocation";
import {
   reportEmitter,
   type ReportEventHandler,
   type SavesTo,
} from "../Report/telemetry";
import type { SessionReport } from "./useBuilderSession";

/**
 * What the builder's session says about a save, as `report.*` events stamped
 * with the report's path and the document's layout as it stands.
 */
export const builderReport = ({
   size,
   path,
   layout,
   savesTo,
   onEvent,
}: {
   size: number;
   path: string;
   layout: ReportLayout;
   savesTo: SavesTo;
   onEvent: ReportEventHandler | undefined;
}): SessionReport => {
   const { emit } = reportEmitter({ path, layout }, onEvent);
   return {
      size,
      saved: ({ size, structural, durationMs, fromOpen }) =>
         emit({
            type: "report.saved",
            tiles: size,
            structural,
            ...(fromOpen ? { converted: true } : {}),
            where: savesTo,
            durationMs,
         }),
      refused: (reason) => emit({ type: "report.save_refused", reason }),
      undone: ({ size, structural, durationMs }) =>
         emit({
            type: "report.save_undone",
            tiles: size,
            structural,
            where: savesTo,
            durationMs,
         }),
      undoRefused: (reason) =>
         emit({ type: "report.save_undo_refused", reason }),
   };
};

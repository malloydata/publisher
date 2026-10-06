// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardEvent } from "../Dashboard/telemetry";
import type { SavesTo } from "./documentSession";
import type { NotebookCreatedEvent } from "../DocumentCreate/events";

/** What the notebook editor reports about itself, for the host to log; context-free like `DashboardEvent`. A one-column dashboard document reports these too. */
export type NotebookEvent =
   | {
        type: "notebook.opened";
        /** The package file, or the host's own copy when that copy is the record. */
        from: "package" | "record";
        cells: number;
        durationMs: number;
     }
   | { type: "notebook.open_refused"; reason: string }
   | {
        type: "notebook.saved";
        cells: number;
        where: SavesTo;
        /** The save added or removed a cell. */
        structural: boolean;
        /** The save rewrote a notebook in the cell format as a layout notebook. */
        converted?: boolean;
        /** The host workspace that took the write; never set for a package save. */
        workspace?: string;
        durationMs: number;
     }
   | { type: "notebook.save_refused"; reason: string }
   | {
        /** Undo save wrote the file back as it was before the last save. */
        type: "notebook.save_undone";
        cells: number;
        where: SavesTo;
        /** Whether the save it took back added or removed a cell. */
        structural: boolean;
        workspace?: string;
        durationMs: number;
     }
   | { type: "notebook.save_undo_refused"; reason: string }
   | NotebookCreatedEvent;

export type NotebookEventHandler = (event: NotebookEvent) => void;

/** Whatever the one builder reports: a notebook-kind document speaks `notebook.*`, a dashboard `dashboard.*`. */
export type BuilderEvent = DashboardEvent | NotebookEvent;

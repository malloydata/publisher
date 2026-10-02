// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { SavesTo } from "../DashboardBuilder/documentSession";
import type { NotebookCreatedEvent } from "../DocumentCreate/events";

/** What the notebook editor reports about itself, for the host to log; context-free like `DashboardEvent`. */
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
        /** The save added or removed a cell, which is when the diff was shown. */
        structural?: boolean;
        /** The host workspace that took the write; never set for a package save. */
        workspace?: string;
        durationMs: number;
     }
   | { type: "notebook.save_refused"; reason: string }
   | NotebookCreatedEvent;

export type NotebookEventHandler = (event: NotebookEvent) => void;

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
        where: "package" | "browser" | "host";
        /** The host workspace that took the write; never set for a package save. */
        workspace?: string;
        durationMs: number;
     }
   | { type: "notebook.save_refused"; reason: string };

export type NotebookEventHandler = (event: NotebookEvent) => void;

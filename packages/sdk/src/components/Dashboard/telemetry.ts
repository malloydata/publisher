// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a dashboard surface reports about itself, for the host to log or
 * count: the operations that matter (open, save, the rows behind a value,
 * exploring a tile), each with its outcome and how long it took.
 *
 * Context-free on purpose — the host that mounted the surface knows which
 * environment, package and dashboard it is, and adds that when it logs.
 */
export type DashboardEvent =
   | {
        type: "dashboard.opened";
        /** The package file, or a draft the host had saved. */
        from: "package" | "draft";
        tiles: number;
        durationMs: number;
     }
   | { type: "dashboard.open_refused"; reason: string }
   | {
        type: "dashboard.saved";
        tiles: number;
        /** Whether a tile was added or removed, which moves declarations. */
        structural: boolean;
        /**
         * Where it went. A save into the package is a change every reader of
         * that server sees; a browser save is one person's copy on one
         * machine. Counting them together makes "dashboards are being edited"
         * unreadable, because a read-only server reports exactly as much
         * saving as a writable one.
         */
        where: "package" | "browser";
        durationMs: number;
     }
   | { type: "dashboard.save_refused"; reason: string }
   | {
        type: "dashboard.rows_shown";
        source: string;
        view: string;
        field: string;
        ok: boolean;
        durationMs: number;
     }
   | { type: "dashboard.explored"; tile: string };

export type DashboardEventHandler = (event: DashboardEvent) => void;

/** A monotonic clock in milliseconds, for durations. */
export const now = (): number =>
   typeof performance !== "undefined" ? performance.now() : Date.now();

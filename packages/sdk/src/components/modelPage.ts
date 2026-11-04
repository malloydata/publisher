// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The model page a server asks for, its `PUBLISHER_MODEL_PAGE`. The Explorer
 * writes the queries it builds back to Malloy text with string values unescaped
 * (`escapeString` in `@malloydata/malloy-interfaces` is a TODO), so a drilled
 * value holding a double quote rewrites its query; `viewer` shows a page that
 * builds none.
 */
export type ModelPage = "explorer" | "viewer";

/**
 * The page to show, from what `/status` answered: `viewer` when it says so, and
 * when it could not be read at all, since a deployment that chose `viewer` has
 * chosen not to show the Explorer and the viewer writes no value into Malloy on
 * any server. `explorer` otherwise, a server that predates the setting
 * included. While `/status` is still loading this says `explorer`; a caller
 * that would draw the Explorer waits on loading first.
 */
export function modelPageOf(
   status: { modelPage?: unknown } | undefined,
   failed: boolean,
): ModelPage {
   if (failed) return "viewer";
   return status?.modelPage === "viewer" ? "viewer" : "explorer";
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Copy for the model page's header, mirroring the connection page's
 * (connectionStatusPresentation.ts): the status line under the title, and the
 * (?) help text beside it.
 *
 * Separate from the components so it can be asserted directly, and so the
 * explorer modules keep exporting only components — a non-component export
 * breaks Vite Fast Refresh for the whole module.
 */

/**
 * What this model holds, in the shape of the connection page's "Indexed 24
 * tables": the count of the thing the page is a browser for.
 */
export function modelSourcesLine(sourceCount: number): string {
   if (sourceCount === 0) return "No sources in this model";
   return `${sourceCount} source${sourceCount === 1 ? "" : "s"} in this model`;
}

/**
 * What a source is and what picking one does — the two things a first-time
 * reader of the sources list cannot guess from a bare count. Kept free of any
 * chat-scoping clause, because the sources dropdown
 * this labels also appears on the standalone model page, where there is no chat
 * to scope.
 */
export const MODEL_SOURCES_HELP =
   "A source is a queryable entry point into the model — pick one to browse its dimensions, measures, views and joins.";

/**
 * The source picker pill's face, beside the source's title on the model page: "5 sources", or on
 * the package's root `index.malloy` — whose sources are the set the package publishes —
 * "4 public sources".
 */
export function sourceCountLabel(sourceCount: number, curated = false): string {
   if (sourceCount === 0) return "No sources";
   return `${sourceCount} ${curated ? "public " : ""}source${sourceCount === 1 ? "" : "s"}`;
}

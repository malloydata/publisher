// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { useServer } from "../ServerProvider";

/**
 * The one read of a report's manifest, `GET …/reports/{path}`, shared by the
 * reader and the builder so switching between them draws from one cache entry
 * rather than reading the report again.
 *
 * `staleTime` is how long a read is reused without asking the server: 0 (every
 * mount asks) unless a caller draws the same report twice in a row, as the
 * builder does over the reader it keeps on screen while it loads. A write this
 * app makes invalidates every `report` entry, but a change it did not make (a
 * watch-mode save, another client's write, a package reload) is only seen by
 * asking, so a standalone reader keeps 0.
 */
export function reportQueryOptions(
   apiClients: ReturnType<typeof useServer>["apiClients"],
   {
      environmentName,
      packageName,
      path,
      versionId,
   }: {
      environmentName: string;
      packageName: string;
      path: string;
      versionId: string | undefined;
   },
   /** Omitted, the query client's staleness for the `report` key decides. */
   staleTime?: number,
) {
   return {
      // Every value the request is built from, so two versions of one report
      // never share a cache entry.
      queryKey: ["report", environmentName, packageName, versionId, path],
      queryFn: async ({ signal }: { signal?: AbortSignal }) =>
         (
            await apiClients.reports.getReport(
               environmentName,
               packageName,
               path,
               versionId,
               { signal },
            )
         ).data,
      ...(staleTime === undefined ? {} : { staleTime }),
   };
}

/** How long the builder, and the reader drawn while it loads, reuse one read of the report. */
export const BUILDER_HANDOFF_STALE_MS = 10_000;

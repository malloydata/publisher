// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { useServer } from "../ServerProvider";

/**
 * The one read of a report's manifest, `GET …/reports/{path}`, shared by the
 * reader and the builder so switching between them draws from one cache entry
 * rather than reading the report again. Fresh for a few seconds, which covers
 * the reader that stays on screen while the builder loads (it mounts twice
 * across that switch); a save invalidates every `report` entry, so a write of
 * our own is never shown stale.
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
      staleTime: 10_000,
   };
}

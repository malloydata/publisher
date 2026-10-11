// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer, type ApiClients } from "../ServerProvider";
import {
   locateReport,
   type ListedReport,
   type LocatedDocument,
} from "./documentLocation";

/** The query key of a package's report listing, so every reader shares one request and one invalidation. */
export const reportsQueryKey = (
   environmentName: string,
   packageName: string,
   versionId?: string,
) => ["reports", environmentName, packageName, versionId];

/**
 * `GET …/reports`, as the array every reader of {@link reportsQueryKey} caches.
 * An older server with no such route (404), or no answer at all, is a package
 * with no reports to list rather than an error.
 */
export async function fetchReportListing(
   apiClients: ApiClients,
   environmentName: string,
   packageName: string,
   versionId?: string,
): Promise<ListedReport[]> {
   try {
      return (
         await apiClients.reports.listReports(
            environmentName,
            packageName,
            versionId,
         )
      ).data;
   } catch (e) {
      const status = (e as { response?: { status?: number } })?.response
         ?.status;
      if (status === 404 || status === undefined) return [];
      throw e;
   }
}

/**
 * The package's reports, from `GET …/reports`: one listing for both layouts.
 *
 * `settled` is false until it has answered. An older server with no such
 * route answers 404, which reads as a package with no reports rather than an
 * error, so a host still renders.
 */
export function useReportListing(
   environmentName: string,
   packageName: string,
   versionId?: string,
   enabled = true,
) {
   const { apiClients } = useServer();
   const query = useQueryWithApiError({
      queryKey: reportsQueryKey(environmentName, packageName, versionId),
      queryFn: () =>
         fetchReportListing(
            apiClients,
            environmentName,
            packageName,
            versionId,
         ),
      enabled,
      // One request per page however many readers mount: the route, the
      // header's Edit button, drill and the builder all read this listing, and
      // a save invalidates it.
      staleTime: 60_000,
   });
   return {
      reports: (query.data ?? []) as ListedReport[],
      settled: query.isSuccess || query.isError,
      isError: query.isError,
      error: query.error,
      refetch: query.refetch,
   };
}

/**
 * The report at a package path, from the package's listing, matched exactly.
 *
 * `settled` is false until the listing has answered; a path the listing does
 * not have (a draft, a model, or an older server) has a `location` of
 * undefined, and the caller decides what that path is instead.
 */
export function useDocumentLocation(
   environmentName: string,
   packageName: string,
   path: string,
   versionId?: string,
): {
   location: LocatedDocument | undefined;
   settled: boolean;
   reports: readonly ListedReport[];
} {
   const { reports, settled } = useReportListing(
      environmentName,
      packageName,
      versionId,
   );
   return {
      settled,
      reports,
      location: settled ? locateReport(path, reports) : undefined,
   };
}

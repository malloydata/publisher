// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";
import {
   locateDocument,
   type DocumentKind,
   type LocatedDocument,
} from "./documentLocation";

/**
 * Where a routed document really lives, from the package's listings.
 *
 * `settled` is false until both listings have answered; a document neither
 * lists (a draft, or an older server) has a `location` of undefined and the
 * caller falls back to the folder its route names.
 */
export function useDocumentLocation(
   environmentName: string,
   packageName: string,
   kind: DocumentKind,
   slug: string,
   versionId?: string,
): { location: LocatedDocument | undefined; settled: boolean } {
   const { apiClients } = useServer();
   const dashboards = useQueryWithApiError({
      queryKey: ["dashboards", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.dashboards.listDashboards(
            environmentName,
            packageName,
            versionId,
         ),
   });
   const notebooks = useQueryWithApiError({
      queryKey: ["notebooks", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.notebooks.listNotebooks(
            environmentName,
            packageName,
            versionId,
         ),
   });
   const settled = [dashboards, notebooks].every(
      (query) => query.isSuccess || query.isError,
   );
   return {
      settled,
      location: settled
         ? locateDocument({
              kind,
              slug,
              dashboards: dashboards.data?.data ?? [],
              notebooks: notebooks.data?.data ?? [],
           })
         : undefined,
   };
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @deprecated `.malloynb` only, and deleted with the format. A `.malloynb`
 * drawn by `Report` from the `notebook` that `GET …/reports/{path}` serves,
 * each code cell's served `runQuery` sent as it is to the governed query
 * endpoint, `POST …/models/{path}/query`, with the notebook's own path as the
 * model.
 */

import { isCancelledError } from "@tanstack/react-query";
import { useCallback } from "react";
import type { ReportCells } from "../../../client";
import {
   queryResultKey,
   requestQueryResult,
   type QueryRequestSpec,
} from "../../../hooks/useQueryResult";
import { encodeResourceUri } from "../../../utils/formatting";
import {
   CHART_RESULT_QUERY_OPTIONS,
   globalQueryClient,
} from "../../../utils/queryClient";
import { givensToRequest } from "../../given/paramCodec";
import { useServer } from "../../ServerProvider";
import {
   CellRunnerView,
   type CellReportProps,
   type CellRunner,
} from "./CellRunnerView";

export interface MalloynbReportProps
   extends Omit<CellReportProps, "resourceUri"> {
   environmentName: string;
   packageName: string;
   versionId?: string;
   /** The notebook's package path, the model every cell runs against. */
   path: string;
   notebook: ReportCells;
}

export function MalloynbReport({
   environmentName,
   packageName,
   versionId,
   path,
   notebook,
   ...props
}: MalloynbReportProps) {
   const { apiClients, server } = useServer();
   const runCell = useCallback<CellRunner>(
      async ({ cell, givens, declaredTypes, signal }) => {
         const query = cell.runQuery;
         // A cell that only imports or declares has nothing to run.
         if (query === undefined) return {};
         signal.throwIfAborted();
         const request = givensToRequest(givens, declaredTypes);
         const spec: QueryRequestSpec = {
            environmentName,
            packageName,
            versionId,
            modelPath: path,
            query,
            givens: Object.keys(request).length > 0 ? request : undefined,
         };
         // A tile's key, server last as `useQueryWithApiError` adds it, so a
         // remount within the cache window reads the answer it already has.
         const queryKey = [...queryResultKey(spec), server];
         const fetch = () =>
            globalQueryClient.fetchQuery({
               queryKey,
               queryFn: ({ signal: querySignal }) =>
                  requestQueryResult(apiClients, spec, querySignal),
               ...CHART_RESULT_QUERY_OPTIONS,
            });
         // Cancelling the entry reverts it, so an abandoned run caches nothing.
         const cancel = () =>
            void globalQueryClient.cancelQueries({ queryKey, exact: true });
         signal.addEventListener("abort", cancel, { once: true });
         try {
            let response;
            try {
               response = await fetch();
            } catch (error) {
               // Another run of the same cell was cancelled under this one.
               if (!isCancelledError(error) || signal.aborted) throw error;
               response = await fetch();
            }
            return { result: response.data.result };
         } finally {
            signal.removeEventListener("abort", cancel);
         }
      },
      [apiClients, server, environmentName, packageName, path, versionId],
   );
   return (
      <CellRunnerView
         {...props}
         resourceUri={encodeResourceUri({
            environmentName,
            packageName,
            modelPath: path,
            versionId,
         })}
         notebook={notebook}
         runCell={runCell}
      />
   );
}

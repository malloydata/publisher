// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback } from "react";
import type { RawNotebook } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { givensToRequest } from "../given/paramCodec";
import {
   CellRunnerView,
   type CellReportProps,
   type CellRunner,
} from "../Report/CellReport";
import { useServer } from "../ServerProvider";

/** The deprecated notebook read, one cache entry per URI however many components ask. */
export function useNotebookQuery(resourceUri: string) {
   const { apiClients } = useServer();
   const {
      environmentName,
      packageName,
      versionId,
      modelPath: notebookPath,
   } = parseResourceUri(resourceUri);
   return useQueryWithApiError<RawNotebook>({
      queryKey: [resourceUri],
      queryFn: async () => {
         const response = await apiClients.notebooks.getNotebook(
            environmentName,
            packageName,
            notebookPath,
            versionId,
         );
         return response.data;
      },
   });
}

/**
 * A notebook's cells read and run through the deprecated notebooks routes,
 * `GET …/notebooks/{path}` and `GET …/notebooks/{path}/cells/{i}`: the only
 * routes that run a `.malloynb`, and what the `Notebook` wrapper has always
 * read a cell-format report through.
 */
export function MalloynbCells(props: CellReportProps) {
   const { apiClients } = useServer();
   const { environmentName, packageName, versionId, modelPath } =
      parseResourceUri(props.resourceUri);
   const query = useNotebookQuery(props.resourceUri);
   const runCell = useCallback<CellRunner>(
      async ({ index, givens, declaredTypes, signal }) => {
         const request = givensToRequest(givens, declaredTypes);
         // `filterParams` and `bypassFilters` go over as undefined: `given:`
         // replaced the `#(filter)` panel, and a `required` filter the server
         // still enforces now fails the cell visibly rather than blankly.
         return (
            await apiClients.notebooks.executeNotebookCell(
               environmentName,
               packageName,
               modelPath,
               index,
               versionId,
               undefined,
               undefined,
               Object.keys(request).length > 0
                  ? JSON.stringify(request)
                  : undefined,
               { signal },
            )
         ).data;
      },
      [
         apiClients.notebooks,
         environmentName,
         packageName,
         modelPath,
         versionId,
      ],
   );
   return (
      <CellRunnerView
         {...props}
         notebook={query.isSuccess ? query.data : undefined}
         error={query.isError ? query.error : undefined}
         runCell={runCell}
      />
   );
}

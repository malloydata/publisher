// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @deprecated `.malloynb` only, and deleted with the format. A `.malloynb`
 * drawn by `Report` from the `notebook` that `GET …/reports/{path}` serves,
 * each code cell's served `runQuery` sent as it is to the governed query
 * endpoint, `POST …/models/{path}/query`, with the notebook's own path as the
 * model.
 */

import { useCallback } from "react";
import type { ReportCells } from "../../../client";
import { encodeResourceUri } from "../../../utils/formatting";
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
   const { apiClients } = useServer();
   const runCell = useCallback<CellRunner>(
      async ({ cell, givens, declaredTypes, signal }) => {
         const query = cell.runQuery;
         // A cell that only imports or declares has nothing to run.
         if (query === undefined) return {};
         const request = givensToRequest(givens, declaredTypes);
         const response = await apiClients.models.executeQueryModel(
            environmentName,
            packageName,
            path,
            {
               query,
               versionId,
               ...(Object.keys(request).length > 0 ? { givens: request } : {}),
            },
            undefined,
            // versionId: carried in the body.
            undefined,
            { signal },
         );
         return { result: response.data.result };
      },
      [apiClients.models, environmentName, packageName, path, versionId],
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

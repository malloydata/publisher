// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DownloadIcon from "@mui/icons-material/Download";
import { Button, Tooltip } from "@mui/material";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { useState } from "react";
import { useServer } from "../ServerProvider";
import type { RowsActionsContext } from "./RowsDialog";
import { resultRecords, resultToCsv, serverCappedAt } from "./rowsCsv";
import { withPreamble } from "./textSource";

const DEFAULT_TIP = "Download every row behind this value as CSV";

/** Hands `text` to the browser as a file named `fileName`. */
function saveFile(text: string, fileName: string) {
   const url = URL.createObjectURL(
      new Blob([text], { type: "text/csv;charset=utf-8" }),
   );
   const link = document.createElement("a");
   link.href = url;
   link.download = fileName;
   document.body.appendChild(link);
   link.click();
   link.remove();
   URL.revokeObjectURL(url);
}

/**
 * The rows dialog's own download, for a host that passes no `rowsActions`:
 * runs the rows query with no row limit through the same query endpoint the
 * dialog uses, and saves the result as CSV. The server's own row cap still
 * applies; when it cut the result short, the file name and the tooltip say
 * how many rows the file holds.
 */
export function DownloadRowsCsv({
   query,
   preamble,
   environmentName,
   packageName,
   modelPath,
   versionId,
   givens,
   fileName,
}: RowsActionsContext) {
   const { apiClients } = useServer();
   const [busy, setBusy] = useState(false);
   const [tip, setTip] = useState(DEFAULT_TIP);

   const download = async () => {
      setBusy(true);
      try {
         const response = await apiClients.models.executeQueryModel(
            environmentName,
            packageName,
            modelPath,
            {
               query:
                  preamble === undefined
                     ? query
                     : withPreamble(preamble, query),
               givens,
               versionId,
            },
         );
         const body = response.data;
         const result = JSON.parse(body.result ?? "{}") as Malloy.Result;
         const capped = serverCappedAt(
            resultRecords(result).length,
            body.queryRowLimit,
            body.queryRowLimitSource,
         );
         setTip(
            capped === undefined
               ? DEFAULT_TIP
               : `First ${capped} rows (server limit)`,
         );
         saveFile(
            resultToCsv(result),
            capped === undefined
               ? `${fileName}.csv`
               : `${fileName}_first_${capped}_rows_server_limit.csv`,
         );
         setBusy(false);
      } catch {
         setTip("The download failed. Try again.");
         setBusy(false);
      }
   };

   return (
      <Tooltip title={tip}>
         <span>
            <Button
               size="small"
               startIcon={<DownloadIcon fontSize="small" />}
               onClick={() => void download()}
               disabled={busy}
            >
               Download CSV
            </Button>
         </span>
      </Tooltip>
   );
}

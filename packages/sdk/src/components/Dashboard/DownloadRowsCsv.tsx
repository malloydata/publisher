// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DownloadIcon from "@mui/icons-material/Download";
import { Box, Button, Tooltip, Typography } from "@mui/material";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { useEffect, useRef, useState } from "react";
import { useServer } from "../ServerProvider";
import type { RowsActionsContext } from "./RowsDialog";
import { resultRecords, resultToCsv, serverCappedAt } from "./rowsCsv";
import { withPreamble } from "./textSource";

const DEFAULT_TIP =
   "Download the rows behind this value as CSV, up to the server's row limit";

/**
 * How long a saved file's object URL outlives the click. Revoking it at once
 * can cancel the save in Firefox and Safari; FileSaver.js waits 40 seconds.
 */
const REVOKE_AFTER_MS = 40_000;

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
   setTimeout(() => URL.revokeObjectURL(url), REVOKE_AFTER_MS);
}

/** The server's own message for a failed request, else the error's. */
function failureMessage(error: unknown): string {
   const response = (error as { response?: { data?: { message?: unknown } } })
      ?.response;
   const message = response?.data?.message;
   if (typeof message === "string" && message !== "") return message;
   return error instanceof Error && error.message !== ""
      ? error.message
      : "The request failed.";
}

/**
 * The rows dialog's own download, for a host that passes no `rowsActions`:
 * runs the rows query with no row limit through the same query endpoint the
 * dialog uses, and saves the result as CSV. The server's own row cap still
 * applies; when the file reached it, the file name and the tooltip say so.
 * A failure is written beside the button, where a keyboard user reaches it,
 * with the server's message. Closing the dialog cancels a download in flight.
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
   const [failure, setFailure] = useState<string | undefined>(undefined);
   const inFlight = useRef<AbortController | undefined>(undefined);
   useEffect(() => () => inFlight.current?.abort(), []);

   const download = async () => {
      inFlight.current?.abort();
      const controller = new AbortController();
      inFlight.current = controller;
      setBusy(true);
      setFailure(undefined);
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
            undefined,
            { signal: controller.signal },
         );
         if (controller.signal.aborted) return;
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
               : `First ${capped} rows: the server's row limit, so there may be more`,
         );
         saveFile(
            resultToCsv(result),
            capped === undefined
               ? `${fileName}.csv`
               : `${fileName}_first_${capped}_rows_server_limit.csv`,
         );
      } catch (error) {
         if (controller.signal.aborted) return;
         setFailure(`Download failed: ${failureMessage(error)}`);
      } finally {
         if (inFlight.current === controller) {
            inFlight.current = undefined;
            setBusy(false);
         }
      }
   };

   return (
      <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
         {failure && (
            <Typography
               role="alert"
               variant="caption"
               color="error"
               sx={{ maxWidth: 320 }}
            >
               {failure}
            </Typography>
         )}
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
      </Box>
   );
}

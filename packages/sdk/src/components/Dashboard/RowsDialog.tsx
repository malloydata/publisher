// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CloseIcon from "@mui/icons-material/Close";
import {
   Box,
   Button,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   IconButton,
   Typography,
} from "@mui/material";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { CHART_RESULT_QUERY_OPTIONS } from "../../utils/queryClient";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import ResultContainer from "../RenderedResult/ResultContainer";
import { useServer } from "../ServerProvider";

/**
 * The rows behind a value: Looker's "show all" on a cell, as one query.
 *
 * A grouped value is an aggregate over rows of the tile's source, and Malloy's
 * `drill:` names exactly those rows: `drill: <view>.<field> = <value>` applies
 * the view's own `where:` — the tile's control bindings included — and the
 * clicked value's, so what comes back is the rows the number was made of.
 * Measured: `where: name = …` alone failed on a view that spelled the column
 * `name is products.name`, because the output name is not a source field;
 * `drill:` resolves it through the view, which is where the name lives.
 */
export const ROW_LIMIT = 200;

export interface RowsRequest {
   /** The source the rows are of: the tile expression's first step. */
   source: string;
   /** The view the value was grouped in: the expression's last step. */
   view: string;
   field: string;
   rawValue: unknown;
   label: string;
}

/** A clicked value as a Malloy literal, or undefined when it has no spelling. */
export function malloyLiteral(value: unknown): string | undefined {
   if (typeof value === "string")
      return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
   if (typeof value === "number")
      return Number.isFinite(value) ? String(value) : undefined;
   if (typeof value === "boolean") return String(value);
   if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) return undefined;
      const date = value.toISOString().slice(0, 10);
      const time = value.toISOString().slice(11, 19);
      return time === "00:00:00" ? `@${date}` : `@${date} ${time}`;
   }
   return undefined;
}

/** The query the dialog runs, or undefined when the value cannot be spelled. */
export function rowsQuery(request: RowsRequest): string | undefined {
   const literal = malloyLiteral(request.rawValue);
   if (literal === undefined) return undefined;
   return `run: ${request.source} -> { drill: ${request.view}.${request.field} = ${literal}; select: *; limit: ${ROW_LIMIT} }`;
}

/**
 * `overview -> revenue_trend` → its source and view; undefined for anything
 * else — an inline stage has no view to drill through, and a longer pipeline
 * has no one view its value came from.
 */
export function stepsOf(
   tileExpression: string | undefined,
): { source: string; view: string } | undefined {
   const steps = (tileExpression ?? "").split("->").map((s) => s.trim());
   if (steps.length !== 2) return undefined;
   const name = /^[A-Za-z_]\w*$/;
   return name.test(steps[0]) && name.test(steps[1])
      ? { source: steps[0], view: steps[1] }
      : undefined;
}

/** The source alone, for opening the explorer on it. */
export const sourceOf = (tileExpression: string | undefined) =>
   stepsOf(tileExpression)?.source;

export function RowsDialog({
   request,
   environmentName,
   packageName,
   versionId,
   modelPath,
   givens,
   onClose,
}: {
   request: RowsRequest | undefined;
   environmentName: string;
   packageName: string;
   versionId?: string;
   modelPath: string;
   /** The applied control row, which the source's own `where:` may read. */
   givens: Record<string, unknown>;
   onClose: () => void;
}) {
   const { apiClients } = useServer();
   const { theme } = usePublisherTheme();
   const query = request ? rowsQuery(request) : undefined;
   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: [
         "dashboardRows",
         environmentName,
         packageName,
         versionId,
         modelPath,
         query,
         JSON.stringify(givens),
      ],
      queryFn: () =>
         apiClients.models.executeQueryModel(
            environmentName,
            packageName,
            modelPath,
            { query, givens, versionId },
         ),
      enabled: query !== undefined,
      ...CHART_RESULT_QUERY_OPTIONS,
   });

   return (
      <Dialog
         open={request !== undefined}
         onClose={onClose}
         maxWidth="lg"
         fullWidth
      >
         <DialogTitle
            sx={{
               display: "flex",
               alignItems: "flex-start",
               justifyContent: "space-between",
               pb: 0.5,
            }}
         >
            <Box>
               {request ? (
                  <>
                     Rows where{" "}
                     <Box component="code" sx={{ fontSize: "0.9em" }}>
                        {request.field}
                     </Box>{" "}
                     is {request.label}
                  </>
               ) : (
                  "Rows"
               )}
               <Typography
                  component="div"
                  variant="caption"
                  sx={{ color: theme.tileTitle, mt: 0.25 }}
               >
                  {request
                     ? `The first ${ROW_LIMIT} rows of ${request.source} behind this value, with the tile's own filters and the controls applied.`
                     : ""}
               </Typography>
            </Box>
            <IconButton
               onClick={onClose}
               aria-label="Close"
               size="small"
               sx={{ color: "text.secondary", ml: 1 }}
            >
               <CloseIcon fontSize="small" />
            </IconButton>
         </DialogTitle>
         <DialogContent dividers sx={{ p: 0, minHeight: 240 }}>
            {query === undefined && request && (
               <Typography variant="body2" sx={{ p: 2 }}>
                  This value cannot be written as a filter, so its rows cannot
                  be fetched.
               </Typography>
            )}
            {query !== undefined && !isSuccess && !isError && (
               <Loading text="Running…" />
            )}
            {isSuccess && (
               <Box sx={{ p: 1 }}>
                  <ResultContainer
                     result={data.data.result}
                     renderLogs={data.data.renderLogs}
                  />
               </Box>
            )}
            {isError && (
               <Box sx={{ p: 2 }}>
                  <ApiErrorDisplay context={query ?? ""} error={error} />
               </Box>
            )}
         </DialogContent>
         <DialogActions sx={{ px: 3, py: 1.5 }}>
            {query !== undefined && (
               <Typography
                  variant="caption"
                  sx={{
                     mr: "auto",
                     color: theme.tileTitle,
                     fontFamily: "ui-monospace, monospace",
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                     whiteSpace: "nowrap",
                  }}
                  title={query}
               >
                  {query}
               </Typography>
            )}
            <Button onClick={onClose}>Close</Button>
         </DialogActions>
      </Dialog>
   );
}

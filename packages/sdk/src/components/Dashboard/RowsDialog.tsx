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
import { useEffect, useRef } from "react";
import { useQueryResult } from "../../hooks/useQueryResult";
import { malloyLiteral } from "../../utils/malloyLiteral";
import { isIdentifier, tileSteps } from "../DashboardBuilder/malloyText";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { ResultPanel } from "../RenderedResult/ResultPanel";
import { now } from "./telemetry";

/**
 * The rows behind a value: "show the rows" on a cell, as one query.
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

/** The query the dialog runs, or undefined when the value cannot be spelled. */
export function rowsQuery(request: RowsRequest): string | undefined {
   const literal = malloyLiteral(request.rawValue);
   if (literal === undefined) return undefined;
   return `run: ${request.source} -> { drill: ${request.view}.${request.field} = ${literal}; select: *; limit: ${ROW_LIMIT} }`;
}

/**
 * `overview -> revenue_trend` → its source and view; undefined for anything
 * else — an inline stage has no view to drill through, a refined one carries
 * filters `drill:` would not, and a longer pipeline has no one view its value
 * came from.
 */
export function stepsOf(
   tileExpression: string | undefined,
): { source: string; view: string } | undefined {
   const steps = tileSteps(tileExpression);
   if (
      !steps ||
      steps.refinement !== undefined ||
      !isIdentifier(steps.source) ||
      !isIdentifier(steps.view)
   )
      return undefined;
   return { source: steps.source, view: steps.view };
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
   onDone,
}: {
   request: RowsRequest | undefined;
   environmentName: string;
   packageName: string;
   versionId?: string;
   modelPath: string;
   /** The applied control row, which the source's own `where:` may read. */
   givens: Record<string, unknown>;
   onClose: () => void;
   /** The query's outcome and how long it took, once per request. */
   onDone?: (ok: boolean, durationMs: number) => void;
}) {
   const { theme } = usePublisherTheme();
   const query = request ? rowsQuery(request) : undefined;

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
            {query !== undefined && (
               <RowsResult
                  query={query}
                  environmentName={environmentName}
                  packageName={packageName}
                  {...(versionId === undefined ? {} : { versionId })}
                  modelPath={modelPath}
                  givens={givens}
                  {...(onDone ? { onDone } : {})}
               />
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

/**
 * The rows themselves. Its own component so the query exists only while a
 * request does: a closed dialog holds nothing in the cache.
 */
function RowsResult({
   query,
   environmentName,
   packageName,
   versionId,
   modelPath,
   givens,
   onDone,
}: {
   query: string;
   environmentName: string;
   packageName: string;
   versionId?: string;
   modelPath: string;
   givens: Record<string, unknown>;
   onDone?: (ok: boolean, durationMs: number) => void;
}) {
   const state = useQueryResult({
      environmentName,
      packageName,
      modelPath,
      ...(versionId === undefined ? {} : { versionId }),
      query,
      givens,
   });
   const { isSuccess, isError } = state;
   // Reported once per request, when the query settles.
   const started = useRef<number | undefined>(undefined);
   useEffect(() => {
      started.current = now();
   }, [query]);
   useEffect(() => {
      if (started.current === undefined || (!isSuccess && !isError)) return;
      onDone?.(isSuccess, now() - started.current);
      started.current = undefined;
   }, [isSuccess, isError, onDone]);
   return (
      <Box sx={{ p: 1 }}>
         <ResultPanel state={state} context={query} />
      </Box>
   );
}

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
import { Fragment, useEffect, useRef } from "react";
import { useQueryResult } from "../../hooks/useQueryResult";
import {
   drillClauseLabels,
   drillClausesText,
   nullDimensions,
   type DrillClause,
   type DrillClauseLabel,
} from "../drill/drillClauses";
import type { DrillRowsRequest } from "../drill/resolveDrill";
import { malloyLiteral } from "../../utils/malloyLiteral";
import { escapeMalloyString } from "../../utils/malloyString";
import { isIdentifier, tileSteps } from "../../utils/malloyText";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { ResultPanel } from "../RenderedResult/ResultPanel";
import { now } from "../../utils/clock";

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
 *
 * `drill:` refuses a clause list that leaves out any of the view's
 * dimensions, so the clicked row's every dimension goes in, from the
 * renderer's own drill data. The clicked value alone is the fallback for a
 * click that came with no row, which is right only for a view grouped by one
 * dimension.
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
   /** The clicked row's dimensions, paths relative to the source. */
   drillClauses?: readonly DrillClause[];
   timezone?: string;
}

/** The dialog's request for a click on `request.context`'s tile, if it names one view. */
export function rowsRequestFor(
   request: DrillRowsRequest,
): RowsRequest | undefined {
   const steps = stepsOf(request.context);
   if (steps === undefined) return undefined;
   return {
      ...steps,
      field: request.field,
      rawValue: request.rawValue,
      label: request.label,
      ...(request.drillClauses === undefined
         ? {}
         : { drillClauses: request.drillClauses }),
      ...(request.timezone === undefined ? {} : { timezone: request.timezone }),
   };
}

/** The query the dialog runs, or undefined when the value cannot be spelled. */
export function rowsQuery(request: RowsRequest): string | undefined {
   const drill = request.drillClauses
      ? drillClausesText(request.drillClauses, request.timezone)
      : singleClause(request);
   if (drill === undefined) return undefined;
   // `drill:` recomputes the view's dimensions in this query's zone, so a
   // tile with its own `timezone:` drills in that zone or matches nothing.
   const zone =
      request.drillClauses && request.timezone
         ? `timezone: '${escapeMalloyString(request.timezone)}'; `
         : "";
   return `run: ${request.source} -> { ${zone}drill: ${drill}; select: *; limit: ${ROW_LIMIT} }`;
}

/** Why a request has no query, for the dialog to say. */
export function rowsRefusal(request: RowsRequest): string {
   const missing = request.drillClauses
      ? nullDimensions(request.drillClauses)
      : [];
   return missing.length > 0
      ? `This row has no value for ${missing.join(" or ")}, and Malloy cannot drill to rows by a missing value, so its rows cannot be fetched.`
      : "This value cannot be written as a filter, so its rows cannot be fetched.";
}

function singleClause(request: RowsRequest): string | undefined {
   const literal = malloyLiteral(request.rawValue);
   return literal && `${request.view}.${request.field} = ${literal}`;
}

/** What the title names: every value of the row, or the clicked one alone. */
export function rowsTitleParts(request: RowsRequest): DrillClauseLabel[] {
   const parts = request.drillClauses
      ? drillClauseLabels(request.drillClauses, request.timezone)
      : [];
   return parts.length > 0
      ? parts
      : [{ field: request.field, label: request.label }];
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
                     {rowsTitleParts(request).map((part, index) => (
                        <Fragment key={index}>
                           {index > 0 && " and "}
                           <Box component="code" sx={{ fontSize: "0.9em" }}>
                              {part.field}
                           </Box>{" "}
                           is {part.label}
                        </Fragment>
                     ))}
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
                  {rowsRefusal(request)}
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
                     fontFamily: theme.ui.font.mono,
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

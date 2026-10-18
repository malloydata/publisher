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
   literalText,
   type DrillClause,
   type DrillClauseLabel,
} from "../drill/drillClauses";
import type { DrillRowsRequest } from "../drill/resolveDrill";
import {
   rowDimensionsOf,
   type RowsDimension,
   type RowsField,
   type RowsTarget,
} from "../drill/rowsTarget";
import { malloyLiteral } from "../../utils/malloyLiteral";
import { escapeMalloyString } from "../../utils/malloyString";
import { isIdentifier, tileSteps } from "../../utils/malloyText";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { ResultPanel } from "../RenderedResult/ResultPanel";
import { withPreamble } from "./textSource";
import { now } from "../../utils/clock";

/**
 * The rows behind a value: "show the rows" on a cell, as one query.
 *
 * A grouped value is an aggregate over rows of the tile's source, and those
 * rows are the source filtered by everything the value was filtered by: the
 * tile's own `where:` (its control bindings included), every enclosing nest's,
 * and each dimension of the clicked row at its value. All of that is read
 * from the result's metadata (`rowsTarget.ts`), so a refined tile, a named
 * query and a nest open their rows the same way a `source -> view` tile does.
 */
export const ROW_LIMIT = 200;

export interface RowsRequest {
   target: RowsTarget;
   field: string;
   rawValue: unknown;
   label: string;
   /** The clicked row's dimensions as code, each with its value. */
   dimensions?: readonly RowsDimension[];
   /** The clicked field's code, for a click that came with no row. */
   code?: string;
   /** The clicked row as the renderer reported it, for the title and a null. */
   drillClauses?: readonly DrillClause[];
   timezone?: string;
}

/** The dialog's request for a click, if its result named everything a query needs. */
export function rowsRequestFor(
   request: DrillRowsRequest,
): RowsRequest | undefined {
   if (request.target === undefined) return undefined;
   if (request.dimensions === undefined && request.code === undefined)
      return undefined;
   return {
      target: request.target,
      field: request.field,
      rawValue: request.rawValue,
      label: request.label,
      ...(request.dimensions === undefined
         ? {}
         : { dimensions: request.dimensions }),
      ...(request.code === undefined ? {} : { code: request.code }),
      ...(request.drillClauses === undefined
         ? {}
         : { drillClauses: request.drillClauses }),
      ...(request.timezone === undefined ? {} : { timezone: request.timezone }),
   };
}

/** The query the dialog runs, or undefined when a value cannot be spelled. */
export function rowsQuery(request: RowsRequest): string | undefined {
   const values = dimensionFilters(request);
   if (values === undefined) return undefined;
   const where = [...request.target.filters, ...values].join(", ");
   // A timestamp is truncated in the query's zone, so a tile with its own
   // `timezone:` is filtered in that zone or matches nothing.
   const zone = request.timezone
      ? `timezone: '${escapeMalloyString(request.timezone)}'; `
      : "";
   return `run: ${request.target.source} -> { ${zone}where: ${where}; select: *; limit: ${ROW_LIMIT} }`;
}

/** Each dimension at its value; undefined if any one cannot be written. */
function dimensionFilters(request: RowsRequest): string[] | undefined {
   if (request.dimensions) {
      const out: string[] = [];
      for (const { code, value } of request.dimensions) {
         // A row whose dimension is null is the rows where it is null.
         if (value.kind === "null_literal") {
            out.push(`${code} is null`);
            continue;
         }
         const literal = literalText(value, request.timezone);
         if (literal === undefined) return undefined;
         out.push(`${code} = ${literal}`);
      }
      return out;
   }
   const literal = malloyLiteral(request.rawValue);
   return request.code !== undefined && literal
      ? [`${request.code} = ${literal}`]
      : undefined;
}

/** What the dialog says when a request has no query. */
export const ROWS_REFUSAL =
   "This value cannot be written as a filter, so its rows cannot be fetched.";

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
 * else.
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

/**
 * Which of a tile's fields may open rows, beyond what the result says.
 *
 * A one-stage `source -> view` reports every dimension through the view
 * (`drill_expression.path = [view]`). One with no path there means the view
 * runs more than one stage, and a later stage's dimension is an earlier
 * stage's output, which the source cannot be filtered by: its rows query would
 * fail. A refined or named query reports a plain source field with no path
 * too, so this is decided only where the tile says it is `source -> view`.
 */
export function rowsGateFor(
   tileExpression: string | undefined,
): (field: RowsField) => boolean {
   if (stepsOf(tileExpression) === undefined) return () => true;
   return (field) =>
      [field, ...rowDimensionsOf(field)].every(
         (d) =>
            d.drillStableExpression?.kind !== "field_reference" ||
            (d.drillStableExpression.path ?? []).length > 0,
      );
}

export function RowsDialog({
   request,
   environmentName,
   packageName,
   versionId,
   modelPath,
   preamble,
   givens,
   onClose,
   onDone,
}: {
   request: RowsRequest | undefined;
   environmentName: string;
   packageName: string;
   versionId?: string;
   modelPath: string;
   /**
    * Text-source mode: the document's definitions, sent ahead of the rows
    * query exactly as they are ahead of each tile's, so the rows run as the
    * viewer's own text on the same model.
    */
   preamble?: string;
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
                     ? `The first ${ROW_LIMIT} rows of ${request.target.source} behind this value, with the tile's own filters and the controls applied.`
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
                  {ROWS_REFUSAL}
               </Typography>
            )}
            {query !== undefined && (
               <RowsResult
                  query={
                     preamble === undefined
                        ? query
                        : withPreamble(preamble, query)
                  }
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

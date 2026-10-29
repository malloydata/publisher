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
import { Fragment, useEffect, useRef, type ReactNode } from "react";
import { useQueryResult } from "../../hooks/useQueryResult";
import {
   drillClauseLabels,
   literalText,
   type DrillClauseLabel,
} from "../drill/drillClauses";
import type { DrillRowsRequest } from "../drill/resolveDrill";
import { type RowsTarget } from "../drill/rowsTarget";
import { malloyLiteral } from "../../utils/malloyLiteral";
import { escapeMalloyString } from "../../utils/malloyString";
import { isBareName, isIdentifier, tileSteps } from "../../utils/malloyText";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { ResultPanel } from "../RenderedResult/ResultPanel";
import { withPreamble } from "./textSource";
import { now } from "../../utils/clock";
import { DownloadRowsCsv } from "./DownloadRowsCsv";
import { rowsFileName } from "./rowsCsv";

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

/** A click's rows request once its result has named everything a query needs. */
export type RowsRequest = DrillRowsRequest & { target: RowsTarget };

/** The dialog's request for a click, if its result named everything a query needs. */
export function rowsRequestFor(
   request: DrillRowsRequest,
): RowsRequest | undefined {
   const { target } = request;
   if (target === undefined) return undefined;
   if (request.dimensions === undefined && request.code === undefined)
      return undefined;
   return { ...request, target };
}

/**
 * What a host's rows actions get: everything needed to fetch the rows behind
 * the value in full, not just the first {@link ROW_LIMIT} the dialog shows.
 */
export interface RowsActionsContext {
   /**
    * The rows query with no `limit:`, so the server's own row cap is the only
    * bound. Without the preamble: in text-source mode send
    * `withPreamble(preamble, query)`, as the dialog does.
    */
   query: string;
   /** Text-source mode: the document's definitions, sent ahead of `query`. */
   preamble?: string;
   environmentName: string;
   packageName: string;
   modelPath: string;
   versionId?: string;
   /** The givens the dialog's own query sends. */
   givens: Record<string, unknown>;
   /** A file name without an extension: the source and each value of the row. */
   fileName: string;
}

/**
 * The host's actions for the rows behind a value (a download, an export),
 * drawn in the rows dialog's title row. Return `null` to show none; omitted,
 * the dialog offers its own Download CSV.
 */
export type RowsActions = (context: RowsActionsContext) => ReactNode;

/**
 * The query the dialog runs, or undefined when a value cannot be spelled.
 * `limit` caps the rows; `null` leaves the query unlimited, so only the
 * server's own cap applies.
 */
export function rowsQuery(
   request: RowsRequest,
   limit: number | null = ROW_LIMIT,
): string | undefined {
   const values = dimensionFilters(request);
   if (values === undefined) return undefined;
   const where = [...request.target.filters, ...values].join(", ");
   // A timestamp is truncated in the query's zone, so a tile with its own
   // `timezone:` is filtered in that zone or matches nothing.
   const zone = request.timezone
      ? `timezone: '${escapeMalloyString(request.timezone)}'; `
      : "";
   const cap = limit === null ? "" : `; limit: ${limit}`;
   const source = sourceName(request.target.source);
   if (source === undefined) return undefined;
   return `run: ${source} -> { ${zone}where: ${where}; select: *${cap} }`;
}

/** A source name as `run:` reads it: back-quoted unless it stands bare. */
function sourceName(name: string): string | undefined {
   if (isBareName(name)) return name;
   return name.includes("`") ? undefined : `\`${name}\``;
}

/**
 * A dimension's code as an operand: parenthesized unless it is a plain name
 * or dotted path, since `region = 'East' or region = 'West' = false` binds
 * the `=` to the last disjunct only.
 */
function operand(code: string): string {
   return /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(code)
      ? code
      : `(${code})`;
}

/** Each dimension at its value; undefined if any one cannot be written. */
function dimensionFilters(request: RowsRequest): string[] | undefined {
   if (request.dimensions) {
      const out: string[] = [];
      for (const { code, value } of request.dimensions) {
         // A row whose dimension is null is the rows where it is null.
         if (value.kind === "null_literal") {
            out.push(`${operand(code)} is null`);
            continue;
         }
         const literal = literalText(value, request.timezone);
         if (literal === undefined) return undefined;
         out.push(`${operand(code)} = ${literal}`);
      }
      return out;
   }
   const literal = malloyLiteral(request.rawValue);
   return request.code !== undefined && literal
      ? [`${operand(request.code)} = ${literal}`]
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
   actions,
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
   /** The host's actions for these rows; omitted, the dialog's own Download CSV. */
   actions?: RowsActions;
}) {
   const { theme } = usePublisherTheme();
   const query = request ? rowsQuery(request) : undefined;
   const unlimited = request ? rowsQuery(request, null) : undefined;
   const actionsContext: RowsActionsContext | undefined =
      request && unlimited !== undefined
         ? {
              query: unlimited,
              ...(preamble === undefined ? {} : { preamble }),
              environmentName,
              packageName,
              modelPath,
              ...(versionId === undefined ? {} : { versionId }),
              givens,
              fileName: rowsFileName(
                 request.target.source,
                 rowsTitleParts(request).map((part) => part.label),
              ),
           }
         : undefined;

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
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, ml: 1 }}>
               {actionsContext &&
                  (actions ? (
                     actions(actionsContext)
                  ) : (
                     <DownloadRowsCsv {...actionsContext} />
                  ))}
               <IconButton
                  onClick={onClose}
                  aria-label="Close"
                  size="small"
                  sx={{ color: "text.secondary" }}
               >
                  <CloseIcon fontSize="small" />
               </IconButton>
            </Box>
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

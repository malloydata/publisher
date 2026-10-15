// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, LinearProgress, Typography } from "@mui/material";
import { useEffect, useMemo } from "react";
import type { QueryResultState } from "../../hooks/useQueryResult";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { DrillBinding } from "../drill/useDrill";
import { Loading, LOADING_COPY } from "../Loading";
import { chartShapeProblem } from "./chartShapeProblem";
import { warmMalloyRenderer } from "./loadRenderer";
import ResultContainer from "./ResultContainer";

export interface ResultPanelProps {
   /** From `useQueryResult`. */
   state: QueryResultState;
   /** Names the request in an error: the tile expression, the query name, the model path. */
   context: string;
   loadingText?: string;
   maxHeight?: number;
   maxResultSize?: number;
   drill?: DrillBinding;
   /** See `ResultContainer`: stretch the result to its cell. */
   fill?: boolean;
   /**
    * Rewrites the result before it is rendered — a dashboard tile promotes a
    * one-row measure result to KPI cards this way. Memoized on the result
    * string, so a large result is not re-parsed on every render around it.
    */
   transform?: (result: string) => string;
}

/**
 * The three states of a running query, once: waiting, failed, and the result
 * itself through `ResultContainer`.
 *
 * Every surface that runs a query goes through the same three, and each used
 * to draw them itself with its own words ("Running…", "Loading results...",
 * "Fetching Query Results...") and, in one case, without the failed state at
 * all. A tile owning its own panel is still what lets a composite dashboard
 * survive a bad tile: the broken one shows its error in place and the rest of
 * the grid still renders.
 */
export function ResultPanel({
   state,
   context,
   loadingText = LOADING_COPY.running,
   maxHeight,
   maxResultSize,
   drill,
   fill,
   transform,
}: ResultPanelProps) {
   const { data, isSuccess, isError, error, fetchStatus, isPlaceholderData } =
      state;
   // Fetch the renderer while the query runs, so the first chart does not wait
   // on it after the rows arrive. Not before: a tile still waiting to scroll
   // into view has asked for nothing, and neither should its renderer.
   const resultOnItsWay = isSuccess || fetchStatus === "fetching";
   useEffect(() => {
      if (resultOnItsWay) warmMalloyRenderer();
   }, [resultOnItsWay]);
   const raw = data?.data.result;
   const result = useMemo(
      () => (raw !== undefined && transform ? transform(raw) : raw),
      [raw, transform],
   );
   // A chart the result cannot fill, said in plain words in place of the
   // renderer's own error.
   const problem = useMemo(() => chartShapeProblem(result), [result]);

   if (isError) {
      return (
         <Box sx={{ p: 2 }}>
            <ApiErrorDisplay context={context} error={error} />
         </Box>
      );
   }
   if (!isSuccess)
      // Nothing is running while a query waits (a tile not yet scrolled near),
      // so it does not say "Running…".
      return (
         <Loading
            text={fetchStatus === "idle" ? LOADING_COPY.loading : loadingText}
         />
      );
   if (problem)
      return (
         <Box sx={{ p: 2 }}>
            <Typography variant="body2" role="status" color="text.secondary">
               {problem}
            </Typography>
         </Box>
      );
   const shown = (
      <ResultContainer
         result={result}
         maxHeight={maxHeight}
         maxResultSize={maxResultSize}
         renderLogs={data.data.renderLogs}
         drill={drill}
         fill={fill}
      />
   );
   // The previous answer, while the new one runs: it stays where it is, eased
   // back after a beat so a quick run never flickers, with a thin bar along
   // the top saying work is under way, and is replaced in place when the rows
   // land. The wrapper is always there, so going stale and back never moves
   // the result in the tree, which would remount (and redraw) the chart.
   return (
      <Box
         aria-busy={isPlaceholderData || undefined}
         sx={{
            position: "relative",
            // Both layers pass the parent's flex column through: the result
            // sizes itself as a flex child (a basis from its own height, or the
            // cell's height under `fill`), and an ordinary block between it and
            // its parent left it zero tall.
            display: "flex",
            flexDirection: "column",
            flex: "1 1 auto",
            minHeight: 0,
            minWidth: 0,
            ...(fill ? { height: "100%" } : {}),
            "& > .result-body": {
               display: "flex",
               flexDirection: "column",
               flex: "1 1 auto",
               minHeight: 0,
               minWidth: 0,
               ...(fill ? { height: "100%" } : {}),
               opacity: isPlaceholderData ? 0.55 : 1,
               transition: isPlaceholderData
                  ? "opacity 160ms ease 200ms"
                  : "opacity 120ms ease",
               "@media (prefers-reduced-motion: reduce)": {
                  transition: "none",
               },
            },
         }}
      >
         {isPlaceholderData && (
            <LinearProgress
               aria-label={loadingText}
               sx={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  right: 0,
                  height: 2,
                  zIndex: 1,
                  bgcolor: "transparent",
                  borderRadius: 1,
               }}
            />
         )}
         <Box className="result-body">{shown}</Box>
      </Box>
   );
}

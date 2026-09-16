// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { useMemo } from "react";
import type { QueryResultState } from "../../hooks/useQueryResult";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { DrillBinding } from "../drill/useDrill";
import { Loading } from "../Loading";
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
   loadingText = "Running…",
   maxHeight,
   maxResultSize,
   drill,
   transform,
}: ResultPanelProps) {
   const { data, isSuccess, isError, error } = state;
   const raw = data?.data.result;
   const result = useMemo(
      () => (raw !== undefined && transform ? transform(raw) : raw),
      [raw, transform],
   );

   if (isError) {
      return (
         <Box sx={{ p: 2 }}>
            <ApiErrorDisplay context={context} error={error} />
         </Box>
      );
   }
   if (!isSuccess) return <Loading text={loadingText} />;
   return (
      <ResultContainer
         result={result}
         maxHeight={maxHeight}
         maxResultSize={maxResultSize}
         renderLogs={data.data.renderLogs}
         drill={drill}
      />
   );
}

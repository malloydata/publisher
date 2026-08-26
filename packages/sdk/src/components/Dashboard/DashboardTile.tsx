// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Paper, Typography } from "@mui/material";
import { DASHBOARD_CARD_PADDING_PX } from "../../theme/buildTableCssVars";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import type { GivenValue } from "../../hooks/givenValue";
import { CHART_RESULT_QUERY_OPTIONS } from "../../utils/queryClient";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { humanizeSlug, type DrillBinding } from "../drill";
import { givensToRequest } from "../given/paramCodec";
import { Loading } from "../Loading";
import ResultContainer from "../RenderedResult/ResultContainer";
import { useServer } from "../ServerProvider";

export interface DashboardTileProps {
   environmentName: string;
   packageName: string;
   modelPath: string;
   /** A named query (the single-query form). */
   queryName?: string;
   /** A run expression (a composite tile). */
   tile?: string;
   /** `# label` on the view the tile names, when it has one. */
   label?: string;
   /** `# subtitle` on it: a second line under the heading. */
   subtitle?: string;
   /** `# borderless` on it: no card around the result. */
   borderless?: boolean;
   /**
    * The whole applied control row, NOT this tile's share of it. The narrowing
    * happens here, in `givensToRequest` below, using {@link givenNames}: the
    * caller does not know which givens a tile references and this component
    * does.
    */
   givens: Map<string, GivenValue>;
   /** Declared type per given name, which decides how a value is encoded. */
   declaredTypes: ReadonlyMap<string, string | undefined>;
   /**
    * Given names this tile references, or undefined when discovery could not
    * resolve the tile. Undefined means "send the whole control row", which the
    * server accepts: a surfaced given a query does not reference is ignored.
    */
   givenNames?: string[];
   height: number;
   maxResultSize?: number;
   /** Cell clicks and their affordance, for the dashboard's `# drill`. */
   drill?: DrillBinding;
}

/**
 * A composite tile's heading when the view it names carries no `# label`: the
 * last step's name as a sentence, so `scoped_sales -> sales_by_month` is titled
 * "Sales by month".
 *
 * The label itself is the same `# label` a nest gets in the single-query form,
 * read off the view server-side, so one view is titled identically whichever way
 * it is consumed. This derivation is the fallback for an untitled view, so a
 * tile shows a heading rather than code.
 */
export function tileTitle(tile: string): string {
   const lastStep = tile.split("->").at(-1)?.trim() ?? tile;
   // Anything that is not a bare name (an inline `{ … }` stage, say) has no
   // sensible title to derive, so it keeps the expression as written.
   return /^[A-Za-z_][\w-]*$/.test(lastStep) ? humanizeSlug(lastStep) : tile;
}

/**
 * One result panel: run it, render it, and keep its failure to itself.
 *
 * A tile owning its own query is what lets a composite dashboard survive a bad
 * tile: the broken one shows its error in place and the rest of the grid still
 * renders, rather than one failure blanking the page.
 */
export function DashboardTile({
   environmentName,
   packageName,
   modelPath,
   queryName,
   tile,
   label,
   subtitle,
   borderless,
   givens,
   declaredTypes,
   givenNames,
   height,
   maxResultSize,
   drill,
}: DashboardTileProps) {
   const { apiClients } = useServer();
   const { theme } = usePublisherTheme();
   const requestGivens = givensToRequest(givens, declaredTypes, givenNames);

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: [
         "dashboardTile",
         environmentName,
         packageName,
         modelPath,
         queryName,
         tile,
         // Re-runs when the applied values change, which is the whole point of
         // the control row.
         JSON.stringify(requestGivens),
      ],
      queryFn: () =>
         apiClients.models.executeQueryModel(
            environmentName,
            packageName,
            modelPath,
            {
               queryName,
               query: tile !== undefined ? `run: ${tile}` : undefined,
               givens: requestGivens,
            },
         ),
      ...CHART_RESULT_QUERY_OPTIONS,
   });

   return (
      <Paper
         elevation={0}
         sx={{
            // The instance theme's border, not MUI's `divider`: the renderer
            // card's edge is this same value, and a card that agrees with the
            // theme everywhere except its outline still reads as a different
            // card. Radius stays on the host's `shape.borderRadius`, which the
            // renderer card is now pointed at too.
            //
            // `# borderless` asks for the result with no card, which the renderer
            // honours by dropping background, border, radius and most padding on
            // its own `.dashboard-item`. Same here, so the tag reads the same on
            // both forms.
            border: borderless ? "none" : theme.border,
            borderRadius: borderless ? 0 : 1,
            background: borderless ? "none" : undefined,
            overflow: "hidden",
            minHeight: 120,
            p: borderless ? "12px 0" : `${DASHBOARD_CARD_PADDING_PX}px`,
         }}
      >
         {tile !== undefined && (
            <Box sx={{ pb: 1.5 }}>
               <Typography
                  variant="subtitle2"
                  sx={{
                     fontWeight: 500,
                     color: theme.tileTitle,
                     fontFamily: theme.font.family,
                  }}
                  // The expression is what actually ran, so it stays reachable
                  // as a tooltip rather than as the heading.
                  title={tile}
               >
                  {label ?? tileTitle(tile)}
               </Typography>
               {subtitle !== undefined && (
                  <Typography
                     variant="caption"
                     sx={{
                        display: "block",
                        color: theme.tileTitle,
                        fontFamily: theme.font.family,
                        opacity: 0.8,
                     }}
                  >
                     {subtitle}
                  </Typography>
               )}
            </Box>
         )}
         {!isSuccess && !isError && <Loading text="Running…" />}
         {isSuccess && (
            <ResultContainer
               result={data.data.result}
               maxHeight={height}
               maxResultSize={maxResultSize}
               renderLogs={data.data.renderLogs}
               drill={drill}
            />
         )}
         {isError && (
            <Box sx={{ p: 2 }}>
               <ApiErrorDisplay
                  context={tile ?? queryName ?? modelPath}
                  error={error}
               />
            </Box>
         )}
      </Paper>
   );
}

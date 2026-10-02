// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ExploreOutlinedIcon from "@mui/icons-material/ExploreOutlined";
import { IconButton, Tooltip } from "@mui/material";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { useQueryResult } from "../../hooks/useQueryResult";
import type { GivenValue } from "../../hooks/givenValue";
import { humanizeSlug, type DrillBinding } from "../drill";
import { givensToRequest } from "../given/paramCodec";
import { ResultPanel } from "../RenderedResult/ResultPanel";
import { promoteMeasureRowToKpis } from "./promoteMeasureRow";
import { TileFilterTag } from "./TileFilterTag";
import {
   TileCard,
   TileHeading,
   type TileChrome,
   type TileHeadingSlots,
} from "./TileCard";

export interface DashboardTileProps {
   environmentName: string;
   packageName: string;
   /** The package version the dashboard's URI named, when it named one. */
   versionId?: string;
   modelPath: string;
   /** A named query (the single-query form). */
   queryName?: string;
   /** A run expression (a composite tile). */
   tile?: string;
   /** Annotation lines placed above the tile's `run:`, which stack on the view's own. */
   annotation?: string;
   /** `# label` on the view the tile names, when it has one. */
   label?: string;
   /** `# subtitle` on it: a second line under the heading. */
   subtitle?: string;
   /** The heading's text as nodes, which replace `label` and `subtitle` where a surface edits them in place. */
   heading?: TileHeadingSlots;
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
   /**
    * Open this tile's query somewhere it can be changed. Shown as a button in
    * the heading, on hover; absent, the heading has no button.
    */
   onExplore?: () => void;
   /** `none` draws the result bare, its label a quiet caption above it. */
   chrome?: TileChrome;
   /** Labels of the filters this tile answers to, shown as a small tag. */
   filterLabels?: readonly string[];
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
   versionId,
   modelPath,
   queryName,
   tile,
   annotation,
   label,
   subtitle,
   heading,
   borderless,
   givens,
   declaredTypes,
   givenNames,
   height,
   maxResultSize,
   drill,
   onExplore,
   chrome = "card",
   filterLabels,
}: DashboardTileProps) {
   const { theme } = usePublisherTheme();
   const state = useQueryResult({
      environmentName,
      packageName,
      modelPath,
      versionId,
      queryName,
      query:
         tile !== undefined
            ? `${annotation ? `${annotation}\n` : ""}run: ${tile}`
            : undefined,
      // Narrowed to the givens this tile references: see `givenNames`.
      givens: givensToRequest(givens, declaredTypes, givenNames),
   });

   return (
      <TileCard
         borderless={borderless}
         chrome={chrome}
         sx={{
            // A table is as wide as its tile body, whatever its columns need.
            "& .malloy-render, & .malloy-table": { width: "100%" },
            // The heading's button shows on hover and keyboard focus, the way
            // a tile's chrome does everywhere else; always-on it competes with
            // the title on every card at once.
            "& .publisher-tile-explore": {
               opacity: 0,
               transition: "opacity 120ms",
            },
            "&:hover .publisher-tile-explore, & .publisher-tile-explore:focus-visible":
               { opacity: 1 },
         }}
      >
         {tile !== undefined && (
            <TileHeading
               title={heading?.title ?? label ?? tileTitle(tile)}
               subtitle={heading ? heading.subtitle : subtitle}
               quiet={chrome === "none"}
               // The expression is what actually ran, so it stays reachable as
               // a tooltip rather than as the heading.
               tooltip={tile}
               action={
                  onExplore && (
                     <Tooltip title="Explore from here">
                        <IconButton
                           className="publisher-tile-explore"
                           size="small"
                           aria-label={`Explore ${label ?? tileTitle(tile)}`}
                           onClick={onExplore}
                           sx={{ mt: -0.5, mr: -0.5, color: theme.tileTitle }}
                        >
                           <ExploreOutlinedIcon fontSize="small" />
                        </IconButton>
                     </Tooltip>
                  )
               }
            />
         )}
         {filterLabels && <TileFilterTag labels={filterLabels} />}
         <ResultPanel
            fill
            state={state}
            context={tile ?? queryName ?? modelPath}
            maxHeight={height}
            maxResultSize={maxResultSize}
            drill={drill}
            // A composite tile that is one row of measures draws as KPI cards,
            // the way Malloyyo splices the same tile into its grid, rather than
            // as a one-row table. Composite only: the single-query form is one
            // result the renderer lays out from the query's own tags, and its
            // aggregates are already tiles.
            transform={tile !== undefined ? promoteMeasureRowToKpis : undefined}
         />
      </TileCard>
   );
}

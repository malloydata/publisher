// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AdsClickIcon from "@mui/icons-material/AdsClick";
import { Button, Popover, Stack, Typography } from "@mui/material";
import { useEffect, useRef } from "react";
import { useDraft } from "./useDraft";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogView } from "./catalog";
import { isQueryTile, type DashboardTile, type QueryTile } from "./document";
import { ChartPicker } from "./ChartPicker";

/**
 * A tile's own settings, on the tile: a popover off its menu button, so
 * editing it never means scrolling away from it. Its chart, clickable cells
 * and removal; its width is set by dragging its right edge; its title and subtitle are edited on the tile. Its row is set by dragging it — a drop
 * into the empty end of a row is what "start a new row" means — and its card
 * is the reader's to decide, so neither is a toggle here. Which controls it
 * answers to is not here either: filters are configured in one place, the
 * strip under the header. A chart pick commits at once, so the tile redraws
 * under the open menu (`useDraft`'s `apply`).
 *
 * A text tile has no menu: its words are written on the tile itself, and it is
 * removed from the × on its corner, as every tile is.
 */
export interface TileMenuProps {
   anchor: HTMLElement | null;
   tile: DashboardTile | undefined;
   onClose: () => void;
   /** Apply the edited tile. Called once, on close, only when something changed. */
   onCommit: (next: DashboardTile) => void;
   /**
    * Open the clickable-cells window for this tile's source. Absent when the
    * source declares no dimension in this file, so there is nothing a drill
    * could go on: the menu then offers no Drill, rather than a window that
    * explains why its own Apply is off.
    */
   onDrills?: () => void;
   /** The catalog's view the tile shows, when the catalog knows it: what decides which charts are offered. */
   view?: Pick<CatalogView, "chart" | "aggregateOnly">;
}

const customChart = (lines: string[] | undefined) =>
   lines && lines.length === 1
      ? `This tile's chart line (${lines[0]}) is not one the builder models, so its chart cannot be changed here.`
      : `This tile has ${lines?.length ?? "several"} chart lines${lines ? ` (${lines.join(" and ")})` : ""}, so its chart cannot be changed here.`;
const INHERITED_CHART =
   "This tile's view is declared on its source, so its chart is set in the model.";

export function TileMenu({
   anchor,
   tile,
   onClose,
   onCommit,
   onDrills,
   view,
}: TileMenuProps) {
   const { theme } = usePublisherTheme();
   const { draft, apply, close } = useDraft(
      tile,
      anchor !== null,
      onCommit,
      onClose,
   );

   const query = draft !== undefined && isQueryTile(draft) ? draft : undefined;
   const editable =
      query !== undefined && query.declaration.kind !== "inherited";
   // The draft is a query tile whenever the controls that call this are shown.
   // Committed at once, so the tile redraws in the new chart while the menu is
   // still open rather than when it closes.
   const applyQuery = (change: (t: QueryTile) => void) =>
      apply((t) => {
         if (isQueryTile(t)) change(t);
      });
   // The chart as the menu opened on it: each pick commits, so the tile prop
   // moves with them, and "back to where it started" means where the menu
   // started.
   const openedChart = useRef<QueryTile["chart"]>(undefined);
   const opened = anchor !== null;
   useEffect(() => {
      if (opened)
         openedChart.current =
            tile && isQueryTile(tile) ? tile.chart : undefined;
      // Only on opening: the tile changes under every pick.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [opened]);

   return (
      <Popover
         open={anchor !== null && draft !== undefined}
         anchorEl={anchor}
         onClose={close}
         anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
         transformOrigin={{ vertical: "top", horizontal: "right" }}
         slotProps={{ paper: { sx: { width: 320, p: 2 } } }}
      >
         {draft && (
            <Stack sx={{ gap: 1.5 }}>
               <Typography
                  variant="overline"
                  sx={{ color: theme.tileTitle, lineHeight: 1.5 }}
               >
                  {query
                     ? `${query.source} → ${query.name}`
                     : `Text · ${draft.name}`}
               </Typography>

               {query && editable && (
                  <>
                     <ChartPicker
                        state={query.chart ?? "default"}
                        view={view}
                        cellLabel={`${query.source} ${query.name}`}
                        {...(query.chart === "custom"
                           ? { disabledReason: customChart(query.chartLines) }
                           : {})}
                        onChange={(next) =>
                           applyQuery((t) => {
                              const was = openedChart.current;
                              // Back to where it started is no edit, so the draft must not differ from the tile.
                              if (next === (was ?? "default")) {
                                 if (was === undefined) delete t.chart;
                                 else t.chart = was;
                                 delete t.chartCarried;
                              } else {
                                 t.chart = next;
                                 if (view)
                                    t.chartCarried = view.chart
                                       ? [view.chart]
                                       : [];
                                 else delete t.chartCarried;
                              }
                           })
                        }
                     />
                  </>
               )}
               {query && !editable && (
                  <>
                     <Typography
                        variant="body2"
                        sx={{ color: theme.tileTitle }}
                     >
                        Declared on its source, so its title and layout are set
                        in the model. It can still be moved.
                     </Typography>
                     <ChartPicker
                        state="default"
                        view={view}
                        cellLabel={`${query.source} ${query.name}`}
                        disabledReason={INHERITED_CHART}
                        onChange={() => {}}
                     />
                  </>
               )}
               {query?.declaration.kind === "opaque" && (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     Its body is {query.declaration.why}, so a filter has no
                     single place to go. Everything else here is editable.
                  </Typography>
               )}
               <Stack direction="row" sx={{ justifyContent: "space-between" }}>
                  {editable && onDrills ? (
                     <Button
                        size="small"
                        startIcon={<AdsClickIcon />}
                        aria-haspopup="dialog"
                        onClick={() => {
                           close();
                           onDrills();
                        }}
                     >
                        Drill
                     </Button>
                  ) : (
                     <span />
                  )}
               </Stack>
            </Stack>
         )}
      </Popover>
   );
}

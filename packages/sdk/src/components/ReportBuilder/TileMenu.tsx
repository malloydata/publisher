// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CheckIcon from "@mui/icons-material/Check";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import {
   Box,
   Divider,
   ListItemIcon,
   ListItemText,
   Menu,
   MenuItem,
   Typography,
} from "@mui/material";
import { useEffect, useRef, useState } from "react";
import { useDraft } from "./useDraft";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogView } from "./catalog";
import { isQueryTile, type DashboardTile, type QueryTile } from "./document";
import { chartChoices } from "./ChartPicker";
import type { ChartState } from "./chartLine";
import { widthLabel } from "./widthLabel";

/**
 * A tile's own settings, on the tile: a menu off its ⋯ button, so editing it
 * never means scrolling away from it. Its chart (a submenu, each pick drawn at
 * once), its width on a grid (the same as dragging its right edge), and its
 * clickable cells; its title and subtitle are edited on the tile, and it is
 * removed from the × on its corner. Its row is set by dragging it — a drop
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
   /** The grid's column count, on a grid: the menu then offers the tile's width. */
   columns?: number;
}

/** The widths a menu offers on a grid of `columns`: the common fractions that land on whole columns, then the full width. */
export function widthChoices(columns: number): number[] {
   const spans = [1 / 4, 1 / 3, 1 / 2, 2 / 3, 3 / 4, 1]
      .map((fraction) => columns * fraction)
      .filter((span) => Number.isInteger(span) && span >= 1);
   return [...new Set(spans)];
}

const customChart = (lines: string[] | undefined) =>
   lines && lines.length === 1
      ? `This tile's chart line (${lines[0]}) is not one the builder models, so its chart cannot be changed here.`
      : `This tile has ${lines?.length ?? "several"} chart lines${lines ? ` (${lines.join(" and ")})` : ""}, so its chart cannot be changed here.`;
const INHERITED_CHART =
   "Declared on its source, so its chart is set in the model.";

export function TileMenu({
   anchor,
   tile,
   onClose,
   onCommit,
   onDrills,
   view,
   columns,
}: TileMenuProps) {
   const { theme } = usePublisherTheme();
   const { draft, apply, close } = useDraft(
      tile,
      anchor !== null,
      onCommit,
      onClose,
   );
   // Which submenu is open, anchored to its item.
   const [sub, setSub] = useState<
      { kind: "chart" | "width"; anchor: HTMLElement } | undefined
   >(undefined);

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
      else setSub(undefined);
      // Only on opening: the tile changes under every pick.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [opened]);

   const chartState: ChartState = query?.chart ?? "default";
   const choices = chartChoices(view, chartState);
   const chartBlocked = !query
      ? undefined
      : !editable
        ? INHERITED_CHART
        : query.chart === "custom"
          ? customChart(query.chartLines)
          : undefined;
   const pickChart = (next: ChartState) =>
      applyQuery((t) => {
         const was = openedChart.current;
         // Back to where it started is no edit, so the draft must not differ from the tile.
         if (next === (was ?? "default")) {
            if (was === undefined) delete t.chart;
            else t.chart = was;
            delete t.chartCarried;
         } else {
            t.chart = next;
            if (view) t.chartCarried = view.chart ? [view.chart] : [];
            else delete t.chartCarried;
         }
      });

   const span = draft?.colspan ?? 1;
   const widths =
      columns !== undefined && columns > 1 ? widthChoices(columns) : [];
   const widthBlocked =
      query !== undefined && !editable
         ? "Its width is set in the model too."
         : undefined;
   const drillBlocked = !editable
      ? undefined
      : onDrills
        ? undefined
        : "Its source declares no dimension a click could filter by.";

   const subOpen =
      (kind: "chart" | "width") => (event: { currentTarget: HTMLElement }) =>
         setSub({ kind, anchor: event.currentTarget });
   const label = (text: string, value?: string, reason?: string) => (
      <>
         <ListItemText
            primary={text}
            {...(reason ? { secondary: reason } : {})}
            // A reason wraps under its item rather than widening the menu.
            secondaryTypographyProps={{
               sx: { whiteSpace: "normal", maxWidth: 240 },
            }}
         />
         {value && (
            <Typography variant="body2" sx={{ color: "text.secondary", ml: 2 }}>
               {value}
            </Typography>
         )}
      </>
   );

   return (
      <>
         <Menu
            open={anchor !== null && draft !== undefined}
            anchorEl={anchor}
            onClose={close}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            MenuListProps={{
               dense: true,
               disabledItemsFocusable: true,
               "aria-label": query ? `${query.source} ${query.name}` : "Tile",
            }}
            slotProps={{ paper: { sx: { minWidth: 240 } } }}
         >
            {query && (
               <MenuItem
                  aria-haspopup="menu"
                  aria-expanded={sub?.kind === "chart"}
                  disabled={chartBlocked !== undefined}
                  onClick={subOpen("chart")}
                  onKeyDown={(event) => {
                     if (event.key === "ArrowRight")
                        setSub({
                           kind: "chart",
                           anchor: event.currentTarget,
                        });
                  }}
               >
                  {label(
                     "Chart",
                     chartBlocked
                        ? undefined
                        : choices.find((c) => c.value === chartState)?.label,
                     chartBlocked,
                  )}
                  <ChevronRightIcon
                     fontSize="small"
                     sx={{ ml: 1, color: "text.secondary" }}
                  />
               </MenuItem>
            )}
            {widths.length > 0 && (
               <MenuItem
                  aria-haspopup="menu"
                  aria-expanded={sub?.kind === "width"}
                  disabled={widthBlocked !== undefined}
                  onClick={subOpen("width")}
                  onKeyDown={(event) => {
                     if (event.key === "ArrowRight")
                        setSub({
                           kind: "width",
                           anchor: event.currentTarget,
                        });
                  }}
               >
                  {label(
                     "Width",
                     widthBlocked ? undefined : `${span} of ${columns}`,
                     widthBlocked,
                  )}
                  <ChevronRightIcon
                     fontSize="small"
                     sx={{ ml: 1, color: "text.secondary" }}
                  />
               </MenuItem>
            )}
            {editable && (
               <MenuItem
                  aria-haspopup="dialog"
                  disabled={drillBlocked !== undefined}
                  onClick={() => {
                     close();
                     onDrills?.();
                  }}
               >
                  {label("Clickable cells…", undefined, drillBlocked)}
               </MenuItem>
            )}
            {query?.declaration.kind === "opaque" && (
               <Typography
                  variant="body2"
                  sx={{
                     color: theme.tileTitle,
                     px: 2,
                     py: 0.75,
                     maxWidth: 260,
                  }}
               >
                  Its body is {query.declaration.why}, so a filter has no single
                  place to go.
               </Typography>
            )}
            {query && <Divider />}
            {query && (
               <Box
                  aria-hidden
                  sx={{
                     px: 2,
                     py: 0.5,
                     fontSize: 12,
                     color: "text.secondary",
                     fontFamily:
                        "ui-monospace, SFMono-Regular, Menlo, monospace",
                  }}
               >
                  {query.source} → {query.name}
               </Box>
            )}
         </Menu>

         <Menu
            open={sub?.kind === "chart"}
            anchorEl={sub?.anchor ?? null}
            onClose={() => setSub(undefined)}
            anchorOrigin={{ vertical: "top", horizontal: "left" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            MenuListProps={{
               dense: true,
               disabledItemsFocusable: true,
               "aria-label": "Chart",
            }}
         >
            {choices
               .filter((choice) => choice.value !== "custom")
               .map((choice) => (
                  <MenuItem
                     key={choice.value}
                     role="menuitemradio"
                     aria-checked={choice.value === chartState}
                     disabled={choice.disabled === true}
                     onClick={() => {
                        pickChart(choice.value);
                        setSub(undefined);
                     }}
                  >
                     <ListItemText
                        primary={choice.label}
                        {...(choice.reason ? { secondary: choice.reason } : {})}
                     />
                     {choice.value === chartState && (
                        <ListItemIcon sx={{ minWidth: 0, ml: 2 }}>
                           <CheckIcon
                              fontSize="small"
                              sx={{ color: theme.accent }}
                           />
                        </ListItemIcon>
                     )}
                  </MenuItem>
               ))}
         </Menu>

         <Menu
            open={sub?.kind === "width"}
            anchorEl={sub?.anchor ?? null}
            onClose={() => setSub(undefined)}
            anchorOrigin={{ vertical: "top", horizontal: "left" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            MenuListProps={{ dense: true, "aria-label": "Width" }}
         >
            {columns !== undefined &&
               (widths.includes(span) ? widths : [span, ...widths]).map(
                  (each) => (
                     <MenuItem
                        key={each}
                        role="menuitemradio"
                        aria-checked={each === span}
                        onClick={() => {
                           apply((t) => {
                              t.colspan = each;
                           });
                           setSub(undefined);
                        }}
                     >
                        <ListItemText primary={widthLabel(each, columns)} />
                        {each === span && (
                           <ListItemIcon sx={{ minWidth: 0, ml: 2 }}>
                              <CheckIcon
                                 fontSize="small"
                                 sx={{ color: theme.accent }}
                              />
                           </ListItemIcon>
                        )}
                     </MenuItem>
                  ),
               )}
         </Menu>
      </>
   );
}

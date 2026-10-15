// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
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
import { edgeColor } from "../controlTokens";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogView } from "./catalog";
import {
   isQueryTile,
   isReadOnlyTile,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { chartChoices, type ViewStatus } from "./ChartPicker";
import type { ChartState } from "./chartLine";

/**
 * A tile's own settings, on the tile: a menu off its ⋯ button, so editing it
 * never means scrolling away from it. Its chart (a submenu, each pick drawn at
 * once), its clickable cells, and a copy of it; its title and subtitle are
 * edited on the tile, its width is set by dragging its right edge, and it is
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
   /** Copy the tile, right after it. Absent, the menu offers no Duplicate. */
   onDuplicate?: () => void;
   /** Why the tile cannot be copied, when it cannot: Duplicate is then listed, greyed, with this under it. */
   duplicateBlocked?: string;
   /** The catalog's view the tile shows, when the catalog knows it: what decides which charts are offered. */
   view?: Pick<CatalogView, "chart" | "aggregateOnly" | "comboShape" | "shape">;
   /** Why `view` is absent: still loading, or a view the catalog does not list. */
   viewStatus?: ViewStatus;
}

const customChart = (lines: string[] | undefined) =>
   lines && lines.length === 1
      ? `This tile's chart line (${lines[0]}) is not one the builder models, so its chart cannot be changed here.`
      : `This tile has ${lines?.length ?? "several"} chart lines${lines ? ` (${lines.join(" and ")})` : ""}, so its chart cannot be changed here.`;
/** Charts whose marks the renderer gives no click to: its click payload is a table cell or a dashboard item, never a mark. */
const CLICKLESS_CHARTS = new Set<string>([
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "combo_chart",
]);
const CHART_DOES_NOT_CLICK =
   "Its chart's bars, lines and points do not respond to a click; show it as a table to click its cells.";
const INHERITED_CHART =
   "Declared on its source, so its chart is set in the model.";
const QUERY_CHART =
   "It names a query, so its chart is set where that query is declared.";

export function TileMenu({
   anchor,
   tile,
   onClose,
   onCommit,
   onDrills,
   onDuplicate,
   duplicateBlocked,
   view,
   viewStatus,
}: TileMenuProps) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const { draft, apply, close } = useDraft(
      tile,
      anchor !== null,
      onCommit,
      onClose,
   );
   // Which submenu is open, anchored to its item.
   const [sub, setSub] = useState<
      { kind: "chart"; anchor: HTMLElement } | undefined
   >(undefined);

   const query = draft !== undefined && isQueryTile(draft) ? draft : undefined;
   const editable = query !== undefined && !isReadOnlyTile(query);
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
   const choices = chartChoices(view, chartState, viewStatus);
   const chartBlocked = !query
      ? undefined
      : query.declaration.kind === "query"
        ? QUERY_CHART
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

   const drillBlocked = !editable
      ? undefined
      : onDrills
        ? undefined
        : "Its source declares no dimension a click could filter by.";

   // The renderer reports a click on a table cell only, so on a tile drawn as
   // a chart the setting does nothing there. Said, not blocked: the tag is on
   // the dimension, and a table tile over the same source does use it.
   const drawn = chartState === "default" ? view?.chart : chartState;
   const drillNote =
      drawn !== undefined && CLICKLESS_CHARTS.has(drawn)
         ? CHART_DOES_NOT_CLICK
         : undefined;

   const subOpen = (kind: "chart") => (event: { currentTarget: HTMLElement }) =>
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
            className={reportClasses.menu}
            open={anchor !== null && draft !== undefined}
            anchorEl={anchor}
            onClose={close}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            MenuListProps={{
               dense: true,
               disabledItemsFocusable: true,
               "aria-label": query
                  ? query.declaration.kind === "query"
                     ? query.name
                     : `${query.source} ${query.name}`
                  : "Tile",
            }}
            // Set like the builder's other menus: a rounded, bordered card.
            slotProps={{
               paper: {
                  sx: {
                     minWidth: 240,
                     mt: 0.5,
                     borderRadius: ui.radius.menu,
                     border: `1px solid ${edgeColor(theme.cardBorder)}`,
                     boxShadow: ui.shadow.menu,
                  },
               },
            }}
         >
            {query && (
               <MenuItem
                  aria-haspopup="menu"
                  aria-expanded={sub?.kind === "chart"}
                  disabled={chartBlocked !== undefined}
                  onClick={subOpen("chart")}
                  onKeyDown={(event) => {
                     // Not from a disabled Chart: the keyboard reached a
                     // submenu the menu says cannot apply, and a pick there
                     // wrote a chart the model owns over its own.
                     if (
                        event.key === "ArrowRight" &&
                        chartBlocked === undefined
                     )
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
            {editable && (
               <MenuItem
                  aria-haspopup="dialog"
                  disabled={drillBlocked !== undefined}
                  onClick={() => {
                     close();
                     onDrills?.();
                  }}
               >
                  {label(
                     "Clickable cells…",
                     undefined,
                     drillBlocked ?? drillNote,
                  )}
               </MenuItem>
            )}
            {query && onDuplicate && (
               <MenuItem
                  disabled={duplicateBlocked !== undefined}
                  onClick={() => {
                     close();
                     onDuplicate();
                  }}
               >
                  {label("Duplicate", undefined, duplicateBlocked)}
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
                     ...ui.type.code,
                     color: "text.secondary",
                  }}
               >
                  {query.source} → {query.name}
               </Box>
            )}
         </Menu>

         <Menu
            className={reportClasses.menu}
            open={sub?.kind === "chart"}
            anchorEl={sub?.anchor ?? null}
            onClose={() => setSub(undefined)}
            anchorOrigin={{ vertical: "top", horizontal: "left" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            MenuListProps={{
               dense: true,
               disabledItemsFocusable: true,
               "aria-label": "Chart",
               // Back out of the submenu the way into it: the arrow.
               onKeyDown: (event) => {
                  if (event.key === "ArrowLeft") {
                     event.preventDefault();
                     setSub(undefined);
                  }
               },
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
      </>
   );
}

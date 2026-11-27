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
   tileExpression,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { chartChoices, type ViewStatus } from "./ChartPicker";
import {
   chartResetFor,
   isSeriesColor,
   seriesColorFrom,
   type ChartState,
} from "./chartLine";

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
    * Open "What clicking a value does" for this tile: the choices a click on
    * one of its values offers, written on its own entry in `tiles=[…]`. Absent
    * when the builder cannot open it, and the item is then greyed with why.
    */
   onDrills?: () => void;
   /** Copy the tile, right after it. Absent, the menu offers no Duplicate. */
   onDuplicate?: () => void;
   /** Why the tile cannot be copied, when it cannot: Duplicate is then listed, greyed, with this under it. */
   duplicateBlocked?: string;
   /** The catalog's view the tile shows, when the catalog knows it: what decides which charts are offered. */
   view?: Pick<
      CatalogView,
      "chart" | "aggregateOnly" | "shape" | "authorPalette"
   >;
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
   const { draft, apply, patch, close } = useDraft(
      tile,
      anchor !== null,
      onCommit,
      onClose,
   );
   // Which submenu is open, anchored to its item.
   const [sub, setSub] = useState<
      { kind: "chart" | "color"; anchor: HTMLElement } | undefined
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
   const openedChart = useRef<
      Pick<QueryTile, "chart" | "chartReset"> | undefined
   >(undefined);
   const otherColor = useRef<HTMLInputElement>(null);
   const opened = anchor !== null;
   useEffect(() => {
      if (opened)
         openedChart.current =
            tile && isQueryTile(tile)
               ? { chart: tile.chart, chartReset: tile.chartReset }
               : undefined;
      else setSub(undefined);
      // Only on opening: the tile changes under every pick.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [opened]);

   const chartState: ChartState = query?.chart ?? "default";
   const choices = chartChoices(view, chartState, viewStatus);
   const chartBlocked = !query
      ? undefined
      : !editable
        ? QUERY_CHART
        : query.chart === "custom"
          ? customChart(query.chartLines)
          : undefined;
   const pickChart = (next: ChartState) =>
      applyQuery((t) => {
         const was = openedChart.current;
         // Back to where it started is no edit, so the draft must not differ from the tile.
         if (next === (was?.chart ?? "default")) {
            if (was?.chart === undefined) delete t.chart;
            else t.chart = was.chart;
            if (was?.chartReset === undefined) delete t.chartReset;
            else t.chartReset = was.chartReset;
            return;
         }
         t.chart = next;
         // Only what the view's chart would otherwise still draw over.
         const reset =
            next === "default" || next === "custom"
               ? []
               : chartResetFor(next, view?.chart);
         if (reset.length > 0) t.chartReset = reset;
         else delete t.chartReset;
      });

   // Every query tile has an entry to write on, so this needs no edit rights
   // over the view: only a value to click.
   const drillBlocked = !onDrills
      ? "This tile can't be set up here."
      : view?.shape?.groupBys === 0
        ? "This tile shows totals only, so it has no value to click."
        : undefined;

   // The renderer reports a click on a table cell only, so on a tile drawn as
   // a chart the setting does nothing there. Said, not blocked: switching the
   // tile back to a table makes it count.
   const drawn = chartState === "default" ? view?.chart : chartState;
   const drillNote =
      drawn !== undefined && CLICKLESS_CHARTS.has(drawn)
         ? CHART_DOES_NOT_CLICK
         : undefined;

   const subOpen =
      (kind: "chart" | "color") => (event: { currentTarget: HTMLElement }) =>
         setSub({ kind, anchor: event.currentTarget });

   // A tile's colour is the chart's first series colour. Offered from the
   // theme's own series, so a pick stays on the palette the page uses, with
   // any other colour a picker away. Not on a table or KPI tile, which draw
   // no series; and not offered at all where the chart is locked, since the
   // Chart item already says why.
   // A multi-colour palette on the view, or one the tile's own tags already
   // carry, is the author's. A colour here is one line written after it, and
   // the last line wins, so the palette would stop drawing.
   const colorBlocked =
      chartBlocked ??
      (drawn === undefined || !COLORED_CHARTS.has(drawn)
         ? "Only a bar, line, scatter or bar and line chart has a color to set."
         : view?.authorPalette
           ? "This view sets its own colors, and a color here would replace them."
           : query?.ownPalette
             ? "This tile sets its own colors, and a color here would replace them."
             : undefined);
   // As the builder writes them, so a pick reads back; one it cannot write is
   // left to the "any other colour" picker.
   const swatches = Array.from(
      new Set(
         theme.series
            .map(seriesColorFrom)
            .filter((c): c is string => c !== undefined),
      ),
   ).slice(0, 8);
   const pickColor = (next: string | undefined) =>
      applyQuery((t) => {
         if (next === undefined) delete t.color;
         else t.color = next.toLowerCase();
      });
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
               "aria-label": query ? tileExpression(query) : "Tile",
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
            {/* Not where the chart itself is locked: the Chart item says why. */}
            {query && chartBlocked === undefined && (
               <MenuItem
                  aria-haspopup="menu"
                  aria-expanded={sub?.kind === "color"}
                  disabled={colorBlocked !== undefined}
                  onClick={subOpen("color")}
                  onKeyDown={(event) => {
                     if (
                        event.key === "ArrowRight" &&
                        colorBlocked === undefined
                     )
                        setSub({ kind: "color", anchor: event.currentTarget });
                  }}
               >
                  {label(
                     "Color",
                     colorBlocked ? undefined : (query.color ?? "Theme"),
                     colorBlocked,
                  )}
                  {!colorBlocked && (
                     <Swatch color={query.color ?? theme.series[0]} trailing />
                  )}
                  <ChevronRightIcon
                     fontSize="small"
                     sx={{ ml: 1, color: "text.secondary" }}
                  />
               </MenuItem>
            )}
            {query && (
               <MenuItem
                  aria-haspopup="dialog"
                  disabled={drillBlocked !== undefined}
                  onClick={() => {
                     close();
                     onDrills?.();
                  }}
               >
                  {label(
                     "What clicking a value does…",
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
                  {/* As the tile's placeholder names it (`TileFrame`): a named query is its name alone. */}
                  {tileExpression(query).replace("->", "→")}
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
                        {...((choice.reason ?? choice.hint)
                           ? { secondary: choice.reason ?? choice.hint }
                           : {})}
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
            className={reportClasses.menu}
            open={sub?.kind === "color"}
            anchorEl={sub?.anchor ?? null}
            onClose={() => setSub(undefined)}
            anchorOrigin={{ vertical: "top", horizontal: "left" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            MenuListProps={{
               dense: true,
               "aria-label": "Color",
               onKeyDown: (event) => {
                  if (event.key === "ArrowLeft") {
                     event.preventDefault();
                     setSub(undefined);
                  }
               },
            }}
         >
            <MenuItem
               role="menuitemradio"
               aria-checked={query?.color === undefined}
               onClick={() => {
                  pickColor(undefined);
                  setSub(undefined);
               }}
            >
               <Swatch color={theme.series[0]} />
               <ListItemText
                  primary="Theme"
                  secondary="The page's own colors"
               />
               {query?.color === undefined && <Check color={theme.accent} />}
            </MenuItem>
            {swatches.map((color) => (
               <MenuItem
                  key={color}
                  role="menuitemradio"
                  aria-checked={query?.color === color.toLowerCase()}
                  onClick={() => {
                     pickColor(color);
                     setSub(undefined);
                  }}
               >
                  <Swatch color={color} />
                  <ListItemText primary={color.toLowerCase()} />
                  {query?.color === color.toLowerCase() && (
                     <Check color={theme.accent} />
                  )}
               </MenuItem>
            ))}
            <Divider />
            {/* Any other colour: the platform's own picker, committed as the
             reader lets go of it. */}
            <MenuItem
               component="label"
               aria-checked={
                  query?.color !== undefined &&
                  !swatches.some((c) => c.toLowerCase() === query.color)
               }
               role="menuitemradio"
               // A label opens its input on a click only. Menu focus sits on
               // this item, not the input, so Enter and Space open the picker
               // here, as they choose any other item.
               onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                     event.preventDefault();
                     otherColor.current?.click();
                  }
               }}
            >
               <input
                  ref={otherColor}
                  type="color"
                  aria-label="Other color"
                  value={query?.color ?? toHex(theme.series[0])}
                  // Held in the draft while the picker is dragged, and
                  // committed once as the menu closes: one undo step, not
                  // one per shade passed on the way.
                  onChange={(event) => {
                     const next = event.target.value;
                     if (isSeriesColor(next))
                        patch((t) => {
                           if (isQueryTile(t)) t.color = next.toLowerCase();
                        });
                  }}
                  style={{
                     width: 18,
                     height: 18,
                     padding: 0,
                     border: "none",
                     background: "none",
                     marginRight: 12,
                  }}
               />
               <ListItemText primary="Other color…" />
            </MenuItem>
         </Menu>
      </>
   );
}

/** Charts that draw a series, and so have a colour to set. */
const COLORED_CHARTS = new Set<string>([
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "combo_chart",
]);

/** A colour input takes `#rrggbb` only. */
const toHex = (color: string | undefined) =>
   (color && seriesColorFrom(color)) ?? "#000000";

/**
 * A colour square. Leading, it sits before a choice's name; trailing, it
 * follows a row's value, spaced from it as the row's chevron is.
 */
function Swatch({
   color,
   trailing = false,
}: {
   color: string | undefined;
   trailing?: boolean;
}) {
   return (
      <Box
         aria-hidden
         data-swatch={trailing ? "trailing" : "leading"}
         sx={{
            width: 14,
            height: 14,
            borderRadius: "3px",
            bgcolor: color,
            border: "1px solid rgba(0,0,0,0.15)",
            ...(trailing ? { ml: 1 } : { mr: 1.5 }),
            flex: "none",
         }}
      />
   );
}

function Check({ color }: { color: string }) {
   return (
      <ListItemIcon sx={{ minWidth: 0, ml: 2 }}>
         <CheckIcon fontSize="small" sx={{ color }} />
      </ListItemIcon>
   );
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import BarChartIcon from "@mui/icons-material/BarChart";
import MapIcon from "@mui/icons-material/Map";
import NumbersIcon from "@mui/icons-material/Numbers";
import ScatterPlotIcon from "@mui/icons-material/ScatterPlot";
import SearchIcon from "@mui/icons-material/Search";
import ShowChartIcon from "@mui/icons-material/ShowChart";
import TableChartIcon from "@mui/icons-material/TableChart";
import { Box, InputAdornment, TextField, Typography } from "@mui/material";
import { useId, useMemo, type ReactNode, type Ref } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";

/**
 * Picking a view: one searchable list of every view on offer, grouped (by
 * source, or by model and source), each by its title and its Malloy name with
 * its chart's icon. The arrow keys move the pick from the search box and Enter
 * takes it; a double click takes one directly.
 *
 * One component for every window that starts from a view (Add a tile, New
 * report), so they look and work the same.
 */
export interface PickerEntry {
   /** Unique across every group. */
   key: string;
   /** The view's name in Malloy. */
   name: string;
   /** What the view is called to a reader: its doc line, or its name read as words. */
   title: string;
   /** The chart the view declares, for its icon; none is a table. */
   chart?: string;
}

export interface PickerGroup {
   key: string;
   /** The group's name: a source. */
   label: string;
   /** A line beside it: the source's doc, or the model it comes from. */
   detail?: string;
   /** The group's accessible name, when its label alone is not unique ("orders in sales.malloy"). */
   ariaLabel?: string;
   entries: PickerEntry[];
}

/** `by_category` -> `By category`: a view's name read as words, where it has no doc line. */
export function readableName(name: string): string {
   const words = name.replace(/_+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
}

/** The icon for the chart a view declares; a table when it declares none. */
export function ChartGlyph({ chart }: { chart: string | undefined }) {
   const sx = { fontSize: 16 };
   switch (chart) {
      case "bar_chart":
         return <BarChartIcon sx={sx} />;
      case "line_chart":
      case "sparkline":
         return <ShowChartIcon sx={sx} />;
      case "scatter_chart":
         return <ScatterPlotIcon sx={sx} />;
      case "shape_map":
      case "segment_map":
         return <MapIcon sx={sx} />;
      case "big_value":
         return <NumbersIcon sx={sx} />;
      default:
         return <TableChartIcon sx={sx} />;
   }
}

export function ViewPicker({
   groups,
   query,
   onQuery,
   picked,
   onPick,
   onTake,
   searchRef,
   empty,
   footer,
   disabled = false,
}: {
   groups: PickerGroup[];
   query: string;
   onQuery: (next: string) => void;
   /** The picked entry's key. */
   picked: string | undefined;
   onPick: (key: string) => void;
   /** Enter on the search, or a double click: take the pick and go. */
   onTake: (key: string) => void;
   /** The search box, for a window to start in. */
   searchRef?: Ref<HTMLInputElement>;
   /** Shown in place of the list when there are no groups at all. */
   empty?: ReactNode;
   /** Under the list, when there is no search. */
   footer?: ReactNode;
   disabled?: boolean;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const listId = useId();
   const needle = query.trim().toLowerCase();
   const shown = useMemo(
      () =>
         groups
            .map((group) => ({
               ...group,
               entries: group.entries.filter(
                  (entry) =>
                     needle === "" ||
                     [entry.name, entry.title, group.label, group.detail ?? ""]
                        .join(" ")
                        .toLowerCase()
                        .includes(needle),
               ),
            }))
            .filter((group) => group.entries.length > 0),
      [groups, needle],
   );
   const flat = shown.flatMap((group) => group.entries);
   const optionId = (key: string) =>
      `${listId}-${key.replace(/[^A-Za-z0-9_-]/g, "_")}`;
   const pick = (key: string) => {
      onPick(key);
      window.requestAnimationFrame(() =>
         window.document
            .getElementById(optionId(key))
            ?.scrollIntoView({ block: "nearest" }),
      );
   };
   const step = (delta: 1 | -1) => {
      if (flat.length === 0) return;
      const at = flat.findIndex((entry) => entry.key === picked);
      const next =
         at === -1
            ? delta === 1
               ? 0
               : flat.length - 1
            : Math.min(flat.length - 1, Math.max(0, at + delta));
      pick(flat[next]!.key);
   };

   return (
      <Box
         sx={{
            display: "flex",
            flexDirection: "column",
            minHeight: 0,
            flex: 1,
         }}
      >
         <Box sx={{ p: 1.5, pb: 1 }}>
            <TextField
               size="small"
               fullWidth
               inputRef={searchRef}
               placeholder="Search views"
               value={query}
               disabled={disabled}
               onChange={(event) => onQuery(event.target.value)}
               onKeyDown={(event) => {
                  if (event.key === "ArrowDown") {
                     event.preventDefault();
                     step(1);
                  } else if (event.key === "ArrowUp") {
                     event.preventDefault();
                     step(-1);
                  } else if (event.key === "Enter" && picked !== undefined) {
                     event.preventDefault();
                     onTake(picked);
                  }
               }}
               inputProps={{
                  "aria-label": "Search views",
                  role: "combobox",
                  "aria-controls": listId,
                  "aria-expanded": true,
                  ...(picked !== undefined
                     ? { "aria-activedescendant": optionId(picked) }
                     : {}),
               }}
               InputProps={{
                  startAdornment: (
                     <InputAdornment position="start">
                        <SearchIcon
                           sx={{ fontSize: 18, color: "text.secondary" }}
                        />
                     </InputAdornment>
                  ),
               }}
            />
         </Box>
         <Box
            id={listId}
            role="listbox"
            aria-label="Views"
            sx={{ overflowY: "auto", flex: 1, minHeight: 0, pb: 1 }}
         >
            {groups.length === 0 && empty}
            {groups.length > 0 && shown.length === 0 && (
               <Typography
                  variant="body2"
                  sx={{ color: theme.tileTitle, px: 2, py: 1 }}
               >
                  No view matches “{query.trim()}”.
               </Typography>
            )}
            {shown.map((group) => (
               <Box
                  key={group.key}
                  role="group"
                  aria-label={group.ariaLabel ?? group.label}
               >
                  <Box
                     aria-hidden
                     sx={{
                        px: 2,
                        pt: 1.25,
                        pb: 0.5,
                        ...ui.type.caption,
                        color: "text.secondary",
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                     }}
                  >
                     <Box
                        component="span"
                        sx={{
                           ...ui.type.label,
                           fontSize: "0.92em",
                           letterSpacing: "0.04em",
                           textTransform: "uppercase",
                        }}
                     >
                        {group.label.replace(/_+/g, " ")}
                     </Box>
                     {group.detail && ` · ${group.detail}`}
                  </Box>
                  {group.entries.map((entry) => {
                     const selected = entry.key === picked;
                     return (
                        <Box
                           key={entry.key}
                           id={optionId(entry.key)}
                           role="option"
                           aria-selected={selected}
                           aria-label={`${entry.title} (${entry.name})`}
                           onClick={() => {
                              if (!disabled) pick(entry.key);
                           }}
                           onDoubleClick={() => {
                              if (!disabled) onTake(entry.key);
                           }}
                           sx={(mui) => ({
                              display: "flex",
                              alignItems: "center",
                              gap: 1.25,
                              mx: 0.75,
                              px: 1.25,
                              py: 0.75,
                              borderRadius: ui.radius.control,
                              cursor: disabled ? "default" : "pointer",
                              bgcolor: selected
                                 ? `color-mix(in srgb, ${theme.accent} 9%, ${mui.palette.background.paper})`
                                 : "transparent",
                              "&:hover":
                                 selected || disabled
                                    ? {}
                                    : { bgcolor: mui.palette.action.hover },
                           })}
                        >
                           <Box
                              aria-hidden
                              sx={(mui) => ({
                                 width: 26,
                                 height: 26,
                                 flex: "none",
                                 borderRadius: ui.radius.control,
                                 display: "flex",
                                 alignItems: "center",
                                 justifyContent: "center",
                                 color: selected
                                    ? theme.accent
                                    : mui.palette.text.secondary,
                                 bgcolor: selected
                                    ? mui.palette.background.paper
                                    : mui.palette.action.hover,
                                 boxShadow: selected
                                    ? `inset 0 0 0 1px color-mix(in srgb, ${theme.accent} 35%, transparent)`
                                    : "none",
                              })}
                           >
                              <ChartGlyph chart={entry.chart} />
                           </Box>
                           <Box sx={{ minWidth: 0 }}>
                              <Box
                                 sx={{
                                    ...ui.type.control,
                                    fontWeight: selected
                                       ? ui.type.label.fontWeight
                                       : 400,
                                    whiteSpace: "nowrap",
                                    overflow: "hidden",
                                    textOverflow: "ellipsis",
                                 }}
                              >
                                 {entry.title}
                              </Box>
                              <Box
                                 sx={{
                                    ...ui.type.code,
                                    color: "text.secondary",
                                 }}
                              >
                                 {entry.name}
                              </Box>
                           </Box>
                        </Box>
                     );
                  })}
               </Box>
            ))}
            {needle === "" && footer}
         </Box>
      </Box>
   );
}

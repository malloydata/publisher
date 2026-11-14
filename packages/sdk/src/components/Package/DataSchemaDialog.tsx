// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AbcIcon from "@mui/icons-material/Abc";
import CalendarTodayOutlinedIcon from "@mui/icons-material/CalendarTodayOutlined";
import DataArrayIcon from "@mui/icons-material/DataArray";
import DataObjectIcon from "@mui/icons-material/DataObject";
import ScheduleOutlinedIcon from "@mui/icons-material/ScheduleOutlined";
import SearchIcon from "@mui/icons-material/Search";
import TagIcon from "@mui/icons-material/Tag";
import ToggleOnOutlinedIcon from "@mui/icons-material/ToggleOnOutlined";
import ViewColumnOutlinedIcon from "@mui/icons-material/ViewColumnOutlined";
import {
   Alert,
   Box,
   InputAdornment,
   InputBase,
   Stack,
   Typography,
} from "@mui/material";
import type { SvgIconComponent } from "@mui/icons-material";
import * as React from "react";
import { useId, useMemo, useState } from "react";
import type { Column, Database } from "../../client";
import { AppDialog } from "../AppDialog";
import { MONO_FONT_FAMILY } from "../styles";
import ContentTypeIcon, { CONTENT_TINT } from "./ContentTypeIcon";

/** A row count as the Data section shows it: "1.2 K rows". */
export function formatRowCount(rows: number): string {
   if (rows >= 1_000_000_000)
      return `${(rows / 1_000_000_000).toFixed(1)} B rows`;
   if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(1)} M rows`;
   if (rows >= 1_000) return `${(rows / 1_000).toFixed(1)} K rows`;
   return `${rows} rows`;
}

/** Wider than this, the columns get a filter box: past a screenful, a reader
 *  is looking for one column rather than reading them all. */
const FILTER_THRESHOLD = 12;

/**
 * The glyph for a column's type. The server reports Malloy's type names
 * (`string`, `number`, `date`, `timestamp`, `boolean`, and arrays and records,
 * which may carry their element types), so the glyph says what kind of value a
 * column holds before its type is read. An unknown type keeps a plain column.
 */
export function columnTypeIcon(type: string | undefined): SvgIconComponent {
   const t = type?.trim().toLowerCase() ?? "";
   if (t.startsWith("array") || t.endsWith("[]")) return DataArrayIcon;
   if (t.startsWith("record") || t.startsWith("struct")) return DataObjectIcon;
   switch (t.split(/[\s(<]/)[0]) {
      case "string":
         return AbcIcon;
      case "number":
         return TagIcon;
      case "date":
         return CalendarTodayOutlinedIcon;
      case "timestamp":
      case "timestamptz":
         return ScheduleOutlinedIcon;
      case "boolean":
         return ToggleOnOutlinedIcon;
      default:
         return ViewColumnOutlinedIcon;
   }
}

/**
 * One column: its type's glyph, its name, its type.
 *
 * The name is mono because it exists verbatim in the file and is what a reader
 * would type into a model; the type is sans and quieter because it describes
 * the column rather than naming it.
 */
function ColumnRow({ column }: { column: Column }) {
   const Icon = columnTypeIcon(column.type);
   return (
      <Box
         component="li"
         sx={(theme) => ({
            display: "flex",
            alignItems: "center",
            gap: 1.25,
            px: 1,
            py: 0.625,
            borderRadius: 1,
            transition: "background-color 0.1s",
            "&:hover": {
               backgroundColor:
                  theme.palette.mode === "dark"
                     ? "rgba(255, 255, 255, 0.06)"
                     : theme.palette.grey[100],
            },
         })}
      >
         <Icon
            aria-hidden
            sx={{ fontSize: 18, color: "primary.main", flexShrink: 0 }}
         />
         <Box
            sx={{
               display: "flex",
               alignItems: "baseline",
               gap: 1,
               minWidth: 0,
               flex: 1,
            }}
         >
            <Typography
               component="span"
               sx={{
                  fontFamily: MONO_FONT_FAMILY,
                  fontSize: "0.8125rem",
                  fontWeight: 500,
                  color: "text.primary",
                  minWidth: 0,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
               }}
            >
               {column.name}
            </Typography>
            {column.type && (
               <Typography
                  component="span"
                  variant="caption"
                  color="text.secondary"
                  sx={{
                     flexShrink: 0,
                     maxWidth: "50%",
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                     whiteSpace: "nowrap",
                  }}
               >
                  {column.type}
               </Typography>
            )}
         </Box>
      </Box>
   );
}

/**
 * A package data file's schema: its columns, opened from the Data section of
 * the package page.
 *
 * The header names the file the way the row that opened it did (its icon, its
 * basename in mono) and repeats the row's facts under it (the folder and the row
 * count), so the dialog confirms what was clicked. The columns sit in one
 * outlined pane under a ruled "Columns · N" band, and the pane scrolls on its
 * own so a wide parquet file does not push the dialog past the viewport.
 */
export function DataSchemaDialog({
   database,
   onClose,
}: {
   /** The file to show; the dialog is open while it is set. */
   database: Database | null;
   onClose: () => void;
}) {
   const open = database !== null;
   // Held through the close, so the dialog fades out showing the file it was
   // showing rather than collapsing to an empty title.
   const [shown, setShown] = useState<Database | null>(database);
   if (database !== null && database !== shown) setShown(database);

   const [filter, setFilter] = useState("");
   const [filterFor, setFilterFor] = useState(shown?.path);
   if (filterFor !== shown?.path) {
      setFilterFor(shown?.path);
      setFilter("");
   }

   const columnsId = useId();
   const path = shown?.path ?? "";
   const fileName = path.split("/").pop() || path;
   const folder = path.slice(0, Math.max(0, path.length - fileName.length - 1));
   const rowCount = shown?.info?.rowCount;
   const subtitle = [
      folder,
      rowCount === undefined ? null : formatRowCount(rowCount),
   ]
      .filter(Boolean)
      .join(" · ");

   const columns = useMemo(() => shown?.info?.columns ?? [], [shown]);
   const needle = filter.trim().toLowerCase();
   const visible = useMemo(
      () =>
         needle
            ? columns.filter(
                 (c) =>
                    c.name?.toLowerCase().includes(needle) ||
                    c.type?.toLowerCase().includes(needle),
              )
            : columns,
      [columns, needle],
   );

   return (
      <AppDialog
         open={open}
         onClose={onClose}
         showClose
         title={
            <Stack
               direction="row"
               alignItems="center"
               spacing={1.5}
               sx={{ minWidth: 0 }}
            >
               <Box
                  aria-hidden
                  sx={{
                     width: 32,
                     height: 32,
                     borderRadius: 1,
                     bgcolor: CONTENT_TINT.data,
                     color: "#FFFFFF",
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                     flexShrink: 0,
                  }}
               >
                  <ContentTypeIcon type="data" size={18} />
               </Box>
               <Box sx={{ minWidth: 0 }}>
                  <Typography
                     component="span"
                     noWrap
                     title={path}
                     sx={{
                        display: "block",
                        fontFamily: MONO_FONT_FAMILY,
                        fontSize: "1rem",
                        fontWeight: 600,
                        letterSpacing: 0,
                        lineHeight: 1.4,
                     }}
                  >
                     {fileName || "Columns"}
                  </Typography>
                  {subtitle && (
                     <Typography
                        component="span"
                        variant="caption"
                        color="text.secondary"
                        noWrap
                        sx={{ display: "block", lineHeight: 1.4 }}
                     >
                        {subtitle}
                     </Typography>
                  )}
               </Box>
            </Stack>
         }
      >
         {shown?.error && (
            <Alert severity="error" variant="outlined">
               {shown.error}
            </Alert>
         )}
         {shown && !shown.error && (
            <Box
               component="section"
               aria-labelledby={columnsId}
               sx={{
                  display: "flex",
                  flexDirection: "column",
                  border: "1px solid",
                  borderColor: "divider",
                  borderRadius: 2,
                  bgcolor: "background.paper",
                  overflow: "hidden",
                  mb: 2,
               }}
            >
               <Stack
                  direction="row"
                  alignItems="center"
                  spacing={1}
                  sx={{
                     px: 1.5,
                     py: 1,
                     minHeight: 44,
                     borderBottom: "1px solid",
                     borderColor: "divider",
                  }}
               >
                  <Typography
                     id={columnsId}
                     component="h3"
                     sx={{ fontSize: "0.875rem", fontWeight: 600 }}
                  >
                     Columns
                     <Typography
                        component="span"
                        variant="caption"
                        color="text.secondary"
                        sx={{ ml: 0.75, fontVariantNumeric: "tabular-nums" }}
                     >
                        · {needle ? `${visible.length} of ` : ""}
                        {columns.length}
                     </Typography>
                  </Typography>
                  {columns.length > FILTER_THRESHOLD && (
                     <InputBase
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                        placeholder="Filter columns"
                        inputProps={{ "aria-label": "Filter columns" }}
                        startAdornment={
                           <InputAdornment position="start">
                              <SearchIcon
                                 sx={{ fontSize: 16, color: "text.secondary" }}
                              />
                           </InputAdornment>
                        }
                        sx={{
                           ml: "auto !important",
                           width: 200,
                           fontSize: "0.8125rem",
                           px: 1,
                           py: 0.25,
                           border: "1px solid",
                           borderColor: "divider",
                           borderRadius: 1.5,
                           "&.Mui-focused": { borderColor: "primary.main" },
                        }}
                     />
                  )}
               </Stack>
               <Box
                  sx={{ overflowY: "auto", maxHeight: 420, px: 0.75, py: 0.75 }}
               >
                  {visible.length === 0 ? (
                     <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{ fontStyle: "italic", px: 1, py: 1 }}
                     >
                        {needle ? "No columns match" : "No columns"}
                     </Typography>
                  ) : (
                     <Box
                        component="ul"
                        aria-labelledby={columnsId}
                        sx={{ listStyle: "none", m: 0, p: 0 }}
                     >
                        {visible.map((column, i) => (
                           <ColumnRow
                              key={`${column.name}-${i}`}
                              column={column}
                           />
                        ))}
                     </Box>
                  )}
               </Box>
            </Box>
         )}
      </AppDialog>
   );
}

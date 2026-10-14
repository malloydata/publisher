// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import BarChartIcon from "@mui/icons-material/BarChart";
import MapIcon from "@mui/icons-material/Map";
import NumbersIcon from "@mui/icons-material/Numbers";
import ScatterPlotIcon from "@mui/icons-material/ScatterPlot";
import SearchIcon from "@mui/icons-material/Search";
import ShowChartIcon from "@mui/icons-material/ShowChart";
import TableChartIcon from "@mui/icons-material/TableChart";
import {
   Box,
   Button,
   InputAdornment,
   TextField,
   ToggleButton,
   ToggleButtonGroup,
   Typography,
} from "@mui/material";
import {
   useEffect,
   useId,
   useMemo,
   useRef,
   useState,
   type ReactNode,
} from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { isBareName } from "../../utils/malloyText";
import { AppDialog } from "../AppDialog";
import { edgeColor, TYPE } from "../controlTokens";
import type { CatalogSource, CatalogView, PackageCatalog } from "./catalog";
import { chartChoices } from "./ChartPicker";
import type { ChartPick } from "./chartLine";
import type { DashboardDocument } from "./document";

/**
 * What a new tile is: a view, picked from the package.
 *
 * The picker is what makes a tile expression correct BY CONSTRUCTION. Every
 * `source -> view` it offers came from the catalog, so the builder never emits
 * a tile that does not resolve — which is why saving needs no compile step
 * beyond the reader's own round-trip.
 *
 * Every source a model of the package exports is offered. A tile's view is
 * declared in an extension of its model source, so a source the file cannot see
 * yet is imported by name when the tile is added (`withSource`); one the file
 * already reaches, by name or through a whole-file import of an exporter, is
 * left alone.
 */
export interface NewTile {
   /** The model source the view lives on. */
   base: string;
   /** The model that exports it, for the import a source the file cannot see yet needs. */
   modelPath: string;
   /** Every model that exports it; absent when only `modelPath` does. */
   exporters?: string[];
   /** The view, as the catalog names it. */
   view: string;
   label?: string;
   /** A chart for the tile; absent keeps the view's own. */
   chart?: ChartPick | "none";
   /** The chart tags the view carries, so the chart line negates only those. */
   chartCarried?: string[];
   colspan: number;
}

/** A view the picker shows a preview of, as the tile it would add. */
export interface ViewPreview {
   base: string;
   /** The model that exports the source: what the preview runs against. */
   modelPath: string;
   view: string;
   /** The title the tile would have. */
   label: string;
   /** A chart for the preview; absent keeps the view's own. */
   chart?: ChartPick | "none";
   chartCarried?: string[];
   /** The givens the source is scoped by, the ones the preview sends. */
   givens: string[];
}

export interface AddTileDialogProps {
   open: boolean;
   document: DashboardDocument;
   catalog: PackageCatalog | undefined;
   columns: number;
   onClose: () => void;
   onAdd: (tile: NewTile) => void;
   /** Add an empty text tile instead, whose markdown is written on the tile itself. */
   onAddText: () => void;
   /** The picked view run as a tile, with the page's filters. Absent, a card says what will run. */
   renderPreview?: (view: ViewPreview) => ReactNode;
}

/** `by_category` -> `By category`: a view's name, read as words, where it has no doc line. */
export function viewTitle(view: CatalogView): string {
   if (view.description) return view.description;
   const words = view.name.replace(/_+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
}

/** The icon for the chart a view declares; a table when it declares none. */
function ChartGlyph({ chart }: { chart: string | undefined }) {
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

/** What a view's own chart tag is, in a sentence. */
const DECLARED: Record<string, string> = {
   bar_chart: "a bar chart",
   line_chart: "a line chart",
   scatter_chart: "a scatter chart",
   shape_map: "a shape map",
   segment_map: "a segment map",
   big_value: "a big value",
   sparkline: "a sparkline",
};

interface Entry {
   source: CatalogSource;
   view: CatalogView;
   key: string;
}

const entryKey = (source: string, view: string) => `${source}\u0000${view}`;

export function AddTileDialog({
   open,
   document,
   catalog,
   columns,
   onClose,
   onAdd,
   onAddText,
   renderPreview,
}: AddTileDialogProps) {
   const { theme } = usePublisherTheme();
   const edge = edgeColor(theme.cardBorder);
   const listId = useId();
   const sources = useMemo(() => catalog?.sources ?? [], [catalog]);
   const [query, setQuery] = useState("");
   const [pickedKey, setPickedKey] = useState<string | undefined>();
   const [label, setLabel] = useState("");
   const [chart, setChart] = useState<ChartPick | "none" | "default">(
      "default",
   );
   const listRef = useRef<HTMLDivElement>(null);
   const searchRef = useRef<HTMLInputElement>(null);

   // The source the tiles already read comes first, so the common case is one
   // click on a view near the top.
   const ordered = useMemo(() => {
      const read = new Set(document.sources.map((s) => s.base));
      return [...sources].sort(
         (a, b) => Number(read.has(b.name)) - Number(read.has(a.name)),
      );
   }, [sources, document]);
   const withViews = ordered.filter((s) => s.views.length > 0);
   const withoutViews = ordered.filter((s) => s.views.length === 0);

   const needle = query.trim().toLowerCase();
   const groups = useMemo(
      () =>
         withViews
            .map((source) => ({
               source,
               entries: source.views
                  .filter(
                     (view) =>
                        needle === "" ||
                        [view.name, view.description ?? "", source.name]
                           .join(" ")
                           .toLowerCase()
                           .includes(needle),
                  )
                  .map(
                     (view): Entry => ({
                        source,
                        view,
                        key: entryKey(source.name, view.name),
                     }),
                  ),
            }))
            .filter((group) => group.entries.length > 0),
      [withViews, needle],
   );
   const flat = groups.flatMap((group) => group.entries);
   const picked =
      flat.find((entry) => entry.key === pickedKey) ??
      // Kept while a search hides it, so typing does not drop the choice.
      withViews
         .flatMap((source) =>
            source.views.map((view) => ({
               source,
               view,
               key: entryKey(source.name, view.name),
            })),
         )
         .find((entry) => entry.key === pickedKey);

   useEffect(() => {
      if (!open) return;
      setQuery("");
      setLabel("");
      setChart("default");
      const first = withViews[0]?.views[0];
      setPickedKey(
         first && withViews[0]
            ? entryKey(withViews[0].name, first.name)
            : undefined,
      );
      // Only on opening: the catalog arriving later must not move the pick.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [open]);

   const pick = (entry: Entry | undefined) => {
      if (!entry) return;
      setPickedKey(entry.key);
      setChart("default");
      window.requestAnimationFrame(() =>
         window.document
            .getElementById(`${listId}-${entry.key}`)
            ?.scrollIntoView({ block: "nearest" }),
      );
   };
   const step = (delta: 1 | -1) => {
      if (flat.length === 0) return;
      const at = flat.findIndex((entry) => entry.key === picked?.key);
      const next =
         at === -1
            ? delta === 1
               ? 0
               : flat.length - 1
            : Math.min(flat.length - 1, Math.max(0, at + delta));
      pick(flat[next]);
   };

   const unwritable = picked
      ? [picked.source.name, picked.view.name]
           .filter((name) => !isBareName(name))
           .map(
              (name) =>
                 `"${name}" is not a plain Malloy name, so a tile cannot be written for it.`,
           )[0]
      : undefined;
   const canAdd = picked !== undefined && unwritable === undefined;
   const title = picked ? viewTitle(picked.view) : "";
   const chartPart =
      chart !== "default" && picked
         ? {
              chart,
              chartCarried: picked.view.chart ? [picked.view.chart] : [],
           }
         : {};
   const add = () => {
      if (!picked || !canAdd) return;
      onAdd({
         base: picked.source.name,
         modelPath: picked.source.modelPath,
         ...(picked.source.exporters
            ? { exporters: picked.source.exporters }
            : {}),
         view: picked.view.name,
         ...(label.trim() ? { label: label.trim() } : {}),
         ...chartPart,
         colspan: Math.ceil(columns / 2),
      });
   };
   // Only the charts this view can take: a choice that cannot apply is not a
   // choice here, where the view is still being picked.
   const charts = picked
      ? chartChoices(picked.view, chart).filter(
           (choice) => choice.value !== "custom" && choice.disabled !== true,
        )
      : [];

   const empty =
      withViews.length === 0 ? (
         <Typography variant="body2" sx={{ color: theme.tileTitle, p: 2 }}>
            {catalog
               ? "This package publishes no source with a view to put on a tile."
               : "The package's sources are still loading."}
         </Typography>
      ) : null;

   return (
      <AppDialog
         open={open}
         onClose={onClose}
         maxWidth="md"
         initialFocus={searchRef}
         title="Add a tile"
         description="Pick a view from this package. You can change its title and chart now or later."
         actions={
            <>
               <Button onClick={onAddText} sx={{ mr: "auto" }}>
                  Add text instead
               </Button>
               <Button onClick={onClose}>Cancel</Button>
               <Button variant="contained" disabled={!canAdd} onClick={add}>
                  Add tile
               </Button>
            </>
         }
      >
         <Box
            sx={{
               // Edge to edge inside the window: two panes, not a form.
               mx: -3,
               mb: -1,
               borderTop: `1px solid ${edge}`,
               borderBottom: `1px solid ${edge}`,
               display: "grid",
               gridTemplateColumns: { xs: "1fr", md: "320px minmax(0, 1fr)" },
               height: { md: 460 },
            }}
         >
            <Box
               sx={{
                  display: "flex",
                  flexDirection: "column",
                  minHeight: 0,
                  borderRight: { md: `1px solid ${edge}` },
                  maxHeight: { xs: 280, md: "none" },
               }}
            >
               <Box sx={{ p: 1.5, pb: 1 }}>
                  <TextField
                     size="small"
                     fullWidth
                     inputRef={searchRef}
                     placeholder="Search views"
                     value={query}
                     onChange={(event) => setQuery(event.target.value)}
                     onKeyDown={(event) => {
                        if (event.key === "ArrowDown") {
                           event.preventDefault();
                           step(1);
                        } else if (event.key === "ArrowUp") {
                           event.preventDefault();
                           step(-1);
                        } else if (event.key === "Enter") {
                           event.preventDefault();
                           add();
                        }
                     }}
                     inputProps={{
                        "aria-label": "Search views",
                        role: "combobox",
                        "aria-controls": listId,
                        "aria-expanded": true,
                        ...(picked
                           ? {
                                "aria-activedescendant": `${listId}-${picked.key}`,
                             }
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
                  ref={listRef}
                  id={listId}
                  role="listbox"
                  aria-label="Views"
                  sx={{ overflowY: "auto", flex: 1, minHeight: 0, pb: 1 }}
               >
                  {empty}
                  {withViews.length > 0 && groups.length === 0 && (
                     <Typography
                        variant="body2"
                        sx={{ color: theme.tileTitle, px: 2, py: 1 }}
                     >
                        No view matches “{query.trim()}”.
                     </Typography>
                  )}
                  {groups.map(({ source, entries }) => (
                     <Box
                        key={source.name}
                        role="group"
                        aria-label={source.name}
                     >
                        <Box
                           aria-hidden
                           sx={{
                              px: 2,
                              pt: 1.25,
                              pb: 0.5,
                              ...TYPE.caption,
                              color: "text.secondary",
                              whiteSpace: "nowrap",
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                           }}
                        >
                           <Box
                              component="span"
                              sx={{
                                 fontWeight: 600,
                                 letterSpacing: "0.04em",
                                 textTransform: "uppercase",
                                 fontSize: 11,
                              }}
                           >
                              {source.name.replace(/_+/g, " ")}
                           </Box>
                           {source.description && ` · ${source.description}`}
                        </Box>
                        {entries.map((entry) => {
                           const selected = entry.key === picked?.key;
                           return (
                              <Box
                                 key={entry.key}
                                 id={`${listId}-${entry.key}`}
                                 role="option"
                                 aria-selected={selected}
                                 aria-label={`${viewTitle(entry.view)} (${entry.view.name})`}
                                 onClick={() => pick(entry)}
                                 onDoubleClick={() => {
                                    pick(entry);
                                    if (
                                       isBareName(entry.source.name) &&
                                       isBareName(entry.view.name)
                                    )
                                       onAdd({
                                          base: entry.source.name,
                                          modelPath: entry.source.modelPath,
                                          ...(entry.source.exporters
                                             ? {
                                                  exporters:
                                                     entry.source.exporters,
                                               }
                                             : {}),
                                          view: entry.view.name,
                                          colspan: Math.ceil(columns / 2),
                                       });
                                 }}
                                 sx={(mui) => ({
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 1.25,
                                    mx: 0.75,
                                    px: 1.25,
                                    py: 0.75,
                                    borderRadius: "7px",
                                    cursor: "pointer",
                                    bgcolor: selected
                                       ? `color-mix(in srgb, ${theme.accent} 9%, ${mui.palette.background.paper})`
                                       : "transparent",
                                    "&:hover": selected
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
                                       borderRadius: "6px",
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
                                    <ChartGlyph chart={entry.view.chart} />
                                 </Box>
                                 <Box sx={{ minWidth: 0 }}>
                                    <Box
                                       sx={{
                                          ...TYPE.control,
                                          fontWeight: selected ? 600 : 400,
                                          whiteSpace: "nowrap",
                                          overflow: "hidden",
                                          textOverflow: "ellipsis",
                                       }}
                                    >
                                       {viewTitle(entry.view)}
                                    </Box>
                                    <Box
                                       sx={{
                                          fontFamily:
                                             "ui-monospace, SFMono-Regular, Menlo, monospace",
                                          fontSize: 11.5,
                                          color: "text.secondary",
                                       }}
                                    >
                                       {entry.view.name}
                                    </Box>
                                 </Box>
                              </Box>
                           );
                        })}
                     </Box>
                  ))}
                  {withoutViews.length > 0 && needle === "" && (
                     <Typography
                        variant="caption"
                        component="p"
                        sx={{ color: "text.secondary", px: 2, pt: 1.5 }}
                     >
                        Also in this package, with no views to show:{" "}
                        {withoutViews.map((s) => s.name).join(", ")}.
                     </Typography>
                  )}
               </Box>
            </Box>

            <Box
               sx={(mui) => ({
                  p: 2.5,
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  minHeight: 0,
                  overflowY: "auto",
                  bgcolor:
                     mui.palette.mode === "dark"
                        ? mui.palette.background.default
                        : "#fafbfc",
               })}
            >
               {picked ? (
                  <>
                     <Box
                        aria-label="Preview"
                        role="region"
                        sx={(mui) => ({
                           height: 240,
                           flex: "none",
                           display: "flex",
                           position: "relative",
                           // A preview, not the tile: what does not fit is cut
                           // off, under a fade that says there is more. The
                           // card is drawn here, round the cut, and the tile
                           // inside it has none of its own.
                           overflow: "hidden",
                           borderRadius: "8px",
                           border: `1px solid ${edge}`,
                           bgcolor: mui.palette.background.paper,
                           "&::after": {
                              content: '""',
                              position: "absolute",
                              left: 0,
                              right: 0,
                              bottom: 0,
                              height: 40,
                              pointerEvents: "none",
                              background: `linear-gradient(to bottom, transparent, ${mui.palette.background.paper})`,
                           },
                        })}
                     >
                        {renderPreview ? (
                           <Box sx={{ flex: 1, minWidth: 0, px: 2, pt: 1.5 }}>
                              {renderPreview({
                                 base: picked.source.name,
                                 modelPath: picked.source.modelPath,
                                 view: picked.view.name,
                                 label: label.trim() || title,
                                 ...(chart !== "default" ? chartPart : {}),
                                 givens: picked.source.givens,
                              })}
                           </Box>
                        ) : (
                           <Box
                              sx={(mui) => ({
                                 flex: 1,
                                 border: `1px solid ${edge}`,
                                 borderRadius: "8px",
                                 bgcolor: mui.palette.background.paper,
                                 display: "flex",
                                 flexDirection: "column",
                                 alignItems: "center",
                                 justifyContent: "center",
                                 gap: 1,
                                 color: "text.secondary",
                              })}
                           >
                              <ChartGlyph chart={picked.view.chart} />
                              <Typography variant="body2">
                                 {picked.source.name} → {picked.view.name}
                              </Typography>
                           </Box>
                        )}
                     </Box>
                     <TextField
                        size="small"
                        label="Title"
                        placeholder={title}
                        value={label}
                        onChange={(event) => setLabel(event.target.value)}
                        InputLabelProps={{ shrink: true }}
                        inputProps={{ "aria-label": "Tile title" }}
                     />
                     <Box>
                        <Typography
                           component="div"
                           sx={{
                              ...TYPE.caption,
                              fontWeight: 600,
                              color: "text.secondary",
                              mb: 0.75,
                           }}
                        >
                           Chart
                        </Typography>
                        <ToggleButtonGroup
                           size="small"
                           exclusive
                           value={chart}
                           aria-label="Chart"
                           onChange={(_event, next) => {
                              if (next !== null)
                                 setChart(
                                    next as ChartPick | "none" | "default",
                                 );
                           }}
                           sx={{
                              flexWrap: "wrap",
                              "& .MuiToggleButton-root": {
                                 textTransform: "none",
                                 px: 1.25,
                                 py: 0.5,
                                 ...TYPE.control,
                              },
                           }}
                        >
                           {charts.map((choice) => (
                              <ToggleButton
                                 key={choice.value}
                                 value={choice.value}
                              >
                                 {choice.label}
                              </ToggleButton>
                           ))}
                        </ToggleButtonGroup>
                        <Typography
                           variant="caption"
                           component="p"
                           sx={{ color: "text.secondary", mt: 0.75 }}
                        >
                           {picked.view.chart
                              ? `The view declares ${DECLARED[picked.view.chart] ?? "a chart"}.`
                              : "The view declares no chart, so it shows as a table."}
                        </Typography>
                     </Box>
                     {unwritable && (
                        <Typography variant="body2" color="error">
                           {unwritable}
                        </Typography>
                     )}
                  </>
               ) : (
                  <Typography
                     variant="body2"
                     sx={{ color: "text.secondary", m: "auto" }}
                  >
                     Pick a view to see it here.
                  </Typography>
               )}
            </Box>
         </Box>
      </AppDialog>
   );
}

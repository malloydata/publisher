// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { segmentedSx } from "./BuilderToolbar";
import { reportClasses } from "../../theme/classes";
import {
   Box,
   Button,
   TextField,
   ToggleButton,
   ToggleButtonGroup,
   Typography,
} from "@mui/material";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { isBareName } from "../../utils/malloyText";
import { AppDialog } from "../AppDialog";
import { edgeColor } from "../controlTokens";
import type { CatalogSource, CatalogView, PackageCatalog } from "./catalog";
import { chartChoices } from "./ChartPicker";
import {
   ChartGlyph,
   readableName,
   ViewPicker,
   type PickerGroup,
} from "./ViewPicker";
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
   return view.description ?? readableName(view.name);
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
   const ui = theme.ui;
   const edge = edgeColor(theme.cardBorder);
   const sources = useMemo(() => catalog?.sources ?? [], [catalog]);
   const [query, setQuery] = useState("");
   const [pickedKey, setPickedKey] = useState<string | undefined>();
   const [label, setLabel] = useState("");
   const [chart, setChart] = useState<ChartPick | "none" | "default">(
      "default",
   );
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

   // Every view on offer, by key: the picker's entries and what a key means.
   const entries = useMemo(
      () =>
         new Map(
            withViews.flatMap((source) =>
               source.views.map(
                  (view) =>
                     [
                        entryKey(source.name, view.name),
                        { source, view, key: entryKey(source.name, view.name) },
                     ] as const,
               ),
            ),
         ),
      [withViews],
   );
   const groups = useMemo<PickerGroup[]>(
      () =>
         withViews.map((source) => ({
            key: source.name,
            label: source.name,
            ...(source.description ? { detail: source.description } : {}),
            entries: source.views.map((view) => ({
               key: entryKey(source.name, view.name),
               name: view.name,
               title: viewTitle(view),
               ...(view.chart ? { chart: view.chart } : {}),
            })),
         })),
      [withViews],
   );
   // Kept while a search hides it, so typing does not drop the choice.
   const picked = pickedKey === undefined ? undefined : entries.get(pickedKey);

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
   const add = (entry: Entry | undefined = picked) => {
      if (!entry) return;
      if (![entry.source.name, entry.view.name].every(isBareName)) return;
      const same = entry.key === picked?.key;
      onAdd({
         base: entry.source.name,
         modelPath: entry.source.modelPath,
         ...(entry.source.exporters
            ? { exporters: entry.source.exporters }
            : {}),
         view: entry.view.name,
         ...(same && label.trim() ? { label: label.trim() } : {}),
         ...(same ? chartPart : {}),
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

   return (
      <AppDialog
         className={`${reportClasses.dialog} ${reportClasses.addTileDialog}`}
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
               <Button
                  variant="contained"
                  disabled={!canAdd}
                  onClick={() => add()}
               >
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
               <ViewPicker
                  groups={groups}
                  query={query}
                  onQuery={setQuery}
                  picked={picked?.key}
                  onPick={(key) => {
                     setPickedKey(key);
                     setChart("default");
                  }}
                  onTake={(key) => add(entries.get(key))}
                  searchRef={searchRef}
                  empty={
                     <Typography
                        variant="body2"
                        sx={{ color: theme.tileTitle, p: 2 }}
                     >
                        {catalog
                           ? "This package publishes no source with a view to put on a tile."
                           : "The package's sources are still loading."}
                     </Typography>
                  }
                  footer={
                     withoutViews.length > 0 && (
                        <Typography
                           variant="caption"
                           component="p"
                           sx={{ color: "text.secondary", px: 2, pt: 1.5 }}
                        >
                           Also in this package, with no views to show:{" "}
                           {withoutViews.map((s) => s.name).join(", ")}.
                        </Typography>
                     )
                  }
               />
            </Box>

            <Box
               sx={(mui) => ({
                  p: 2.5,
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  minHeight: 0,
                  overflowY: "auto",
                  // A shade off the paper, in the host's own colours.
                  bgcolor: `color-mix(in srgb, ${mui.palette.text.primary} 3%, ${mui.palette.background.paper})`,
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
                           borderRadius: ui.radius.tile,
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
                                 borderRadius: ui.radius.tile,
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
                              ...ui.type.label,
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
                           // The builder's own segmented control, as the bar's
                           // layout switch is, so it takes the host's tokens.
                           sx={{
                              ...segmentedSx(edge, ui),
                              "& .MuiToggleButton-root": {
                                 ...segmentedSx(edge, ui)[
                                    "& .MuiToggleButton-root"
                                 ],
                                 textTransform: "none",
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

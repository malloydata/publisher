// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Button,
   Stack,
   TextField,
   ToggleButton,
   ToggleButtonGroup,
   Typography,
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { PackageCatalog } from "./catalog";
import type { ChartPick } from "./chartLine";
import { isBareName } from "./malloyText";
import type { DashboardDocument } from "./document";
import { ChartPicker } from "./ChartPicker";
import { AppDialog } from "../AppDialog";
import { SourceViewPicker } from "./SourceViewPicker";

/**
 * What a new tile is: a view, picked from the package.
 *
 * The picker is what makes a tile expression correct BY CONSTRUCTION. Every
 * `source -> view` it offers came from the catalog, so the builder never emits
 * a tile that does not resolve — which is why saving needs no compile step
 * beyond the reader's own round-trip.
 *
 * Every source the package publishes is offered. A tile's view is declared in
 * an extension of its model source, so a source the file cannot see yet is
 * imported by name when the tile is added (`withSource`); one the file already
 * reaches, by name or through a whole-file import of its model, is left alone.
 */
export interface NewTile {
   /** The model source the view lives on. */
   base: string;
   /** The model that declares it, for the import a source the file cannot see yet needs. */
   modelPath: string;
   /** The view, as the catalog names it. */
   view: string;
   label?: string;
   /** A chart for the tile; absent keeps the view's own. */
   chart?: ChartPick | "none";
   /** The chart tags the view carries, so the chart line negates only those. */
   chartCarried?: string[];
   colspan: number;
}

export interface AddTileDialogProps {
   open: boolean;
   document: DashboardDocument;
   catalog: PackageCatalog | undefined;
   columns: number;
   onClose: () => void;
   onAdd: (tile: NewTile) => void;
   /** Add an empty text tile, whose markdown is written on the tile itself. */
   onAddText: () => void;
}

export function AddTileDialog({
   open,
   document,
   catalog,
   columns,
   onClose,
   onAdd,
   onAddText,
}: AddTileDialogProps) {
   const { theme } = usePublisherTheme();
   const noun = document.kind === "notebook" ? "notebook" : "dashboard";
   const sources = useMemo(() => catalog?.sources ?? [], [catalog]);
   const [mode, setMode] = useState<"query" | "text">("query");
   const [base, setBase] = useState<string>("");
   const [view, setView] = useState<string>("");
   const [label, setLabel] = useState("");
   const [chart, setChart] = useState<ChartPick | "none" | "default">(
      "default",
   );
   const [colspan, setColspan] = useState<number>(Math.ceil(columns / 2));

   useEffect(() => {
      if (!open) return;
      setMode("query");
      // Open on the source the tiles already read, so the common case is one
      // click on a view.
      const withViews = (name: string) =>
         sources.some((s) => s.name === name && s.views.length > 0);
      setBase(
         document.sources.map((s) => s.base).find(withViews) ??
            sources.find((s) => s.views.length > 0)?.name ??
            "",
      );
      setView("");
      setLabel("");
      setChart("default");
      setColspan(Math.ceil(columns / 2));
   }, [open, document, sources, columns]);

   const canAdd = base !== "" && view !== "";
   const unwritable = [base, view]
      .filter((name) => name !== "" && !isBareName(name))
      .map(
         (name) =>
            `"${name}" is not a plain Malloy name, so a tile cannot be written for it.`,
      )[0];
   const picked = sources
      .find((source) => source.name === base)
      ?.views.find((candidate) => candidate.name === view);

   return (
      <AppDialog
         open={open}
         onClose={onClose}
         title="Add a tile"
         description={`A tile shows one view of one source this ${noun} imports.`}
         actions={
            <>
               <Button onClick={onClose}>Cancel</Button>
               <Button
                  variant="contained"
                  disabled={
                     mode === "query" && (!canAdd || unwritable !== undefined)
                  }
                  onClick={() =>
                     mode === "text"
                        ? onAddText()
                        : onAdd({
                             base,
                             modelPath:
                                sources.find((s) => s.name === base)
                                   ?.modelPath ?? "",
                             view,
                             ...(label.trim() ? { label: label.trim() } : {}),
                             ...(chart !== "default"
                                ? {
                                     chart,
                                     ...(picked
                                        ? {
                                             chartCarried: picked.chart
                                                ? [picked.chart]
                                                : [],
                                          }
                                        : {}),
                                  }
                                : {}),
                             colspan,
                          })
                  }
               >
                  {mode === "text" ? "Add text" : "Add tile"}
               </Button>
            </>
         }
      >
         <Stack sx={{ gap: 2, pt: 1 }}>
            <ToggleButtonGroup
               size="small"
               exclusive
               value={mode}
               aria-label="Tile type"
               onChange={(_, next: "query" | "text" | null) => {
                  if (next !== null) setMode(next);
               }}
            >
               <ToggleButton value="query" sx={{ px: 1.5 }}>
                  Query
               </ToggleButton>
               <ToggleButton value="text" sx={{ px: 1.5 }}>
                  Text
               </ToggleButton>
            </ToggleButtonGroup>
            {mode === "text" ? (
               <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                  A text tile holds markdown: a heading, a paragraph, a list. It
                  is added empty, and written on the tile itself.
               </Typography>
            ) : sources.length === 0 ? (
               <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                  {catalog
                     ? `This ${noun} imports no source by name, so there is nothing to put a tile on. Import a source in the file first.`
                     : "The package's sources are still loading."}
               </Typography>
            ) : (
               <>
                  <SourceViewPicker
                     sources={sources}
                     source={base}
                     view={view}
                     onSource={(name) => {
                        setBase(name);
                        setView("");
                     }}
                     onView={(name) => {
                        setView(name);
                        setChart("default");
                     }}
                  />
                  {unwritable && (
                     <Typography variant="body2" color="error">
                        {unwritable}
                     </Typography>
                  )}
                  <ChartPicker
                     state={chart}
                     view={picked}
                     cellLabel="new tile"
                     {...(picked
                        ? {}
                        : {
                             disabledReason: "Pick a view to choose its chart.",
                          })}
                     onChange={(next) =>
                        setChart(next as ChartPick | "none" | "default")
                     }
                  />
                  <Stack direction="row" sx={{ gap: 1.5 }}>
                     <TextField
                        size="small"
                        label="Title"
                        placeholder="Uses the view's name"
                        value={label}
                        onChange={(event) => setLabel(event.target.value)}
                        inputProps={{ "aria-label": "Tile title" }}
                        sx={{ flex: 1 }}
                     />
                     {columns > 1 && (
                        <TextField
                           size="small"
                           type="number"
                           label={`Width (of ${columns})`}
                           value={colspan}
                           onChange={(event) =>
                              setColspan(
                                 Math.min(
                                    Math.max(
                                       Number(event.target.value) || 1,
                                       1,
                                    ),
                                    columns,
                                 ),
                              )
                           }
                           inputProps={{
                              min: 1,
                              max: columns,
                              "aria-label": "Tile width",
                           }}
                           sx={{ width: 140 }}
                        />
                     )}
                  </Stack>
               </>
            )}
         </Stack>
      </AppDialog>
   );
}

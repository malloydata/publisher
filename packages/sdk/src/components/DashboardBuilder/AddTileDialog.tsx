// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Button,
   Chip,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   List,
   ListItemButton,
   ListItemText,
   MenuItem,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogSource, PackageCatalog } from "./catalog";
import type { DashboardDocument } from "./document";

/**
 * What a new tile is: a view, picked from the package, on a source this file
 * can reach.
 *
 * The picker is what makes a tile expression correct BY CONSTRUCTION. Every
 * `source -> view` it offers came from the catalog, so the builder never emits
 * a tile that does not resolve — which is why saving needs no compile step
 * beyond the reader's own round-trip.
 *
 * "Can reach" is the rule the writer enforces and the reason some sources are
 * not offered: a tile's view is declared in an extension of a model source, and
 * that source has to be in this file's scope. The builder never adds an
 * import, so a model source is offered only when the file already extends it,
 * or imports it BY NAME. A bare `import "../m.malloy"` may well bring it in,
 * but the file cannot say so, and a guess here fails the whole package load.
 */
export interface NewTile {
   /** The model source the view lives on. */
   base: string;
   /** The view, as the catalog names it. */
   view: string;
   label?: string;
   colspan: number;
}

export interface AddTileDialogProps {
   open: boolean;
   document: DashboardDocument;
   catalog: PackageCatalog | undefined;
   columns: number;
   onClose: () => void;
   onAdd: (tile: NewTile) => void;
}

/** The catalog sources this file can put a tile on; see the note above. */
function reachableSources(
   document: DashboardDocument,
   catalog: PackageCatalog | undefined,
): CatalogSource[] {
   if (!catalog) return [];
   const reachable = new Set<string>();
   for (const source of document.sources) reachable.add(source.base);
   for (const imported of document.imports)
      if (imported.kind === "names")
         for (const name of imported.names) reachable.add(name);
   return catalog.sources.filter((source) => reachable.has(source.name));
}

export function AddTileDialog({
   open,
   document,
   catalog,
   columns,
   onClose,
   onAdd,
}: AddTileDialogProps) {
   const { theme } = usePublisherTheme();
   const sources = useMemo(
      () => reachableSources(document, catalog),
      [document, catalog],
   );
   const [base, setBase] = useState<string>("");
   const [view, setView] = useState<string>("");
   const [label, setLabel] = useState("");
   const [colspan, setColspan] = useState<number>(Math.ceil(columns / 2));

   useEffect(() => {
      if (!open) return;
      // Open on the source the tiles already read, so the common case is one
      // click on a view.
      setBase(document.sources[0]?.base ?? sources[0]?.name ?? "");
      setView("");
      setLabel("");
      setColspan(Math.ceil(columns / 2));
   }, [open, document, sources, columns]);

   const source = sources.find((s) => s.name === base);
   const canAdd = base !== "" && view !== "";

   return (
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
         <DialogTitle sx={{ pb: 0.5 }}>Add a tile</DialogTitle>
         <DialogContent>
            <Stack sx={{ gap: 2, pt: 1 }}>
               {sources.length === 0 ? (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     {catalog
                        ? "This dashboard imports no source by name, so there is nothing to put a tile on. Import a source in the file first."
                        : "The package's sources are still loading."}
                  </Typography>
               ) : (
                  <>
                     <TextField
                        select
                        size="small"
                        label="Source"
                        value={base}
                        onChange={(event) => {
                           setBase(event.target.value);
                           setView("");
                        }}
                        inputProps={{ "aria-label": "Source" }}
                     >
                        {sources.map((s) => (
                           <MenuItem key={s.name} value={s.name}>
                              {s.name}
                              {s.description && (
                                 <Typography
                                    component="span"
                                    variant="caption"
                                    sx={{ ml: 1, opacity: 0.6 }}
                                 >
                                    {s.description}
                                 </Typography>
                              )}
                           </MenuItem>
                        ))}
                     </TextField>
                     <Box>
                        <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                           View
                        </Typography>
                        <List
                           dense
                           disablePadding
                           aria-label="Views"
                           sx={{
                              maxHeight: 280,
                              overflowY: "auto",
                              border: theme.border,
                              borderRadius: 1,
                           }}
                        >
                           {(source?.views ?? []).map((v) => (
                              <ListItemButton
                                 key={v.name}
                                 selected={view === v.name}
                                 onClick={() => setView(v.name)}
                                 aria-label={`View ${v.name}`}
                              >
                                 <ListItemText
                                    primary={v.name}
                                    secondary={v.description}
                                    primaryTypographyProps={{
                                       fontFamily: "ui-monospace, monospace",
                                       fontSize: 13,
                                    }}
                                 />
                                 {v.chart && (
                                    <Chip
                                       size="small"
                                       variant="outlined"
                                       label={v.chart.replace(/_chart$/, "")}
                                    />
                                 )}
                              </ListItemButton>
                           ))}
                           {source && source.views.length === 0 && (
                              <Typography
                                 variant="body2"
                                 sx={{ p: 1.5, color: theme.tileTitle }}
                              >
                                 This source declares no views.
                              </Typography>
                           )}
                        </List>
                     </Box>
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
                     </Stack>
                  </>
               )}
            </Stack>
         </DialogContent>
         <DialogActions sx={{ px: 3, py: 1.5 }}>
            <Button onClick={onClose}>Cancel</Button>
            <Button
               variant="contained"
               disabled={!canAdd}
               onClick={() =>
                  onAdd({
                     base,
                     view,
                     ...(label.trim() ? { label: label.trim() } : {}),
                     colspan,
                  })
               }
            >
               Add tile
            </Button>
         </DialogActions>
      </Dialog>
   );
}

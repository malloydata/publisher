// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Alert,
   Box,
   Button,
   Checkbox,
   FormControlLabel,
   IconButton,
   Paper,
   Stack,
   TextField,
   Tooltip,
   Typography,
} from "@mui/material";
import { useState, type ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { DEFAULT_COLUMNS, tileGridColumn } from "../Dashboard/Dashboard";
import type { DashboardDocument, DashboardTile } from "./document";
import { useDashboardEditor } from "./useDashboardEditor";

/**
 * The dashboard builder's editing surface.
 *
 * Lays the tiles out on the SAME grid the dashboard renders on — the colspan
 * rule comes from `Dashboard` rather than being restated here, so what you
 * arrange is what a reader will see, and the two cannot drift.
 *
 * The TILE ITSELF is the caller's business, through `renderTile`. A tile's
 * result comes from running a query, and where that query runs differs between
 * a saved package dashboard and an unsaved draft; keeping it out here means the
 * editing surface can be mounted, tested and reviewed without a server.
 *
 * `renderTile` replaces the tile rather than filling it, because `DashboardTile`
 * draws its own card AND its own heading. Nesting one inside a card of ours
 * would show a card in a card under two titles — which is precisely not what a
 * reader sees. So selection is drawn as an outline AROUND whatever the caller
 * renders, and the label and subtitle being edited appear where they really
 * will.
 *
 * Editing is property-level: a tile's presentation and, later, its filters.
 * Adding, removing and reordering tiles is refused by the writer, so it is not
 * offered here either — a control that always fails is worse than no control.
 */
export interface DashboardBuilderProps {
   /** The file being edited. */
   source: string;
   /** The document that file produced. */
   document: DashboardDocument;
   /** Persist the patched file. Left out, the builder edits without saving. */
   onSave?: (source: string) => Promise<void> | void;
   /**
    * Renders a tile, card and heading included — this is where a real
    * `DashboardTile` goes. Without it, tiles show what they will run.
    */
   renderTile?: (tile: DashboardTile) => ReactNode;
}

export function DashboardBuilder({
   source,
   document,
   onSave,
   renderTile,
}: DashboardBuilderProps) {
   const editor = useDashboardEditor({
      source,
      document,
      ...(onSave ? { onSave } : {}),
   });
   const [selected, setSelected] = useState<number | undefined>(undefined);
   const [saving, setSaving] = useState(false);
   const theme = usePublisherTheme().theme;

   const columns = editor.document.columns ?? DEFAULT_COLUMNS;
   const tile =
      selected === undefined ? undefined : editor.document.tiles[selected];

   const setTile = (change: (draft: DashboardTile) => void) => {
      if (selected === undefined) return;
      editor.update((draft) => change(draft.tiles[selected]));
   };

   return (
      <Stack sx={{ gap: 2 }}>
         <Stack
            direction="row"
            sx={{ gap: 1, alignItems: "center", flexWrap: "wrap" }}
         >
            <Typography variant="h5" sx={{ fontWeight: 600, flexGrow: 1 }}>
               {editor.document.title || "Untitled dashboard"}
            </Typography>
            <Tooltip title="Undo">
               {/* A span, because a disabled button dispatches no events and a
                   tooltip on one would never show. */}
               <span>
                  <IconButton
                     aria-label="Undo"
                     size="small"
                     disabled={!editor.canUndo}
                     onClick={editor.undo}
                  >
                     <UndoIcon fontSize="small" />
                  </IconButton>
               </span>
            </Tooltip>
            <Tooltip title="Redo">
               <span>
                  <IconButton
                     aria-label="Redo"
                     size="small"
                     disabled={!editor.canRedo}
                     onClick={editor.redo}
                  >
                     <RedoIcon fontSize="small" />
                  </IconButton>
               </span>
            </Tooltip>
            {onSave && (
               <Button
                  variant="contained"
                  size="small"
                  disabled={!editor.dirty || saving}
                  onClick={() => {
                     setSaving(true);
                     void editor.save().finally(() => setSaving(false));
                  }}
               >
                  {/* Says what will happen, and afterwards what did. */}
                  {editor.dirty ? "Save changes" : "Saved"}
               </Button>
            )}
         </Stack>

         {editor.error && (
            // The edit is still here; the message says what stopped it reaching
            // the file, which is a different thing from losing the work.
            <Alert severity="warning">{editor.error}</Alert>
         )}

         <Box
            sx={{
               display: "grid",
               gridTemplateColumns: {
                  xs: "1fr",
                  md: `repeat(${columns}, minmax(0, 1fr))`,
               },
               gap: 2,
            }}
         >
            {editor.document.tiles.map((each, index) => (
               <Box
                  key={`${each.source}.${each.name}`}
                  sx={{ gridColumn: { md: tileGridColumn(each, columns) } }}
               >
                  <Box
                     onClick={() => setSelected(index)}
                     aria-label={`Tile ${each.name}`}
                     aria-current={index === selected}
                     sx={{
                        cursor: "pointer",
                        borderRadius: 1,
                        // An outline rather than a border, and outside the tile
                        // rather than on it: the tile already has an edge of its
                        // own, and an outline neither doubles that edge nor
                        // takes up space, so selecting a tile cannot shift the
                        // layout being arranged.
                        outline:
                           index === selected
                              ? `2px solid ${theme.drillLink}`
                              : "none",
                        outlineOffset: 2,
                     }}
                  >
                     {renderTile ? (
                        renderTile(each)
                     ) : (
                        // No caller-supplied tile: say what this one will run,
                        // so the surface is still legible without a server.
                        <Paper
                           elevation={0}
                           sx={{
                              p: 2,
                              minHeight: 140,
                              background: theme.tile,
                              borderRadius: 1,
                              border: theme.border,
                           }}
                        >
                           <Typography
                              variant="subtitle2"
                              sx={{ fontWeight: 500, color: theme.tileTitle }}
                           >
                              {each.label ?? each.name}
                           </Typography>
                           {each.subtitle && (
                              <Typography
                                 variant="caption"
                                 sx={{
                                    display: "block",
                                    color: theme.tileTitle,
                                    opacity: 0.8,
                                 }}
                              >
                                 {each.subtitle}
                              </Typography>
                           )}
                           <Typography
                              variant="caption"
                              sx={{
                                 display: "block",
                                 mt: 1,
                                 color: theme.tileTitle,
                                 opacity: 0.7,
                              }}
                           >
                              {each.source} → {each.name}
                           </Typography>
                        </Paper>
                     )}
                  </Box>
               </Box>
            ))}
         </Box>

         {tile && (
            <Paper elevation={0} sx={{ p: 2, border: theme.border }}>
               <Typography
                  variant="subtitle2"
                  sx={{ fontWeight: 600, mb: 1.5 }}
               >
                  {tile.label ?? tile.name}
               </Typography>

               {tile.declaration.kind === "inherited" ? (
                  // Its tags live on the model's view, and the builder does not
                  // write model files. Saying so beats offering controls whose
                  // every use would be refused.
                  <Alert severity="info">
                     This tile is declared on <code>{tile.source}</code> rather
                     than in this dashboard, so its appearance is set by the
                     model.
                  </Alert>
               ) : (
                  <Stack sx={{ gap: 2 }}>
                     <TextField
                        id="builder-tile-label"
                        label="Label"
                        size="small"
                        value={tile.label ?? ""}
                        onChange={(event) =>
                           setTile((draft) => {
                              const next = event.target.value;
                              if (next === "") delete draft.label;
                              else draft.label = next;
                           })
                        }
                     />
                     <TextField
                        id="builder-tile-subtitle"
                        label="Subtitle"
                        size="small"
                        value={tile.subtitle ?? ""}
                        onChange={(event) =>
                           setTile((draft) => {
                              const next = event.target.value;
                              if (next === "") delete draft.subtitle;
                              else draft.subtitle = next;
                           })
                        }
                     />
                     <TextField
                        id="builder-tile-colspan"
                        label={`Width (of ${columns})`}
                        size="small"
                        type="number"
                        inputProps={{ min: 1, max: columns }}
                        value={tile.colspan ?? ""}
                        onChange={(event) =>
                           setTile((draft) => {
                              const next = Number(event.target.value);
                              if (!Number.isFinite(next) || next < 1)
                                 delete draft.colspan;
                              else draft.colspan = Math.min(next, columns);
                           })
                        }
                     />
                     <FormControlLabel
                        control={
                           <Checkbox
                              id="builder-tile-break"
                              size="small"
                              checked={tile.break ?? false}
                              onChange={(event) =>
                                 setTile((draft) => {
                                    if (event.target.checked)
                                       draft.break = true;
                                    else delete draft.break;
                                 })
                              }
                           />
                        }
                        label="Start a new row"
                     />
                     <FormControlLabel
                        control={
                           <Checkbox
                              id="builder-tile-borderless"
                              size="small"
                              checked={tile.borderless ?? false}
                              onChange={(event) =>
                                 setTile((draft) => {
                                    if (event.target.checked)
                                       draft.borderless = true;
                                    else delete draft.borderless;
                                 })
                              }
                           />
                        }
                        label="No card around it"
                     />
                  </Stack>
               )}
            </Paper>
         )}
      </Stack>
   );
}

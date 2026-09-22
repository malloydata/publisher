// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import TuneIcon from "@mui/icons-material/Tune";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import { Button, Chip, Divider, IconButton, Tooltip } from "@mui/material";
import type { ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { DashboardBar } from "../Dashboard/DashboardBar";
import { MOD } from "./useBuilderShortcuts";

/**
 * The edit bar across the top of the builder: that you are editing on the
 * left, what you can do about it on the right, in the same {@link DashboardBar}
 * the reader's view uses, so switching modes swaps the contents of one bar
 * rather than replacing one bar with a different one.
 *
 * The right-hand controls are grouped by what they do, separated rather than
 * run together: change the page (a tile, its settings), take a change back or
 * put it down (undo, redo, save), and leave (Done, where the reader's view has
 * Edit). Sticky, so undo and save stay in reach on a long dashboard; the
 * reader's own header (title and description) stays in the page below it
 * rather than being repeated here.
 */
export interface BuilderToolbarProps {
   canUndo: boolean;
   canRedo: boolean;
   onUndo: () => void;
   onRedo: () => void;
   dirty: boolean;
   saving: boolean;
   /** Absent when the builder has nowhere to save: no Save, no unsaved marker. */
   onSave?: () => void;
   /** The host's way out of editing: Done, at the right edge. */
   actions?: ReactNode;
   /** Open the add-tile picker. Absent when the host passed no catalog to pick from. */
   onAddTile?: () => void;
   /** Open the page's settings, anchored to the button that asked. */
   onSettings: (anchor: HTMLElement) => void;
}

export function BuilderToolbar({
   canUndo,
   canRedo,
   onUndo,
   onRedo,
   dirty,
   saving,
   onSave,
   actions,
   onAddTile,
   onSettings,
}: BuilderToolbarProps) {
   const { theme } = usePublisherTheme();
   return (
      <DashboardBar
         left={
            <Chip
               // The app's theme makes every chip small; this one stands in a
               // row of buttons, so it says otherwise and takes their height
               // and type size. One size across the bar, or the state reads as
               // a label that shrank away from the controls.
               size="medium"
               variant="outlined"
               icon={<EditOutlinedIcon sx={{ fontSize: 20 }} />}
               label="Editing"
               sx={{
                  height: 37,
                  fontSize: "0.875rem",
                  fontWeight: 500,
                  // The app's own chip: the page's edge and its secondary
                  // text, not a filled blue badge borrowed from the drill
                  // link, which read as a notification rather than a state.
                  border: theme.border,
                  color: theme.tileTitle,
                  "& .MuiChip-icon": { color: "inherit" },
                  "& .MuiChip-label": { fontSize: "0.875rem" },
               }}
            />
         }
      >
         {/* What the page is made of. */}
         {onAddTile && (
            <Button
               startIcon={<AddIcon />}
               onClick={onAddTile}
               aria-label="Add tile"
            >
               Tile
            </Button>
         )}
         <Button
            startIcon={<TuneIcon />}
            onClick={(event) => onSettings(event.currentTarget)}
         >
            Settings
         </Button>

         {/* What happens to a change: take it back, or put it down. */}
         <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
         <Tooltip title={`Undo (${MOD}Z)`}>
            {/* A span, because a disabled button dispatches no events and a
                tooltip on one would never show. */}
            <span>
               <IconButton
                  aria-label="Undo"
                  disabled={!canUndo}
                  onClick={onUndo}
                  sx={{ width: 37, height: 37 }}
               >
                  <UndoIcon fontSize="small" />
               </IconButton>
            </span>
         </Tooltip>
         <Tooltip title={`Redo (${MOD}⇧Z)`}>
            <span>
               <IconButton
                  aria-label="Redo"
                  disabled={!canRedo}
                  onClick={onRedo}
                  sx={{ width: 37, height: 37 }}
               >
                  <RedoIcon fontSize="small" />
               </IconButton>
            </span>
         </Tooltip>
         {onSave && (
            <>
               <Tooltip title={dirty ? `Save (${MOD}S)` : ""}>
                  <span>
                     <Button
                        variant={dirty ? "contained" : "outlined"}
                        disabled={!dirty || saving}
                        onClick={onSave}
                        // Wide enough for the longest of the three labels, so
                        // the bar does not reflow as the state cycles.
                        sx={{ minWidth: 124 }}
                     >
                        {/* Says what will happen, then that it is happening,
                            then what did. One tick in the bar, on "Done
                            editing"; here the word carries the state. */}
                        {saving ? "Saving…" : dirty ? "Save changes" : "Saved"}
                     </Button>
                  </span>
               </Tooltip>
            </>
         )}

         {/* Leaving, where the reader's view has Edit. */}
         {actions && (
            <>
               <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
               {actions}
            </>
         )}
      </DashboardBar>
   );
}

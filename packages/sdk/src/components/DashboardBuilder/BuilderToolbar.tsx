// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import TuneIcon from "@mui/icons-material/Tune";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Button,
   Chip,
   Divider,
   IconButton,
   Tooltip,
   Typography,
} from "@mui/material";
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
            <>
               <Chip
                  size="small"
                  variant="outlined"
                  icon={<EditOutlinedIcon sx={{ fontSize: 14 }} />}
                  label="Editing"
                  sx={{
                     fontWeight: 500,
                     // The app's own chip: the page's edge and its secondary
                     // text, not a filled blue badge borrowed from the drill
                     // link, which read as a notification rather than a state.
                     border: theme.border,
                     color: theme.tileTitle,
                     "& .MuiChip-icon": { color: "inherit" },
                  }}
               />
               <Typography
                  variant="body2"
                  noWrap
                  sx={{
                     color: theme.tileTitle,
                     // One line or none: a hint that wraps makes the bar taller
                     // than the reader's, and the point of the bar is that
                     // switching modes moves nothing.
                     display: { xs: "none", lg: "block" },
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                  }}
               >
                  Drag a tile to move it, its right edge to resize it, or into
                  the empty end of a row to move it up.
               </Typography>
            </>
         }
      >
         {/* What the page is made of. */}
         {onAddTile && (
            <Button
               size="small"
               startIcon={<AddIcon fontSize="small" />}
               onClick={onAddTile}
               aria-label="Add tile"
            >
               Tile
            </Button>
         )}
         <Button
            size="small"
            startIcon={<TuneIcon fontSize="small" />}
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
                  size="small"
                  disabled={!canUndo}
                  onClick={onUndo}
               >
                  <UndoIcon fontSize="small" />
               </IconButton>
            </span>
         </Tooltip>
         <Tooltip title={`Redo (${MOD}⇧Z)`}>
            <span>
               <IconButton
                  aria-label="Redo"
                  size="small"
                  disabled={!canRedo}
                  onClick={onRedo}
               >
                  <RedoIcon fontSize="small" />
               </IconButton>
            </span>
         </Tooltip>
         {onSave && (
            <>
               {/* "Unsaved changes" beside Save: the button's label alone
                   reads as a command, not as a state. */}
               <Typography
                  variant="caption"
                  aria-live="polite"
                  sx={{
                     color: theme.tileTitle,
                     minWidth: 108,
                     textAlign: "right",
                     // Room between the state and the button that acts on it:
                     // at the row's 4px gap the two read as one label.
                     mr: 1.5,
                     opacity: dirty && !saving ? 1 : 0,
                     transition: "opacity 120ms",
                  }}
               >
                  Unsaved changes
               </Typography>
               <Tooltip title={dirty ? `Save (${MOD}S)` : ""}>
                  <span>
                     <Button
                        variant={dirty ? "contained" : "outlined"}
                        disabled={!dirty || saving}
                        onClick={onSave}
                        startIcon={
                           !dirty && !saving ? <CheckIcon /> : undefined
                        }
                        sx={{ minWidth: 124 }}
                     >
                        {/* Says what will happen, then that it is happening,
                            then what did. */}
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

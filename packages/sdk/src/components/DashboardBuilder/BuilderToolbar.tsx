// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CheckIcon from "@mui/icons-material/Check";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Button,
   Chip,
   Divider,
   IconButton,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { MOD } from "./useBuilderShortcuts";

/**
 * The edit bar across the top of the builder — Looker's shape: the fact that
 * you are editing on the left, the things you can do about it on the right.
 *
 * Sticky, so undo and save stay in reach on a long dashboard; the reader's own
 * header (title and description) stays in the page below it rather than being
 * repeated here, which is also what Looker does.
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
}

export function BuilderToolbar({
   canUndo,
   canRedo,
   onUndo,
   onRedo,
   dirty,
   saving,
   onSave,
}: BuilderToolbarProps) {
   const { theme } = usePublisherTheme();
   return (
      <Stack
         direction="row"
         sx={{
            position: "sticky",
            top: 0,
            zIndex: 5,
            alignItems: "center",
            gap: 1,
            px: 1.5,
            py: 1,
            // Its own ground, so the tiles scrolling under it do not show
            // through, and an edge so it reads as a bar rather than a row.
            // The Publisher theme's ground and edge, like every other surface
            // here — MUI's own palette would not follow an instance theme or
            // its dark mode.
            bgcolor: theme.background,
            borderBottom: theme.border,
            borderRadius: 1,
         }}
      >
         <Chip
            size="small"
            icon={<EditOutlinedIcon sx={{ fontSize: 14 }} />}
            label="Editing"
            sx={{
               fontWeight: 500,
               bgcolor: theme.drillLink,
               color: theme.tile,
               "& .MuiChip-icon": { color: "inherit" },
            }}
         />
         <Typography
            variant="body2"
            sx={{
               color: theme.tileTitle,
               display: { xs: "none", md: "block" },
            }}
         >
            Drag a tile to move it, its right edge to resize it, or into the
            empty end of a row to move it up.
         </Typography>

         <Stack
            direction="row"
            sx={{ ml: "auto", alignItems: "center", gap: 0.5 }}
         >
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
                  <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
                  {/* Looker says "Unsaved changes" beside Save, and it is the
                      right thing to say: the button's label alone reads as a
                      command, not as a state. */}
                  <Typography
                     variant="caption"
                     aria-live="polite"
                     sx={{
                        color: theme.tileTitle,
                        minWidth: 108,
                        textAlign: "right",
                        // Room between the state and the button that acts on
                        // it: at the row's 4px gap the two read as one label.
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
                           size="small"
                           disabled={!dirty || saving}
                           onClick={onSave}
                           startIcon={
                              !dirty && !saving ? (
                                 <CheckIcon fontSize="small" />
                              ) : undefined
                           }
                           sx={{ minWidth: 124 }}
                        >
                           {/* Says what will happen, then that it is happening,
                               then what did. */}
                           {saving
                              ? "Saving…"
                              : dirty
                                ? "Save changes"
                                : "Saved"}
                        </Button>
                     </span>
                  </Tooltip>
               </>
            )}
         </Stack>
      </Stack>
   );
}

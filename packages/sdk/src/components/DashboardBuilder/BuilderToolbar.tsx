// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Box,
   Button,
   Divider,
   IconButton,
   Stack,
   Tooltip,
} from "@mui/material";
import type { ReactNode, Ref } from "react";
import type { SavesTo } from "./documentSession";
import { SecondaryButton } from "../buttons";
import { MOD } from "./useBuilderShortcuts";

/** Where Save puts the document, in the words under the button. */
export const SAVE_TARGET: Record<SavesTo, string> = {
   package: "Saves to the package file",
   browser: "Saves in this browser",
   host: "Saves to the app this is embedded in",
};

/**
 * The builder's actions, as one row with no bar of its own, at the right of
 * the page title's line.
 *
 * Grouped by what they do, separated rather than run together: change the page
 * (a tile), take a change back (undo, redo), and keep it (Save,
 * which saves in place: the builder stays open, and leaving is the page's
 * own navigation).
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
   /** Where Save writes, shown in its tooltip. */
   savesTo?: SavesTo;
   /** The backend's own words for where Save writes; replaces the generic line for `savesTo`. */
   saveLabel?: string;
   /** The Save button, so focus can return to it after an Undo save. */
   saveButton?: Ref<HTMLButtonElement>;
   /** The host's own extra actions, beside Save. */
   actions?: ReactNode;
   /** Open the add-tile picker. Absent when the host passed no catalog to pick from. */
   onAddTile?: () => void;
   /**
    * Leave the builder: draws Close after Save, for a host that opts in. The
    * Console leaves from its own header and passes nothing.
    */
   onExit?: () => void;
}

export function BuilderToolbar({
   canUndo,
   canRedo,
   onUndo,
   onRedo,
   dirty,
   saving,
   onSave,
   savesTo = "package",
   saveLabel,
   saveButton,
   actions,
   onAddTile,
   onExit,
}: BuilderToolbarProps) {
   return (
      <Stack
         direction="row"
         aria-label="Builder actions"
         sx={{
            alignItems: "center",
            gap: 0.5,
            // A button's label never wraps onto a second line; the filter
            // chips beside the row give way first.
            flexShrink: 0,
            "& .MuiButton-root": { whiteSpace: "nowrap" },
         }}
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

         {/* What happens to a change: take it back, or put it down. */}
         {onAddTile && (
            <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
         )}
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
         {/* The host's own actions, then Save, which keeps the builder open,
          then Close only when the host opted into it with `onExit`. */}
         {(actions || onSave || onExit) && (
            <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
         )}
         {actions}
         {onSave ? (
            <Tooltip
               title={
                  dirty
                     ? `Save (${MOD}S) · ${saveLabel ?? SAVE_TARGET[savesTo]}`
                     : (saveLabel ?? SAVE_TARGET[savesTo])
               }
            >
               <span>
                  <Button
                     ref={saveButton}
                     variant={dirty ? "contained" : "outlined"}
                     disabled={!dirty || saving}
                     onClick={onSave}
                     // Wide enough for the longest of the three labels, so
                     // the row does not reflow as the state cycles.
                     sx={{ minWidth: 124 }}
                  >
                     {saving ? "Saving…" : dirty ? "Save" : "Saved"}
                  </Button>
               </span>
            </Tooltip>
         ) : null}
         {onSave && (
            // The save state, said aloud: the button's face changes, which a
            // screen reader does not announce on its own.
            <Box
               component="span"
               aria-live="polite"
               sx={{
                  position: "absolute",
                  width: "1px",
                  height: "1px",
                  overflow: "hidden",
                  clipPath: "inset(50%)",
                  whiteSpace: "nowrap",
               }}
            >
               {saving
                  ? "Saving"
                  : dirty
                    ? "Unsaved changes"
                    : "All changes saved"}
            </Box>
         )}
         {onExit && (
            <SecondaryButton
               label="Close"
               icon={<CheckIcon />}
               onClick={onExit}
            />
         )}
      </Stack>
   );
}

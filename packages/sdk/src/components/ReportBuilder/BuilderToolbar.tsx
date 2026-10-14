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
   ToggleButton,
   ToggleButtonGroup,
   Tooltip,
} from "@mui/material";
import type { ReactNode, Ref } from "react";
import type { SavesTo } from "./documentSession";
import { SecondaryButton } from "../buttons";
import { MOD } from "./useBuilderShortcuts";
import {
   CONTROL_HEIGHT_PX,
   CONTROL_RADIUS,
   edgeColor,
   hoverEdgeColor,
   TYPE,
} from "../controlTokens";
import { usePublisherTheme } from "../../theme/ThemeContext";

/**
 * The toolbar's controls at the report surfaces' one size, set here rather
 * than left to the host theme: the row was five shapes (a 29px square toggle,
 * a text link, round 37px icons, a 37px pill), because each control took a
 * different library default.
 */
export const controlSx = (edge: string, hoverEdge: string) =>
   ({
      height: CONTROL_HEIGHT_PX,
      minHeight: CONTROL_HEIGHT_PX,
      px: 1.5,
      borderRadius: CONTROL_RADIUS,
      boxShadow: "none",
      ...TYPE.control,
      "&:hover": { boxShadow: "none" },
      "& .MuiButton-startIcon": { mr: 0.5, "& svg": { fontSize: 18 } },
      // Outlined in the report's one edge, as the segmented control, the
      // filter buttons and the tiles are, rather than the host's button grey.
      "&.MuiButton-outlined": {
         borderColor: edge,
         "&:hover": { borderColor: hoverEdge },
      },
   }) as const;

export const iconControlSx = {
   width: CONTROL_HEIGHT_PX,
   height: CONTROL_HEIGHT_PX,
   borderRadius: CONTROL_RADIUS,
   "& svg": { fontSize: 18 },
} as const;

/** Short rules between groups: the height of a label, not of the row. */
export const dividerSx = {
   height: 20,
   alignSelf: "center",
   mx: 0.5,
   my: 0,
} as const;

/**
 * A segmented control, built as an outlined button group: the same edge,
 * height, radius and padding as "+ Tile" beside it, each segment the full
 * height of the control with a rule between them, and the chosen one tinted
 * edge to edge. An inset chip inside the outline read as a control within a
 * control, a size smaller than the button next to it.
 */
export const segmentedSx = (edge: string) =>
   ({
      height: CONTROL_HEIGHT_PX,
      boxSizing: "border-box",
      borderRadius: CONTROL_RADIUS,
      border: `1px solid ${edge}`,
      overflow: "hidden",
      bgcolor: "background.paper",
      "& .MuiToggleButton-root": {
         border: "none",
         borderRadius: "0 !important",
         m: "0 !important",
         px: 1.5,
         py: 0,
         height: "100%",
         ...TYPE.control,
         color: "text.secondary",
         "& + .MuiToggleButton-root": { borderLeft: `1px solid ${edge}` },
         "&:hover": { bgcolor: "action.hover" },
         "&.Mui-selected, &.Mui-selected:hover": {
            bgcolor: "action.selected",
            color: "text.primary",
         },
      },
   }) as const;

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
   /** The report's layout, for the switch. Absent, no switch is drawn. */
   layout?: "grid" | "linear";
   /** Switch the layout: rewrites `layout=` in the tag and nothing else. */
   onLayoutChange?: (layout: "grid" | "linear") => void;
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
   layout,
   onLayoutChange,
}: BuilderToolbarProps) {
   const { theme } = usePublisherTheme();
   const edge = edgeColor(theme.cardBorder);
   const hoverEdge = hoverEdgeColor(theme.cardBorder, theme.tileTitle);
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
         {/* How the page is laid out: one property of the tag, switched in place. */}
         {layout && onLayoutChange && (
            <>
               <Tooltip title="Layout: tiles side by side, or one after another">
                  <ToggleButtonGroup
                     size="small"
                     exclusive
                     value={layout}
                     aria-label="Layout"
                     onChange={(_event, next: "grid" | "linear" | null) => {
                        if (next && next !== layout) onLayoutChange(next);
                     }}
                     sx={segmentedSx(edge)}
                  >
                     <ToggleButton value="linear" aria-label="Linear layout">
                        Linear
                     </ToggleButton>
                     <ToggleButton value="grid" aria-label="Grid layout">
                        Grid
                     </ToggleButton>
                  </ToggleButtonGroup>
               </Tooltip>
               <Divider orientation="vertical" sx={dividerSx} />
            </>
         )}
         {/* What the page is made of. */}
         {onAddTile && (
            <Button
               variant="outlined"
               startIcon={<AddIcon />}
               onClick={onAddTile}
               aria-label="Add tile"
               sx={controlSx(edge, hoverEdge)}
            >
               Tile
            </Button>
         )}

         {/* What happens to a change: take it back, or put it down. */}
         {onAddTile && <Divider orientation="vertical" sx={dividerSx} />}
         <Tooltip title={`Undo (${MOD}Z)`}>
            {/* A span, because a disabled button dispatches no events and a
                tooltip on one would never show. */}
            <span>
               <IconButton
                  aria-label="Undo"
                  disabled={!canUndo}
                  onClick={onUndo}
                  sx={iconControlSx}
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
                  sx={iconControlSx}
               >
                  <RedoIcon fontSize="small" />
               </IconButton>
            </span>
         </Tooltip>
         {/* The host's own actions, then Save, which keeps the builder open,
          then Close only when the host opted into it with `onExit`. */}
         {(actions || onSave || onExit) && (
            <Divider orientation="vertical" sx={dividerSx} />
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
                     sx={{ ...controlSx(edge, hoverEdge), minWidth: 88 }}
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

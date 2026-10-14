// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Box,
   Button,
   Divider,
   IconButton,
   ListItemText,
   Menu,
   MenuItem,
   Stack,
   ToggleButton,
   ToggleButtonGroup,
   Tooltip,
   useMediaQuery,
} from "@mui/material";
import { useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor, hoverEdgeColor, TYPE } from "../controlTokens";
import type { BuilderControls } from "./builderControls";
import {
   controlSx,
   dividerSx,
   iconControlSx,
   segmentedSx,
} from "./BuilderToolbar";
import { MOD } from "./useBuilderShortcuts";

export interface BuilderBarProps {
   /** The builder's controls, from `onControlsChange`. Undefined while it opens: only Done is drawn. */
   controls: BuilderControls | undefined;
   /** Leave the builder for the reader. Absent, no Done is drawn. */
   onDone?: () => void;
}

/**
 * The builder's controls as one bar, for a host's header: the save status,
 * undo and redo, the layout switch, an Add menu, then Done and, while there
 * are unsaved edits, Save. The page under it stays the reader's page, with the
 * tile and filter controls on the things they change.
 *
 * Done leaves; a host whose leave guard asks about unsaved edits (as the
 * Console's does) is asked there, so Done never discards on its own.
 */
export function BuilderBar({ controls, onDone: onDoneProp }: BuilderBarProps) {
   // A host that passed the builder `onExit` and draws the bar without its own
   // Done still gets a way out.
   const onDone = onDoneProp ?? controls?.exit;
   // A phone's header has room for the way out and little else: the bar there
   // is the status, Done and Save, and the rest is on the page.
   const compact = useMediaQuery("(max-width: 599.95px)");
   const { theme } = usePublisherTheme();
   const edge = edgeColor(theme.cardBorder);
   const hoverEdge = hoverEdgeColor(theme.cardBorder, theme.tileTitle);
   const [addAnchor, setAddAnchor] = useState<HTMLElement | null>(null);
   const unsaved = controls?.save !== undefined && controls.status !== "saved";
   const add = controls?.add;
   const canAdd =
      add !== undefined &&
      (add.tile !== undefined ||
         add.text !== undefined ||
         add.filter !== undefined ||
         add.filterUnavailable !== undefined);
   const pick = (action: (() => void) | undefined) => () => {
      setAddAnchor(null);
      action?.();
   };

   return (
      <Stack
         direction="row"
         role="toolbar"
         aria-label="Report builder"
         sx={{
            alignItems: "center",
            gap: 0.5,
            flexShrink: 0,
            "& .MuiButton-root": { whiteSpace: "nowrap" },
         }}
      >
         {controls && (
            <>
               {controls.save && (
                  <Tooltip title={controls.saveTarget}>
                     <Box
                        component="span"
                        role="status"
                        aria-live="polite"
                        sx={(mui) => ({
                           display: "inline-flex",
                           alignItems: "center",
                           gap: 0.75,
                           pr: 0.75,
                           ...TYPE.control,
                           color:
                              controls.status === "saved"
                                 ? mui.palette.text.secondary
                                 : mui.palette.text.primary,
                        })}
                     >
                        {controls.status === "saved" ? (
                           <CheckIcon aria-hidden sx={{ fontSize: 16 }} />
                        ) : (
                           <Box
                              aria-hidden
                              sx={(mui) => ({
                                 width: 7,
                                 height: 7,
                                 borderRadius: "50%",
                                 bgcolor:
                                    controls.status === "saving"
                                       ? mui.palette.text.disabled
                                       : mui.palette.warning.main,
                              })}
                           />
                        )}
                        {controls.status === "saved"
                           ? "Saved"
                           : controls.status === "saving"
                             ? "Saving…"
                             : "Unsaved changes"}
                     </Box>
                  </Tooltip>
               )}
               {!compact && (
                  <>
                     <Tooltip title={`Undo (${MOD}Z)`}>
                        {/* A span, because a disabled button dispatches no events and a
                   tooltip on one would never show. */}
                        <span>
                           <IconButton
                              aria-label="Undo"
                              disabled={!controls.canUndo}
                              onClick={controls.undo}
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
                              disabled={!controls.canRedo}
                              onClick={controls.redo}
                              sx={iconControlSx}
                           >
                              <RedoIcon fontSize="small" />
                           </IconButton>
                        </span>
                     </Tooltip>
                     {(controls.layout || canAdd) && (
                        <Divider orientation="vertical" sx={dividerSx} />
                     )}
                     {controls.layout && (
                        <Tooltip title="Layout: tiles side by side, or one after another">
                           <ToggleButtonGroup
                              size="small"
                              exclusive
                              value={controls.layout.value}
                              aria-label="Layout"
                              onChange={(
                                 _event,
                                 next: "grid" | "linear" | null,
                              ) => {
                                 if (next && next !== controls.layout?.value)
                                    controls.layout?.set(next);
                              }}
                              sx={segmentedSx(edge)}
                           >
                              <ToggleButton
                                 value="linear"
                                 aria-label="Linear layout"
                              >
                                 Linear
                              </ToggleButton>
                              <ToggleButton
                                 value="grid"
                                 aria-label="Grid layout"
                              >
                                 Grid
                              </ToggleButton>
                           </ToggleButtonGroup>
                        </Tooltip>
                     )}
                     {canAdd && (
                        <>
                           <Button
                              variant="outlined"
                              startIcon={<AddIcon />}
                              endIcon={<ExpandMoreIcon />}
                              aria-haspopup="menu"
                              aria-expanded={addAnchor !== null}
                              onClick={(event) =>
                                 setAddAnchor(event.currentTarget)
                              }
                              sx={{
                                 ...controlSx(edge, hoverEdge),
                                 "& .MuiButton-endIcon": {
                                    ml: 0.25,
                                    "& svg": { fontSize: 16 },
                                 },
                              }}
                           >
                              Add
                           </Button>
                           <Menu
                              anchorEl={addAnchor}
                              open={addAnchor !== null}
                              onClose={() => setAddAnchor(null)}
                              anchorOrigin={{
                                 vertical: "bottom",
                                 horizontal: "right",
                              }}
                              transformOrigin={{
                                 vertical: "top",
                                 horizontal: "right",
                              }}
                           >
                              {add?.tile && (
                                 <MenuItem onClick={pick(add.tile)}>
                                    <ListItemText
                                       primary="Tile"
                                       secondary="A view from the model"
                                    />
                                 </MenuItem>
                              )}
                              {add?.text && (
                                 <MenuItem onClick={pick(add.text)}>
                                    <ListItemText
                                       primary="Text"
                                       secondary="Prose between the tiles"
                                    />
                                 </MenuItem>
                              )}
                              {(add?.filter || add?.filterUnavailable) && (
                                 <MenuItem
                                    onClick={pick(add.filter)}
                                    disabled={add.filter === undefined}
                                 >
                                    <ListItemText
                                       primary="Filter"
                                       secondary={
                                          add.filterUnavailable ??
                                          "A control the tiles read"
                                       }
                                    />
                                 </MenuItem>
                              )}
                           </Menu>
                        </>
                     )}
                  </>
               )}
            </>
         )}
         {(onDone || unsaved) && controls && (
            <Divider orientation="vertical" sx={dividerSx} />
         )}
         {onDone && (
            <Button
               variant={unsaved ? "outlined" : "contained"}
               disableElevation
               onClick={onDone}
               sx={controlSx(edge, hoverEdge)}
            >
               Done
            </Button>
         )}
         {controls?.save && unsaved && (
            <Tooltip title={`Save (${MOD}S) · ${controls.saveTarget}`}>
               <span>
                  <Button
                     variant="contained"
                     disableElevation
                     disabled={controls.status === "saving"}
                     onClick={controls.save}
                     sx={{ ...controlSx(edge, hoverEdge), minWidth: 72 }}
                  >
                     {controls.status === "saving" ? "Saving…" : "Save"}
                  </Button>
               </span>
            </Tooltip>
         )}
      </Stack>
   );
}

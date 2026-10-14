// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
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
   // Done writes the edits itself when the builder offers it, so Save is then
   // only the keyboard's (⌘S), for a checkpoint without leaving.
   const finish = controls?.done ?? onDone;
   const separateSave = unsaved && controls?.done === undefined;
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
               {controls.save && <SaveStatus controls={controls} />}
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
         {(finish || separateSave) && controls && (
            <Divider orientation="vertical" sx={dividerSx} />
         )}
         {finish && (
            <Tooltip
               title={
                  controls?.done && unsaved
                     ? `Saves your edits, then shows the report · ${controls.saveTarget}`
                     : ""
               }
            >
               <span>
                  <Button
                     variant={separateSave ? "outlined" : "contained"}
                     disableElevation
                     disabled={controls?.status === "saving"}
                     onClick={finish}
                     sx={{ ...controlSx(edge, hoverEdge), minWidth: 72 }}
                  >
                     {controls?.done && controls.status === "saving"
                        ? "Saving…"
                        : "Done"}
                  </Button>
               </span>
            </Tooltip>
         )}
         {controls?.save && separateSave && (
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

/**
 * Where the edits stand: saved, saving, kept in this browser, or (with no
 * draft) unsaved. While there are edits to drop, it opens a menu offering
 * Discard edits, which is one undoable edit back to the saved report.
 */
function SaveStatus({ controls }: { controls: BuilderControls }) {
   const { theme } = usePublisherTheme();
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);
   const { status, draftKeptAt, discard } = controls;
   const words =
      status === "saved"
         ? "Saved"
         : status === "saving"
           ? "Saving…"
           : draftKeptAt !== undefined
             ? "Draft saved"
             : "Unsaved changes";
   const hint =
      status === "unsaved" && draftKeptAt !== undefined
         ? "Your edits are kept in this browser. Done saves them to the report."
         : controls.saveTarget;
   const body = (
      <Box
         component="span"
         role="status"
         aria-live="polite"
         sx={(mui) => ({
            display: "inline-flex",
            alignItems: "center",
            gap: 0.75,
            ...TYPE.control,
            color:
               status === "saved"
                  ? mui.palette.text.secondary
                  : mui.palette.text.primary,
         })}
      >
         {status === "saved" ? (
            <CheckIcon aria-hidden sx={{ fontSize: 16 }} />
         ) : (
            <Box
               aria-hidden
               sx={(mui) => ({
                  width: 7,
                  height: 7,
                  borderRadius: "50%",
                  bgcolor:
                     status === "saving" || draftKeptAt !== undefined
                        ? mui.palette.text.disabled
                        : mui.palette.warning.main,
               })}
            />
         )}
         {words}
      </Box>
   );
   if (!discard || status !== "unsaved")
      return (
         <Tooltip title={hint} describeChild>
            <Box component="span" sx={{ pr: 0.75 }}>
               {body}
            </Box>
         </Tooltip>
      );
   return (
      <>
         <Tooltip title={hint} describeChild>
            <Button
               aria-haspopup="menu"
               aria-expanded={anchor !== null}
               endIcon={<ExpandMoreIcon />}
               onClick={(event) => setAnchor(event.currentTarget)}
               sx={{
                  textTransform: "none",
                  color: "inherit",
                  px: 1,
                  minWidth: 0,
                  "& .MuiButton-endIcon": {
                     ml: 0.25,
                     "& svg": { fontSize: 16 },
                  },
               }}
            >
               {body}
            </Button>
         </Tooltip>
         <Menu
            anchorEl={anchor}
            open={anchor !== null}
            onClose={() => setAnchor(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
            transformOrigin={{ vertical: -4, horizontal: "left" }}
            slotProps={{
               paper: {
                  sx: {
                     minWidth: 240,
                     borderRadius: "10px",
                     border: `1px solid ${edgeColor(theme.cardBorder)}`,
                     boxShadow: "0 8px 24px rgba(16, 24, 40, 0.12)",
                  },
               },
            }}
            MenuListProps={{ dense: true, sx: { py: 0.5 } }}
         >
            {draftKeptAt !== undefined && (
               <Box
                  component="li"
                  role="presentation"
                  sx={{
                     px: 1.75,
                     pt: 0.75,
                     pb: 1,
                     ...TYPE.caption,
                     color: "text.secondary",
                  }}
               >
                  Kept in this browser at{" "}
                  {new Date(draftKeptAt).toLocaleTimeString([], {
                     hour: "numeric",
                     minute: "2-digit",
                  })}
               </Box>
            )}
            <MenuItem
               onClick={() => {
                  setAnchor(null);
                  discard();
               }}
               sx={{
                  mx: 0.5,
                  borderRadius: "6px",
                  gap: 1.25,
                  color: "error.main",
                  // The menu focuses its item on open; a full focus ring
                  // there read as two stray rules across the menu.
                  "&.Mui-focusVisible": {
                     outline: "none",
                     bgcolor: "action.hover",
                  },
               }}
            >
               <RestartAltIcon fontSize="small" />
               Discard edits
            </MenuItem>
         </Menu>
      </>
   );
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import FilterListIcon from "@mui/icons-material/FilterList";
import InsertChartOutlinedIcon from "@mui/icons-material/InsertChartOutlined";
import NotesIcon from "@mui/icons-material/Notes";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Box,
   Button,
   ButtonBase,
   Divider,
   IconButton,
   Menu,
   MenuItem,
   Stack,
   ToggleButton,
   ToggleButtonGroup,
   Tooltip,
   useMediaQuery,
} from "@mui/material";
import type { Theme } from "@mui/material";
import { useState, type ReactNode } from "react";
import { MOTION_FAST } from "../../theme/motion";
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
                              // A pick opens a window or an editor that takes focus;
                              // the menu handing it back to Add as it closed took it
                              // away again (the add-tile search lost its caret).
                              disableRestoreFocus
                              anchorOrigin={{
                                 vertical: "bottom",
                                 horizontal: "right",
                              }}
                              transformOrigin={{
                                 vertical: -6,
                                 horizontal: "right",
                              }}
                              slotProps={{ paper: { sx: menuPaperSx(edge) } }}
                              MenuListProps={{ sx: { p: 0.5 } }}
                           >
                              {add?.tile && (
                                 <AddItem
                                    icon={<InsertChartOutlinedIcon />}
                                    title="Tile"
                                    detail="A view from the model"
                                    onClick={pick(add.tile)}
                                 />
                              )}
                              {add?.text && (
                                 <AddItem
                                    icon={<NotesIcon />}
                                    title="Text"
                                    detail="Prose between the tiles"
                                    onClick={pick(add.text)}
                                 />
                              )}
                              {(add?.filter || add?.filterUnavailable) && (
                                 <AddItem
                                    icon={<FilterListIcon />}
                                    title="Filter"
                                    detail={
                                       add.filterUnavailable ??
                                       "A control the tiles read"
                                    }
                                    disabled={add.filter === undefined}
                                    onClick={pick(add.filter)}
                                 />
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
               // Only for a pointer that rests: the click that starts a save
               // leaves the pointer here, and the tip popped up over Saving….
               enterDelay={800}
               enterNextDelay={800}
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
                     // Wide enough for "Saving…", so the label changes in
                     // place: the bar is set against the header's right
                     // edge, and a wider button pushed every control left.
                     sx={{ ...controlSx(edge, hoverEdge), width: 92 }}
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
   // One element for every state, set the same way: the same type and
   // padding, an icon slot of one size (a dot, or the check), and the
   // chevron's room kept whether or not the menu is offered. Each state used
   // to be drawn differently (a menu button for a draft, bare text otherwise),
   // so Draft saved → Saving… → Saved changed font, padding and width at every
   // step, and the controls beside it jumped. A minimum width, with the words
   // set against the undo buttons, keeps them still.
   const menu = discard !== undefined && status === "unsaved";
   const sx = (mui: Theme) => ({
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "flex-end",
      gap: 0.75,
      minWidth: 132,
      height: 32,
      px: 1,
      borderRadius: "6px",
      font: "inherit",
      ...TYPE.control,
      whiteSpace: "nowrap" as const,
      color:
         status === "saved"
            ? mui.palette.text.secondary
            : mui.palette.text.primary,
      transition: `color ${MOTION_FAST}, background-color ${MOTION_FAST}`,
      ...(menu ? { "&:hover": { bgcolor: mui.palette.action.hover } } : {}),
   });
   const inner = (
      <>
         <Box
            component="span"
            aria-hidden
            sx={{
               width: 16,
               height: 16,
               flex: "none",
               display: "inline-flex",
               alignItems: "center",
               justifyContent: "center",
            }}
         >
            {status === "saved" ? (
               <CheckIcon sx={{ fontSize: 16 }} />
            ) : (
               <Box
                  component="span"
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
         </Box>
         <Box component="span" aria-hidden>
            {words}
         </Box>
         <ExpandMoreIcon
            aria-hidden
            sx={{
               fontSize: 16,
               ml: -0.25,
               visibility: menu ? "visible" : "hidden",
            }}
         />
      </>
   );
   // The words a screen reader hears as they change, beside the control
   // rather than in it, so a button around them is named by its words.
   const announce = (
      <Box
         component="span"
         role="status"
         aria-live="polite"
         sx={VISUALLY_HIDDEN}
      >
         {words}
      </Box>
   );
   return (
      <>
         {announce}
         <Tooltip title={hint} describeChild>
            {menu ? (
               <ButtonBase
                  aria-label={words}
                  aria-haspopup="menu"
                  aria-expanded={anchor !== null}
                  onClick={(event) => setAnchor(event.currentTarget)}
                  sx={sx}
               >
                  {inner}
               </ButtonBase>
            ) : (
               <Box component="span" sx={sx}>
                  {inner}
               </Box>
            )}
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

/** In the accessibility tree, not on screen: the status line's announcement. */
const VISUALLY_HIDDEN = {
   position: "absolute",
   width: 1,
   height: 1,
   margin: -1,
   padding: 0,
   overflow: "hidden",
   clip: "rect(0 0 0 0)",
   whiteSpace: "nowrap",
   border: 0,
} as const;

/** The builder's menus: a card set like its windows, rounded and bordered. */
const menuPaperSx = (edge: string) => ({
   minWidth: 260,
   mt: 0.5,
   borderRadius: "10px",
   border: `1px solid ${edge}`,
   boxShadow: "0 8px 24px rgba(16, 24, 40, 0.12)",
});

/**
 * One thing the Add menu adds: its picture in a tile, its name, and a line
 * saying what it is (or, when it cannot be added, why not).
 */
function AddItem({
   icon,
   title,
   detail,
   disabled = false,
   onClick,
}: {
   icon: ReactNode;
   title: string;
   detail: string;
   disabled?: boolean;
   onClick: () => void;
}) {
   const { theme } = usePublisherTheme();
   return (
      <MenuItem
         onClick={onClick}
         disabled={disabled}
         sx={(mui) => ({
            gap: 1.5,
            px: 1.25,
            py: 1,
            borderRadius: "7px",
            alignItems: "center",
            "& .add-icon": {
               width: 32,
               height: 32,
               flex: "none",
               borderRadius: "8px",
               display: "flex",
               alignItems: "center",
               justifyContent: "center",
               color: mui.palette.text.secondary,
               bgcolor: mui.palette.action.hover,
               transition: `color ${MOTION_FAST}, background-color ${MOTION_FAST}`,
               "& svg": { fontSize: 18 },
            },
            "&:hover .add-icon, &.Mui-focusVisible .add-icon": {
               color: theme.accent,
               bgcolor: `color-mix(in srgb, ${theme.accent} 10%, ${mui.palette.background.paper})`,
            },
            // The menu focuses its first item on open; a full ring there read
            // as a box drawn round a box.
            "&.Mui-focusVisible": {
               outline: "none",
               bgcolor: mui.palette.action.hover,
            },
            "&.Mui-disabled": { opacity: 1 },
            "&.Mui-disabled .add-text": { opacity: 0.5 },
            "&.Mui-disabled .add-icon": { opacity: 0.5 },
         })}
      >
         <Box className="add-icon" aria-hidden>
            {icon}
         </Box>
         <Box className="add-text" sx={{ minWidth: 0 }}>
            <Box sx={{ ...TYPE.control, fontWeight: 600, lineHeight: 1.3 }}>
               {title}
            </Box>
            <Box
               sx={{
                  ...TYPE.caption,
                  color: "text.secondary",
                  whiteSpace: "normal",
                  maxWidth: 220,
               }}
            >
               {detail}
            </Box>
         </Box>
      </MenuItem>
   );
}

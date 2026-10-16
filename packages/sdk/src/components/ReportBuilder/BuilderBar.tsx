// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import AddIcon from "@mui/icons-material/Add";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import FilterListIcon from "@mui/icons-material/FilterList";
import InsertChartOutlinedIcon from "@mui/icons-material/InsertChartOutlined";
import NotesIcon from "@mui/icons-material/Notes";
import RedoIcon from "@mui/icons-material/Redo";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Box,
   Button,
   Divider,
   IconButton,
   Menu,
   MenuItem,
   Stack,
   Tooltip,
   useMediaQuery,
} from "@mui/material";
import { useState, type ReactNode } from "react";
import { MOTION_FAST } from "../../theme/motion";
import type { ReportUi } from "../../theme/ui";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor, hoverEdgeColor } from "../controlTokens";
import type { BuilderControls } from "./builderControls";
import { controlSx, dividerSx, iconControlSx } from "./BuilderToolbar";
import { SaveStatus } from "./SaveStatus";
import { MOD } from "./useBuilderShortcuts";

export interface BuilderBarProps {
   /** The builder's controls, from `onControlsChange`. Undefined while it opens: only Done is drawn. */
   controls: BuilderControls | undefined;
   /** Leave the builder for the reader. Absent, no Done is drawn. */
   onDone?: () => void;
}

/**
 * The builder's controls as one bar, for a host's header: the save status,
 * undo and redo, an Add menu, then Done and, while there
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
         className={reportClasses.builderBar}
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
               {controls.save && <SaveStatus {...controls} />}
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
                              sx={iconControlSx(theme.ui)}
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
                              sx={iconControlSx(theme.ui)}
                           >
                              <RedoIcon fontSize="small" />
                           </IconButton>
                        </span>
                     </Tooltip>
                     {canAdd && (
                        <Divider orientation="vertical" sx={dividerSx} />
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
                              slotProps={{
                                 paper: { sx: menuPaperSx(edge, theme.ui) },
                              }}
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
                     sx={{ ...controlSx(edge, hoverEdge, theme.ui), width: 92 }}
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
                     sx={{
                        ...controlSx(edge, hoverEdge, theme.ui),
                        minWidth: 72,
                     }}
                  >
                     {controls.status === "saving" ? "Saving…" : "Save"}
                  </Button>
               </span>
            </Tooltip>
         )}
      </Stack>
   );
}

/** The builder's menus: a card set like its windows, rounded and bordered. */
const menuPaperSx = (edge: string, ui: ReportUi) => ({
   minWidth: 260,
   mt: 0.5,
   borderRadius: ui.radius.menu,
   border: `1px solid ${edge}`,
   boxShadow: ui.shadow.menu,
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
   const ui = theme.ui;
   return (
      <MenuItem
         onClick={onClick}
         disabled={disabled}
         sx={(mui) => ({
            gap: 1.5,
            px: 1.25,
            py: 1,
            borderRadius: ui.radius.control,
            alignItems: "center",
            "& .add-icon": {
               width: 32,
               height: 32,
               flex: "none",
               borderRadius: ui.radius.tile,
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
            <Box
               sx={{
                  ...ui.type.control,
                  fontWeight: ui.type.label.fontWeight,
                  lineHeight: 1.3,
               }}
            >
               {title}
            </Box>
            <Box
               sx={{
                  ...ui.type.caption,
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

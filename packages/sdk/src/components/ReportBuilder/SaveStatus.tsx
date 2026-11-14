// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import CheckIcon from "@mui/icons-material/Check";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
import { Box, ButtonBase, Menu, MenuItem, Tooltip } from "@mui/material";
import type { Theme } from "@mui/material";
import { useState } from "react";
import { MOTION_FAST } from "../../theme/motion";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor } from "../controlTokens";
import type { BuilderControls } from "./builderControls";

/** What the status reads: a slice of the builder's controls. */
export type SaveStatusProps = Pick<
   BuilderControls,
   | "status"
   | "saveTarget"
   | "draftKeptAt"
   | "discard"
   | "restored"
   | "dismissRestored"
>;

/**
 * Where the edits stand: saved, saving, kept in this browser, or (with no
 * draft) unsaved. While there are edits to drop, it opens a menu offering
 * Discard edits, which is one undoable edit back to the saved report.
 *
 * Edits put back from this browser on opening are said here, not in a banner
 * over the page: the dot takes the accent, and the menu's first line says when
 * the edits are from, until the menu has been opened once or the author edits
 * again.
 */
export function SaveStatus(props: SaveStatusProps) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);
   const { status, draftKeptAt, discard, restored, dismissRestored } = props;
   // A stale draft is the page's to ask about; only edits actually put back
   // are news for the status.
   const news =
      restored !== undefined && !restored.pending && status === "unsaved"
         ? restored
         : undefined;
   const words =
      status === "saved"
         ? "Saved"
         : status === "saving"
           ? "Saving…"
           : draftKeptAt !== undefined
             ? "Draft saved"
             : "Unsaved changes";
   const restoredLine = news
      ? `Restored your unsaved edits from ${clockTime(news.savedAt)}`
      : undefined;
   const hint =
      status === "unsaved" && draftKeptAt !== undefined
         ? "Your edits are kept in this browser. Done saves them to the report."
         : props.saveTarget;
   // One element for every state, set the same way: the same type and
   // padding, an icon slot of one size (a dot, or the check), and the
   // chevron's room kept whether or not the menu is offered. Each state used
   // to be drawn differently (a menu button for a draft, bare text otherwise),
   // so Draft saved → Saving… → Saved changed font, padding and width at every
   // step, and the controls beside it jumped. A minimum width, with the words
   // set against the undo buttons, keeps them still.
   const menu = discard !== undefined && status === "unsaved";
   const close = () => {
      setAnchor(null);
      // Seen: the menu said it once, and the status goes back to plain.
      if (news) dismissRestored?.();
   };
   const sx = (mui: Theme) => ({
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "flex-end",
      gap: 0.75,
      minWidth: 132,
      height: ui.size.control,
      px: 1,
      borderRadius: ui.radius.control,
      font: "inherit",
      ...ui.type.control,
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
                  data-restored={news ? "true" : undefined}
                  sx={(mui) => ({
                     width: 7,
                     height: 7,
                     borderRadius: "50%",
                     bgcolor: news
                        ? theme.accent
                        : status === "saving" || draftKeptAt !== undefined
                          ? mui.palette.text.disabled
                          : mui.palette.warning.main,
                     transition: `background-color ${MOTION_FAST}`,
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
         {restoredLine ? `${words}. ${restoredLine}.` : words}
      </Box>
   );
   return (
      <>
         {announce}
         <Tooltip title={restoredLine ?? hint} describeChild>
            {menu ? (
               <ButtonBase
                  className={reportClasses.saveStatus}
                  aria-label={words}
                  {...(restoredLine
                     ? { "aria-description": restoredLine }
                     : {})}
                  aria-haspopup="menu"
                  aria-expanded={anchor !== null}
                  onClick={(event) => setAnchor(event.currentTarget)}
                  sx={sx}
               >
                  {inner}
               </ButtonBase>
            ) : (
               <Box
                  component="span"
                  className={reportClasses.saveStatus}
                  sx={sx}
               >
                  {inner}
               </Box>
            )}
         </Tooltip>
         <Menu
            className={reportClasses.menu}
            anchorEl={anchor}
            open={anchor !== null}
            onClose={close}
            anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
            transformOrigin={{ vertical: -4, horizontal: "left" }}
            slotProps={{
               paper: {
                  sx: {
                     minWidth: 240,
                     borderRadius: ui.radius.menu,
                     border: `1px solid ${edgeColor(theme.cardBorder)}`,
                     boxShadow: ui.shadow.menu,
                  },
               },
            }}
            MenuListProps={{ dense: true, sx: { py: 0.5 } }}
         >
            {(restoredLine !== undefined || draftKeptAt !== undefined) && (
               <Box
                  component="li"
                  role="presentation"
                  sx={{
                     px: 1.75,
                     pt: 0.75,
                     pb: 1,
                     ...ui.type.caption,
                     color: "text.secondary",
                  }}
               >
                  {restoredLine ??
                     `Kept in this browser at ${clockTime(draftKeptAt!)}`}
               </Box>
            )}
            <MenuItem
               onClick={() => {
                  setAnchor(null);
                  discard?.();
               }}
               sx={{
                  mx: 0.5,
                  borderRadius: ui.radius.control,
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

/** A time of day, or a date and time when not today: when a draft was kept. */
export function clockTime(at: number): string {
   const when = new Date(at);
   const today = new Date().toDateString() === when.toDateString();
   return today
      ? when.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })
      : when.toLocaleString([], {
           month: "short",
           day: "numeric",
           hour: "numeric",
           minute: "2-digit",
        });
}

/** In the accessibility tree, not on screen: the status line's announcement. */
const VISUALLY_HIDDEN = {
   position: "absolute",
   // Strings, not numbers: in `sx` a width or height of 1 is 100%, and a
   // margin of -1 is one spacing unit. As numbers, this "hidden" span was as
   // wide as the host's header and scrolled its panel sideways.
   width: "1px",
   height: "1px",
   margin: "-1px",
   padding: 0,
   overflow: "hidden",
   clip: "rect(0 0 0 0)",
   whiteSpace: "nowrap",
   border: 0,
} as const;

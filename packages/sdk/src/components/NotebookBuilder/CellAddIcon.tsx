// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import NoteAddIcon from "@mui/icons-material/NoteAdd";
import PlaylistAddIcon from "@mui/icons-material/PlaylistAdd";
import { Box } from "@mui/material";

/** The add-text and add-query icons, with an arrow to the side of the cell they add on, so the four buttons tell apart at a glance. */
export function CellAddIcon({
   kind,
   side,
}: {
   kind: "text" | "query";
   side: "above" | "below";
}) {
   const Base = kind === "text" ? NoteAddIcon : PlaylistAddIcon;
   const Arrow = side === "above" ? KeyboardArrowUpIcon : KeyboardArrowDownIcon;
   return (
      <Box
         component="span"
         aria-hidden
         sx={{
            display: "inline-flex",
            flexDirection: side === "above" ? "column-reverse" : "column",
            alignItems: "center",
            lineHeight: 0,
         }}
      >
         <Base sx={{ fontSize: 18 }} />
         <Arrow
            sx={{
               fontSize: 12,
               mt: side === "below" ? -0.25 : 0,
               mb: side === "above" ? -0.25 : 0,
            }}
         />
      </Box>
   );
}

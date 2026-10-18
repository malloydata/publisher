// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DashboardOutlinedIcon from "@mui/icons-material/DashboardOutlined";
import {
   IconButton,
   ListItemText,
   Menu,
   MenuItem,
   Tooltip,
} from "@mui/material";
import { useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor } from "../controlTokens";
import { iconControlSx } from "./BuilderToolbar";

/** What can be done to a placed report's layout as a whole. */
export interface ArrangeActions {
   /** Slide every tile up into the gaps above it. Absent when there are none. */
   pack?: () => void;
   /** Take every tile's place off, so the report flows again. */
   flow: () => void;
}

/**
 * The layout of a placed grid report as a whole: closing its gaps, or letting
 * its tiles flow again. One menu shared by the builder's own toolbar and the
 * bar a host draws, so the two cannot offer different things.
 */
export function ArrangeMenu({ arrange }: { arrange: ArrangeActions }) {
   const { theme } = usePublisherTheme();
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);
   const pick = (action: (() => void) | undefined) => () => {
      setAnchor(null);
      action?.();
   };
   return (
      <>
         <Tooltip title="Arrange tiles">
            <IconButton
               aria-label="Arrange tiles"
               aria-haspopup="menu"
               aria-expanded={anchor !== null}
               onClick={(event) => setAnchor(event.currentTarget)}
               sx={iconControlSx(theme.ui)}
            >
               <DashboardOutlinedIcon fontSize="small" />
            </IconButton>
         </Tooltip>
         <Menu
            anchorEl={anchor}
            open={anchor !== null}
            onClose={() => setAnchor(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            transformOrigin={{ vertical: -6, horizontal: "right" }}
            slotProps={{
               paper: {
                  sx: {
                     minWidth: 260,
                     borderRadius: theme.ui.radius.menu,
                     border: `1px solid ${edgeColor(theme.cardBorder)}`,
                     boxShadow: theme.ui.shadow.menu,
                  },
               },
            }}
         >
            <MenuItem disabled={!arrange.pack} onClick={pick(arrange.pack)}>
               <ListItemText
                  primary="Pack tiles upward"
                  secondary={
                     arrange.pack
                        ? "Close every gap above a tile"
                        : "There are no gaps to close"
                  }
               />
            </MenuItem>
            <MenuItem onClick={pick(arrange.flow)}>
               <ListItemText
                  primary="Let tiles flow"
                  secondary="Clear each tile's place; keep its size"
               />
            </MenuItem>
         </Menu>
      </>
   );
}

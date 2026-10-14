// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Stack,
   Typography,
   type SxProps,
   type Theme,
} from "@mui/material";
import type { ReactNode } from "react";
import { MOTION_FAST, reducedMotionSx } from "../../theme/motion";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor, TYPE } from "../controlTokens";
import { TileCard } from "../Dashboard/TileCard";
import { STUCK_TOP_PX, useStuck } from "../Dashboard/useStuck";

/**
 * The parts of a report page that the reader and the builder both draw, in
 * either layout: the page's arrangement, its header, its pinned control row,
 * and a text tile's frame.
 *
 * Each surface fills the same slots with its own content (the reader with
 * text, the builder with the same text made editable in place) rather than
 * drawing a page of its own, so a change to how a report looks is made here
 * once and editing a page can never draw it differently from reading it.
 */

/**
 * The control row's stacking level: above anything a tile raises inside
 * itself as it scrolls under (a result's floating buttons sit at 1–2), and
 * below MUI's app bar, popovers and dialogs.
 */
export const STICKY_CONTROLS_Z = 4;

/** The space between the page's parts and between its tiles' rows, in theme units. */
const PAGE_GAP = 2;

/**
 * A report page, top to bottom: anything that must be said before it
 * (`notice`), the header, the control row, then the body (the tiles, and
 * whatever the surface adds under them).
 */
export function ReportPage({
   layout,
   notice,
   header,
   controls,
   children,
   sx,
}: {
   /** Written as `data-layout` on the page, so a test or a host can tell the two apart. */
   layout?: "grid" | "linear";
   notice?: ReactNode;
   header: ReactNode;
   controls?: ReactNode;
   children: ReactNode;
   sx?: SxProps<Theme>;
}) {
   return (
      <Stack
         data-layout={layout}
         sx={[{ gap: PAGE_GAP }, ...(Array.isArray(sx) ? sx : [sx])]}
      >
         {notice}
         {header}
         {controls}
         {children}
      </Stack>
   );
}

/**
 * A report's title, with the surface's actions on its line (the builder's
 * toolbar), and its description beneath. The title and description are slots:
 * text in the reader, the same text editable in place in the builder.
 */
export function ReportHeader({
   title,
   description,
   actions,
}: {
   title: ReactNode;
   description?: ReactNode;
   actions?: ReactNode;
}) {
   return (
      <Stack sx={{ gap: 1.5 }}>
         <Stack direction="row" sx={{ alignItems: "center", gap: 2 }}>
            <Typography
               component="h1"
               sx={{ ...TYPE.pageTitle, m: 0, flex: 1, minWidth: 0 }}
            >
               {title}
            </Typography>
            {/* Overhangs the title's line rather than heightening it, so the
             title sits at the same height with or without actions. */}
            {actions && <Box sx={{ my: "-4px" }}>{actions}</Box>}
         </Stack>
         {description}
      </Stack>
   );
}

/**
 * The frame a text tile (and the page description) is drawn in: none. Prose
 * is part of the page, not a result, so it never takes a card, in either
 * layout. One definition, so every text block on every surface agrees.
 */
export function TextTileFrame({
   sx,
   children,
}: {
   sx?: SxProps<Theme>;
   children: ReactNode;
}) {
   return (
      <TileCard chrome="none" kind="text" {...(sx ? { sx } : {})}>
         {children}
      </TileCard>
   );
}

/**
 * The control row, pinned to the top of the scroller so the controls stay in
 * reach while the tiles scroll under, with a hairline under it only while
 * pinned: tiles passing beneath read as going under a bar rather than being
 * cut off by nothing.
 */
export function StickyControls({ children }: { children: ReactNode }) {
   const { theme } = usePublisherTheme();
   const { stickyRef, stuck } = useStuck();
   return (
      <Box
         ref={stickyRef}
         data-stuck={stuck || undefined}
         sx={{
            position: "sticky",
            top: `${STUCK_TOP_PX}px`,
            zIndex: STICKY_CONTROLS_Z,
            bgcolor: "background.default",
            // Room under the bar for the edge, so the line never sits on the
            // controls themselves.
            pb: 1,
            // And room over them, for what stands above a control (a filter's
            // × in the builder): pinned, the bar's top is the scroller's edge,
            // and the badge was cut off there. Taken back by the margin, so the
            // row sits where it did while unpinned.
            pt: 1,
            mt: -1,
            // A shadow rather than a border: drawing the edge must not change
            // the bar's height, or the page jumps as it pins.
            boxShadow: stuck
               ? `0 1px 0 ${edgeColor(theme.cardBorder)}`
               : "none",
            transition: `box-shadow ${MOTION_FAST}`,
            ...reducedMotionSx,
         }}
      >
         {children}
      </Box>
   );
}

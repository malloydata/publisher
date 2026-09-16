// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Stack } from "@mui/material";
import type { ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";

/**
 * The bar above a dashboard, in the one shape both the reader's view and the
 * builder use.
 *
 * Reading and editing are the same page in two states, so the bar that carries
 * the switch is the same bar: same height, same ground, same edge, and the
 * same left and right margins as the dashboard under it — the state on the
 * left, the way out of it on the right. The fixed height is the load-bearing
 * part: it is what makes switching modes move nothing on the page.
 *
 * No horizontal padding of its own: it is laid out inside whatever insets the
 * dashboard, so the left item lines up with the title below it and the right
 * item with the right edge of the grid.
 */
export function DashboardBar({
   left,
   children,
}: {
   /** The page's state, at the left edge: a chip, or nothing while reading. */
   left?: ReactNode;
   /** What can be done about it, at the right edge. */
   children?: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <Stack
         direction="row"
         sx={{
            position: "sticky",
            top: 0,
            zIndex: 5,
            alignItems: "center",
            justifyContent: "space-between",
            gap: 1,
            // One height whether or not the bar has anything on the left, so
            // switching modes moves nothing.
            minHeight: 48,
            py: 0.5,
            // The rule is the bar's own edge, not the page's underline: the
            // title below needs room to read as a heading rather than as the
            // bar's caption.
            mb: 3,
            // Its own ground, so the tiles scrolling under it do not show
            // through, and an edge so it reads as a bar rather than a row. The
            // Publisher theme's ground and edge, like every other surface here
            // — MUI's own palette would not follow an instance theme or its
            // dark mode.
            bgcolor: theme.background,
            borderBottom: theme.border,
         }}
      >
         <Stack
            direction="row"
            sx={{
               alignItems: "center",
               gap: 1,
               // The side that gives way: without a zero min-width a hint that
               // will not wrap makes the whole bar wider than the page, and the
               // controls on the right end up past the fold.
               minWidth: 0,
               overflow: "hidden",
            }}
         >
            {left}
         </Stack>
         <Stack
            direction="row"
            sx={{
               alignItems: "center",
               gap: 0.5,
               // A button's label never wraps onto a second line; anything
               // beside them gives way first.
               flexShrink: 0,
               "& .MuiButton-root": { whiteSpace: "nowrap" },
            }}
         >
            {children}
         </Stack>
      </Stack>
   );
}

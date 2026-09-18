// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Paper,
   Typography,
   type SxProps,
   type Theme,
} from "@mui/material";
import type { ReactNode } from "react";
import { DASHBOARD_CARD_PADDING_PX } from "../../theme/buildTableCssVars";
import { usePublisherTheme } from "../../theme/ThemeContext";

/**
 * The card a dashboard tile sits in, and nothing else — so the builder, which
 * draws a stand-in tile when it has no server to run one, draws the same card
 * the reader does rather than restating its geometry.
 *
 * The instance theme's `cardBorder`, not MUI's `divider` and not the `border`
 * a table's gridlines take: the renderer card's edge is this same value, and a
 * card that agrees with the theme everywhere except its outline still reads as
 * a different card. Both surfaces moved together when the edge was darkened;
 * either one left behind is a dashboard whose tiles are outlined two ways.
 *
 * Radius stays on the host's
 * `shape.borderRadius`, which the renderer card is pointed at too. The
 * background is `theme.tile`, the value the renderer card paints — left unset,
 * on a theme whose page is also white the composite tiles lost the tint that
 * separates a card from the page while the single-query form kept it.
 *
 * `borderless` is `# borderless` on the view: the result with no card, which
 * the renderer honours by dropping background, border, radius and most padding
 * on its own `.dashboard-item`. Same here, so the tag reads the same on both
 * forms.
 *
 * `overflow: hidden` and `minWidth: 0` are load-bearing for RESIZING, not
 * tidiness: this card is a grid item, and a grid item that clips gets a
 * minimum width of zero instead of its content's, which is what lets it
 * narrow below the chart it holds so the renderer redraws the chart to fit.
 * See the `minWidth: 0` note in `DashboardGrid` for the rest of that chain.
 */
export function TileCard({
   borderless = false,
   sx,
   children,
}: {
   borderless?: boolean;
   sx?: SxProps<Theme>;
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <Paper
         elevation={0}
         sx={[
            {
               border: borderless ? "none" : theme.cardBorder,
               borderRadius: borderless ? 0 : 1,
               background: borderless ? "none" : theme.tile,
               overflow: "hidden",
               minWidth: 0,
               minHeight: 120,
               p: borderless ? "12px 0" : `${DASHBOARD_CARD_PADDING_PX}px`,
            },
            ...(Array.isArray(sx) ? sx : [sx]),
         ]}
      >
         {children}
      </Paper>
   );
}

/**
 * A tile's heading: its `# label`, and its `# subtitle` under it. In the
 * instance theme's title colour and face, so a tile is titled the same
 * whichever surface draws it.
 */
export function TileHeading({
   title,
   subtitle,
   tooltip,
   action,
}: {
   title: string;
   subtitle?: string;
   /** What actually ran, kept reachable without being the heading. */
   tooltip?: string;
   /** A control on the heading's row, hard right — the tile's Explore button. */
   action?: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <Box sx={{ pb: 1.5, display: "flex", alignItems: "flex-start", gap: 1 }}>
         <Box sx={{ minWidth: 0, flex: 1 }}>
            <Typography
               variant="subtitle2"
               sx={{
                  fontWeight: 500,
                  color: theme.tileTitle,
                  fontFamily: theme.font.family,
               }}
               title={tooltip}
            >
               {title}
            </Typography>
            {subtitle !== undefined && (
               <Typography
                  variant="caption"
                  sx={{
                     display: "block",
                     color: theme.tileTitle,
                     fontFamily: theme.font.family,
                     opacity: 0.8,
                  }}
               >
                  {subtitle}
               </Typography>
            )}
         </Box>
         {action}
      </Box>
   );
}

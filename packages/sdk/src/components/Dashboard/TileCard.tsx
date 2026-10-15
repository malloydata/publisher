// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import {
   Box,
   Paper,
   Typography,
   type SxProps,
   type Theme,
} from "@mui/material";
import type { ReactNode, Ref } from "react";
import { DASHBOARD_CARD_PADDING_PX } from "../../theme/buildTableCssVars";
import { usePublisherTheme } from "../../theme/ThemeContext";

/** `card` is a dashboard tile's panel; `none` is a notebook's bare flow. */
export type TileChrome = "card" | "none";

/**
 * The least a carded tile is drawn at, by kind: a query tile holds a spinner
 * and then a result, so it keeps room for one; a text tile holds a line or two
 * of prose and is no taller than its text needs, past a floor that keeps a
 * one-word tile from reading as a sliver. A bare (`none`) tile has no floor.
 */
export const TILE_MIN_HEIGHT = { query: 120, text: 72 } as const;

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
 * Radius is the report surfaces' tile radius ({@link ui.radius.tile}), which the
 * renderer card is pointed at too, rather than the host's `shape.borderRadius`:
 * a host's control radius is not a card radius, and taking it made tiles as
 * round as buttons. The
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
   chrome = "card",
   kind = "query",
   sx,
   cardRef,
   children,
}: {
   borderless?: boolean;
   /** What the tile holds, which sets its {@link TILE_MIN_HEIGHT}. */
   kind?: keyof typeof TILE_MIN_HEIGHT;
   /** `none` drops the card entirely, for a document that reads top to bottom. */
   chrome?: TileChrome;
   sx?: SxProps<Theme>;
   /** The card's element, for a caller that watches where it is on the page. */
   cardRef?: Ref<HTMLDivElement>;
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const bare = borderless || chrome === "none";
   return (
      <Paper
         ref={cardRef}
         className={reportClasses.tile}
         elevation={0}
         data-chrome={chrome}
         // What it holds, for a host waiting on the query tiles to draw.
         data-tile-kind={kind}
         sx={[
            {
               border: bare ? "none" : theme.cardBorder,
               borderRadius: bare ? 0 : ui.radius.tile,
               background: bare ? "none" : theme.tile,
               overflow: "hidden",
               // Its own stacking context, so nothing inside a tile is drawn
               // over the page around it. A table's pinned header row is
               // `z-index: 100` in the renderer's CSS; without this it painted
               // over the sticky control row (z 4) as the tile scrolled under
               // it. Popups (tooltips, menus) are portalled out and unaffected.
               isolation: "isolate",
               // A column so a tile's result can take the room under its heading.
               display: "flex",
               flexDirection: "column",
               minWidth: 0,
               minHeight: chrome === "none" ? 0 : TILE_MIN_HEIGHT[kind],
               p:
                  chrome === "none"
                     ? 0
                     : borderless
                       ? "12px 0"
                       : `${DASHBOARD_CARD_PADDING_PX}px`,
            },
            ...(Array.isArray(sx) ? sx : [sx]),
         ]}
      >
         {children}
      </Paper>
   );
}

/**
 * The class on a tile heading's root, so a surface that draws controls over
 * the heading (the builder's tile menu) can leave room for them.
 */
export const TILE_HEADING_CLASS = reportClasses.tileHeading;

/** The heading's text as nodes, for a surface that makes it editable in place. */
export interface TileHeadingSlots {
   title: ReactNode;
   subtitle: ReactNode;
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
   quiet = false,
}: {
   /** A tighter gap under the heading, for a tile with no card around it. */
   quiet?: boolean;
   title: ReactNode;
   subtitle?: ReactNode;
   /** What actually ran, kept reachable without being the heading. */
   tooltip?: string;
   /** A control on the heading's row, hard right — the tile's Explore button. */
   action?: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   return (
      <Box
         className={TILE_HEADING_CLASS}
         sx={{
            pb: quiet ? 0.5 : 1.5,
            display: "flex",
            alignItems: "flex-start",
            gap: 1,
         }}
      >
         <Box sx={{ minWidth: 0, flex: 1 }}>
            {/* One title style in both layouts: a linear report's tile titles
             were 12px captions, smaller than the prose around them. */}
            <Typography
               component="div"
               sx={{
                  ...ui.type.tileTitle,
                  display: "block",
                  color: theme.tileTitle,
                  fontFamily: theme.font.family,
               }}
               title={tooltip}
            >
               {title}
            </Typography>
            {subtitle !== undefined && (
               <Typography
                  component="div"
                  sx={{
                     ...ui.type.caption,
                     display: "block",
                     mt: "2px",
                     color: theme.tileTitle,
                     fontFamily: theme.font.family,
                     opacity: 0.75,
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

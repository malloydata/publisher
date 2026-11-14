// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The model viewer's page column and pane geometry. */

/** The page's reading column. */
export const CONTENT_COLUMN_SX = {
   width: "100%",
   maxWidth: "1200px",
   mx: "auto",
} as const;

/** The gap between a page's header and its first line (theme spacing units). */
export const FILE_VIEW_TOP_GAP = 3;

/** A card's inner inset (theme spacing units). */
export const PANE_INSET = 1.5;

/**
 * A read-only card: bordered, rounded, on the paper colour. Text's I-beam
 * cursor advertises an edit these cards don't offer, so the default cursor is
 * set once; anything interactive declares `pointer`.
 */
export const PANE_CARD_SX = {
   display: "flex",
   flexDirection: "column",
   minWidth: 0,
   minHeight: 0,
   border: "1px solid",
   borderColor: "divider",
   borderRadius: 2,
   bgcolor: "background.paper",
   overflow: "hidden",
   cursor: "default",
} as const;

/** The same pane with no card around it: the page's own surface. */
export const FLAT_PANE_SX = {
   ...PANE_CARD_SX,
   border: "none",
   borderRadius: 0,
   bgcolor: "transparent",
   overflow: "visible",
} as const;

/** A pane's body, under its header. */
export const PANE_BODY_SX = {
   flex: 1,
   minHeight: 0,
   py: 1,
} as const;

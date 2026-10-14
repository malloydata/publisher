// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The measurements every report surface draws its controls and tiles with:
 * one control height, three radii, a short type scale.
 *
 * Literal values rather than the host theme's `shape.borderRadius` and button
 * sizes, because the SDK is drawn inside other hosts' themes: a builder whose
 * buttons took the host's pill radius and 37px height beside its own 6px,
 * 28px chips is what made one toolbar row show five shapes. Colour still
 * comes from the host (palette) and the instance theme (accent, card edge).
 */

/** Every button, segmented control, filter button and input. */
export const CONTROL_HEIGHT_PX = 32;
/** Filter chips: the one smaller control. */
export const CHIP_HEIGHT_PX = 24;

/** Controls and chips. */
export const CONTROL_RADIUS = "6px";
/** Tiles, menus, popovers. */
export const TILE_RADIUS = "8px";
/** Dialogs. */
export const DIALOG_RADIUS = "12px";

/** Report and builder type: a page title, a tile title, prose, captions. */
export const TYPE = {
   pageTitle: { fontSize: 24, fontWeight: 600, letterSpacing: "-0.01em" },
   tileTitle: { fontSize: 14, fontWeight: 600, lineHeight: 1.4 },
   prose: { fontSize: 15, lineHeight: 1.6 },
   control: { fontSize: 13, fontWeight: 500 },
   caption: { fontSize: 12, lineHeight: 1.4 },
} as const;

/** How wide report prose runs before it wraps: a readable line, not the grid's width. */
export const PROSE_MAX_WIDTH = "72ch";

/**
 * The colour of the instance theme's card edge (`cardBorder`, a `1px solid`
 * shorthand). Every outlined thing on a report (tiles, filter buttons, their
 * popovers) is drawn in it, so the page has one edge weight, not two.
 */
export const edgeColor = (cardBorder: string) =>
   cardBorder.replace(/^1px solid /, "");

/** The edge one step darker: what a hovered tile or filter button shows. */
export const hoverEdgeColor = (cardBorder: string, ink: string) =>
   `color-mix(in srgb, ${edgeColor(cardBorder)}, ${ink} 25%)`;

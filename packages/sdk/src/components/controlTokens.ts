// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DEFAULT_UI } from "../theme/ui";

/**
 * Helpers every report surface draws its edges and prose with. The sizes,
 * radii and type are the theme's `ui` tokens (`theme/ui.ts`), one set the host
 * can override, rather than the host MUI theme's `shape.borderRadius` and
 * button sizes: a builder whose buttons took the host's pill radius and 37px
 * height beside its own 6px, 28px chips is what made one toolbar row show five
 * shapes. Colour comes from the host (palette) and the instance theme (accent,
 * card edge).
 */

/**
 * The SDK's default chrome tokens, as constants. Components read the host's
 * through the theme (`usePublisherTheme().theme.ui`), so a host's `ui` reaches
 * them; these are the defaults it overrides, kept for code written against them.
 */
/** @deprecated Read `theme.ui.size.control`. */
export const CONTROL_HEIGHT_PX = DEFAULT_UI.size.control;
/** @deprecated Read `theme.ui.size.chip`. */
export const CHIP_HEIGHT_PX = DEFAULT_UI.size.chip;
/** @deprecated Read `theme.ui.radius.control`. */
export const CONTROL_RADIUS = DEFAULT_UI.radius.control;
/** @deprecated Read `theme.ui.radius.tile`. */
export const TILE_RADIUS = DEFAULT_UI.radius.tile;
/** @deprecated Read `theme.ui.radius.dialog`. */
export const DIALOG_RADIUS = DEFAULT_UI.radius.dialog;
/** @deprecated Read `theme.ui.type`. */
export const TYPE = DEFAULT_UI.type;

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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PALETTE } from "@malloy-publisher/sdk";

/**
 * The Console's neutrals and status colours.
 *
 * Both are drawn to sit with the SDK's `PALETTE`, which supplies every colour
 * that carries meaning — content tints, surface tints, chart series, and the
 * primary. This file supplies the rest: the greys a page is built out of, and
 * the four states a message can be in.
 *
 * The greys are cool, on the same slate ramp as dark mode. They used to be warm
 * while dark mode was already slate, so switching modes shifted the temperature
 * of every border and caption on the page, and a warm grey beside a saturated
 * blue reads as a slightly dirty one.
 */
export const colors = {
   white: "#FFFFFF",
   offWhite: "#F8FAFC",
   grey: {
      light: "#E2E8F0",
      mid: "#64748B",
   },
   black: "#0F172A",

   /**
    * Status, from the palette rather than beside it: emerald, amber, red and
    * the anchor blue. `light` is each hue's tint, for an Alert's ground; `dark`
    * is a step down, for a hover or a border.
    *
    * These were muted earth tones — sage, mustard, terracotta — belonging to a
    * warm identity the rest of the app no longer has. An error has to read as
    * an error beside a saturated chart, and a desaturated brick does not.
    */
   semantic: {
      success: { main: PALETTE.emerald, light: "#D1FAE5", dark: "#047857" },
      warning: { main: PALETTE.amber, light: "#FEF3C7", dark: "#92400E" },
      error: { main: PALETTE.red, light: "#FEE2E2", dark: "#B91C1C" },
      info: { main: PALETTE.blue, light: "#DBEAFE", dark: "#1D4ED8" },
   },
} as const;

export const MONO_FONT_FAMILY =
   '"JetBrains Mono", "ui-monospace", "SFMono-Regular", "Menlo", monospace';
export const SANS_FONT_FAMILY =
   '"Inter", "Helvetica Neue", "Arial", sans-serif';

/** The slate ramp, which dark mode's surfaces are already points on. */
export const greyScale = {
   50: colors.offWhite,
   100: "#F1F5F9",
   200: colors.grey.light,
   300: "#CBD5E1",
   400: "#94A3B8",
   500: colors.grey.mid,
   600: "#475569",
   700: "#334155",
   800: "#1E293B",
   900: colors.black,
} as const;

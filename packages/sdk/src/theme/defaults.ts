// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PALETTE } from "../components/styles";
import type { Theme } from "./types";

/**
 * The categorical series, from the Console's own `PALETTE`.
 *
 * Ordered by separation rather than by hue: consecutive series are the ones a
 * reader most needs to tell apart, so the sequence alternates warm and cool
 * instead of walking the colour wheel and putting two greens side by side. The
 * first three carry most charts.
 */
const DEFAULT_SERIES = [
   PALETTE.blue,
   PALETTE.orange,
   PALETTE.emerald,
   PALETTE.pink,
   PALETTE.violet,
   PALETTE.amber,
   PALETTE.cyan,
   PALETTE.lime,
];

const DEFAULT_FONT_FAMILY =
   "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";

/**
 * Built-in theme used when nothing has been configured. Series palette
 * and font are shared across modes; only colours that should genuinely
 * change in dark mode (backgrounds, foregrounds) are per-mode.
 */
export const DEFAULT_THEME: Required<Theme> = {
   defaultMode: "light",
   allowUserToggle: true,
   palette: {
      series: DEFAULT_SERIES,
      // The chart canvas. One ground for the whole dashboard in both modes —
      // canvas, card and the panel between cards are all this value, and the
      // borders do the separating. See `tile`.
      background: {
         light: "#ffffff",
         dark: "#0f172a",
      },
      tableHeader: {
         light: "#475569",
         dark: "#cbd5e1",
      },
      // Background of the table header row, independent of the
      // tile (dashboard tile container) so the operator can theme
      // the header band on its own.
      tableHeaderBackground: {
         light: "#f8fafc",
         dark: "#1e293b",
      },
      tableBody: {
         light: "#475569",
         dark: "#e2e8f0",
      },
      // The padded container that wraps each chart / table in a dashboard.
      //
      // The SAME value as the canvas it holds and the panel it sits on, in
      // both modes: a card is separated by its border and its radius, not by
      // a wash. A tinted card reads as a recess — something switched off —
      // and stacking card, canvas and panel as three values put three greys
      // on one tile.
      //
      // Both modes are built the same way on purpose. They used to differ in
      // structure, not just in value — light separated by border, dark by
      // elevation — so a change to how a card reads had to be reasoned about
      // twice and could land correct in one mode and wrong in the other.
      tile: {
         light: "#ffffff",
         dark: "#0f172a",
      },
      tileTitle: {
         light: "#475569",
         dark: "#94a3b8",
      },
      // Saturated end of the choropleth / heatmap gradient. The renderer
      // pairs this with a near-neutral low end. Matches the first series
      // colour, so a map and a chart on one page ramp to the same hue.
      mapColor: {
         light: PALETTE.blue,
         dark: PALETTE.blue,
      },
   },
   font: {
      family: DEFAULT_FONT_FAMILY,
      size: 12,
   },
};

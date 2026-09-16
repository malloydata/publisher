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
      background: {
         light: "#ffffff",
         // Slate to match the Publisher app's sidebar surface so
         // rendered charts sit on the same elevation as the chrome
         // around them.
         dark: "#1e293b",
      },
      tableHeader: {
         light: "#5d626b",
         dark: "#cbd5e1",
      },
      // Background of the table header row, independent of the
      // tile (dashboard tile container) so the operator can theme
      // the header band on its own.
      tableHeaderBackground: {
         light: "#f5fafc",
         dark: "#1e293b",
      },
      tableBody: {
         light: "#727883",
         dark: "#e2e8f0",
      },
      // The padded container that wraps each chart / table in a
      // dashboard. Light mode: a faint tint so tiles read as recessed
      // cards on the page; dark mode: page-outer slate.
      tile: {
         light: "#f5fafc",
         dark: "#0f172a",
      },
      tileTitle: {
         light: "#5d626b",
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

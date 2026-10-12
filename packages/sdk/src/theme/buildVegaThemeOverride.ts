// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ResolvedTheme } from "./types";

/**
 * Produce a `vegaConfigOverride` callback for `new MalloyRenderer({...})`.
 * The renderer invokes the callback once per chart type and merges the
 * returned object into the Vega spec's config. We set the category
 * colour scale, the global font, the page background, and the text, axis
 * and gridline colours so chart text and axis chrome inherit the active mode.
 *
 * Text, axis and gridline values come from {@link ResolvedTheme}
 * (computed once in `resolveTheme`), so this builder no longer branches
 * on mode itself.
 *
 * The same config is returned for every chart type except `shape_map`,
 * which also moves its legend below the map.
 */
export function buildVegaThemeOverride(theme: ResolvedTheme) {
   const { foreground, axisFaint, gridline, font } = theme;

   const config: Record<string, unknown> = {
      background: theme.background,
      font: font.family,
      title: { color: foreground, font: font.family },
      axis: {
         labelColor: foreground,
         titleColor: foreground,
         domainColor: axisFaint,
         tickColor: axisFaint,
         gridColor: gridline,
         labelFont: font.family,
         titleFont: font.family,
      },
      legend: {
         labelColor: foreground,
         titleColor: foreground,
         labelFont: font.family,
         titleFont: font.family,
      },
      header: { labelColor: foreground, titleColor: foreground },
      range: { category: theme.series },
   };

   // The renderer draws a shape_map fixed-width with its legend on the right, which clips in a tile.
   const shapeMapConfig = {
      ...config,
      legend: { ...(config.legend as object), orient: "bottom" },
   };

   return (chartType: string) =>
      chartType === "shape_map" ? shapeMapConfig : config;
}

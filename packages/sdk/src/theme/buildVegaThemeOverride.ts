import type { ResolvedTheme } from "./types";

/**
 * Produce a `vegaConfigOverride` callback for `new MalloyRenderer({...})`.
 * The renderer invokes the callback once per chart type and merges the
 * returned object into the Vega spec's config. We set the category
 * colour scale, the global font, the page background, and foreground /
 * faint colours so chart text and axis chrome inherit the active mode.
 *
 * Foreground and axis-faint values come from {@link ResolvedTheme}
 * (computed once in `resolveTheme`), so this builder no longer branches
 * on mode itself.
 *
 * The same config is returned for every chart type today. The
 * parameter is reserved for future per-chart-type tweaks.
 */
export function buildVegaThemeOverride(theme: ResolvedTheme) {
   const { foreground, axisFaint, font } = theme;

   const config: Record<string, unknown> = {
      background: theme.background,
      font: font.family,
      title: { color: foreground, font: font.family },
      axis: {
         labelColor: foreground,
         titleColor: foreground,
         domainColor: axisFaint,
         tickColor: axisFaint,
         gridColor: axisFaint,
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

   return (_chartType: string) => config;
}

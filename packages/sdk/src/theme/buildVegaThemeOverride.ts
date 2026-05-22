import type { ResolvedTheme } from "./types";

/**
 * Produce a `vegaConfigOverride` callback for `new MalloyRenderer({...})`.
 * The renderer invokes the callback once per chart type and merges the
 * returned object into the Vega spec's config. We set the category color
 * scale, the global font, the page background, and the foreground text
 * color so charts inherit the same theme as the surrounding chrome.
 *
 * The same config is returned for every chart type today. The parameter
 * is reserved for future per-chart-type tweaks (e.g., dialing legend
 * placement on bar vs line).
 */
export function buildVegaThemeOverride(theme: ResolvedTheme) {
   const foreground = theme.mode === "dark" ? "#e2e8f0" : "#1f2937";
   const axisFaint = theme.mode === "dark" ? "#475569" : "#d1d5db";

   const config: Record<string, unknown> = {
      background: theme.background,
      font: theme.font.family,
      title: { color: foreground, font: theme.font.family },
      axis: {
         labelColor: foreground,
         titleColor: foreground,
         domainColor: axisFaint,
         tickColor: axisFaint,
         gridColor: axisFaint,
         labelFont: theme.font.family,
         titleFont: theme.font.family,
      },
      legend: {
         labelColor: foreground,
         titleColor: foreground,
         labelFont: theme.font.family,
         titleFont: theme.font.family,
      },
      header: { labelColor: foreground, titleColor: foreground },
      range: { category: theme.series },
   };

   return (_chartType: string) => config;
}

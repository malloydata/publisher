import type { Theme } from "../client";

export type { Theme };

export type ThemeMode = "light" | "dark";

/**
 * A Theme with every field downstream consumers need actually filled in.
 * Produced by `resolveTheme(layers, mode)`, which flattens the cascade
 * and the per-mode colour split into a single mode-agnostic shape — so
 * `buildTableCssVars`, `buildVegaThemeOverride`, and
 * `buildMalloyExplicitTheme` never have to branch on mode themselves.
 *
 * The five `palette.*` colour keys are stored per-mode on the raw Theme
 * (one of light/dark per field) and collapse to the active mode here.
 * The five "derived" fields (border, pinnedBorder, valueColor,
 * foreground, axisFaint) are computed once from the resolved mode so
 * the same hex literal isn't repeated across three builders.
 */
export interface ResolvedTheme {
   mode: ThemeMode;

   series: string[];
   font: {
      family: string;
      size: number;
   };

   background: string;
   tableHeader: string;
   tableHeaderBackground: string;
   tableBody: string;
   tile: string;
   tileTitle: string;
   /**
    * Saturated end of the gradient used by sequential color scales
    * (choropleth maps, heatmaps). The renderer pairs it with a
    * near-neutral low-end so the operator only picks the brand end.
    */
   mapColor: string;

   border: string;
   pinnedBorder: string;
   valueColor: string;
   foreground: string;
   axisFaint: string;
   /**
    * Background for the renderer's HTML chrome (the area between
    * dashboard tiles). Mode-keyed and intentionally NOT
    * operator-customizable — when an operator picks a bold accent
    * for `palette.background` (the chart canvas), the surrounding
    * panel should stay neutral so the accent reads cleanly. In dark
    * mode it paints slate so the panel doesn't sit as a stark white
    * box on the dark page chrome.
    */
   dashboardRoot: string;
   /**
    * Background for the renderer's table interior. Follows the
    * operator's `palette.background` (the chart canvas colour) so
    * tables and charts share a single "viz surface" colour. In dark
    * mode this also keeps the (light-slate) header/body text from
    * painting unreadable light-on-white against the renderer's
    * hardcoded default. Distinct from `dashboardRoot`, which is the
    * panel BETWEEN tiles and stays mode-keyed / neutral.
    */
   tableBackground: string;
}

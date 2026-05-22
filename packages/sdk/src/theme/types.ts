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
   tableBody: string;
   tile: string;
   tileTitle: string;

   border: string;
   pinnedBorder: string;
   valueColor: string;
   foreground: string;
   axisFaint: string;
}

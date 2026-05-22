import type { Theme } from "../client";

export type { Theme };

export type ThemeMode = "light" | "dark";

/**
 * A Theme with all the keys downstream consumers need actually filled in.
 * Produced by applying the cascade (defaults → instance → environment →
 * per-chart) and resolving the active mode. Keeps the wide optional
 * surface of {@link Theme} out of the renderer-integration code.
 */
export interface ResolvedTheme {
   mode: ThemeMode;
   series: string[];
   background: string;
   tableHeader: string;
   tableBody: string;
   tile: string;
   tileTitle: string;
   font: {
      family: string;
      size: number;
   };
}

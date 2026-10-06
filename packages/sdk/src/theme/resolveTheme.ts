// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { accentFor, legibleOn } from "./accent";
import { DEFAULT_THEME } from "./defaults";
import { PER_MODE_COLOR_KEYS, type PerModeColorKey } from "./keys";
import type { ResolvedTheme, Theme, ThemeMode } from "./types";

/**
 * Collapse the layered theme cascade into a {@link ResolvedTheme} that
 * downstream code can consume without null-checks or mode branches.
 * Layer order is low → high precedence; later layers overwrite earlier
 * ones for the keys they set. Per-mode colour objects merge per-mode
 * (a layer that sets only `palette.tile.dark` doesn't clobber the
 * instance-level `palette.tile.light`).
 *
 * The chrome fields on ResolvedTheme (border, cardBorder, pinnedBorder,
 * valueColor, foreground, axisFaint, gridline) are resolved once here from
 * the per-mode palette keys (border, cardBorder, value, chartText, axis,
 * gridline) so the builders that consume the theme never branch on mode.
 */
export function resolveTheme(
   layers: Array<Theme | undefined>,
   mode: ThemeMode,
): ResolvedTheme {
   const defaultPalette = DEFAULT_THEME.palette ?? {};
   const defaultFont = DEFAULT_THEME.font ?? {
      family: "sans-serif",
      size: 12,
   };

   let series: string[] = [...((defaultPalette.series as string[]) ?? [])];
   let fontFamily: string = defaultFont.family ?? "sans-serif";
   let fontSize: number = defaultFont.size ?? 12;

   const perMode: Record<PerModeColorKey, { light?: string; dark?: string }> = {
      background: { ...(defaultPalette.background ?? {}) },
      tableHeader: { ...(defaultPalette.tableHeader ?? {}) },
      tableHeaderBackground: {
         ...(defaultPalette.tableHeaderBackground ?? {}),
      },
      tableBody: { ...(defaultPalette.tableBody ?? {}) },
      tile: { ...(defaultPalette.tile ?? {}) },
      tileTitle: { ...(defaultPalette.tileTitle ?? {}) },
      mapColor: { ...(defaultPalette.mapColor ?? {}) },
      border: { ...(defaultPalette.border ?? {}) },
      cardBorder: { ...(defaultPalette.cardBorder ?? {}) },
      axis: { ...(defaultPalette.axis ?? {}) },
      gridline: { ...(defaultPalette.gridline ?? {}) },
      chartText: { ...(defaultPalette.chartText ?? {}) },
      value: { ...(defaultPalette.value ?? {}) },
   };

   // Which per-mode colours a layer set for each mode, as against the
   // defaults: a colour set for light alone is carried into dark (lifted to
   // read there) rather than dropped for the default.
   const setFor: Record<ThemeMode, Set<PerModeColorKey>> = {
      light: new Set(),
      dark: new Set(),
   };
   for (const layer of layers) {
      if (!layer) continue;
      if (Array.isArray(layer.palette?.series)) {
         series = [...(layer.palette.series as string[])];
      }
      for (const key of PER_MODE_COLOR_KEYS) {
         const override = layer.palette?.[key];
         if (override) {
            perMode[key] = { ...perMode[key], ...override };
            if (override.light !== undefined) setFor.light.add(key);
            if (override.dark !== undefined) setFor.dark.add(key);
         }
      }
      if (typeof layer.font?.family === "string") {
         fontFamily = layer.font.family;
      }
      if (typeof layer.font?.size === "number") {
         fontSize = layer.font.size;
      }
   }

   const isDark = mode === "dark";
   const pick = (key: PerModeColorKey): string =>
      perMode[key][mode] ?? (defaultPalette[key]?.[mode] as string);

   const background = pick("background");
   // The map's brand end, set for light only: lifted into dark rather than
   // swapped for the default blue, so a themed map stays the operator's hue.
   const mapColor =
      isDark && setFor.light.has("mapColor") && !setFor.dark.has("mapColor")
         ? legibleOn(perMode.mapColor.light as string, background)
         : pick("mapColor");
   return {
      mode,
      // One series list for both modes, picked against a light page: in dark,
      // a colour too dark to see on the canvas is lifted until it reads.
      series: isDark ? series.map((c) => legibleOn(c, background)) : series,
      ...accentFor(series[0], mode, background),
      font: { family: fontFamily, size: fontSize },
      background,
      tableHeader: pick("tableHeader"),
      tableHeaderBackground: pick("tableHeaderBackground"),
      tableBody: pick("tableBody"),
      tile: pick("tile"),
      tileTitle: pick("tileTitle"),
      mapColor,
      // Table interior follows the operator's chart background so
      // tables and chart canvases share a single "viz surface" colour.
      tableBackground: background,
      // Chrome colours. Each is a per-mode palette key (defaults in
      // DEFAULT_THEME); unlike mapColor, a value set only for light is not
      // carried into dark: dark falls back to its own default, because a
      // light-mode rule or text colour rarely reads on the dark ground.
      border: `1px solid ${pick("border")}`,
      // A card's edge, one stop darker than a table's gridline on the same
      // slate ramp. See `cardBorder` on ResolvedTheme for why the two are not
      // the same value.
      cardBorder: `1px solid ${pick("cardBorder")}`,
      // The pinned table header's rule is the card edge's colour.
      pinnedBorder: `1px solid ${pick("cardBorder")}`,
      valueColor: pick("value"),
      foreground: pick("chartText"),
      axisFaint: pick("axis"),
      gridline: pick("gridline"),
      shadow: isDark
         ? {
              lift: "0 2px 12px rgba(0, 0, 0, 0.55), 0 0 0 1px rgba(255, 255, 255, 0.08)",
              drag: "0 12px 32px rgba(0, 0, 0, 0.7), 0 0 0 1px rgba(255, 255, 255, 0.12)",
           }
         : {
              lift: "0 2px 10px rgba(0, 0, 0, 0.10)",
              drag: "0 12px 32px rgba(0, 0, 0, 0.22)",
           },
      // Dashboard panel background (the area BETWEEN tiles). The page's own
      // ground in both modes, so the panel, the cards on it and the canvases
      // inside them are one surface that borders divide up — see
      // `palette.tile`.
      //
      // Still NOT tied to `palette.background`, which an operator may set to
      // a bold accent for the chart canvas. The panel stays the neutral it is
      // here so that accent cannot bleed into the surrounding chrome.
      dashboardRoot: isDark ? "#0f172a" : "#ffffff",
      // Drill link hover: the palette's anchor blue, so a drill reads as the
      // same affordance as every other primary action; dark lightens it for
      // contrast on the slate panel.
      // The Console's default accent, not the palette's: a drill reads as a
      // link whatever the operator picked for the data.
      drillLink: accentFor(undefined, mode).accent,
   };
}

/**
 * Pick the active mode for a viewer given the operator's `defaultMode`,
 * any persisted viewer choice, and a snapshot of the OS dark-mode
 * preference. "auto" is resolved here; callers see only "light" or
 * "dark".
 */
export function resolveMode(
   defaultMode: Theme["defaultMode"] | undefined,
   userChoice: ThemeMode | "auto" | undefined,
   prefersDark: boolean,
): ThemeMode {
   const effective = userChoice ?? defaultMode ?? "light";
   if (effective === "auto") return prefersDark ? "dark" : "light";
   return effective;
}

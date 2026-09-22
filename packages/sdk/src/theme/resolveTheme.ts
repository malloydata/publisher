// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PALETTE } from "../components/styles";
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
 * The derived fields on ResolvedTheme (border, cardBorder, pinnedBorder,
 * valueColor, foreground, axisFaint) are computed once here from the
 * active mode so the three builders that consume the theme stop
 * recomputing them with duplicated hex literals.
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
   return {
      mode,
      series,
      font: { family: fontFamily, size: fontSize },
      background,
      tableHeader: pick("tableHeader"),
      tableHeaderBackground: pick("tableHeaderBackground"),
      tableBody: pick("tableBody"),
      tile: pick("tile"),
      tileTitle: pick("tileTitle"),
      mapColor: pick("mapColor"),
      // Table interior follows the operator's chart background so
      // tables and chart canvases share a single "viz surface" colour.
      tableBackground: background,
      // Derived, mode-keyed defaults. Operators don't edit these in
      // v1; they're consistent borders / readable foreground text for
      // each mode. If a user later asks to customise them, expose them
      // on the schema and the editor and replace the literals below.
      border: isDark ? "1px solid #334155" : "1px solid #e2e8f0",
      // A card's edge, one stop darker than a table's gridline on the same
      // slate ramp. See `cardBorder` on ResolvedTheme for why the two are not
      // the same value.
      cardBorder: isDark ? "1px solid #475569" : "1px solid #cbd5e1",
      // Slate, not the teal-cast `#daedf3` this was: a pinned table header
      // outlined in a hue no longer anywhere else on the page.
      pinnedBorder: isDark ? "1px solid #475569" : "1px solid #cbd5e1",
      valueColor: isDark ? "#f1f5f9" : "#0f172a",
      foreground: isDark ? "#e2e8f0" : "#0f172a",
      axisFaint: isDark ? "#475569" : "#cbd5e1",
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
      drillLink: isDark ? "#60a5fa" : PALETTE.blue,
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

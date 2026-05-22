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
 * The derived fields on ResolvedTheme (border, pinnedBorder,
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
      tableBody: { ...(defaultPalette.tableBody ?? {}) },
      tile: { ...(defaultPalette.tile ?? {}) },
      tileTitle: { ...(defaultPalette.tileTitle ?? {}) },
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

   return {
      mode,
      series,
      font: { family: fontFamily, size: fontSize },
      background: pick("background"),
      tableHeader: pick("tableHeader"),
      tableBody: pick("tableBody"),
      tile: pick("tile"),
      tileTitle: pick("tileTitle"),
      // Derived, mode-keyed defaults. Operators don't edit these in
      // v1; they're consistent borders / readable foreground text for
      // each mode. If a user later asks to customise them, expose them
      // on the schema and the editor and replace the literals below.
      border: isDark ? "1px solid #334155" : "1px solid #e5e7eb",
      pinnedBorder: isDark ? "1px solid #475569" : "1px solid #daedf3",
      valueColor: isDark ? "#f1f5f9" : "#1f2937",
      foreground: isDark ? "#e2e8f0" : "#1f2937",
      axisFaint: isDark ? "#475569" : "#d1d5db",
      // Dashboard panel background. Light keeps white so the page stays
      // visually unchanged. Dark uses slate so the panel doesn't read as
      // a bright box against the dark page chrome.
      dashboardRoot: isDark ? "#1e293b" : "#ffffff",
      // Table interior background. Same pattern as dashboardRoot: light
      // keeps white (no regression), dark goes slate so header / body
      // text don't paint light-on-white in dark mode.
      tableBackground: isDark ? "#1e293b" : "#ffffff",
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

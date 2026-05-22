import { DEFAULT_THEME } from "./defaults";
import type { ResolvedTheme, Theme, ThemeMode } from "./types";

/**
 * Collapse the layered theme cascade into a {@link ResolvedTheme} that
 * downstream code can consume without null-checks. Order is low → high
 * precedence: built-in defaults, then each layer overrides the keys it
 * sets.
 *
 * The cascade is shallow-deep: top-level theme keys merge per-key, and
 * the `palette.background` / `palette.tableHeader` sub-objects merge
 * per-mode (light/dark).
 */
export function resolveTheme(
   layers: Array<Theme | undefined>,
   mode: ThemeMode,
): ResolvedTheme {
   const merged: Required<Theme> = {
      defaultMode: DEFAULT_THEME.defaultMode,
      allowUserToggle: DEFAULT_THEME.allowUserToggle,
      palette: {
         series: { ...(DEFAULT_THEME.palette?.series ?? {}) },
         background: { ...(DEFAULT_THEME.palette?.background ?? {}) },
         tableHeader: { ...(DEFAULT_THEME.palette?.tableHeader ?? {}) },
         tableBody: { ...(DEFAULT_THEME.palette?.tableBody ?? {}) },
         tile: { ...(DEFAULT_THEME.palette?.tile ?? {}) },
         tileTitle: { ...(DEFAULT_THEME.palette?.tileTitle ?? {}) },
      },
      font: {
         family: { ...(DEFAULT_THEME.font?.family ?? {}) },
         size: { ...(DEFAULT_THEME.font?.size ?? {}) },
      },
   };

   // Per-mode keys all share the same merge shape: per-mode {light,dark}
   // overlay. Iterate so adding a new key only requires touching the list.
   const perModeKeys = [
      "background",
      "tableHeader",
      "tableBody",
      "tile",
      "tileTitle",
   ] as const;

   for (const layer of layers) {
      if (!layer) continue;
      if (layer.defaultMode) merged.defaultMode = layer.defaultMode;
      if (typeof layer.allowUserToggle === "boolean") {
         merged.allowUserToggle = layer.allowUserToggle;
      }
      if (layer.palette?.series) {
         merged.palette!.series = {
            ...(merged.palette!.series ?? {}),
            ...layer.palette.series,
         };
      }
      for (const key of perModeKeys) {
         const override = layer.palette?.[key];
         if (override) {
            merged.palette![key] = {
               ...(merged.palette![key] ?? {}),
               ...override,
            };
         }
      }
      if (layer.font?.family) {
         merged.font!.family = {
            ...(merged.font!.family ?? {}),
            ...layer.font.family,
         };
      }
      if (layer.font?.size) {
         merged.font!.size = {
            ...(merged.font!.size ?? {}),
            ...layer.font.size,
         };
      }
   }

   // Pick the active mode's value for each per-mode key, falling back to
   // DEFAULT_THEME when both the merged layers and the defaults somehow
   // disagree (shouldn't happen, but the type still admits undefined).
   const pickPerMode = (key: (typeof perModeKeys)[number]): string => {
      const fromLayers = merged.palette![key]?.[mode];
      if (typeof fromLayers === "string") return fromLayers;
      return DEFAULT_THEME.palette![key]![mode]!;
   };

   const series =
      merged.palette!.series?.[mode] ?? DEFAULT_THEME.palette!.series![mode]!;
   const family =
      merged.font!.family?.[mode] ?? DEFAULT_THEME.font!.family![mode]!;
   const size = merged.font!.size?.[mode] ?? DEFAULT_THEME.font!.size![mode]!;

   return {
      mode,
      series: [...series],
      background: pickPerMode("background"),
      tableHeader: pickPerMode("tableHeader"),
      tableBody: pickPerMode("tableBody"),
      tile: pickPerMode("tile"),
      tileTitle: pickPerMode("tileTitle"),
      font: { family, size },
   };
}

/**
 * Pick the active mode for a viewer given the operator's `defaultMode`
 * preference, any persisted choice the viewer has made (typically read
 * from localStorage by the caller), and a snapshot of the OS dark-mode
 * preference. Returns one of `"light" | "dark"` — `"auto"` is resolved
 * here, not propagated.
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

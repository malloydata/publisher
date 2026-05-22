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
         series: [...(DEFAULT_THEME.palette?.series ?? [])],
         background: { ...(DEFAULT_THEME.palette?.background ?? {}) },
         tableHeader: { ...(DEFAULT_THEME.palette?.tableHeader ?? {}) },
      },
      font: { ...DEFAULT_THEME.font },
   };

   for (const layer of layers) {
      if (!layer) continue;
      if (layer.defaultMode) merged.defaultMode = layer.defaultMode;
      if (typeof layer.allowUserToggle === "boolean") {
         merged.allowUserToggle = layer.allowUserToggle;
      }
      if (layer.palette?.series) {
         // An explicit empty array clears the cascade rather than falling
         // through to defaults; operators may legitimately want no series
         // palette (e.g., to force the renderer's own monochrome fallback).
         merged.palette!.series = [...layer.palette.series];
      }
      if (layer.palette?.background) {
         merged.palette!.background = {
            ...merged.palette!.background,
            ...layer.palette.background,
         };
      }
      if (layer.palette?.tableHeader) {
         merged.palette!.tableHeader = {
            ...merged.palette!.tableHeader,
            ...layer.palette.tableHeader,
         };
      }
      if (layer.font?.family) merged.font.family = layer.font.family;
      if (typeof layer.font?.size === "number") {
         merged.font.size = layer.font.size;
      }
   }

   const background =
      merged.palette!.background?.[mode] ??
      (mode === "dark"
         ? DEFAULT_THEME.palette!.background!.dark!
         : DEFAULT_THEME.palette!.background!.light!);
   const tableHeader =
      merged.palette!.tableHeader?.[mode] ??
      (mode === "dark"
         ? DEFAULT_THEME.palette!.tableHeader!.dark!
         : DEFAULT_THEME.palette!.tableHeader!.light!);

   return {
      mode,
      series: merged.palette!.series ?? [],
      background,
      tableHeader,
      font: {
         family: merged.font.family ?? DEFAULT_THEME.font.family,
         size: merged.font.size ?? DEFAULT_THEME.font.size,
      },
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

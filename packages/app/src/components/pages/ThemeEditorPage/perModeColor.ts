// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DEFAULT_THEME,
   type PerModeColorKey,
   type Theme,
   type ThemeMode,
} from "@malloy-publisher/sdk";

/**
 * The colour a per-mode picker shows: the saved theme's value for the active
 * mode if set, otherwise the SDK default for that mode, so the picker shows
 * the colour that will actually render rather than an empty input.
 */
export function perModeColor(
   theme: Theme,
   key: PerModeColorKey,
   mode: ThemeMode,
): string {
   const fromTheme = theme.palette?.[key]?.[mode];
   if (typeof fromTheme === "string") return fromTheme;
   return DEFAULT_THEME.palette?.[key]?.[mode] ?? "";
}

/**
 * The theme with one per-mode colour set for the active mode, the other
 * mode's value kept.
 *
 * Guards the spread against legacy non-object shapes on disk: a
 * pre-per-mode Publisher persisted these slots as bare strings, and
 * spreading a string in object position produces character-indexed
 * garbage like {0:'#',1:'2',...,light:hex}. Only spread when the existing
 * value is already a {light,dark}-shaped object, otherwise start fresh.
 */
export function withPerModeColor(
   theme: Theme,
   key: PerModeColorKey,
   mode: ThemeMode,
   hex: string,
): Theme {
   const existing = theme.palette?.[key];
   const base =
      existing && typeof existing === "object" && !Array.isArray(existing)
         ? existing
         : {};
   return {
      ...theme,
      palette: {
         ...theme.palette,
         [key]: { ...base, [mode]: hex },
      },
   };
}

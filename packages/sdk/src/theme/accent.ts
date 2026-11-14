// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PALETTE } from "../components/styles";
import type { ThemeMode } from "./types";

/** The Console's accent, its hover state, and the label colour that sits on it. */
export interface Accent {
   accent: string;
   accentHover: string;
   accentContrast: string;
}

/**
 * The defaults, kept to the value: an unthemed Console looks exactly as it did
 * before the accent followed the palette. Dark inverts the pair rather than
 * shifting the blue — a bright fill with a near-black label reads at 7:1 on a
 * slate page, where a mid blue with white text does not reach 4.5:1.
 *
 * The one definition: the Console's MUI theme and the drill-link colour read
 * these through {@link accentFor} rather than keeping copies.
 */
const DEFAULT_ACCENT: Record<ThemeMode, Accent> = {
   light: {
      accent: PALETTE.blue,
      accentHover: "#1d4ed8",
      accentContrast: "#ffffff",
   },
   dark: {
      accent: "#60a5fa",
      accentHover: "#93c5fd",
      accentContrast: "#0f172a",
   },
};

/** The page each mode's accent is drawn on, when the caller does not say. */
const DEFAULT_GROUND: Record<ThemeMode, string> = {
   light: "#ffffff",
   dark: "#0f172a",
};

/**
 * WCAG's minimum for a graphical object — a focus ring, a selection outline, a
 * slider track — against what it sits on.
 */
const GRAPHIC_CONTRAST = 3;

type Rgb = [number, number, number];

const rgbOf = (hex: string): Rgb | undefined => {
   const match = /^#?([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(hex.trim());
   if (!match) return undefined;
   const digits =
      match[1].length === 3
         ? [...match[1]].map((d) => d + d).join("")
         : match[1];
   return [0, 2, 4].map((at) => parseInt(digits.slice(at, at + 2), 16)) as Rgb;
};

const hexOf = (rgb: number[]) =>
   `#${rgb.map((c) => Math.round(c).toString(16).padStart(2, "0")).join("")}`;

/** `from` moved `share` of the way towards `to` (0 black, 255 white). */
const mix = (from: Rgb, to: number, share: number) =>
   hexOf(from.map((c) => c + (to - c) * share));

/** WCAG relative luminance, 0 (black) to 1 (white). */
const luminance = (rgb: Rgb) => {
   const [r, g, b] = rgb.map((c) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
   });
   return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};

/** The WCAG contrast ratio of two colours, 1 to 21. */
const contrastOf = (a: Rgb, b: Rgb) => {
   const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
   return (hi + 0.05) / (lo + 0.05);
};

/** The contrast ratio of two hex colours, or undefined when either is not one. */
export function contrastRatio(a: string, b: string): number | undefined {
   const x = rgbOf(a);
   const y = rgbOf(b);
   return x && y ? contrastOf(x, y) : undefined;
}

/** The near-black label the dark defaults use. */
const INK: Rgb = [0x0f, 0x17, 0x2a];

/**
 * `color`, moved just far enough to stand at `min`:1 against `ground` —
 * toward white on a dark ground, toward black on a light one — so a colour
 * picked for one page still reads on the other, and one that already reads
 * comes back exactly as given. A colour that is not a hex value comes back as
 * given too, since it cannot be measured.
 */
export function legibleOn(
   color: string,
   ground: string,
   min = GRAPHIC_CONTRAST,
): string {
   const rgb = rgbOf(color);
   const base = rgbOf(ground);
   if (!rgb || !base || contrastOf(rgb, base) >= min) return color;
   const toward = luminance(base) < 0.5 ? 255 : 0;
   for (let share = 0.05; share < 1; share += 0.05) {
      const moved = rgbOf(mix(rgb, toward, share));
      if (moved && contrastOf(moved, base) >= min) return hexOf(moved);
   }
   return toward === 255 ? "#ffffff" : "#000000";
}

/**
 * The accent the Console's chrome is drawn in — its primary buttons, sliders,
 * the builder's selection — from the first colour of the palette's series, so
 * a page has one accent and it is the operator's: the button that saves a
 * dashboard and the first line on it are the same hue.
 *
 * In either mode the colour is kept as picked when it already reads against
 * the page at 3:1, and otherwise moved only as far as it takes to read: lifted
 * on the dark page, deepened on the light one. The label on it is whichever of
 * white and near-black reads better. With no palette, the Console's own blue.
 */
export function accentFor(
   first: string | undefined,
   mode: ThemeMode,
   ground: string = DEFAULT_GROUND[mode],
): Accent {
   const fallback = DEFAULT_ACCENT[mode];
   if (!first || first.toLowerCase() === PALETTE.blue) return fallback;
   if (!rgbOf(first))
      return mode === "light"
         ? { ...fallback, accent: first, accentHover: first }
         : fallback;
   const accent = legibleOn(first, ground);
   const accentRgb = rgbOf(accent) as Rgb;
   const accentHover =
      mode === "dark" ? mix(accentRgb, 255, 0.25) : mix(accentRgb, 0, 0.2);
   const onWhite = contrastOf(accentRgb, [255, 255, 255]);
   const onInk = contrastOf(accentRgb, INK);
   return {
      accent,
      accentHover,
      accentContrast: onWhite >= onInk ? "#ffffff" : "#0f172a",
   };
}

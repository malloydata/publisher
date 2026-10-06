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

const rgbOf = (hex: string): [number, number, number] | undefined => {
   const match = /^#?([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(hex.trim());
   if (!match) return undefined;
   const digits =
      match[1].length === 3
         ? [...match[1]].map((d) => d + d).join("")
         : match[1];
   return [0, 2, 4].map((at) => parseInt(digits.slice(at, at + 2), 16)) as [
      number,
      number,
      number,
   ];
};

/** The near-black label the dark defaults use. */
const INK: [number, number, number] = [0x0f, 0x17, 0x2a];

const hexOf = (rgb: number[]) =>
   `#${rgb.map((c) => Math.round(c).toString(16).padStart(2, "0")).join("")}`;

/** `from` moved `share` of the way towards `to`. */
const mix = (from: [number, number, number], to: number, share: number) =>
   hexOf(from.map((c) => c + (to - c) * share));

/** WCAG relative luminance, 0 (black) to 1 (white). */
const luminance = (rgb: [number, number, number]) => {
   const [r, g, b] = rgb.map((c) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
   });
   return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};

/**
 * The accent the Console's chrome is drawn in — its primary buttons, sliders,
 * the builder's selection — from the first colour of the palette's series, so
 * a page has one accent and it is the operator's: the button that saves a
 * dashboard and the first line on it are the same hue.
 *
 * Dark mode lifts it toward white, so a dark brand colour still reads against
 * a slate page; the label on it is whichever of white and near-black reads.
 * A colour that is not a hex value is used as given in light mode and leaves
 * the dark default alone, since it cannot be lifted.
 */
export function accentFor(first: string | undefined, mode: ThemeMode): Accent {
   const fallback = DEFAULT_ACCENT[mode];
   if (!first || first.toLowerCase() === PALETTE.blue) return fallback;
   const rgb = rgbOf(first);
   if (!rgb)
      return mode === "light"
         ? { ...fallback, accent: first, accentHover: first }
         : fallback;
   const accent = mode === "dark" ? mix(rgb, 255, 0.5) : hexOf(rgb);
   const accentRgb = rgbOf(accent) ?? rgb;
   const accentHover =
      mode === "dark" ? mix(accentRgb, 255, 0.3) : mix(accentRgb, 0, 0.2);
   // Whichever label reads better: contrast is (lighter + 0.05) / (darker + 0.05).
   const lit = luminance(accentRgb);
   const onWhite = 1.05 / (lit + 0.05);
   const onInk = (lit + 0.05) / (luminance(INK) + 0.05);
   const accentContrast = onWhite >= onInk ? "#ffffff" : "#0f172a";
   return { accent, accentHover, accentContrast };
}

const contrast = (a: [number, number, number], b: [number, number, number]) => {
   const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
   return (hi + 0.05) / (lo + 0.05);
};

/**
 * `color`, lifted toward white just far enough to stand at `min`:1 against
 * `ground` — for a brand colour picked on a light page and drawn on a dark one,
 * where `#2d323d` on slate is a bar nobody can see. A colour that already
 * reads, or is not a hex value, comes back as given.
 */
export function legibleOn(color: string, ground: string, min = 3): string {
   const rgb = rgbOf(color);
   const base = rgbOf(ground);
   if (!rgb || !base || contrast(rgb, base) >= min) return color;
   for (let share = 0.05; share < 1; share += 0.05) {
      const lifted = mix(rgb, 255, share);
      const liftedRgb = rgbOf(lifted);
      if (liftedRgb && contrast(liftedRgb, base) >= min) return lifted;
   }
   return "#ffffff";
}

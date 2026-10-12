// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Theme } from "@mui/material";

/** The one duration for the builder's and viewer's small state changes: a fade, an outline, a lift. */
export const MOTION_FAST = "120ms";

/**
 * Turns transitions and animations off for a reader who asked the system for
 * less motion. Spread into the `sx` of anything that animates.
 */
export const reducedMotionSx = {
   "@media (prefers-reduced-motion: reduce)": {
      transition: "none",
      animation: "none",
   },
} as const;

/** Smooth scrolling, unless the reader asked for less motion. */
export const scrollBehavior = (): ScrollBehavior =>
   typeof window !== "undefined" &&
   window.matchMedia?.("(prefers-reduced-motion: reduce)").matches
      ? "auto"
      : "smooth";

/**
 * An affordance that appears on hover shows at rest where there is no hover
 * (a touch screen): otherwise it could never be reached there.
 */
export const visibleWithoutHoverSx = {
   "@media (hover: none)": { opacity: 1 },
} as const;

/**
 * The colour of a destructive action's text (Delete, Remove filter): the host
 * MUI theme's error palette, its deeper `dark` shade on a light page and
 * `main` on a dark one, so the host's own red is used and a host adjusts it
 * through its theme rather than here.
 */
export const dangerTextColor = (theme: Theme) =>
   theme.palette.mode === "dark"
      ? theme.palette.error.main
      : theme.palette.error.dark;

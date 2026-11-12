// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MONO_FONT_FAMILY as SDK_MONO_FONT_FAMILY, PALETTE } from "../styles";

/**
 * The model viewer's type steps and colour roles, in one place so the rows,
 * pills and cards are set from the same few values. Built on the SDK palette.
 *
 * Every neutral is one of MUI's own palette keys (`text.*`, `action.*`), which
 * every theme defines in both modes, never a hex or a key only some hosts add:
 * a fixed light grey was a near-white pill under light text in a dark theme.
 * So each role is for an `sx` colour prop, where MUI resolves it.
 */

/** The custom property the viewer's root sets from the host's `ui.font.mono`. */
export const MONO_FONT_VAR = "--model-viewer-mono";

/** The viewer's code face: the host theme's mono face, set on the viewer's root. */
export const MONO_FONT_FAMILY = `var(${MONO_FONT_VAR}, ${SDK_MONO_FONT_FAMILY})`;
export const SANS_FONT_FAMILY = "inherit";

export const fontSizes = {
   /** 11px: the floor, for a pill's text. */
   micro: "0.6875rem",
   /** 12px: secondary row detail. */
   caption: "0.75rem",
   /** 13px: dense rows. */
   small: "0.8125rem",
   /** 14px: body. */
   body: "0.875rem",
   /** 16px: prose meant to be read at length. */
   bodyLarge: "1rem",
} as const;

export const fontWeights = { regular: 400, medium: 500, strong: 500 } as const;

const tinted = (main: string) => ({
   tint: `color-mix(in srgb, ${main} 12%, transparent)`,
   onTint: main,
});

export const roles = {
   text: {
      primary: "text.primary",
      secondary: "text.secondary",
      tertiary: "text.disabled",
   },
   /** A row's hover and a quiet pill's fill, then one step up: a selected row, a pill on a hovered row. */
   background: { muted: "action.hover", strong: "action.selected" },
   accent: { sage: tinted(PALETTE.lime) },
   warning: tinted(PALETTE.amber),
   error: tinted(PALETTE.red),
   info: tinted(PALETTE.blue),
} as const;

/** Tertiary text: lighter than MUI's `text.secondary`, for a row's least words. */
export const TEXT_TERTIARY = roles.text.tertiary;

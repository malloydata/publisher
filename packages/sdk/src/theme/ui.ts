// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The design tokens the report reader and builder draw their chrome with:
 * radii, control sizes, the type scale, the monospace face, the menu shadow.
 *
 * Kept apart from {@link Theme}, which is the operator's (the server's) and
 * says how DATA looks: series colours, the chart canvas, the table. These say
 * how the CHROME around it looks, and they belong to the host app: a host
 * with a design system of its own passes its values to `ThemeProvider` as
 * `ui`, and every builder and report surface follows. Colour is not here:
 * text, hover and surfaces come from the host's MUI palette, the accent and
 * the card edge from the instance theme.
 *
 * The defaults are what the SDK draws when nothing is passed; any subset can
 * be overridden, down to one field.
 */

/** A text style: what an `sx` spread takes. */
export interface UiTextStyle {
   fontSize: number | string;
   fontWeight?: number | string;
   lineHeight?: number | string;
   letterSpacing?: string;
   fontFamily?: string;
}

export interface ReportUi {
   radius: {
      /** Buttons, segmented controls, filter buttons, inputs. */
      control: string;
      /** Tiles, cards, popovers. */
      tile: string;
      /** Menus. */
      menu: string;
      /** Dialogs. */
      dialog: string;
   };
   size: {
      /** Every button, segmented control, filter button and input, in px. */
      control: number;
      /** Chips: the one smaller control, in px. */
      chip: number;
   };
   type: {
      pageTitle: UiTextStyle;
      tileTitle: UiTextStyle;
      prose: UiTextStyle;
      /** Buttons, menu items, list rows. */
      control: UiTextStyle;
      /** Secondary lines, hints, captions. */
      caption: UiTextStyle;
      /** A small heading over part of a window ("Chart", "Preview"). */
      label: UiTextStyle;
      /** Malloy names: a view, a field, a source. */
      code: UiTextStyle;
   };
   font: {
      /** The face Malloy names are set in. */
      mono: string;
   };
   shadow: {
      /** Under a menu or a popover. */
      menu: string;
   };
}

/** Any part of {@link ReportUi}, down to one field of one text style. */
export interface ReportUiOverrides {
   radius?: Partial<ReportUi["radius"]>;
   size?: Partial<ReportUi["size"]>;
   type?: { [K in keyof ReportUi["type"]]?: Partial<UiTextStyle> };
   font?: Partial<ReportUi["font"]>;
   shadow?: Partial<ReportUi["shadow"]>;
}

const MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";

export const DEFAULT_UI: ReportUi = {
   radius: { control: "6px", tile: "8px", menu: "10px", dialog: "12px" },
   size: { control: 32, chip: 24 },
   type: {
      pageTitle: { fontSize: 24, fontWeight: 600, letterSpacing: "-0.01em" },
      tileTitle: { fontSize: 14, fontWeight: 600, lineHeight: 1.4 },
      prose: { fontSize: 15, lineHeight: 1.6 },
      control: { fontSize: 13, fontWeight: 500 },
      caption: { fontSize: 12, lineHeight: 1.4 },
      label: { fontSize: 12, fontWeight: 600, lineHeight: 1.4 },
      code: { fontSize: 11.5, fontFamily: MONO },
   },
   font: { mono: MONO },
   shadow: { menu: "0 8px 24px rgba(16, 24, 40, 0.12)" },
};

/** {@link DEFAULT_UI} with a host's overrides laid over it, one group at a time. */
export function mergeUi(overrides: ReportUiOverrides | undefined): ReportUi {
   if (!overrides) return DEFAULT_UI;
   const font = { ...DEFAULT_UI.font, ...overrides.font };
   const type = { ...DEFAULT_UI.type };
   // A host's mono face is the code style's face unless it says otherwise.
   type.code = { ...type.code, fontFamily: font.mono };
   for (const [key, style] of Object.entries(overrides.type ?? {}))
      type[key as keyof ReportUi["type"]] = {
         ...type[key as keyof ReportUi["type"]],
         ...style,
      };
   return {
      radius: { ...DEFAULT_UI.radius, ...overrides.radius },
      size: { ...DEFAULT_UI.size, ...overrides.size },
      type,
      font,
      shadow: { ...DEFAULT_UI.shadow, ...overrides.shadow },
   };
}

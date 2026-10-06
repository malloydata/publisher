// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Theme } from "../client";

export type { Theme };

export type ThemeMode = "light" | "dark";

/**
 * A Theme with every field downstream consumers need actually filled in.
 * Produced by `resolveTheme(layers, mode)`, which flattens the cascade
 * and the per-mode colour split into a single mode-agnostic shape — so
 * `buildTableCssVars`, `buildVegaThemeOverride`, and
 * `buildMalloyExplicitTheme` never have to branch on mode themselves.
 *
 * The thirteen per-mode `palette.*` colour keys are stored per-mode on the
 * raw Theme (one of light/dark per field) and collapse to the active mode
 * here. The chrome fields keep their historical names: `border`,
 * `cardBorder` and `pinnedBorder` are `1px solid <colour>` shorthands from
 * `palette.border` / `palette.cardBorder`; `valueColor` is `palette.value`,
 * `foreground` is `palette.chartText`, `axisFaint` is `palette.axis`, and
 * `gridline` is `palette.gridline`.
 */
export interface ResolvedTheme {
   mode: ThemeMode;

   series: string[];
   /**
    * The Console's accent — primary buttons, sliders, the builder's selection
    * — taken from the first series colour, lifted toward white in dark mode.
    * See `accentFor`.
    */
   accent: string;
   /** The accent's hover state. */
   accentHover: string;
   /** The label colour that reads on the accent. */
   accentContrast: string;
   font: {
      family: string;
      size: number;
   };

   background: string;
   tableHeader: string;
   tableHeaderBackground: string;
   tableBody: string;
   tile: string;
   tileTitle: string;
   /**
    * Saturated end of the gradient used by sequential color scales
    * (choropleth maps, heatmaps). The renderer pairs it with a
    * near-neutral low-end so the operator only picks the brand end.
    */
   mapColor: string;

   /** Table gridline / row rule, as a `1px solid <colour>` shorthand. */
   border: string;
   /**
    * The edge of a dashboard CARD, kept off `border` on purpose. `border` is
    * the gridline inside a table, where a hairline is right because there are
    * dozens of them and they only have to separate rows. A card's edge has one
    * job — say where the card stops — and at the gridline's weight, on a page
    * whose ground and tile are both white, it did not do it: the cards read as
    * floating content rather than as cards. A step down the same slate ramp,
    * so the two still read as one system.
    */
   cardBorder: string;
   /** Pinned table header rule: `palette.cardBorder` as a border shorthand. */
   pinnedBorder: string;
   /** The big-value (KPI) number colour (`palette.value`). */
   valueColor: string;
   /** Chart text: axis labels/titles, legend, titles (`palette.chartText`). */
   foreground: string;
   /** Chart axis domain and tick lines (`palette.axis`). */
   axisFaint: string;
   /** Chart gridlines (`palette.gridline`). */
   gridline: string;
   /**
    * The lift under something that responds to the pointer (a builder tile on
    * hover), and under something being carried (a tile mid-drag). Mode-keyed:
    * a black shadow alone disappears on the dark page, so dark adds an edge.
    */
   shadow: { lift: string; drag: string };
   /**
    * Background for the renderer's HTML chrome (the area between
    * dashboard tiles). Mode-keyed and intentionally NOT
    * operator-customizable — when an operator picks a bold accent
    * for `palette.background` (the chart canvas), the surrounding
    * panel should stay neutral so the accent reads cleanly. In dark
    * mode it paints slate so the panel doesn't sit as a stark white
    * box on the dark page chrome.
    */
   dashboardRoot: string;
   /**
    * Colour a `# drill` cell takes on hover, when it reads as a link. A link
    * blue rather than anything from `palette.series`, deliberately: the
    * affordance has to stay legible as a link against whatever the operator
    * picked for the data, and a drillable bar or cell should not look like it
    * belongs to a series. Mode-keyed and not operator-customizable, like
    * `dashboardRoot`.
    */
   drillLink: string;
   /**
    * Background for the renderer's table interior. Follows the
    * operator's `palette.background` (the chart canvas colour) so
    * tables and charts share a single "viz surface" colour. In dark
    * mode this also keeps the (light-slate) header/body text from
    * painting unreadable light-on-white against the renderer's
    * hardcoded default. Distinct from `dashboardRoot`, which is the
    * panel BETWEEN tiles and stays mode-keyed / neutral.
    */
   tableBackground: string;
}

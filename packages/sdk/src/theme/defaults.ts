import { MALLOY_BRAND } from "../components/styles";
import type { Theme } from "./types";

const DEFAULT_SERIES = [
   MALLOY_BRAND.teal,
   MALLOY_BRAND.orange,
   MALLOY_BRAND.darkBlue,
   "#66cedc",
   "#ec72b8",
   "#f9c85b",
   "#aacd85",
   "#b87ced",
];

const DEFAULT_FONT_FAMILY =
   "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";

/**
 * Default theme used when no operator or per-chart override is set. Sources
 * its colors from {@link MALLOY_BRAND} so the brand palette can be retuned
 * in one place. Every field is per-mode so operators can edit light and
 * dark independently from the start; the defaults below seed both modes
 * with the same series and font so the unedited experience is consistent.
 */
export const DEFAULT_THEME: Required<Theme> = {
   defaultMode: "light",
   allowUserToggle: true,
   palette: {
      series: {
         light: [...DEFAULT_SERIES],
         dark: [...DEFAULT_SERIES],
      },
      background: {
         light: "#ffffff",
         // Slate to match the Publisher app's sidebar surface so
         // rendered charts sit on the same elevation as the chrome
         // around them. The page outer (MUI background.default) is the
         // darker #0f172a; charts are intentionally a step lighter so
         // they read as elevated cards, not a void.
         dark: "#1e293b",
      },
      tableHeader: {
         light: "#5d626b",
         dark: "#cbd5e1",
      },
      tableBody: {
         light: "#727883",
         dark: "#e2e8f0",
      },
      // The padded container that wraps each chart / table in a dashboard.
      // Light mode: a faint tint so tiles read as recessed cards on the
      // page; dark mode: page-outer slate so tiles sit a step below the
      // chart background.
      tile: {
         light: "#f5fafc",
         dark: "#0f172a",
      },
      tileTitle: {
         light: "#5d626b",
         dark: "#94a3b8",
      },
   },
   font: {
      family: {
         light: DEFAULT_FONT_FAMILY,
         dark: DEFAULT_FONT_FAMILY,
      },
      size: {
         light: 12,
         dark: 12,
      },
   },
};

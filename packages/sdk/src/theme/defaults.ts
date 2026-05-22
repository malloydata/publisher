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
 * Built-in theme used when nothing has been configured. Series palette
 * and font are shared across modes; only colours that should genuinely
 * change in dark mode (backgrounds, foregrounds) are per-mode.
 */
export const DEFAULT_THEME: Required<Theme> = {
   defaultMode: "light",
   allowUserToggle: true,
   palette: {
      series: DEFAULT_SERIES,
      background: {
         light: "#ffffff",
         // Slate to match the Publisher app's sidebar surface so
         // rendered charts sit on the same elevation as the chrome
         // around them.
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
      // The padded container that wraps each chart / table in a
      // dashboard. Light mode: a faint tint so tiles read as recessed
      // cards on the page; dark mode: page-outer slate.
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
      family: DEFAULT_FONT_FAMILY,
      size: 12,
   },
};

import { MALLOY_BRAND } from "../components/styles";
import type { Theme } from "./types";

/**
 * Default theme used when no operator or per-chart override is set. Sources
 * its colors from {@link MALLOY_BRAND} so the brand palette can be retuned
 * in one place.
 */
export const DEFAULT_THEME: Required<Theme> = {
   defaultMode: "light",
   allowUserToggle: true,
   palette: {
      series: [
         MALLOY_BRAND.teal,
         MALLOY_BRAND.orange,
         MALLOY_BRAND.darkBlue,
         "#66cedc",
         "#ec72b8",
         "#f9c85b",
         "#aacd85",
         "#b87ced",
      ],
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
   },
   font: {
      family:
         "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
      size: 12,
   },
};

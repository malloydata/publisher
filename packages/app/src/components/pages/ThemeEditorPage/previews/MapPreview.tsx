import type { ResolvedTheme } from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";

/**
 * Small static preview for the choropleth gradient the renderer
 * generates from `theme.mapColor`. Renders 5 cells stepped through
 * the same low-to-high ramp the renderer applies internally
 * (#f5f5f5 → theme.mapColor) so the operator gets immediate feedback
 * when they change the saturated end colour.
 */
export function MapPreview({ theme }: { theme: ResolvedTheme }) {
   // Low end matches getColorScale's MAP_GRADIENT_LOW constant. Keep
   // these in lockstep if either is retuned.
   const LOW = "#f5f5f5";
   const high = theme.mapColor;
   const stops = [0, 0.25, 0.5, 0.75, 1];

   const hexToRgb = (hex: string) => {
      const m = hex.match(/^#([0-9a-fA-F]{6})$/);
      if (!m) return { r: 0, g: 0, b: 0 };
      const n = parseInt(m[1], 16);
      return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
   };
   const lowRgb = hexToRgb(LOW);
   const highRgb = hexToRgb(high);
   const lerp = (a: number, b: number, t: number) =>
      Math.round(a + (b - a) * t);
   const colourAt = (t: number) =>
      `rgb(${lerp(lowRgb.r, highRgb.r, t)}, ${lerp(lowRgb.g, highRgb.g, t)}, ${lerp(lowRgb.b, highRgb.b, t)})`;

   return (
      <Box
         sx={{
            backgroundColor: theme.background,
            borderRadius: 1,
            p: 1.5,
            display: "inline-block",
         }}
         aria-label="Map gradient preview"
      >
         <Box sx={{ display: "flex", gap: 0.5, alignItems: "flex-end" }}>
            {stops.map((t, i) => (
               <Box
                  key={i}
                  sx={{
                     backgroundColor: colourAt(t),
                     width: 40,
                     height: 56,
                     borderRadius: 0.5,
                     border: theme.border,
                  }}
                  aria-label={`Gradient stop ${i + 1}`}
               />
            ))}
         </Box>
         <Box
            sx={{
               display: "flex",
               justifyContent: "space-between",
               mt: 0.5,
            }}
         >
            <Typography variant="caption" sx={{ color: theme.tableHeader }}>
               low
            </Typography>
            <Typography variant="caption" sx={{ color: theme.tableHeader }}>
               high
            </Typography>
         </Box>
      </Box>
   );
}

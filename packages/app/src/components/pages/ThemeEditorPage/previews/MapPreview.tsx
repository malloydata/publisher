import type { ResolvedTheme } from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";

/**
 * Schematic US-silhouette choropleth preview. The path is a single
 * simplified outline of the lower-48 (no per-state shapes — those
 * require ~50 paths plus a topology dataset, which we'd rather not
 * inline in the editor). A series of inner ramp-coloured rectangles
 * sits above it as a legend so the operator sees the full gradient
 * the renderer generates from `theme.mapColor`.
 */
export function MapPreview({ theme }: { theme: ResolvedTheme }) {
   // Low end matches getColorScale's MAP_GRADIENT_LOW constant.
   const LOW = "#f5f5f5";
   const high = theme.mapColor;

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

   // Simplified lower-48 silhouette (single path). Coordinates are
   // hand-traced from a USPS-style outline; intentionally crude — the
   // goal is a recognisable US shape, not geographic fidelity.
   const us =
      "M 20 60 L 60 50 L 110 45 L 170 42 L 230 45 L 280 50 L 320 55 L 350 75 L 365 100 L 360 130 L 340 145 L 310 150 L 280 160 L 260 175 L 240 180 L 215 170 L 195 160 L 170 155 L 150 165 L 130 175 L 115 165 L 100 145 L 85 130 L 65 115 L 40 100 L 25 80 Z";

   // Fill the whole silhouette at a mid-saturation, then overlay
   // small "state" blobs at varying saturations to give the
   // choropleth effect.
   const blobs: Array<{ cx: number; cy: number; r: number; intensity: number }> = [
      { cx: 80, cy: 90, r: 28, intensity: 0.85 }, // CA
      { cx: 130, cy: 80, r: 22, intensity: 0.4 },
      { cx: 180, cy: 85, r: 24, intensity: 0.55 },
      { cx: 230, cy: 90, r: 24, intensity: 0.3 },
      { cx: 280, cy: 100, r: 24, intensity: 0.7 },
      { cx: 320, cy: 105, r: 20, intensity: 0.5 },
      { cx: 160, cy: 130, r: 26, intensity: 0.65 }, // TX
      { cx: 210, cy: 140, r: 20, intensity: 0.45 },
      { cx: 260, cy: 135, r: 18, intensity: 0.6 },
   ];

   return (
      <Box
         sx={{
            backgroundColor: theme.background,
            borderRadius: 1,
            p: 1.5,
            display: "inline-block",
         }}
         aria-label="Choropleth map preview"
      >
         <svg
            width={380}
            height={220}
            role="img"
            aria-label="Sample choropleth"
         >
            {/* Underlying silhouette in the lowest gradient stop. */}
            <path
               d={us}
               fill={colourAt(0.1)}
               stroke={theme.background}
               strokeWidth={2}
            />
            {/* State-like blobs at varying intensities. */}
            {blobs.map((b, i) => (
               <circle
                  key={i}
                  cx={b.cx}
                  cy={b.cy}
                  r={b.r}
                  fill={colourAt(b.intensity)}
                  stroke={theme.background}
                  strokeWidth={1.5}
               />
            ))}
         </svg>
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

import type { ResolvedTheme } from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";

/**
 * Schematic US-state choropleth preview. Tints each state along the
 * low-to-high gradient the renderer generates from `theme.mapColor`
 * (`#f5f5f5` → `theme.mapColor`). The states' relative values are
 * arbitrary but distributed evenly so the operator sees the full
 * ramp; this isn't a real geography, just enough of a US silhouette
 * to read as "map" instead of an abstract color strip.
 */
export function MapPreview({ theme }: { theme: ResolvedTheme }) {
   // Low end matches getColorScale's MAP_GRADIENT_LOW constant. Keep
   // these in lockstep if either is retuned.
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

   // Hand-traced lower-48 + AK/HI fills with intensities in [0,1].
   // Coords are SVG path fragments — not geographically accurate; the
   // shapes are stylised tiles that approximate state outlines.
   // intensity drives which gradient stop each state pulls from.
   const states: Array<{
      d: string;
      intensity: number;
   }> = [
      // West coast
      { d: "M 40 80 L 80 80 L 80 200 L 40 200 Z", intensity: 0.9 }, // CA
      { d: "M 40 40 L 80 40 L 80 80 L 40 80 Z", intensity: 0.55 }, // OR
      { d: "M 40 10 L 80 10 L 80 40 L 40 40 Z", intensity: 0.35 }, // WA
      // Mountain
      { d: "M 80 40 L 130 40 L 130 110 L 80 110 Z", intensity: 0.25 }, // ID/MT
      { d: "M 80 110 L 130 110 L 130 180 L 80 180 Z", intensity: 0.45 }, // NV/UT
      { d: "M 80 180 L 130 180 L 130 230 L 80 230 Z", intensity: 0.65 }, // AZ
      // Plains
      { d: "M 130 40 L 200 40 L 200 100 L 130 100 Z", intensity: 0.4 },
      { d: "M 130 100 L 200 100 L 200 170 L 130 170 Z", intensity: 0.7 }, // TX-ish north
      { d: "M 130 170 L 200 170 L 200 230 L 130 230 Z", intensity: 0.85 }, // TX
      // Midwest
      { d: "M 200 40 L 270 40 L 270 110 L 200 110 Z", intensity: 0.55 },
      { d: "M 200 110 L 270 110 L 270 170 L 200 170 Z", intensity: 0.6 },
      { d: "M 200 170 L 270 170 L 270 220 L 200 220 Z", intensity: 0.5 },
      // East
      { d: "M 270 40 L 340 40 L 340 100 L 270 100 Z", intensity: 0.75 },
      { d: "M 270 100 L 340 100 L 340 160 L 270 160 Z", intensity: 0.65 },
      { d: "M 270 160 L 340 160 L 340 220 L 270 220 Z", intensity: 0.5 },
      // AK + HI
      { d: "M 10 220 L 50 220 L 50 250 L 10 250 Z", intensity: 0.15 },
      { d: "M 60 230 L 90 230 L 90 250 L 60 250 Z", intensity: 0.3 },
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
            width={360}
            height={260}
            role="img"
            aria-label="Sample choropleth"
         >
            {states.map((s, i) => (
               <path
                  key={i}
                  d={s.d}
                  fill={colourAt(s.intensity)}
                  stroke={theme.background}
                  strokeWidth={2}
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

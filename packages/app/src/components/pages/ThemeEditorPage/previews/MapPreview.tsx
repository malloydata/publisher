import type { Theme } from "@malloy-publisher/sdk";
import { Box } from "@mui/material";

interface MapPreviewProps {
   /** Heatmap ramp colors low → high. Falls back to neutral grey. */
   heatmap?: string[];
   background: string;
}

/**
 * Schematic US-state-like grid that paints with the heatmap ramp.
 * 6 cells, each tinted from the lowest to highest stop. No real
 * geography — exists for color decisions only.
 */
export function MapPreview({ heatmap = [], background }: MapPreviewProps) {
   const ramp = heatmap.length > 0 ? heatmap : ["#dddddd"];
   const cells = Array.from({ length: 6 }, (_, i) => {
      const t = i / Math.max(1, 5);
      const idx = Math.min(ramp.length - 1, Math.floor(t * ramp.length));
      return ramp[idx];
   });

   return (
      <Box
         sx={{
            backgroundColor: background,
            borderRadius: 1,
            p: 1.5,
            display: "inline-flex",
            gap: 0.5,
         }}
         aria-label="Map preview"
      >
         {cells.map((fill, i) => (
            <Box
               key={i}
               sx={{
                  width: 40,
                  height: 40,
                  backgroundColor: fill,
                  borderRadius: 0.5,
               }}
            />
         ))}
      </Box>
   );
}

export type ThemeForMap = Pick<Theme, "palette">;

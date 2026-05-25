import type { ResolvedTheme } from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";
import { geoAlbersUsa, geoPath } from "d3-geo";
import { useMemo } from "react";
import { feature } from "topojson-client";
import usAtlas from "us-atlas/states-10m.json";

/**
 * Real US-states choropleth preview. Renders the same `us-atlas`
 * states-10m TopoJSON the renderer's shape-map plugin uses, so the
 * preview shape matches the actual `sales_by_state`-style map a
 * viewer sees on a package page. Each state is tinted along the
 * gradient the renderer generates from `theme.mapColor`
 * (`#f5f5f5` → `theme.mapColor`) using a stable hash-based intensity
 * so re-renders don't reshuffle the colour assignment.
 */
export function MapPreview({ theme }: { theme: ResolvedTheme }) {
   // Low end matches getColorScale's MAP_GRADIENT_LOW constant in
   // the renderer. Keep these in lockstep if either is retuned.
   const LOW = "#f5f5f5";
   const high = theme.mapColor;

   const { paths } = useMemo(() => {
      // us-atlas ships a single TopoJSON with `states` as an object;
      // `feature` extracts the GeoJSON FeatureCollection.
      // The TopoJSON types are wide so we cast at the boundary.
      const collection = feature(
         usAtlas as Parameters<typeof feature>[0],
         (usAtlas as { objects: { states: unknown } }).objects
            .states as Parameters<typeof feature>[1],
      ) as { features: Array<{ id?: string; properties?: { name?: string } }> };

      const projection = geoAlbersUsa().fitSize([380, 220], collection);
      const path = geoPath(projection);

      // Stable intensity per state: hash the state id (FIPS code) to
      // a [0,1] number so the gradient assignment stays consistent
      // across renders without needing a real data field.
      const hash = (s: string) => {
         let h = 0;
         for (let i = 0; i < s.length; i++) {
            h = (h * 31 + s.charCodeAt(i)) & 0xffffffff;
         }
         return ((h >>> 0) % 1000) / 1000;
      };

      const out = collection.features.map((feat) => ({
         d: path(feat as Parameters<typeof path>[0]) ?? "",
         intensity: hash(String(feat.id ?? feat.properties?.name ?? "")),
      }));
      return { paths: out };
   }, []);

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
         aria-label="Choropleth map preview"
      >
         <svg
            width={380}
            height={220}
            role="img"
            aria-label="Sample US choropleth"
         >
            {paths.map((p, i) => (
               <path
                  key={i}
                  d={p.d}
                  fill={colourAt(p.intensity)}
                  stroke={theme.background}
                  strokeWidth={0.5}
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

import type { ResolvedTheme } from "@malloy-publisher/sdk";
import { Box } from "@mui/material";

/**
 * Small static SVG bar chart that paints in the editor's draft theme.
 * Not a real Malloy renderer — exists so color decisions get instant
 * feedback without spinning up a query.
 */
export function BarChartPreview({ theme }: { theme: ResolvedTheme }) {
   const series = theme.series.length > 0 ? theme.series : ["#999999"];
   const values = [62, 41, 78, 35, 54, 80, 27, 49];
   const max = Math.max(...values);
   const width = 280;
   const height = 100;
   const barW = width / values.length;

   return (
      <Box
         sx={{
            backgroundColor: theme.background,
            color: theme.tableHeader,
            borderRadius: 1,
            p: 1,
            display: "inline-block",
         }}
      >
         <svg
            width={width}
            height={height}
            role="img"
            aria-label="Bar chart preview"
         >
            {values.map((v, i) => {
               const h = (v / max) * (height - 16);
               return (
                  <rect
                     key={i}
                     x={i * barW + 4}
                     y={height - h - 4}
                     width={barW - 8}
                     height={h}
                     fill={series[i % series.length]}
                     rx={2}
                  />
               );
            })}
         </svg>
      </Box>
   );
}

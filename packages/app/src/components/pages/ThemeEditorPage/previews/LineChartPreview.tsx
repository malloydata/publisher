// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ResolvedTheme } from "@malloy-publisher/sdk";
import { Box } from "@mui/material";

/**
 * Small static SVG line chart that paints in the editor's draft theme.
 * Sits next to {@link BarChartPreview} so the operator can see how
 * single-series line visuals (the most common dashboard chart) respond
 * to background and series-1 colour choices alongside the multi-series
 * bar chart.
 */
export function LineChartPreview({ theme }: { theme: ResolvedTheme }) {
   const series = theme.series.length > 0 ? theme.series : ["#999999"];
   const stroke = series[0];
   const values = [22, 35, 28, 48, 42, 60, 55, 72, 68, 86];
   const width = 280;
   const height = 100;
   const padX = 8;
   const padY = 8;
   const max = Math.max(...values);
   const min = Math.min(...values);
   const xStep = (width - padX * 2) / (values.length - 1);
   const yScale = (v: number) =>
      height - padY - ((v - min) / (max - min || 1)) * (height - padY * 2);

   const points = values
      .map((v, i) => `${padX + i * xStep},${yScale(v)}`)
      .join(" ");

   return (
      <Box
         sx={{
            backgroundColor: theme.background,
            color: theme.foreground,
            borderRadius: 1,
            p: 1,
            display: "inline-block",
         }}
      >
         <svg
            width={width}
            height={height}
            role="img"
            aria-label="Line chart preview"
         >
            {/* Two gridlines, in the gridline colour, as Vega draws them. */}
            {[0.33, 0.66].map((f) => (
               <line
                  key={f}
                  x1={padX}
                  y1={padY + f * (height - padY * 2)}
                  x2={width - padX}
                  y2={padY + f * (height - padY * 2)}
                  stroke={theme.gridline}
                  strokeWidth={1}
               />
            ))}
            {/* Baseline so the line has a horizontal reference, in the
                axis colour: the same role as Vega's axis domain. */}
            <line
               x1={padX}
               y1={height - padY}
               x2={width - padX}
               y2={height - padY}
               stroke={theme.axisFaint}
               strokeWidth={1}
            />
            {/* An axis title, in the chart text colour. */}
            <text
               x={padX}
               y={padY + 2}
               fontSize={10}
               fontFamily={theme.font.family}
               fill={theme.foreground}
            >
               total_sales
            </text>
            <polyline
               points={points}
               fill="none"
               stroke={stroke}
               strokeWidth={2.5}
               strokeLinecap="round"
               strokeLinejoin="round"
            />
         </svg>
      </Box>
   );
}

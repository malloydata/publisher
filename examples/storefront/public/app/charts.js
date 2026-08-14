// Chart.js wrappers.
//
// Chart.js draws these, so unlike the Console and the React SDK, which read the
// server's configured theme, the colors are the page's to pick. They are read
// from the CSS custom properties in index.html rather than repeated here, so the
// palette has one home and a dark-mode swap moves the charts with the rest of
// the page.

import { formatValue, monthLabel } from "./format.js";

const cssVar = (name) =>
   getComputedStyle(document.documentElement).getPropertyValue(name).trim();

export function readTheme() {
   return {
      series: [1, 2, 3, 4, 5, 6, 7, 8].map((n) => cssVar(`--series-${n}`)),
      grid: cssVar("--chart-grid"),
      axis: cssVar("--text-secondary"),
      font: cssVar("--font-sans"),
   };
}

// Keyed by canvas, and weakly: a tile swaps its canvas out when an empty state
// takes its place, and nothing should keep the detached one alive.
/** Chart.js needs one instance per canvas; drop the old one before drawing. */
const charts = new WeakMap();

function release(canvas) {
   charts.get(canvas)?.destroy();
   charts.delete(canvas);
}

function draw(canvas, config) {
   release(canvas);
   const theme = readTheme();
   Chart.defaults.font.family = theme.font;
   Chart.defaults.font.size = 12;
   Chart.defaults.color = theme.axis;
   charts.set(canvas, new Chart(canvas, config));
}

export const destroyChart = release;

const valueAxis = (theme, format) => ({
   beginAtZero: true,
   border: { display: false },
   grid: { color: theme.grid, drawTicks: false },
   ticks: { callback: (v) => formatValue(v, format), padding: 6 },
});

/**
 * A categorical axis: one tick per bar, so every label is drawn. Dropping one
 * would leave a bar standing over nothing. Chart.js rotates only as far as it
 * must, so a short set stays flat and a crowded one leans instead of colliding.
 */
const categoryAxis = (theme, { rotate = true } = {}) => ({
   border: { color: theme.grid },
   grid: { display: false },
   ticks: {
      autoSkip: false,
      maxRotation: rotate ? 45 : 0,
      minRotation: 0,
      padding: 4,
      color: theme.axis,
   },
});

/**
 * A continuous axis, three years of months. These labels are a scale rather than
 * a set of things, so thinning them is right where rotating them is not: auto
 * skip is left on, and `autoSkipPadding` asks for space between the ones kept.
 */
const timeAxis = (theme) => ({
   border: { color: theme.grid },
   grid: { display: false },
   ticks: {
      autoSkip: true,
      autoSkipPadding: 12,
      maxRotation: 0,
      padding: 4,
   },
});

const tooltip = (format) => ({
   callbacks: {
      label: (item) => ` ${formatValue(item.parsed.y ?? item.parsed.x, format)}`,
   },
});

/**
 * A bar chart. `horizontal` is for long category labels: a state name reads
 * better beside its bar than rotated under it.
 */
export function barChart(canvas, { labels, values, format, horizontal = false }) {
   const theme = readTheme();
   draw(canvas, {
      type: "bar",
      data: {
         labels,
         datasets: [
            {
               data: values,
               backgroundColor: theme.series[0],
               hoverBackgroundColor: theme.series[2],
               borderRadius: 2,
               maxBarThickness: horizontal ? 18 : 42,
            },
         ],
      },
      options: {
         indexAxis: horizontal ? "y" : "x",
         responsive: true,
         maintainAspectRatio: false,
         plugins: { legend: { display: false }, tooltip: tooltip(format) },
         scales: horizontal
            ? {
                 x: valueAxis(theme, format),
                 y: categoryAxis(theme, { rotate: false }),
              }
            : { y: valueAxis(theme, format), x: categoryAxis(theme) },
      },
   });
}

export function lineChart(canvas, { labels, values, format }) {
   const theme = readTheme();
   draw(canvas, {
      type: "line",
      data: {
         labels,
         datasets: [
            {
               data: values,
               borderColor: theme.series[0],
               backgroundColor: `${theme.series[0]}1f`,
               borderWidth: 2,
               fill: true,
               tension: 0.3,
               pointRadius: 0,
               pointHoverRadius: 4,
            },
         ],
      },
      options: {
         responsive: true,
         maintainAspectRatio: false,
         interaction: { mode: "index", intersect: false },
         plugins: { legend: { display: false }, tooltip: tooltip(format) },
         scales: { y: valueAxis(theme, format), x: timeAxis(theme) },
      },
   });
}

/** One line per series value, which is how the seasonality view reads: a year each. */
export function seriesLineChart(canvas, { labels, series, format }) {
   const theme = readTheme();
   draw(canvas, {
      type: "line",
      data: {
         labels,
         datasets: series.map((s, i) => ({
            label: s.label,
            data: s.values,
            borderColor: theme.series[i % theme.series.length],
            backgroundColor: theme.series[i % theme.series.length],
            borderWidth: 2,
            tension: 0.3,
            pointRadius: 0,
            pointHoverRadius: 4,
         })),
      },
      options: {
         responsive: true,
         maintainAspectRatio: false,
         interaction: { mode: "index", intersect: false },
         plugins: {
            legend: {
               position: "bottom",
               labels: { boxWidth: 10, boxHeight: 10, usePointStyle: true },
            },
            tooltip: tooltip(format),
         },
         scales: { y: valueAxis(theme, format), x: timeAxis(theme) },
      },
   });
}

// ---- shaping rows for a chart -------------------------------------------------
// Kept next to the charts because a chart's axes are the reason these exist.

export const monthLabels = (rows, field) =>
   rows.map((row) => monthLabel(row[field]));

/** Group by one dimension into one dataset per distinct value, ordered. */
export function toSeries(rows, { x, series, value, xLabel }) {
   const xs = [...new Set(rows.map((row) => row[x]))].sort((a, b) => a - b);
   const keys = [...new Set(rows.map((row) => row[series]))].sort(
      (a, b) => a - b,
   );
   const byKey = new Map(keys.map((key) => [key, new Map()]));
   for (const row of rows) byKey.get(row[series])?.set(row[x], row[value]);
   return {
      labels: xs.map(xLabel ?? String),
      series: keys.map((key) => ({
         label: String(key),
         values: xs.map((xv) => byKey.get(key).get(xv) ?? null),
      })),
   };
}

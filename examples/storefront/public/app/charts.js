// Chart.js wrappers.
//
// Chart.js draws these, so unlike the Console and the React SDK — which read the
// server's configured theme — the colors are the page's to pick. They are read
// from the CSS custom properties in index.html rather than repeated here, so the
// palette has one home and a dark-mode swap moves the charts with the rest of the
// page.

import { formatValue, monthLabel, monthOfYearLabel } from "./format.js";

const cssVar = (name) =>
   getComputedStyle(document.documentElement).getPropertyValue(name).trim();

export function readTheme() {
   return {
      series: [1, 2, 3, 4, 5, 6, 7, 8].map((n) => cssVar(`--series-${n}`)),
      grid: cssVar("--chart-grid"),
      axis: cssVar("--text-secondary"),
      link: cssVar("--drill-link"),
      font: cssVar("--font-sans"),
   };
}

// Keyed by canvas, and weakly: a tile swaps its canvas out when an empty state
// takes its place, and nothing should keep the detached one alive.
/** Chart.js needs one instance per canvas; drop the old one before drawing. */
const charts = new WeakMap();
/** Teardown for the drill listeners, which outlive their chart otherwise. */
const detachers = new WeakMap();

function release(canvas) {
   charts.get(canvas)?.destroy();
   charts.delete(canvas);
   detachers.get(canvas)?.();
   detachers.delete(canvas);
   // The cursor is set inline while hovering a drillable label, and the canvas
   // outlives the chart: clear it, or a redraw that is no longer drillable keeps
   // promising a click.
   canvas.style.cursor = "";
}

function draw(canvas, config, attach) {
   release(canvas);
   const theme = readTheme();
   Chart.defaults.font.family = theme.font;
   Chart.defaults.font.size = 12;
   Chart.defaults.color = theme.axis;
   const chart = new Chart(canvas, config);
   charts.set(canvas, chart);
   if (attach) detachers.set(canvas, attach(chart));
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
const categoryAxis = (theme, { rotate = true, tickColor } = {}) => ({
   border: { color: theme.grid },
   grid: { display: false },
   ticks: {
      autoSkip: false,
      maxRotation: rotate ? 45 : 0,
      minRotation: 0,
      padding: 4,
      color: tickColor ?? theme.axis,
   },
});

/**
 * A continuous axis — three years of months. These labels are a scale rather
 * than a set of things, so thinning them is right where rotating them is not.
 *
 * No `maxTicksLimit`: Chart.js's auto-skip measures the labels and keeps as many
 * as fit, but a `maxTicksLimit` *replaces* that measurement rather than capping
 * it, forcing its number through whether they fit or not. That is what ran
 * "Jan 2023May 2023Sep 2023" together across the bottom of the trend charts.
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

/**
 * Clickable category labels and bars, for a chart whose dimension carries a
 * `# drill`.
 *
 * Chart.js has no "an axis label was clicked" event, so this hit-tests the index
 * scale's own box — the strip its labels are drawn in — and maps the pointer to
 * a tick. A click on the bar itself resolves to the same index through Chart.js's
 * element lookup, so the label and its bar behave alike; a reader who sees one
 * light up will try the other.
 *
 * Canvas text takes no CSS, so unlike a drillable table cell the affordance has
 * to be painted: the tick under the pointer is redrawn in the same link color,
 * and the cursor turns to a pointer over both label and bar.
 *
 * Only category axes get this. On a time axis auto-skip drops ticks, so a tick
 * index no longer identifies a row — and a thinned-out month label is not
 * something to click anyway.
 *
 * The listeners are the canvas's own rather than Chart.js's `onClick`/`onHover`,
 * which fire only for the plot area: the axis strip is outside it, so the labels
 * this exists for would never report a click.
 */
function drillableAxis(theme, indexAxis, onPick) {
   if (!onPick) return { tickColor: undefined, attach: undefined };
   let hovered = null;

   /**
    * Canvas-relative CSS pixels, which is the space Chart.js's scales are in.
    * The canvas carries no border or padding, so the bounding rect is the whole
    * conversion; its backing store is devicePixelRatio-scaled but its layout box
    * is not.
    */
   const positionOf = (chart, event) => {
      const rect = chart.canvas.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
   };

   /** The row under the pointer, and whether it was found on the label strip. */
   const hitTest = (chart, event) => {
      const { x, y } = positionOf(chart, event);
      const scale = chart.scales[indexAxis];
      const count = chart.data.labels?.length ?? 0;
      if (
         scale &&
         x >= scale.left &&
         x <= scale.right &&
         y >= scale.top &&
         y <= scale.bottom
      ) {
         const index = scale.getValueForPixel(indexAxis === "x" ? x : y);
         const inRange = Number.isInteger(index) && index >= 0 && index < count;
         return { index: inRange ? index : null, onAxis: true };
      }
      const [hit] = chart.getElementsAtEventForMode(
         event,
         "nearest",
         { intersect: true },
         true,
      );
      return { index: hit ? hit.index : null, onAxis: false };
   };

   const highlight = (chart, index) => {
      if (index === hovered) return;
      hovered = index;
      // 'none': repaint the tick in its new color without re-running the bar
      // animation on every pointer move. A plain render would not do it — the
      // scale caches its label items until an update clears them.
      chart.update("none");
   };

   return {
      tickColor: (ctx) => (ctx.index === hovered ? theme.link : theme.axis),
      attach(chart) {
         const canvas = chart.canvas;
         const onMove = (event) => {
            const { index, onAxis } = hitTest(chart, event);
            canvas.style.cursor = index === null ? "" : "pointer";
            // Only a label is repainted on hover. A bar has its own hover color
            // and its own tooltip, and redrawing the chart under an open tooltip
            // makes it flicker.
            highlight(chart, onAxis ? index : null);
         };
         const onLeave = () => {
            canvas.style.cursor = "";
            highlight(chart, null);
         };
         const onClick = (event) => {
            const { index } = hitTest(chart, event);
            if (index !== null) onPick(index, event);
         };
         canvas.addEventListener("mousemove", onMove);
         canvas.addEventListener("mouseleave", onLeave);
         canvas.addEventListener("click", onClick);
         return () => {
            canvas.removeEventListener("mousemove", onMove);
            canvas.removeEventListener("mouseleave", onLeave);
            canvas.removeEventListener("click", onClick);
         };
      },
   };
}

const tooltip = (format) => ({
   callbacks: {
      label: (item) => ` ${formatValue(item.parsed.y ?? item.parsed.x, format)}`,
   },
});

/**
 * A bar chart. `horizontal` is for long category labels — a state name reads
 * better beside its bar than rotated under it. `onPick(index, event)` makes the
 * labels and bars drill; leave it off and the chart is inert.
 */
export function barChart(
   canvas,
   { labels, values, format, horizontal = false, onPick },
) {
   const theme = readTheme();
   const indexAxis = horizontal ? "y" : "x";
   const { tickColor, attach } = drillableAxis(theme, indexAxis, onPick);
   draw(
      canvas,
      {
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
            indexAxis,
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false }, tooltip: tooltip(format) },
            scales: horizontal
               ? {
                    x: valueAxis(theme, format),
                    y: categoryAxis(theme, { rotate: false, tickColor }),
                 }
               : {
                    y: valueAxis(theme, format),
                    x: categoryAxis(theme, { tickColor }),
                 },
         },
      },
      attach,
   );
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

export const monthOfYearLabels = monthOfYearLabel;

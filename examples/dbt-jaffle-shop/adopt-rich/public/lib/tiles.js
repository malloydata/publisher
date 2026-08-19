import { money, percent, count, month } from "./format.js";
import { lineChart, barChart, table } from "./charts.js";

const MODEL = "jaffle_shop.malloy";

// Every tile names a view the model defines. Nothing here computes analysis: the page asks a
// named question and renders the answer, which is what a rich model buys.
export const tiles = [
   {
      title: "Headline",
      caption: "dbt's own metrics. Every one of these reconciles against MetricFlow exactly.",
      model: MODEL,
      query: "run: order_items -> { aggregate: revenue, order_gross_profit, order_item_count }",
      render: (rows) => {
         const r = rows[0];
         return `<div class="kpis">
           <div class="kpi"><div class="v">${money(r.revenue)}</div><div class="k">Revenue</div></div>
           <div class="kpi"><div class="v">${money(r.order_gross_profit)}</div><div class="k">Gross profit</div></div>
           <div class="kpi"><div class="v">${count(r.order_item_count)}</div><div class="k">Order lines</div></div>
         </div>`;
      },
   },
   {
      title: "Revenue by month",
      caption: "The model's <code>revenue_by_month</code> view, tagged <code># line_chart</code>.",
      model: MODEL,
      query: "run: order_items -> revenue_by_month",
      render: (rows) => lineChart(rows, "revenue_month", "revenue"),
   },
   {
      title: "Month-over-month growth",
      caption:
         "The model's <code>revenue_growth_mom</code> view. dbt declares this metric but its " +
         "value is an offset window, which a measure cannot hold, so it is a view. It returns a " +
         "fraction; dbt returns percentage points, a 100x difference in the raw value.",
      model: MODEL,
      query: "run: order_items -> revenue_growth_mom",
      render: (rows) =>
         table(rows, [
            { key: "revenue_month", label: "Month", fmt: month },
            { key: "revenue", label: "Revenue", numeric: true, fmt: money },
            { key: "revenue_growth_mom", label: "MoM", numeric: true, fmt: (v) => percent(v) },
         ]),
   },
   {
      title: "Food against drink",
      caption: "Grouped through the declared join to <code>products</code>.",
      model: MODEL,
      query: "run: order_items -> revenue_by_product_type",
      render: (rows) => barChart(rows, "product_type", "revenue", { fmt: money }),
   },
   {
      title: "Top products",
      model: MODEL,
      query: "run: order_items -> top_products",
      render: (rows) =>
         table(rows, [
            { key: "product_name", label: "Product" },
            { key: "revenue", label: "Revenue", numeric: true, fmt: money },
            { key: "order_gross_profit", label: "Gross profit", numeric: true, fmt: money },
         ]),
   },
   {
      title: "Customer cohorts by first-order month",
      wide: true,
      caption:
         "<strong>Read the normalized column.</strong> Lifetime spend falls off for recent " +
         "cohorts because they have had less time to accumulate, not because they are worse. " +
         "The model carries <code>spend_per_tenure_month</code> for exactly this reason, so the " +
         "correction is inherited rather than reapplied per chart.",
      model: MODEL,
      query: "run: customers -> cohorts_by_first_order_month",
      render: (rows) =>
         table(rows, [
            { key: "first_order_month", label: "Cohort", fmt: month },
            { key: "customers", label: "Customers", numeric: true, fmt: count },
            { key: "lifetime_spend", label: "Lifetime spend (confounded)", numeric: true, fmt: money },
            { key: "spend_per_tenure_month", label: "Per tenure month", numeric: true, fmt: money },
         ]),
   },
];

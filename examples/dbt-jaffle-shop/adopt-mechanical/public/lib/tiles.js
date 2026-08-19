import { money, percent, count, month, withMoM } from "./format.js";
import { lineChart, barChart, table } from "./charts.js";

const MODEL = "jaffle_shop.malloy";

// The same six tiles as the rich package, against the mechanically converted model.
//
// Note what changed. The model defines 20 measures and three views, all inherited from dbt's
// saved queries and all grouped by day, so almost every tile here has to write its own Malloy:
// invent the time grouping, know that `product_type` lives across a join, pick a `top:`. The
// month-over-month column is computed in JavaScript because no model entity holds it. And the
// last tile cannot be built at all.
export const tiles = [
   {
      title: "Headline",
      caption: "dbt's own metrics. These reconcile exactly, same as the rich model.",
      model: MODEL,
      query: "run: order_items -> { aggregate: revenue, order_gross_profit }",
      render: (rows) => {
         const r = rows[0];
         return `<div class="kpis">
           <div class="kpi"><div class="v">${money(r.revenue)}</div><div class="k">Revenue</div></div>
           <div class="kpi"><div class="v">${money(r.order_gross_profit)}</div><div class="k">Gross profit</div></div>
         </div>`;
      },
   },
   {
      title: "Revenue by month",
      caption:
         "Written in the page. The model's only revenue view is grouped by <em>day</em>, so the " +
         "monthly grain and its ordering are decided here rather than in the model.",
      model: MODEL,
      query:
         "run: order_items -> { group_by: revenue_month is ordered_at.month; " +
         "aggregate: revenue; order_by: revenue_month }",
      render: (rows) => lineChart(rows, "revenue_month", "revenue"),
   },
   {
      title: "Month-over-month growth",
      caption:
         "Computed in JavaScript. dbt declares a <code>revenue_growth_mom</code> metric, but its " +
         "offset window cannot be a measure and the conversion has nowhere to put it, so it is " +
         "absent from the model. Every consumer that wants it reimplements it, and each one " +
         "decides for itself what a missing prior month means.",
      model: MODEL,
      query:
         "run: order_items -> { group_by: revenue_month is ordered_at.month; " +
         "aggregate: revenue; order_by: revenue_month }",
      render: (rows) =>
         table(withMoM(rows, "revenue"), [
            { key: "revenue_month", label: "Month", fmt: month },
            { key: "revenue", label: "Revenue", numeric: true, fmt: money },
            { key: "mom", label: "MoM (page-computed)", numeric: true, fmt: (v) => percent(v) },
         ]),
   },
   {
      title: "Food against drink",
      caption:
         "Written in the page, and it requires knowing that <code>product_type</code> is reached " +
         "through the join to <code>products</code>. The model states the join; it does not state " +
         "that this is a question worth asking.",
      model: MODEL,
      query:
         "run: order_items -> { group_by: products.product_type; aggregate: revenue; " +
         "order_by: revenue desc }",
      render: (rows) => barChart(rows, "product_type", "revenue", { fmt: money }),
   },
   {
      title: "Top products",
      caption: "Written in the page: the model has no leaderboard view, so the page picks the cutoff.",
      model: MODEL,
      query: "run: order_items -> { top: 10; group_by: product_name; aggregate: revenue }",
      render: (rows) =>
         table(rows, [
            { key: "product_name", label: "Product" },
            { key: "revenue", label: "Revenue", numeric: true, fmt: money },
         ]),
   },
   {
      title: "Customer cohorts by first-order month",
      wide: true,
      caption:
         "The cohort grouping is writable in the page, and the result is <strong>misleading</strong>.",
      model: MODEL,
      query:
         "run: customers -> { group_by: first_order_month is first_ordered_at.month; " +
         "aggregate: customers, lifetime_spend; order_by: first_order_month }",
      render: (rows) =>
         table(rows, [
            { key: "first_order_month", label: "Cohort", fmt: month },
            { key: "customers", label: "Customers", numeric: true, fmt: count },
            { key: "lifetime_spend", label: "Lifetime spend (confounded)", numeric: true, fmt: money },
         ]) +
         `<div class="missing" style="margin-top:12px">
            <strong>The column above is confounded by tenure and there is no correction available
            here.</strong> Lifetime spend drops from about $37,400 for the 2024-09 cohort to about
            $900 for 2025-08 largely because the first has had twelve months to accumulate and the
            second has had one. Normalizing needs a per-customer tenure measure. dbt declares none,
            so the mechanical model has none, and
            <code>run: customers -&gt; { aggregate: spend_per_tenure_month }</code> is refused with
            <code>Query target is not queryable</code>. The rich package carries
            <code>spend_per_tenure_month</code>, which inverts the ranking: 2024-10 is the strongest
            cohort at about $202 per tenure month, and the fall across the year is roughly 31%
            rather than 97%.
          </div>`,
   },
];

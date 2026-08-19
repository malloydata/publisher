// The tabs, as data.
//
// Every tile queries `scoped_orders` from ../../data_app.malloy, the source that
// applies the five givens the control row binds. Nothing below interpolates a
// value into a query string: the filtering is entirely in the givens, so a tile
// is just a view name and how to draw it.
//
// Adding a tile is a row in this file. Adding a filter is a `given:` in
// ../../givens.malloy, an import in data_app.malloy, and a clause naming it in
// `scoped_orders`' `where:`. Neither needs a change to any other file in app/.

const SOURCE = "scoped_orders";
const run = (view) => `run: ${SOURCE} -> ${view}`;

export const TABS = [
   {
      slug: "overview",
      label: "Overview",
      kpis: {
         query: `run: ${SOURCE} -> { aggregate: total_sales, total_margin, order_count, avg_order_value }`,
         colspan: 3,
      },
      tiles: [
         {
            id: "month",
            title: "Revenue by month",
            colspan: 6,
            query: run("sales_by_month"),
            render: "line",
            x: "order_month",
            y: "total_sales",
         },
         {
            id: "state",
            title: "Revenue by state",
            note: "Top 10",
            colspan: 6,
            query: run("sales_by_state + { limit: 10 }"),
            render: "bar",
            horizontal: true,
            x: "state",
            y: "revenue",
         },
         {
            id: "categories",
            title: "Category performance",
            colspan: 12,
            query: run("category_performance"),
            render: "table",
         },
      ],
   },
   {
      slug: "category",
      label: "Category detail",
      kpis: {
         query: `run: ${SOURCE} -> { aggregate: total_sales, margin_rate, return_rate }`,
         colspan: 4,
      },
      tiles: [
         {
            id: "brands-chart",
            title: "Top brands",
            colspan: 6,
            query: run("top_brands"),
            render: "bar",
            x: "brand",
            y: "total_sales",
         },
         {
            id: "brands-table",
            title: "Brand performance",
            colspan: 6,
            query: run("brand_performance"),
            render: "table",
         },
         {
            id: "products",
            title: "Top products",
            colspan: 12,
            query: run("top_products"),
            render: "table",
         },
      ],
   },
   {
      slug: "regions",
      label: "Regions",
      kpis: {
         query: `run: ${SOURCE} -> { aggregate: total_sales, customer_count, orders_per_customer }`,
         colspan: 4,
      },
      tiles: [
         {
            id: "regions-table",
            title: "Revenue by region",
            colspan: 6,
            query: run("sales_by_region"),
            render: "table",
         },
         {
            id: "regions-month",
            title: "Revenue by month",
            colspan: 6,
            query: run("sales_by_month"),
            render: "line",
            x: "order_month",
            y: "total_sales",
         },
         {
            id: "customers",
            title: "Top customers",
            colspan: 12,
            query: run("top_customers"),
            render: "table",
         },
      ],
   },
   {
      slug: "seasonality",
      label: "Seasonality",
      tiles: [
         {
            id: "by-month",
            title: "Revenue by month",
            colspan: 4,
            query: run("sales_by_month"),
            render: "line",
            x: "order_month",
            y: "total_sales",
         },
         {
            id: "by-year",
            title: "Revenue by year",
            colspan: 4,
            query: run("sales_by_year"),
            render: "bar",
            x: "order_year",
            y: "total_sales",
         },
         {
            id: "shape",
            title: "Seasonality by year",
            colspan: 4,
            query: run("seasonality"),
            render: "series",
            x: "month_of_year",
            series: "order_year",
            y: "total_sales",
         },
      ],
   },
];

export const findTab = (slug) => TABS.find((tab) => tab.slug === slug);
export const hasTab = (slug) => findTab(slug) !== undefined;
export const DEFAULT_TAB = TABS[0].slug;

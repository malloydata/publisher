// The tabs, as data.
//
// One tab per dashboard in ../../dashboards/, and each tab's slug IS that
// dashboard's slug — which is what lets a `# drill { to=category }` land here:
// the tag names a dashboard, and this page has a tab of the same name to send it
// to. A tab whose slug no dashboard uses would still work; it just could never be
// a drill destination.
//
// Every tile queries `scoped_orders` from ../../data_app.malloy, the source that
// applies the five givens the control row binds. Nothing below interpolates a
// value into a query string: the filtering is entirely in the givens.
//
// Where a tab differs from its dashboard, the reason is the renderer, not the
// data. Chart.js has no US choropleth, so revenue by state is a bar chart here
// and a `# shape_map` there; and drill lives on tables in both products, so a tab
// carries a table for each dimension it wants clickable.

const SOURCE = "scoped_orders";
const run = (view) => `run: ${SOURCE} -> ${view}`;

export const TABS = [
   {
      slug: "overview",
      label: "Overview",
      // dashboards/overview.malloy
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
      // dashboards/category.malloy
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
      // dashboards/regions.malloy
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
      // dashboards/seasonality.malloy, the composite: three views tiled together
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

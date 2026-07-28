import { expect, test, type Page } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";

/**
 * The storefront HTML data app: a hand-authored page under the package's
 * `public/`, served by Publisher and driven by `Publisher.query`.
 *
 * What is worth pinning here is the part the page does by hand that a dashboard
 * gets from the SDK — one tab per dashboard, a control row built from the
 * model's given declarations, and drill: the affordance, the destination menu,
 * the seeded given, and the URL that carries it. The page has no build step, so
 * a browser is the only place any of it can be checked.
 */

const PAGE = `/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}/index.html`;

/**
 * The slice of the page's vendored Chart.js the chart tests reach into. Types
 * only, so they erase before the callback is serialized into the browser — where
 * `Chart` is the global the page loaded.
 */
type ChartScale = {
   top: number;
   ticks: { label: string }[];
   getPixelForValue(value: number): number;
   getPixelForTick(index: number): number;
};
type ChartGlobal = {
   getChart(canvas: HTMLCanvasElement): {
      ctx: CanvasRenderingContext2D;
      data: { labels: unknown[] };
      scales: Record<string, ChartScale>;
   };
   defaults: { font: { size: number; family: string } };
};

/** Panels for other tabs stay in the DOM, so every query is scoped to the visible one. */
const visible = (page: Page) => page.locator(".panel:not([hidden])");
const kpi = (page: Page, field: string) =>
   visible(page).locator(`[data-kpi="${field}"] .kpi-value`);
const tile = (page: Page, id: string) =>
   visible(page).locator(`[data-tile="${id}"]`);
const activeTab = (page: Page) => page.locator("#tabs button.active");

/** The page is ready when a query has come back and rendered a drillable table. */
async function open(page: Page, query = "") {
   await page.goto(`${PAGE}${query}`);
   await expect(page.locator("td.publisher-drill").first()).toBeVisible();
   await expect(kpi(page, "total_sales")).toHaveText(/^\$[\d,]+$/);
}

test.describe("package-data-app", () => {
   test("has one tab per dashboard in the package", async ({
      page,
      baseURL,
   }) => {
      const response = await fetch(
         `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}/dashboards`,
      );
      const dashboards = (await response.json()) as { name: string }[];
      const slugs = dashboards.map((dashboard) => dashboard.name).sort();

      await open(page);
      const tabs = await page
         .locator("#tabs button")
         .evaluateAll((buttons) =>
            buttons.map((button) => (button as HTMLElement).dataset.tab),
         );
      // Not cosmetic: a `# drill { to=<slug> }` can only land somewhere if the
      // page has a tab of that name.
      expect(tabs.slice().sort()).toEqual(slugs);
   });

   test("renders the control row from the model's given declarations", async ({
      page,
   }) => {
      await open(page);
      // In the order givens.malloy declares them, which is the order the model
      // endpoint returns and the page renders without an opinion of its own.
      await expect(page.locator("#filters .control-label")).toHaveText([
         "Category",
         "Brand",
         "Region",
         "Ordered since",
         "Minimum line total",
      ]);
      // The select's options are the model's own distinct values, not a list
      // written into the page.
      await expect(page.locator("#given-CATEGORY option")).toContainText([
         "All categories",
         "Accessories",
      ]);
   });

   test("switching tabs swaps the panel and the URL", async ({ page }) => {
      await open(page);
      await expect(activeTab(page)).toHaveText("Overview");
      await expect(tile(page, "categories")).toBeVisible();

      await page.locator('#tabs button[data-tab="seasonality"]').click();
      await expect(activeTab(page)).toHaveText("Seasonality");
      await expect(page).toHaveURL(/\?tab=seasonality$/);
      await expect(tile(page, "by-year")).toBeVisible();
      await expect(tile(page, "categories")).toBeHidden();
   });

   test("a drillable cell reads as a link on hover, and an aggregate never does", async ({
      page,
   }) => {
      await open(page);
      const cell = tile(page, "categories")
         .locator("td.publisher-drill")
         .first();

      // At rest it is ordinary text: colored cells would compete with the data.
      await expect(cell).toHaveCSS("cursor", "pointer");
      await expect(cell).toHaveCSS("text-decoration-line", "none");

      await cell.hover();
      await expect(cell).toHaveCSS("text-decoration-line", "underline");
      await expect(cell).toHaveCSS("color", "rgb(37, 99, 235)");

      // `# drill` sits on the category dimension; the measures beside it inherit
      // nothing clickable.
      await expect(
         tile(page, "categories").locator("td.num.publisher-drill"),
      ).toHaveCount(0);
   });

   test("two destinations pop a menu, and choosing one seeds the given", async ({
      page,
   }) => {
      await open(page);
      const cell = tile(page, "categories")
         .locator("td.publisher-drill")
         .first();
      const category = (await cell.textContent()) ?? "";

      await cell.click();
      await expect(page.locator(".drill-menu-label")).toHaveText(
         `${category} \u2192`,
      );
      await expect(page.locator(".drill-menu-item")).toHaveText([
         "Category",
         "Filter this data app",
      ]);

      await page.locator(".drill-menu-item", { hasText: "Category" }).click();
      await expect(activeTab(page)).toHaveText("Category detail");
      await expect(page).toHaveURL(
         new RegExp(
            `\\?tab=category&CATEGORY=${encodeURIComponent(category)}$`,
         ),
      );
      await expect(page.locator("#given-CATEGORY")).toHaveValue(category);
      // The drill narrowed the numbers, and Back undoes it.
      const filtered = await kpi(page, "total_sales").textContent();
      await page.goBack();
      await expect(activeTab(page)).toHaveText("Overview");
      await expect(kpi(page, "total_sales")).not.toHaveText(filtered ?? "");
   });

   test("a single destination filters in place with no menu", async ({
      page,
   }) => {
      await open(page, "?tab=category");
      const cell = tile(page, "brands-table")
         .locator("td.publisher-drill")
         .first();
      const brand = (await cell.textContent()) ?? "";

      await cell.click();
      await expect(page.locator(".drill-menu")).toHaveCount(0);
      await expect(page).toHaveURL(/\?tab=category&BRAND=/);
      await expect(activeTab(page)).toHaveText("Category detail");
      await expect(tile(page, "brands-table").locator("tbody tr")).toHaveCount(
         1,
      );
      await expect(
         tile(page, "brands-table").locator("tbody td").first(),
      ).toHaveText(brand);
   });

   test("a chart's category labels drill the way its table cells do", async ({
      page,
   }) => {
      await open(page, "?tab=category");
      const canvas = tile(page, "brands-chart").locator("canvas");

      // Chart.js owns the pixels, so ask it where the first tick label sits
      // rather than guessing at an offset into the canvas. `scale.top` is the
      // top of the strip the labels are drawn in, which is outside the plot
      // area — the reason the page hit-tests this itself.
      const label = await canvas.evaluate((el: HTMLCanvasElement) => {
         const chart = (
            window as unknown as { Chart: ChartGlobal }
         ).Chart.getChart(el);
         const scale = chart.scales.x;
         return {
            x: scale.getPixelForValue(0),
            y: scale.top + 8,
            text: String(chart.data.labels[0]),
         };
      });
      const at = { position: { x: label.x, y: label.y } };

      await canvas.hover(at);
      await expect(canvas).toHaveCSS("cursor", "pointer");

      await canvas.click(at);
      // `brand` names one destination, so the click acts rather than asking.
      await expect(page.locator(".drill-menu")).toHaveCount(0);
      await expect(page).toHaveURL(
         `${PAGE}?tab=category&BRAND=${encodeURIComponent(label.text)}`,
      );
      await expect(tile(page, "brands-table").locator("tbody tr")).toHaveCount(
         1,
      );
   });

   test("a trend axis thins its labels out rather than overprinting them", async ({
      page,
   }) => {
      await open(page);
      // Three years of months is more ticks than any axis can spell out. What
      // matters is not how many are drawn but that the ones drawn are legible,
      // so this measures them: the gap between two neighbours has to clear the
      // half-width of each.
      const tightest = await tile(page, "month")
         .locator("canvas")
         .evaluate((el: HTMLCanvasElement) => {
            const Chart = (window as unknown as { Chart: ChartGlobal }).Chart;
            const chart = Chart.getChart(el);
            const scale = chart.scales.x;
            const ctx = chart.ctx;
            ctx.save();
            ctx.font = `${Chart.defaults.font.size}px ${Chart.defaults.font.family}`;
            // Only the ticks Chart.js kept: `scale.ticks` is the post-auto-skip
            // set, and `getPixelForTick` indexes into it.
            const drawn = scale.ticks.map((tick, i) => ({
               center: scale.getPixelForTick(i),
               half: ctx.measureText(tick.label).width / 2,
            }));
            ctx.restore();
            const gaps = drawn
               .slice(1)
               .map(
                  (tick, i) =>
                     tick.center - drawn[i].center - tick.half - drawn[i].half,
               );
            return Math.min(...gaps);
         });
      expect(tightest).toBeGreaterThan(0);
   });

   test("a link restores its filters, and Reset clears them", async ({
      page,
   }) => {
      await open(page, "?CATEGORY=Outerwear&MIN_SALE=%3E%3D+100");
      await expect(page.locator("#given-CATEGORY")).toHaveValue("Outerwear");
      await expect(page.locator("#given-MIN_SALE")).toHaveValue("100");
      await expect(tile(page, "categories").locator("tbody tr")).toHaveCount(1);
      const filtered = await kpi(page, "total_sales").textContent();

      await page.locator("#reset").click();
      await expect(page).toHaveURL(PAGE);
      await expect(kpi(page, "total_sales")).not.toHaveText(filtered ?? "");
      await expect(
         tile(page, "categories").locator("tbody tr"),
      ).not.toHaveCount(1);
   });
});

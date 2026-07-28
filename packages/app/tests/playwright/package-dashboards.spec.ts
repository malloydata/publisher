import { expect, test, type Locator, type Page } from "@playwright/test";
import path from "path";
import { fileURLToPath } from "url";
import { tmpName } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * The dashboard viewer, end to end in a browser: the Dashboards section on the
 * package page, the control row the manifest's given specs produce, URL-carried
 * filter state, Apply mode, and the composite tile grid.
 *
 * Runs against its own environment built from the server's dashboards fixture
 * rather than the bundled examples, which declare no dashboards. Registered in
 * `beforeAll` through the same REST call a user would make and removed after,
 * so the suite leaves the server as it found it.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(
   __dirname,
   "../../../server/tests/fixtures/dashboards-test",
);
const PKG = "dashboards-test";

let env: string;
let baseURL: string;

test.describe("package-dashboards", () => {
   // Playwright requires the first argument to be a destructuring pattern even
   // when no fixture is wanted, so the empty pattern is load-bearing here.
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      env = tmpName("dashboards");
      const res = await fetch(`${baseURL}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: env,
            packages: [{ name: PKG, location: FIXTURE }],
            connections: [],
         }),
      });
      test.skip(
         res.status === 405 || res.status === 403,
         "publisher is read-only",
      );
      expect(res.ok, await res.text()).toBe(true);
   });

   test.afterAll(async () => {
      if (!env || !baseURL) return;
      await fetch(`${baseURL}/api/v0/environments/${env}`, {
         method: "DELETE",
      }).catch(() => undefined);
   });

   const openDashboard = async (page: Page, slug: string) => {
      await page.goto(`/${env}/${PKG}/dashboards/${slug}`);
   };

   test("the package page lists dashboards and lists them only once", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, env);
      await openPackage(page, env, PKG);

      await expect(
         page.getByRole("heading", { name: "Dashboards", level: 6 }),
      ).toBeVisible({ timeout: 60_000 });
      await expect(
         page.getByRole("button", { name: /Business Overview/ }),
      ).toBeVisible();

      // A dashboard belongs in the Dashboards section, not a second time under
      // Semantic Models where clicking it would open the Explorer instead.
      await expect(
         page.getByRole("button", { name: /dashboards\/overview\.malloy/ }),
      ).toHaveCount(0);
      // An untagged include in dashboards/ is not a dashboard and stays a model.
      await expect(
         page.getByRole("button", { name: /dashboards\/_shared\.malloy/ }),
      ).toBeVisible();

      await page.getByRole("button", { name: /Business Overview/ }).click();
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/overview$`),
      );
   });

   test("lists notebooks by title, the way it lists dashboards", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, env);
      await openPackage(page, env, PKG);

      await expect(
         page.getByRole("heading", { name: "Notebooks", level: 6 }),
      ).toBeVisible({ timeout: 60_000 });

      // An explicit `## title=`, and a title read from the first markdown
      // heading. Both rows keep the filename as the secondary label, so the
      // path a reader needs in order to find the file is never lost.
      const titled = page.getByRole("button", {
         name: /Orders in a window/,
      });
      await expect(titled).toBeVisible();
      await expect(titled).toContainText("orders-since.malloynb");
      await expect(page.getByRole("button", { name: /Brands/ })).toContainText(
         "brands.malloynb",
      );

      await titled.click();
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/orders-since.malloynb$`),
      );
   });

   test("renders the title and the control row its given specs describe", async ({
      page,
   }) => {
      await openDashboard(page, "overview");

      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page.getByText("Order health at a glance.")).toBeVisible();

      // `control=select` becomes a combobox, labelled by `# label=` rather than
      // by the given's name.
      await expect(page.getByRole("combobox", { name: "Brand" })).toBeVisible();
      // `range_min`/`range_max` become a slider, and no control appears for the
      // givens this dashboard's query does not reference.
      await expect(
         page.getByRole("slider", { name: "Minimum amount" }),
      ).toBeVisible();
      await expect(page.getByRole("combobox", { name: "Region" })).toHaveCount(
         0,
      );

      await expect(page.locator("canvas, svg, table").first()).toBeVisible({
         timeout: 30_000,
      });
   });

   test("a select is populated by its suggest query and lands in the URL", async ({
      page,
   }) => {
      await openDashboard(page, "overview");

      const brand = page.getByRole("combobox", { name: "Brand" });
      await expect(brand).toBeVisible({ timeout: 30_000 });
      await brand.click();

      // Options come from `suggest { source=orders dimension=brand }`, run
      // through the ordinary query endpoint.
      await expect(page.getByRole("option", { name: "Nike" })).toBeVisible({
         timeout: 30_000,
      });
      await page.getByRole("option", { name: "Nike" }).click();

      // Applied values are URL state, so this view is a shareable link.
      await expect(page).toHaveURL(/[?&]BRAND=Nike/);

      // And that link restores the control on a cold load.
      await page.goto(`/${env}/${PKG}/dashboards/overview?BRAND=Nike`);
      await expect(page.getByRole("combobox", { name: "Brand" })).toHaveValue(
         "Nike",
         { timeout: 30_000 },
      );
   });

   test("autorun=false gets an Apply button and its starting values", async ({
      page,
   }) => {
      await openDashboard(page, "regions");

      await expect(
         page.getByRole("heading", { name: "Orders by region" }),
      ).toBeVisible({ timeout: 30_000 });

      // `# artifact { givens { REGION=f'US' } }` is where the control starts.
      // A multiselect holds its selection as chips, so the input itself stays
      // empty and the chip is what to look for.
      const region = page.locator(".MuiAutocomplete-root", {
         has: page.getByRole("combobox", { name: "Region" }),
      });
      await expect(region.getByText("US", { exact: true })).toBeVisible({
         timeout: 30_000,
      });

      // Nothing to apply until something changes.
      const applyButton = page.getByRole("button", { name: "Apply" });
      await expect(applyButton).toBeVisible();
      await expect(applyButton).toBeDisabled();
   });

   test("a multiselect keeps its list open across picks", async ({ page }) => {
      await openDashboard(page, "regions");

      const region = page.getByRole("combobox", { name: "Region" });
      await expect(region).toBeVisible({ timeout: 30_000 });
      await region.click();
      const options = page.getByRole("option");
      await expect(options.first()).toBeVisible({ timeout: 30_000 });
      const offered = await options.count();

      // Picking a second value should not cost a second click to reopen. MUI's
      // Autocomplete closes on select by default, which is right for the
      // single-value picker next to this one and wrong for this one.
      await options.first().click();
      await expect(page.getByRole("option")).toHaveCount(offered);
   });

   test("a composite dashboard renders one panel per tile", async ({
      page,
   }) => {
      await openDashboard(page, "combined");

      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         { timeout: 30_000 },
      );

      // The control row is the union across tiles; `orders -> totals`
      // references nothing and so contributes none.
      await expect(page.getByRole("combobox", { name: "Brand" })).toBeVisible();
      await expect(
         page.getByRole("combobox", { name: "Region" }),
      ).toBeVisible();

      // One panel per tile, headed by the humanized view name rather than the
      // run expression from `tiles=[…]`, which stays as the heading's tooltip.
      for (const [tile, heading] of [
         ["orders -> by_brand", "By brand"],
         ["orders -> by_region", "By region"],
         ["orders -> totals", "Totals"],
      ]) {
         const title = page.getByText(heading, { exact: true });
         await expect(title).toBeVisible({ timeout: 30_000 });
         await expect(title).toHaveAttribute("title", tile);
      }
   });

   test("the grid lines up: colspans that sum, and a break between the rows", async ({
      page,
   }) => {
      await openDashboard(page, "grid");
      await expect(page.getByRole("heading", { name: "Grid" })).toBeVisible({
         timeout: 30_000,
      });

      // Cards and tiles are the same kind of grid item, which is the point: two
      // cards at 6 and two tiles at 6 have to come out as two rows that end in
      // the same place. What "the dashboard looks janky" reduces to is this
      // failing — a KPI card at its natural width, or a tile flowing in beside
      // the cards because the break is missing.
      const items = page.locator(".dashboard-item");
      await expect(items).toHaveCount(4, { timeout: 30_000 });
      const boxes = await items.evaluateAll((elements) =>
         elements.map((element) => {
            const rect = element.getBoundingClientRect();
            return {
               left: Math.round(rect.left),
               right: Math.round(rect.right),
               top: Math.round(rect.top),
               width: Math.round(rect.width),
            };
         }),
      );

      const rows = [...new Set(boxes.map((box) => box.top))].sort(
         (a, b) => a - b,
      );
      expect(rows).toHaveLength(2);
      for (const top of rows) {
         const row = boxes.filter((box) => box.top === top);
         expect(row).toHaveLength(2);
         // Half the grid each, so the seam between them is in the same place on
         // both rows.
         expect(Math.abs(row[0].width - row[1].width)).toBeLessThanOrEqual(1);
      }
      const [cards, tiles] = rows.map((top) =>
         boxes.filter((box) => box.top === top),
      );
      expect(Math.abs(cards[0].left - tiles[0].left)).toBeLessThanOrEqual(1);
      expect(
         Math.abs(cards.at(-1)!.right - tiles.at(-1)!.right),
      ).toBeLessThanOrEqual(1);
   });

   /**
    * A clicked cell in the rendered result. The renderer owns this DOM, so
    * there is no test id to hang onto — the cell's own text is the handle, and
    * it is scoped to the tile so a value appearing in two tiles is unambiguous.
    * The tile is named by its run expression, which the panel keeps as its
    * heading's tooltip once the heading itself is humanized.
    */
   const cell = (page: Page, tile: string, text: string) =>
      page
         .locator(".MuiPaper-root")
         .filter({ has: page.locator(`[title="${tile}"]`) })
         .getByText(text, { exact: true })
         .first();

   /**
    * The table cell containing `text` — the element the affordance is marked on,
    * one level up from the text node `cell()` returns.
    */
   const valueCell = (page: Page, text: string) =>
      page.locator(".column-cell.td").filter({ hasText: text }).first();

   /** How a cell reads to a user: at rest, and under the pointer. */
   async function readsAs(target: Locator) {
      const at = (locator: Locator) =>
         locator.evaluate((element) => {
            const content = element.querySelector(".cell-content");
            return {
               cursor: getComputedStyle(element).cursor,
               color: content ? getComputedStyle(content).color : "",
               underlined: content
                  ? getComputedStyle(content).textDecorationLine === "underline"
                  : false,
            };
         });
      const resting = await at(target);
      await target.hover();
      // The hover rule is CSS, so it lands on the next style recalculation.
      await target.page().waitForTimeout(250);
      return { resting, hovered: await at(target) };
   }

   // The affordance, which is what tells a reader a cell does anything at all.
   // Asserted on both surfaces because a drill's whole premise is that the tag
   // is declared once on a dimension and behaves the same wherever it is
   // grouped — a notebook cell that navigates but doesn't say so is the same
   // feature only in principle.
   for (const surface of [
      {
         name: "dashboard",
         open: async (page: Page) => {
            await openDashboard(page, "combined");
            await expect(
               page.getByRole("heading", { name: "Combined" }),
            ).toBeVisible({ timeout: 30_000 });
         },
      },
      {
         name: "notebook",
         open: async (page: Page) => {
            await page.goto(`/${env}/${PKG}/brands.malloynb`);
            await expect(
               page.getByRole("heading", { name: "Brands" }),
            ).toBeVisible({ timeout: 60_000 });
         },
      },
   ]) {
      test(`a drillable cell reads as a link in a ${surface.name}, and its neighbours do not`, async ({
         page,
      }) => {
         await surface.open(page);

         // `brand_name` carries `# drill`; `total_amount`, beside it in the same
         // table, does not.
         const drillable = valueCell(page, "Nike");
         await expect(drillable).toBeVisible({ timeout: 30_000 });
         await expect(drillable).toHaveClass(/publisher-drill/);

         const drill = await readsAs(drillable);
         // Ordinary text at rest — a column painted like a link competes with
         // the data — and a link on hover.
         expect(drill.resting.cursor).toBe("pointer");
         expect(drill.resting.underlined).toBe(false);
         expect(drill.hovered.underlined).toBe(true);
         expect(drill.hovered.color).not.toBe(drill.resting.color);

         // The aggregate beside it: same table, same row height, no drill.
         const measure = page.locator(".column-cell.td.numeric").first();
         await expect(measure).not.toHaveClass(/publisher-drill/);
         const plain = await readsAs(measure);
         expect(plain.resting.cursor).not.toBe("pointer");
         expect(plain.hovered.underlined).toBe(false);
         expect(plain.hovered.color).toBe(plain.resting.color);
      });
   }

   test("a single-destination drill navigates with the clicked value seeded", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         {
            timeout: 30_000,
         },
      );

      // `# drill { to=overview given=BRAND }` on the brand_name dimension, which
      // no dashboard declares — the tile is clickable because it groups by it.
      await cell(page, "orders -> by_brand", "Nike").click({ timeout: 30_000 });

      // One destination acts immediately: the slug becomes the route and the
      // clicked value becomes the given.
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/overview\\?BRAND=Nike$`),
      );
      // Arriving seeded means arriving filtered, so the destination's control
      // shows the drilled value.
      await expect(page.getByRole("combobox", { name: "Brand" })).toHaveValue(
         "Nike",
         { timeout: 30_000 },
      );
   });

   test("a two-destination drill offers a menu, and `self` filters in place", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         {
            timeout: 30_000,
         },
      );

      // `# drill { to=["regions", "self"] given=REGION }`: more than one
      // destination is a choice, not a guess.
      await cell(page, "orders -> by_region", "EU").click({ timeout: 30_000 });
      // The destination reads as a sentence rather than as a filename, which is
      // how Malloyyo labels the same menu.
      await expect(page.getByRole("menuitem", { name: "Regions" })).toBeVisible(
         {
            timeout: 30_000,
         },
      );

      // `self` never leaves the page; it sets the control instead.
      await page
         .getByRole("menuitem", { name: "Filter this dashboard" })
         .click();
      await expect(page).toHaveURL(/[?&]REGION=EU/);
      const region = page.locator(".MuiAutocomplete-root", {
         has: page.getByRole("combobox", { name: "Region" }),
      });
      await expect(region.getByText("EU", { exact: true })).toBeVisible({
         timeout: 30_000,
      });
   });

   test("the other menu destination navigates to that dashboard", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         {
            timeout: 30_000,
         },
      );

      await cell(page, "orders -> by_region", "EU").click({ timeout: 30_000 });
      await page.getByRole("menuitem", { name: "Regions" }).click();

      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/regions\\?REGION=EU$`),
      );
      // regions is autorun=false, and a drill arrives applied rather than
      // pending: the point of a drill is to land on the filtered view.
      await expect(
         page.getByRole("heading", { name: "Orders by region" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole("button", { name: "Apply" })).toBeDisabled();
   });

   test("a notebook cell drills into a dashboard", async ({ page }) => {
      // The payoff of declaring drill on the dimension: `brands.malloynb` says
      // nothing about drill, but its cell groups by `brand_name`, so the same
      // click path works with no notebook-specific code.
      await page.goto(`/${env}/${PKG}/brands.malloynb`);
      await expect(page.getByRole("heading", { name: "Brands" })).toBeVisible({
         timeout: 60_000,
      });

      await page
         .getByText("Nike", { exact: true })
         .first()
         .click({ timeout: 60_000 });

      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/overview\\?BRAND=Nike$`),
      );
      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
   });

   // A notebook and a dashboard run the same givens state, the same controls,
   // and the same drill, so the interactivity a reader gets should not depend on
   // which of the two an author reached for. These are the dashboard tests
   // above, aimed at a notebook.
   test("a notebook's parameters are URL state, batched behind Apply", async ({
      page,
   }) => {
      // `orders-since.malloynb` carries `## autorun=false`, the notebook
      // spelling of the dashboard's `# artifact { autorun=false }`.
      await page.goto(`/${env}/${PKG}/orders-since.malloynb`);
      await expect(
         page.getByRole("heading", { name: "Orders since" }),
      ).toBeVisible({ timeout: 60_000 });

      const applyButton = page.getByRole("button", { name: "Apply" });
      await expect(applyButton).toBeVisible();
      await expect(applyButton).toBeDisabled();

      // The notebook's one cell counts orders on or after SINCE, which defaults
      // to 2024-01-01 — all six of them. Deliberately not `.first()`: the count
      // is the only bare number on the page, so a second match means this is
      // matching something other than the result, and should fail.
      const count = (n: string) => page.getByText(n, { exact: true });
      await expect(count("6")).toBeVisible({ timeout: 30_000 });

      // The control's label comes from the given declaration, which is where a
      // dashboard's comes from too.
      await page
         .getByRole("textbox", { name: "Ordered since" })
         .fill("03/01/2024");

      // Pending, not applied: the URL is untouched and the cell has not re-run,
      // which is the whole point of batching.
      await expect(applyButton).toBeEnabled();
      expect(page.url()).not.toContain("SINCE");
      await expect(count("6")).toBeVisible();

      await applyButton.click();
      await expect(page).toHaveURL(/[?&]SINCE=2024-03-01/);
      // Two of the six are on or after 2024-03-01. Getting here at all is the
      // date codec working: a full ISO timestamp is rejected with a 400.
      await expect(count("2")).toBeVisible({ timeout: 30_000 });
      await expect(count("6")).toBeHidden();

      // And the link restores the value on a cold load.
      await page.goto(`/${env}/${PKG}/orders-since.malloynb?SINCE=2024-03-01`);
      await expect(
         page.getByRole("textbox", { name: "Ordered since" }),
      ).toHaveValue("03/01/2024", { timeout: 30_000 });
   });

   test("a notebook starts where `## givens` says, and a URL beats it", async ({
      page,
   }) => {
      // `orders-start.malloynb` carries `## givens { SINCE="2024-03-01" }`, the
      // notebook spelling of a dashboard's `# artifact { givens { … } }`.
      await page.goto(`/${env}/${PKG}/orders-start.malloynb`);
      const since = page.getByRole("textbox", { name: "Ordered since" });
      await expect(since).toHaveValue("03/01/2024", { timeout: 60_000 });

      // Applied, not merely displayed: two of the six orders are in the window.
      const count = (n: string) => page.getByText(n, { exact: true });
      await expect(count("2")).toBeVisible({ timeout: 30_000 });

      // And written into the URL, so what a reader copies out of the address bar
      // is what they are looking at. A dashboard's starting values do the same
      // (`dashboards/regions?REGION=US`).
      await expect(page).toHaveURL(/[?&]SINCE=2024-03-01/);

      // A link overrides the file, so a shared URL shows the sender's view.
      await page.goto(`/${env}/${PKG}/orders-start.malloynb?SINCE=2024-01-01`);
      await expect(since).toHaveValue("01/01/2024", { timeout: 60_000 });
      await expect(count("6")).toBeVisible({ timeout: 30_000 });
   });

   test("a notebook cell drills into the notebook itself", async ({ page }) => {
      await page.goto(`/${env}/${PKG}/brands.malloynb`);
      await expect(page.getByRole("heading", { name: "Brands" })).toBeVisible({
         timeout: 60_000,
      });

      // Same tag, same menu as on the `combined` dashboard: `to=["regions",
      // "self"]`, where self means this document.
      await page
         .getByText("EU", { exact: true })
         .first()
         .click({ timeout: 60_000 });
      // The surface names itself: a notebook filtering itself is not "this
      // dashboard", though the tag it came from is the same one.
      await page
         .getByRole("menuitem", { name: "Filter this notebook" })
         .click();

      // Filtering in place is a URL change, not a navigation: still the
      // notebook, now with the given set.
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/brands\\.malloynb\\?REGION=EU$`),
      );
      const region = page.locator(".MuiAutocomplete-root", {
         has: page.getByRole("combobox", { name: "Region" }),
      });
      await expect(region.getByText("EU", { exact: true })).toBeVisible({
         timeout: 30_000,
      });
   });

   test("a cell with no drill tag is not clickable", async ({ page }) => {
      await openDashboard(page, "combined");
      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         {
            timeout: 30_000,
         },
      );

      // `orders -> totals` groups by nothing, so its measures carry no drill
      // tag. Clicking one must leave the page exactly where it was.
      const before = page.url();
      await cell(page, "orders -> totals", "6").click({ timeout: 30_000 });
      await expect(page.getByRole("menuitem")).toHaveCount(0);
      expect(page.url()).toBe(before);
   });

   // A dashboard renders from its tags, in the page. Publisher runs no
   // author-written dashboard component, so nothing here should ever be framed
   // — see docs/malloyyo-dashboards-design.md §"Custom JSX components".
   test("renders in the page, with no iframe", async ({ page }) => {
      await openDashboard(page, "overview");
      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page.locator("iframe")).toHaveCount(0);
   });
});

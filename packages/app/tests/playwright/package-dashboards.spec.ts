// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Locator, type Page } from "@playwright/test";
import path from "path";
import { fileURLToPath } from "url";
import {
   closeFilter,
   expectFilterValue,
   filterButton,
   openFilter,
} from "./helpers/filters";
import { tmpName } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * The dashboard viewer, end to end in a browser: the Reports section on the
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
/**
 * Linear `.malloy` reports beside copies of the orders model, its givens and
 * the two dashboards its drills name: Apply batching, starting givens, and
 * drill from a `run:` cell. A package of its own, so the dashboards-test
 * listings other specs pin stay as they are.
 */
const LINEAR_FIXTURE = path.resolve(
   __dirname,
   "../../../server/tests/fixtures/linear-reports-ui",
);
const LINEAR_PKG = "linear-reports-ui";

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
            packages: [
               { name: PKG, location: FIXTURE },
               { name: LINEAR_PKG, location: LINEAR_FIXTURE },
            ],
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

   test("the package page lists dashboards and notebooks as artifacts, each once, with New on the heading row", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, env);
      await openPackage(page, env, PKG);

      await expect(
         page.getByRole("heading", { name: "Reports", level: 6, exact: true }),
      ).toBeVisible({ timeout: 60_000 });
      await expect(
         page.getByRole("heading", { name: "Dashboards", exact: true }),
      ).toHaveCount(0);
      await expect(
         page.getByRole("heading", { name: "Notebooks", exact: true }),
      ).toHaveCount(0);
      // One list: a dashboard row and a notebook row, each once, by name.
      const artifacts = page.getByRole("region", {
         name: "Reports",
         exact: true,
      });
      await expect(
         artifacts.getByRole("button", { name: /Business Overview/ }),
      ).toHaveCount(1);
      await expect(
         artifacts.getByRole("button", { name: /Orders in a window/ }),
      ).toHaveCount(1);
      await expect(
         artifacts.getByRole("button", { name: "Add report", exact: true }),
      ).toBeVisible();
      await expect(
         page.getByRole("button", { name: /Business Overview/ }),
      ).toBeVisible();

      // A report belongs in the Reports section, not a second time under
      // Models where clicking it would open the Explorer instead. (Its
      // row in Reports shows the path, so the check is scoped to the models.)
      const models = page.getByRole("region", { name: "Models", exact: true });
      await expect(
         models.getByRole("button", { name: /dashboards\/_shared\.malloy/ }),
      ).toBeVisible();
      await expect(
         models.getByRole("button", { name: /dashboards\/overview\.malloy/ }),
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

   test("both lists are ordered by the name they show", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, env);
      await openPackage(page, env, PKG);
      await expect(
         page.getByRole("heading", { name: "Reports", level: 6, exact: true }),
      ).toBeVisible({ timeout: 60_000 });

      // Polled rather than snapshotted once: the rows arrive after the section
      // heading does, and a single `allTextContents()` read them mid-render and
      // reported a row as missing rather than as out of order.
      const positions = async (names: string[]) => {
         const labels = await page.getByRole("button").allTextContents();
         return names.map((name) => labels.findIndex((l) => l.includes(name)));
      };
      const ascending = (xs: number[]) =>
         xs.every((x, i) => x >= 0 && (i === 0 || x > xs[i - 1]));

      // Sorted by the string on screen, not by the slug underneath it. By slug
      // this reads Combined, Grid, Business Overview, Orders by region, which
      // looks unsorted to anyone reading the titles.
      await expect
         .poll(
            async () =>
               ascending(
                  await positions([
                     "Business Overview",
                     "Combined",
                     "Grid",
                     "Orders by region",
                  ]),
               ),
            { timeout: 30_000 },
         )
         .toBe(true);

      // Same for notebooks, which this release started labelling by title while
      // still ordering them by filename: by path `orders-since` precedes
      // `orders-start`, but "Orders from a start" precedes "Orders in a window".
      await expect
         .poll(
            async () =>
               ascending(
                  await positions([
                     "Brands",
                     "Orders from a start",
                     "Orders in a window",
                  ]),
               ),
            { timeout: 30_000 },
         )
         .toBe(true);
   });

   test("renders the title and the control row its given specs describe", async ({
      page,
   }) => {
      await openDashboard(page, "overview");

      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page.getByText("Order health at a glance.")).toBeVisible();

      // One button per filter, named by `# label=` rather than by the given's
      // name. `control=select` opens a combobox.
      const brand = await openFilter(page, "Brand");
      await expect(
         brand.getByRole("combobox", { name: "Brand" }),
      ).toBeVisible();
      await closeFilter(page);
      // `range_min`/`range_max` open a two-handled range slider, and no
      // control appears for the givens this dashboard's query does not
      // reference.
      const amount = await openFilter(page, "Minimum amount");
      await expect(
         amount.getByRole("slider", { name: "Minimum amount from" }),
      ).toBeVisible();
      await expect(
         amount.getByRole("slider", { name: "Minimum amount to" }),
      ).toBeVisible();
      await closeFilter(page);
      await expect(filterButton(page, "Region")).toHaveCount(0);

      await expect(page.locator("canvas, svg, table").first()).toBeVisible({
         timeout: 30_000,
      });
   });

   test("a select is populated by its suggest query and lands in the URL", async ({
      page,
   }) => {
      await openDashboard(page, "overview");

      const filter = await openFilter(page, "Brand");
      await filter.getByRole("combobox", { name: "Brand" }).click();

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
      await expectFilterValue(page, "Brand", "Nike");
   });

   test("an unrelated query parameter survives load and a control change", async ({
      page,
   }) => {
      // The page's query string is not the dashboard's alone. Replacing it
      // wholesale dropped anything else on LOAD, as soon as a manifest carrying a
      // declared given arrived, and did it inconsistently: on a warm cache the
      // first report never fired and the parameter survived, so whether a shared
      // link kept its tracking tag depended on whether the reader had opened that
      // dashboard before.
      await page.goto(
         `/${env}/${PKG}/dashboards/overview?BRAND=Nike&utm_campaign=spring`,
      );
      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });

      await expect(page).toHaveURL(/[?&]utm_campaign=spring/);
      await expect(page).toHaveURL(/[?&]BRAND=Nike/);

      // And it still survives once a control writes to the query string.
      const filter = await openFilter(page, "Brand");
      await filter.getByRole("combobox", { name: "Brand" }).click();
      await page.getByRole("option", { name: "Levi's" }).click({
         timeout: 30_000,
      });
      await expect(page).toHaveURL(/[?&]BRAND=Levi/);
      await expect(page).toHaveURL(/[?&]utm_campaign=spring/);
   });

   test("clearing a control removes only its own parameter", async ({
      page,
   }) => {
      // The DELETE half of the merge, which had no test at all: every other
      // dashboard test only ever ADDS or changes a value, and a merge that never
      // deletes passes all of them while leaving a cleared control's parameter
      // stuck in the address bar, silently re-applying to anyone opening the
      // link.
      await page.goto(
         `/${env}/${PKG}/dashboards/overview?BRAND=Nike&utm_campaign=spring`,
      );
      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page).toHaveURL(/[?&]BRAND=Nike/);

      // A set filter's button carries its own clear (×).
      await page.getByRole("button", { name: "Clear Brand" }).click();

      // Its own parameter goes...
      await expect(page).not.toHaveURL(/[?&]BRAND=/);
      // ...and the one that was never the dashboard's stays.
      await expect(page).toHaveURL(/[?&]utm_campaign=spring/);
   });

   test("a select control looks like one", async ({ page }) => {
      // MUI hides the dropdown arrow whenever `freeSolo` is set, which every
      // given control needs so a reader can type a value the suggest query did
      // not offer. Without `forcePopupIcon` the control renders as a bare text
      // box, so nothing says it has options: a reader-reported complaint, and it
      // had no test until a mutation sweep pointed out that both select tests
      // click the control directly and would pass with the arrow gone.
      await openDashboard(page, "overview");
      const filter = await openFilter(page, "Brand");
      const brand = filter.locator(".MuiAutocomplete-root", {
         has: page.getByRole("combobox", { name: "Brand" }),
      });
      await expect(brand).toBeVisible();
      await expect(
         brand.locator(".MuiAutocomplete-popupIndicator"),
      ).toHaveCount(1);
   });

   test("autorun=false gets an Apply button and its starting values", async ({
      page,
   }) => {
      await openDashboard(page, "regions");

      await expect(
         page.getByRole("heading", { name: "Orders by region" }),
      ).toBeVisible({ timeout: 30_000 });

      // `# artifact { givens { REGION=f'US' } }` is where the control starts,
      // and the filter's button says so.
      await expectFilterValue(page, "Region", "US");

      // Nothing to apply until something changes.
      const applyButton = page.getByRole("button", { name: "Apply" });
      await expect(applyButton).toBeVisible();
      await expect(applyButton).toBeDisabled();
   });

   test("a multiselect keeps its list open across picks", async ({ page }) => {
      await openDashboard(page, "regions");

      const filter = await openFilter(page, "Region");
      await filter.getByRole("combobox", { name: "Region" }).click();
      const options = page.getByRole("option");
      await expect(options.first()).toBeVisible({ timeout: 30_000 });
      const offered = await options.count();

      // Picking a second value should not cost a second click to reopen. MUI's
      // Autocomplete closes on select by default, which is right for the
      // single-value picker next to this one and wrong for this one.
      await options.first().click();
      await expect(page.getByRole("option")).toHaveCount(offered);
   });

   test("a grid report spanning views renders one panel per tile, with the union of their controls", async ({
      page,
   }) => {
      await openDashboard(page, "combined");

      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         { timeout: 30_000 },
      );

      // The control row is the union across tiles; `orders -> totals`
      // references nothing and so contributes none.
      await expect(filterButton(page, "Brand")).toBeVisible();
      await expect(filterButton(page, "Region")).toBeVisible();

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

   test("the grid lines up: two rows of two, each pair equal and flush", async ({
      page,
   }) => {
      await openDashboard(page, "tiled");
      await expect(
         page.getByRole("heading", { name: "Tiled", level: 1 }),
      ).toBeVisible({ timeout: 30_000 });

      // Cards and tiles are the same kind of grid item, which is the point: two
      // cards at 6 and two tiles at 6 have to come out as two rows that end in
      // the same place. What "the dashboard looks janky" reduces to is this
      // failing, a KPI card at its natural width or a row that does not reach
      // the same edge as the one above it.
      //
      // It does NOT pin the `# break`, despite what this test used to be called.
      // Measured: removing the break from the fixture changes nothing, because
      // two cards at 6 already fill a 12-column row and the next item wraps on
      // its own. That is the documented rule ("a break is for interrupting a row
      // that would otherwise be shared"), so pinning the break needs a fixture
      // whose first row does not fill, which this one deliberately is not.
      const items = page.locator(".MuiPaper-root:has(.malloy-render)");

      // Retried as a block, because waiting for the four items to EXIST is not
      // waiting for them to be laid out. The grid items are in the DOM before
      // their results render, and in that window all four report `top: 0`, so a
      // measurement taken then sees one row rather than two and the test fails
      // against a page that is about to be correct. Observed at roughly one run
      // in four by the slice stacked on this one, whose failure output showed
      // exactly that: four items, every top zero.
      //
      // `toPass` rather than a sleep or a height gate: the assertions below are
      // unchanged and still have to hold, so a genuinely broken grid still
      // fails, it just gets the layout a chance to settle first. A height gate
      // would be a second rule about when the grid is ready, and this file
      // would then own two of them.
      await expect(async () => {
         await expect(items).toHaveCount(4);
         const boxes = await items.evaluateAll((elements) =>
            elements.map((element) => {
               const rect = element.getBoundingClientRect();
               return {
                  left: Math.round(rect.left),
                  right: Math.round(rect.right),
                  top: Math.round(rect.top),
                  width: Math.round(rect.width),
                  height: Math.round(rect.height),
               };
            }),
         );

         // The precondition the old version left implicit. A collapsed item has
         // no height, and its top is whatever the container's edge happens to
         // be, which is what made every row look like row one.
         for (const box of boxes) expect(box.height).toBeGreaterThan(0);

         const rows = [...new Set(boxes.map((box) => box.top))].sort(
            (a, b) => a - b,
         );
         expect(rows).toHaveLength(2);
         for (const top of rows) {
            const row = boxes.filter((box) => box.top === top);
            expect(row).toHaveLength(2);
            // Half the grid each, so the seam between them is in the same place
            // on both rows.
            expect(Math.abs(row[0].width - row[1].width)).toBeLessThanOrEqual(
               1,
            );
         }
         const [cards, tiles] = rows.map((top) =>
            boxes.filter((box) => box.top === top),
         );
         expect(Math.abs(cards[0].left - tiles[0].left)).toBeLessThanOrEqual(1);
         expect(
            Math.abs(cards.at(-1)!.right - tiles.at(-1)!.right),
         ).toBeLessThanOrEqual(1);
      }).toPass({ timeout: 30_000 });
   });

   test("a short table fills its card beside a chart, with no filter warning", async ({
      page,
   }) => {
      await openDashboard(page, "tiled");
      const heading = page.locator('[title="tiles -> brand_tile"]');
      await expect(heading).toBeVisible({ timeout: 30_000 });
      const card = page
         .locator('[data-chrome="card"]')
         .filter({ has: heading });
      const table = card.locator(".malloy-table.root");
      await expect(table).toBeVisible({ timeout: 30_000 });
      const chartCard = page
         .locator('[data-chrome="card"]')
         .filter({ has: page.locator('[title="tiles -> region_tile"]') });
      await expect(
         chartCard.locator('[data-malloy-sizing="container"]'),
      ).toBeVisible({
         timeout: 30_000,
      });

      // The card pads its body, so the table ends one padding short of its edge.
      await expect(async () => {
         const [cardBox, tableBox, chartBox] = await Promise.all([
            card.boundingBox(),
            table.boundingBox(),
            chartCard.boundingBox(),
         ]);
         const padding = await card.evaluate((el) =>
            parseFloat(getComputedStyle(el).paddingBottom),
         );
         expect(
            Math.abs(cardBox!.height - chartBox!.height),
         ).toBeLessThanOrEqual(1);
         expect(
            cardBox!.y +
               cardBox!.height -
               padding -
               (tableBox!.y + tableBox!.height),
         ).toBeLessThanOrEqual(3);
      }).toPass({ timeout: 30_000 });

      // Every tile reads BRAND through the extension's own `where:`, so none warns.
      await expect(page.getByTestId("tile-filter-tag")).toHaveCount(0);
   });

   test("a tile that ignores a filter says so above its result", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      const card = page
         .locator('[data-chrome="card"]')
         .filter({ has: page.locator('[title="orders -> by_brand"]') });
      const table = card.locator(".malloy-table.root");
      await expect(table).toBeVisible({ timeout: 30_000 });

      const tag = card.getByTestId("tile-filter-tag");
      await expect(tag).toHaveText("Doesn't respond to Region");
      const tagBox = (await tag.boundingBox())!;
      const tableBox = (await table.boundingBox())!;
      expect(tagBox.y + tagBox.height).toBeLessThanOrEqual(tableBox.y);
   });

   /**
    * A clicked cell in the rendered result. The renderer owns this DOM, so
    * there is no test id to hang onto: the cell's own text is the handle, and
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
    * The table cell containing `text`, the element the affordance is marked on,
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
   // grouped: a report cell that navigates but doesn't say so is the same
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
         name: "linear report",
         open: async (page: Page) => {
            await page.goto(`/${env}/${LINEAR_PKG}/brands`);
            await expect(
               page.getByRole("heading", { name: "Brands", level: 1 }).first(),
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
         // Ordinary text at rest (a column painted like a link competes with
         // the data), and a link on hover.
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

   test("a single-destination drill says where it goes, and navigates with the clicked value seeded", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      await expect(page.getByRole("heading", { name: "Combined" })).toBeVisible(
         {
            timeout: 30_000,
         },
      );

      // `# drill { to="dashboards/overview" given=BRAND }` on the brand_name dimension, which
      // no dashboard declares: the tile is clickable because it groups by it.
      await cell(page, "orders -> by_brand", "Nike").click({ timeout: 30_000 });

      // Even one destination is a menu: a click says what it will do before
      // doing it, each item naming the value, and the rows behind the value
      // are offered beside it. No header row: every entry is an action.
      await expect(
         page.getByRole("menuitem", { name: "Open Overview for Nike" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(
         page.getByRole("menuitem", { name: /^Show rows for Nike/ }),
      ).toBeVisible();
      await expect(page.getByText("Nike →")).toHaveCount(0);
      await page
         .getByRole("menuitem", { name: "Open Overview for Nike" })
         .click();

      // The slug becomes the route and the clicked value becomes the given.
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/overview\\?BRAND=Nike$`),
      );
      // Arriving seeded means arriving filtered, so the destination's control
      // shows the drilled value.
      await expectFilterValue(page, "Brand", "Nike");
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

      // `# drill { to=["dashboards/regions", "self"] given=REGION }`: more than one
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
      await page.getByRole("menuitem", { name: "Filter this report" }).click();
      await expect(page).toHaveURL(/[?&]REGION=EU/);
      await expectFilterValue(page, "Region", "EU");
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

   // Distinct from the test above, which drills with nothing applied. That case
   // cannot show this bug: the hook suppresses a report that repeats what it last
   // reported, and with no given applied both are empty. With one applied, the
   // destination's manifest is briefly undefined, so the hook reports "no values"
   // while still holding the PREVIOUS dashboard's record, and a host that trusts
   // that report clears the given the drill just seeded.
   test("a drill keeps its seeded given when one was already applied", async ({
      page,
   }) => {
      await openDashboard(page, "combined");

      const filter = await openFilter(page, "Brand");
      await filter.getByRole("combobox", { name: "Brand" }).click();
      await page
         .getByRole("option", { name: "Nike" })
         .click({ timeout: 30_000 });
      await expect(page).toHaveURL(/[?&]BRAND=Nike/);
      // The popover hides the page from the pointer and from the
      // accessibility tree until it closes.
      await closeFilter(page);

      await cell(page, "orders -> by_region", "EU").click({ timeout: 30_000 });
      await page.getByRole("menuitem", { name: "Regions" }).click();

      const seeded = new RegExp(
         `/${env}/${PKG}/dashboards/regions\\?REGION=EU$`,
      );
      await expect(page).toHaveURL(seeded);
      // Asserted again AFTER the destination has rendered, because the first
      // assertion can pass on the pushed URL and `toHaveURL` stops looking once
      // it matches. The wipe lands a commit later, so only a check past the
      // destination's own paint can see it.
      await expect(
         page.getByRole("heading", { name: "Orders by region" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page).toHaveURL(seeded);
      // The value has to reach the control too, not just survive in the URL.
      await expectFilterValue(page, "Region", "EU");
   });

   test("a linear report's cell drills into a dashboard", async ({ page }) => {
      // The payoff of declaring drill on the dimension: `brands.malloy` says
      // nothing about drill, but its cell groups by `brand_name`, so the same
      // click path works with no report-specific code.
      await page.goto(`/${env}/${LINEAR_PKG}/brands`);
      await expect(
         page.getByRole("heading", { name: "Brands", level: 1 }).first(),
      ).toBeVisible({ timeout: 60_000 });

      await page
         .getByText("Nike", { exact: true })
         .first()
         .click({ timeout: 60_000 });
      await page.getByRole("menuitem", { name: "Open Overview" }).click();

      await expect(page).toHaveURL(
         new RegExp(`/${env}/${LINEAR_PKG}/dashboards/overview\\?BRAND=Nike$`),
      );
      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
   });

   // A linear report and a grid run the same givens state, the same controls,
   // and the same drill, so the interactivity a reader gets should not depend
   // on which layout an author reached for. These are the grid tests above,
   // aimed at a linear report.
   test("a linear report's parameters are URL state, batched behind Apply", async ({
      page,
   }) => {
      // `orders-since.malloy` carries `autorun=false` in its artifact tag.
      await page.goto(`/${env}/${LINEAR_PKG}/orders-since`);
      await expect(
         page.getByRole("heading", { name: "Orders since" }),
      ).toBeVisible({ timeout: 60_000 });

      const applyButton = page.getByRole("button", { name: "Apply" });
      await expect(applyButton).toBeVisible();
      await expect(applyButton).toBeDisabled();

      // The report's one cell counts orders on or after SINCE, which defaults
      // to 2024-01-01, all six of them. Deliberately not `.first()`: the count
      // is the only bare number on the page, so a second match means this is
      // matching something other than the result, and should fail.
      const count = (n: string) => page.getByText(n, { exact: true });
      await expect(count("6")).toBeVisible({ timeout: 30_000 });

      // The control's label comes from the given declaration, which is where a
      // grid's comes from too.
      const since = await openFilter(page, "Ordered since");
      await since
         .getByRole("textbox", { name: "Ordered since" })
         .fill("03/01/2024");
      await closeFilter(page);

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
      await page.goto(`/${env}/${LINEAR_PKG}/orders-since?SINCE=2024-03-01`);
      await expectFilterValue(page, "Ordered since", "Mar 1, 2024");
   });

   test("a linear report starts where its `givens { … }` say, and a URL beats it", async ({
      page,
   }) => {
      // `orders-start.malloy` carries `givens { SINCE=@2024-03-01 }` in its tag.
      await page.goto(`/${env}/${LINEAR_PKG}/orders-start`);
      await expectFilterValue(page, "Ordered since", "Mar 1, 2024", 60_000);

      // Applied, not merely displayed: two of the six orders are in the window.
      const count = (n: string) => page.getByText(n, { exact: true });
      await expect(count("2")).toBeVisible({ timeout: 30_000 });

      // A starting value is the file's, not the reader's, so it stays out of
      // the URL, in either layout; only a value the reader sets is written there.
      expect(page.url()).not.toContain("SINCE=");

      // A link overrides the file, so a shared URL shows the sender's view.
      await page.goto(`/${env}/${LINEAR_PKG}/orders-start?SINCE=2024-01-01`);
      await expectFilterValue(page, "Ordered since", "Jan 1, 2024", 60_000);
      await expect(count("6")).toBeVisible({ timeout: 30_000 });
   });

   test("a linear report's cell drills into the report itself", async ({
      page,
   }) => {
      await page.goto(`/${env}/${LINEAR_PKG}/brands`);
      await expect(
         page.getByRole("heading", { name: "Brands", level: 1 }).first(),
      ).toBeVisible({ timeout: 60_000 });

      // Same tag, same menu as on the `combined` dashboard: `to=["dashboards/regions",
      // "self"]`, where self means this report.
      await page
         .getByText("EU", { exact: true })
         .first()
         .click({ timeout: 60_000 });
      await page.getByRole("menuitem", { name: "Filter this report" }).click();

      // Filtering in place is a URL change, not a navigation: still the
      // report, now with the given set.
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${LINEAR_PKG}/brands\\?REGION=EU$`),
      );
      await expectFilterValue(page, "Region", "EU");
   });

   // DEPRECATED FEATURE COVERAGE — delete together with
   // `.malloynb` notebooks (see RELEASE_NOTES deprecations). Only what is
   // particular to the format: its listing, and its `##` spellings of a
   // report's `autorun=false` and starting `givens { … }`. Drill, controls
   // and URL state are the linear-report tests above.
   test.describe("deprecated: .malloynb notebooks", () => {
      test("lists notebooks by title, the way it lists dashboards", async ({
         page,
      }) => {
         await gotoHome(page);
         await openEnvironment(page, env);
         await openPackage(page, env, PKG);

         await expect(
            page.getByRole("heading", {
               name: "Reports",
               level: 6,
               exact: true,
            }),
         ).toBeVisible({ timeout: 60_000 });

         // An explicit `## title=`, and a title read from the first markdown
         // heading. Both rows keep the filename as the secondary label, so the
         // path a reader needs in order to find the file is never lost.
         const titled = page.getByRole("button", {
            name: /Orders in a window/,
         });
         await expect(titled).toBeVisible();
         await expect(titled).toContainText("orders-since.malloynb");
         await expect(
            page.getByRole("button", { name: /Brands/ }),
         ).toContainText("brands.malloynb");

         await titled.click();
         await expect(page).toHaveURL(
            new RegExp(`/${env}/${PKG}/orders-since.malloynb$`),
         );
      });

      test("reads `## autorun=false` and `## givens { … }`", async ({
         page,
      }) => {
         await page.goto(`/${env}/${PKG}/orders-since.malloynb`);
         await expect(
            page.getByRole("heading", { name: "Orders since" }),
         ).toBeVisible({ timeout: 60_000 });
         await expect(
            page.getByRole("button", { name: "Apply" }),
         ).toBeVisible();

         await page.goto(`/${env}/${PKG}/orders-start.malloynb`);
         await expectFilterValue(page, "Ordered since", "Mar 1, 2024", 60_000);
         await expect(page).toHaveURL(/[?&]SINCE=2024-03-01/);
      });
   });

   test("a drillable cell is reachable and firable from the keyboard", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      // `cell` resolves the inner `.cell-content`; the affordance attributes and
      // the focus live on the cell that WRAPS it, which is the element the
      // marking pass writes to.
      const target = cell(page, "orders -> by_region", "US").locator("..");
      await expect(target).toBeVisible({ timeout: 30_000 });

      // Marked cells are reachable and announce themselves, which is the whole
      // difference between a drill a keyboard user can find and one only a mouse
      // can reach.
      await expect(target).toHaveAttribute("role", "button");
      // Focusable, but not necessarily the column's tab stop: a drillable column
      // gets ONE stop and the arrow keys move within it. Marking every cell
      // tabbable instead put a whole result column in the tab order, which at the
      // server's row cap is hundreds of presses to reach the next tile.
      await expect(target).toHaveAttribute("tabindex", /^(0|-1)$/);
      const tile = page.locator(".MuiPaper-root").filter({
         has: page.locator('[title="orders -> by_region"]'),
      });
      await expect(tile.locator('.publisher-drill[tabindex="0"]')).toHaveCount(
         1,
      );

      await target.focus();
      await page.keyboard.press("Enter");
      // Enter opens the same menu a click does: the two destinations, then the
      // rows behind the value.
      await expect(page.getByRole("menuitem")).toHaveCount(3);
      await expect(page.getByRole("menuitem").nth(0)).toHaveText(
         /^Filter this report to /,
      );
      await expect(page.getByRole("menuitem").nth(2)).toHaveText(
         /^Show rows for /,
      );
   });

   test("arrow keys move within a drillable column and the stop follows", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      const tile = page.locator(".MuiPaper-root").filter({
         has: page.locator('[title="orders -> by_region"]'),
      });
      const drillCells = tile.locator(".publisher-drill");
      await expect(drillCells.first()).toBeVisible({ timeout: 30_000 });
      const total = await drillCells.count();
      expect(total).toBeGreaterThan(1);

      // The stop starts on the first row, which is what Tab reaches.
      await expect(drillCells.nth(0)).toHaveAttribute("tabindex", "0");
      await drillCells.nth(0).focus();

      await page.keyboard.press("ArrowDown");
      // Focus AND the stop move together, so tabbing away and back returns to the
      // row the reader was on rather than to the top of the column.
      await expect(drillCells.nth(1)).toBeFocused();
      await expect(drillCells.nth(1)).toHaveAttribute("tabindex", "0");
      await expect(drillCells.nth(0)).toHaveAttribute("tabindex", "-1");
      // Still exactly one stop: the point of the change.
      await expect(tile.locator('.publisher-drill[tabindex="0"]')).toHaveCount(
         1,
      );

      // Clamped rather than wrapping, which is how a table's rows read.
      await page.keyboard.press("ArrowUp");
      await expect(drillCells.nth(0)).toBeFocused();
      await page.keyboard.press("ArrowUp");
      await expect(drillCells.nth(0)).toBeFocused();

      // End jumps to the last row, and the drill still fires from there.
      await page.keyboard.press("End");
      await expect(drillCells.nth(total - 1)).toBeFocused();
      await page.keyboard.press("Enter");
      // Filter this report, Open Regions, Show rows.
      await expect(page.getByRole("menuitem")).toHaveCount(3);
   });

   test("clicking a cell moves the tab stop to it", async ({ page }) => {
      await openDashboard(page, "combined");
      const tile = page.locator(".MuiPaper-root").filter({
         has: page.locator('[title="orders -> by_region"]'),
      });
      const cells = tile.locator(".publisher-drill");
      await expect(cells.first()).toBeVisible({ timeout: 30_000 });
      await expect(cells.nth(0)).toHaveAttribute("tabindex", "0");
      await expect(cells.nth(1)).toHaveAttribute("tabindex", "-1");

      // A `tabindex="-1"` cell is still click-focusable, so focus can arrive
      // without going through the arrow handler that moves the stop. Before the
      // focusin handler, clicking row 2 then tabbing away and back returned the
      // reader to row 1.
      await cells.nth(1).click();
      await page.keyboard.press("Escape");

      await expect(cells.nth(1)).toHaveAttribute("tabindex", "0");
      await expect(cells.nth(0)).toHaveAttribute("tabindex", "-1");
      await expect(tile.locator('.publisher-drill[tabindex="0"]')).toHaveCount(
         1,
      );
   });

   test("Space opens the drill menu without choosing from it", async ({
      page,
   }) => {
      await openDashboard(page, "combined");
      const target = cell(page, "orders -> by_region", "US").locator("..");
      await expect(target).toBeVisible({ timeout: 30_000 });

      // Space activates on keyUP, the native button rule. Firing it on keydown
      // instead left the same keypress's keyup landing on the freshly-focused
      // first menu item, so a Space drill chose a destination it never showed
      // the reader. The URL check is what catches that regressing.
      const before = page.url();
      await target.focus();
      await page.keyboard.press("Space");
      await expect(page.getByRole("menuitem")).toHaveCount(3);
      expect(page.url()).toBe(before);
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
   // author-written dashboard component, so nothing here should ever be framed.
   // See docs/malloyyo-dashboards-design.md §"Custom JSX components".
   test("renders in the page, with no iframe", async ({ page }) => {
      await openDashboard(page, "overview");
      await expect(
         page.getByRole("heading", { name: "Business Overview" }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(page.locator("iframe")).toHaveCount(0);
   });

   // REGRESSION GUARD, not a fix. The operator's font already reaches the text
   // the renderer draws, and this pins that it keeps doing so.
   //
   // It is guarding a genuinely fragile arrangement. The renderer declares
   // `font-family: var(--malloy-render--font-family)` on .malloy-render and then,
   // in the same stylesheet, an `@supports (font-variation-settings: normal)`
   // block re-declares `font-family: InterVariable, Inter, system-ui sans-serif`.
   // That later rule would win on source order and pin every host to Inter. It
   // does not, because `system-ui sans-serif` is an unquoted multi-word family
   // name, and both `system-ui` and `sans-serif` are generic-family keywords, so
   // the identifier sequence is invalid and the browser drops the whole
   // declaration. Verified against `document.styleSheets`: the block declares
   // `font-feature-settings: 'liga' 1, 'calt' 1` in source and computes to
   // `font-feature-settings: "liga", "calt"`, with no `font-family` surviving.
   //
   // So a well-meaning upstream fix to that missing comma would silently pin
   // every embedding host's dashboards to Inter, and nothing else in this repo
   // would notice. This test is what notices.
   //
   // Asserted on the computed value rather than the emitted variable: the
   // variable was always emitted correctly, so a test at that level passes
   // whether or not the value reaches any text. The sentinel family need not
   // exist on the machine, since getComputedStyle returns the specified
   // font-family list rather than the face that resolved.
   test("the instance theme's font reaches renderer-drawn text", async ({
      page,
   }) => {
      const SENTINEL = "PublisherThemeProbe";
      await page.route("**/api/v0/status", async (route) => {
         const res = await route.fetch();
         const body = await res.json();
         await route.fulfill({
            response: res,
            json: {
               ...body,
               theme: {
                  ...(body.theme ?? {}),
                  font: {
                     ...(body.theme?.font ?? {}),
                     family: `"${SENTINEL}", monospace`,
                  },
               },
            },
         });
      });

      // `grid` is one tile holding a nested query, so every cell on it is the
      // renderer's own output rather than the SDK's tile chrome. The tile-title
      // half, which is Publisher's own `Typography`, is pinned by the test below
      // this one; it used to be the hole in this guard.
      await openDashboard(page, "grid");
      // `.malloy-table`, not `table`: the renderer builds its tables from divs,
      // which is why this suite addresses cells by class throughout.
      //
      // 30s, not 60s: the per-test budget is 60s (playwright.config.ts), and the
      // two waits below claim 15s and 5s of it, so a 60s wait here could never
      // be spent and a cold-render failure would surface as a bare test timeout
      // naming no wait. 30 + 15 + 5 leaves headroom for the navigation.
      await expect(
         page.locator(".malloy-render .malloy-table").first(),
      ).toBeVisible({
         timeout: 30_000,
      });

      // The root the renderer pins its own family on.
      await expect
         .poll(
            () =>
               page
                  .locator(".malloy-render")
                  .first()
                  .evaluate((el) => getComputedStyle(el).fontFamily),
            { timeout: 15_000 },
         )
         .toContain(SENTINEL);

      // And that it inherits all the way to a BODY cell, which is the text a
      // reader actually sees. `.td` deliberately: header cells carry
      // `column-cell` too and come first in DOM order, so a bare
      // `.column-cell.first()` pins the `th` instead, and a future
      // `font-family` on the header would let body text regress unnoticed.
      // Explicit timeout because `actionTimeout` is unset, so a table that
      // renders empty would otherwise hang to the test cap without naming the
      // locator it could not find.
      const cellFont = await page
         .locator(".malloy-render .column-cell.td")
         .first()
         .evaluate((el) => getComputedStyle(el).fontFamily, undefined, {
            timeout: 5_000,
         });
      expect(cellFont).toContain(SENTINEL);
   });

   // The other half, and the one that was missing: a tile's heading is
   // Publisher's own MUI `Typography`, not renderer output, so it does not ride
   // the `.malloy-render` cascade the test above follows. It inherited the app
   // font and `text.secondary` regardless of the instance theme, which made a
   // themed dashboard's tile titles the one piece of it that did not follow.
   //
   // Asserted on the computed value for the same reason as above: the token was
   // always resolved correctly, and the question is whether it lands.
   test("the instance theme's font and title colour reach a tile heading", async ({
      page,
   }) => {
      const SENTINEL = "PublisherTileTitleProbe";
      const TITLE_COLOR = "rgb(1, 2, 3)";
      await page.route("**/api/v0/status", async (route) => {
         const res = await route.fetch();
         const body = await res.json();
         await route.fulfill({
            response: res,
            json: {
               ...body,
               theme: {
                  ...(body.theme ?? {}),
                  font: {
                     ...(body.theme?.font ?? {}),
                     family: `"${SENTINEL}", monospace`,
                  },
                  palette: {
                     ...(body.theme?.palette ?? {}),
                     // Mode-keyed, like every other palette entry.
                     tileTitle: { light: "#010203", dark: "#010203" },
                  },
               },
            },
         });
      });

      await openDashboard(page, "tiled");
      const heading = page.locator('[title="tiles -> brand_tile"]');
      await expect(heading).toBeVisible({ timeout: 30_000 });
      await expect
         .poll(
            () => heading.evaluate((el) => getComputedStyle(el).fontFamily),
            { timeout: 15_000 },
         )
         .toContain(SENTINEL);
      expect(await heading.evaluate((el) => getComputedStyle(el).color)).toBe(
         TITLE_COLOR,
      );
   });

   // A tile's card takes the theme's tile colour rather than MUI's white, so on
   // a theme whose page is also white the tiles still read as cards.
   test("a tile paints its card with the theme's tile colour", async ({
      page,
   }) => {
      const TILE_COLOR = "rgb(4, 5, 6)";
      await page.route("**/api/v0/status", async (route) => {
         const res = await route.fetch();
         const body = await res.json();
         await route.fulfill({
            response: res,
            json: {
               ...body,
               theme: {
                  ...(body.theme ?? {}),
                  palette: {
                     ...(body.theme?.palette ?? {}),
                     tile: { light: "#040506", dark: "#040506" },
                  },
               },
            },
         });
      });

      await openDashboard(page, "tiled");
      const tile = page.locator(
         '.MuiPaper-root:has([title="tiles -> brand_tile"])',
      );
      await expect(tile).toBeVisible({ timeout: 30_000 });
      await expect
         .poll(
            () => tile.evaluate((el) => getComputedStyle(el).backgroundColor),
            { timeout: 15_000 },
         )
         .toBe(TILE_COLOR);
   });

   // A dashboard's description is its narrative header, and it is MARKDOWN: the
   // doc comment can carry paragraphs, emphasis, lists and inline code, and
   // Malloy delivers the block with its newlines intact. Rendered as plain text
   // it collapsed onto one line with the asterisks showing, which is what this
   // asserts against: `<strong>` exists, and no literal `**` survives.
   test("a dashboard's description renders as markdown", async ({ page }) => {
      await openDashboard(page, "tiled");
      // The title's row and the description below it.
      const header = page.locator("h1", { hasText: "Tiled" }).locator("../..");
      await expect(header).toBeVisible({ timeout: 30_000 });
      await expect(
         header.locator("strong", { hasText: "tiles" }),
      ).toBeVisible();
      expect(await header.innerText()).not.toContain("**");
   });
});

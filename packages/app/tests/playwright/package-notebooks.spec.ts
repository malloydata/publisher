import { expect, test, type Page } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("package-notebooks", () => {
   test("Notebooks section lists .malloynb files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await expect(
         page.getByText("storefront.malloynb", { exact: true }),
      ).toBeVisible();
   });

   test("opening a notebook routes into the workbook view", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await page.getByText("storefront.malloynb", { exact: true }).click();

      // Router uses a workbook-scoped path; assert we navigated off the package route.
      await expect(page).not.toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.storefront}/?$`),
      );
      await expect(page).toHaveURL(/storefront\.malloynb/);
   });

   test("workbook renders authored content from the notebook", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await page.getByText("storefront.malloynb", { exact: true }).click();
      // The storefront.malloynb renders an authored H1 ("Storefront — a guided
      // tour"): presence confirms the Workbook mounted and executed the notebook.
      await expect(
         page.getByRole("heading", {
            name: "Storefront — a guided tour",
            level: 1,
         }),
      ).toBeVisible();
   });

   test("clicking a notebook row keeps the package segment in the URL", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.governed);

      // orders.malloynb shares no name overlap with the governed-analytics
      // package, so a dropped package segment lands on /examples/orders.malloynb
      // and 404s. The storefront.malloynb case above cannot catch that (the
      // URL still matches either way).
      await page.getByText("orders.malloynb", { exact: true }).click();

      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.governed}/orders\\.malloynb$`),
      );
      await expect(page.getByText(/does not exist/i)).toHaveCount(0);
   });
});

/**
 * The bundled storefront notebook's controls.
 *
 * `notebook-givens.spec.ts` covers the panel's mechanics against a fixture it
 * writes itself; this covers the shipped example, which is the thing a reader
 * meets first and the thing that can rot without any code changing — an import
 * dropped from the notebook's first cell takes the whole panel with it, and
 * nothing else fails.
 */
test.describe("package-notebooks — storefront givens", () => {
   const NOTEBOOK = `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront.malloynb`;

   // Twelve cells run on load, each its own query.
   test.setTimeout(150_000);

   const open = async (page: Page, search = "") => {
      await page.goto(`${NOTEBOOK}${search}`);
      await expect(
         page.getByRole("heading", { name: "Parameters", level: 6 }),
      ).toBeVisible({ timeout: 60_000 });
   };

   const categoryControl = (page: Page) =>
      page.getByRole("combobox", { name: /Category/i }).first();

   /** A value cell in a rendered result, which is what a drill clicks. */
   const valueCell = (page: Page, text: string) =>
      page.locator(".column-cell.td").filter({ hasText: text }).first();

   test("panel renders a control per imported given, and none for the rest", async ({
      page,
   }) => {
      await open(page);

      // One control per given the notebook's first cell imports, each the
      // widget its declaration in givens.malloy asked for.
      await expect(categoryControl(page)).toBeVisible();
      await expect(
         page.getByRole("combobox", { name: /Brand/i }),
      ).toBeVisible();
      await expect(page.getByLabel(/Ordered since/i)).toBeVisible();
      await expect(page.locator('input[type="range"]')).toHaveCount(1);

      // REGION is declared in the same file and deliberately not imported here,
      // since no cell filters by it. A control that moves nothing is worse than
      // no control, so the panel shows what the document can actually use.
      await expect(page.getByRole("combobox", { name: /Region/i })).toHaveCount(
         0,
      );
   });

   test("a select's options come from the data", async ({ page }) => {
      await open(page);

      await categoryControl(page).click();
      // `suggest { source=products dimension=category }` on the declaration.
      await expect(
         page.getByRole("option", { name: "Outerwear", exact: true }),
      ).toBeVisible({ timeout: 30_000 });
   });

   test("a multiselect takes several values, joined into one filter", async ({
      page,
   }) => {
      await open(page);

      // `suggest { query=brand_suggest dimension=brand }`, and a multiselect,
      // so the list stays open across picks — same control, same behaviour as
      // the dashboard filter row.
      const brand = page.getByRole("combobox", { name: /Brand/i }).first();
      await brand.click();
      await expect(
         page.getByRole("option", { name: "Loft", exact: true }),
      ).toBeVisible({ timeout: 30_000 });
      await page.getByRole("option", { name: "Loft", exact: true }).click();
      // Each pick round-trips through the URL before the control reads its own
      // state back, so a second click sent immediately lands on the pre-first-pick
      // selection and replaces Loft instead of joining it.
      await expect(page).toHaveURL(/[?&]BRAND=Loft(?:[&]|$)/);
      await page.getByRole("option", { name: "Aurora", exact: true }).click();

      // Two picks become one `filter<string>` value, which is the wire form a
      // Malloy filter takes.
      await expect(page).toHaveURL(/[?&]BRAND=Loft%2C\+Aurora/);
   });

   // The first cell's Revenue KPI, all categories against Outerwear alone.
   // Asserted on rather than on the disappearance of a category name, because a
   // notebook clears its results while re-running: "Footwear is gone" is true
   // for a second whatever the controls say, and a test that accepts it would
   // pass with the givens unplumbed.
   const REVENUE_ALL = "$2,098,177.97";
   const REVENUE_OUTERWEAR = "$464,861.08";
   const REVENUE_HARBOR = "$145,199.75";

   test("choosing a value re-runs every cell and is addressable in the URL", async ({
      page,
   }) => {
      await open(page);
      await expect(page.getByText(REVENUE_ALL).first()).toBeVisible({
         timeout: 60_000,
      });

      await categoryControl(page).click();
      await page
         .getByRole("option", { name: "Outerwear", exact: true })
         .click();

      await expect(page).toHaveURL(
         new RegExp(`${NOTEBOOK}\\?CATEGORY=Outerwear`),
      );
      await expect(page.getByText(REVENUE_OUTERWEAR).first()).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByText(REVENUE_ALL)).toHaveCount(0);
   });

   test("a given in the URL arrives applied, so a filtered notebook is a link", async ({
      page,
   }) => {
      await open(page, "?CATEGORY=Outerwear");

      await expect(categoryControl(page)).toHaveValue("Outerwear");
      await expect(page.getByText(REVENUE_OUTERWEAR).first()).toBeVisible({
         timeout: 60_000,
      });
   });

   test("a drill offers both destinations, and `self` writes into the panel", async ({
      page,
   }) => {
      await open(page);
      await expect(page.getByText(REVENUE_ALL).first()).toBeVisible({
         timeout: 60_000,
      });

      // The same `# drill { to=["category", "self"] given=CATEGORY }` a
      // dashboard tile honors. `self` is on offer here only because this
      // notebook imports CATEGORY — that is what it has to write into.
      await valueCell(page, "Outerwear").click({ timeout: 60_000 });
      await expect(
         page.getByRole("menuitem", { name: "Category" }),
      ).toBeVisible();
      await page
         .getByRole("menuitem", { name: "Filter this notebook" })
         .click();

      // Filters in place: the control, the URL, and the results, without
      // leaving the page.
      await expect(categoryControl(page)).toHaveValue("Outerwear");
      await expect(page).toHaveURL(
         new RegExp(`${NOTEBOOK}\\?CATEGORY=Outerwear`),
      );
      await expect(page.getByText(REVENUE_OUTERWEAR).first()).toBeVisible({
         timeout: 60_000,
      });
   });

   test("a one-destination drill filters in place with no menu", async ({
      page,
   }) => {
      await open(page);
      await expect(page.getByText(REVENUE_ALL).first()).toBeVisible({
         timeout: 60_000,
      });

      // `brand` carries `# drill { to=self given=BRAND }` — one destination, so
      // there is nothing to choose between and the click acts directly. Exact
      // text: the products table further down holds "Harbor Utility Short".
      await page
         .locator(".column-cell.td")
         .filter({ hasText: /^Harbor$/ })
         .first()
         .click({ timeout: 60_000 });

      await expect(page).toHaveURL(new RegExp(`${NOTEBOOK}\\?BRAND=Harbor`));
      await expect(page.getByRole("menuitem")).toHaveCount(0);
      await expect(page.getByText(REVENUE_HARBOR).first()).toBeVisible({
         timeout: 60_000,
      });
   });
});

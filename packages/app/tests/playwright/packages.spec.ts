import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES, tmpName } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

test.describe("packages — read", () => {
   test("environment page shows the packages heading", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await expect(
         page.getByRole("heading", {
            name: `${DEFAULT_ENV} packages`,
            level: 6,
         }),
      ).toBeVisible();
   });

   test("fixture packages are listed", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      for (const pkg of [PACKAGES.imdb, PACKAGES.ecommerce, PACKAGES.faa]) {
         await expect(page.getByText(pkg, { exact: true })).toBeVisible();
      }
   });

   test("clicking a package tile routes to /:env/:package", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);
   });
});

test.describe("packages — mutable CRUD", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("Add Package button is present on environment page", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await expect(
         page.getByRole("button", { name: "Add Package" }),
      ).toBeVisible();
   });

   test("package tile exposes Package actions menu", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      const actions = page.getByRole("button", {
         name: `Package actions for ${PACKAGES.imdb}`,
      });
      await expect(actions).toBeVisible();
      await actions.click();
      await expect(page.getByRole("menu")).toBeVisible();
      await page.keyboard.press("Escape");
   });

   test("create env → add package from git → open → back → delete package → delete env", async ({
      page,
   }) => {
      test.setTimeout(180_000);

      const envName = tmpName("env");
      const packageName = "ecommerce";
      const packageLocation =
         "https://github.com/credibledata/malloy-samples/tree/main/ecommerce";
      const packageDescription = `playwright fixture ${new Date().toISOString()}`;

      // --- 1. Create a disposable environment ---
      await gotoHome(page);
      await page
         .getByRole("button", { name: "Create New Environment" })
         .click();
      const createEnvDialog = page.getByRole("dialog", {
         name: "Create New Environment",
      });
      await createEnvDialog.getByLabel("Environment Name").fill(envName);
      await createEnvDialog
         .getByRole("button", { name: "Create Environment" })
         .click();
      await expect(createEnvDialog).toBeHidden();
      await expect(
         page.getByRole("heading", { name: envName, level: 6 }),
      ).toBeVisible();

      // --- 2. Open it ---
      // Walk from the env's heading up to its containing card, then click that card's Open Environment.
      const envCard = page
         .getByRole("heading", { name: envName, level: 6 })
         .locator("xpath=ancestor::div[contains(@class,'MuiCard-root')][1]");
      await envCard.getByRole("button", { name: "Open Environment" }).click();
      await expect(page).toHaveURL(new RegExp(`/${envName}/?$`));

      // Fresh env: no packages yet.
      await expect(
         page.getByRole("heading", {
            name: `${envName} packages`,
            level: 6,
         }),
      ).toBeVisible();

      // --- 3. Add ecommerce package from git ---
      await page.getByRole("button", { name: "Add Package" }).click();
      const addPkgDialog = page.getByRole("dialog", {
         name: "Create New Package",
      });
      await expect(addPkgDialog).toBeVisible();
      await addPkgDialog.getByLabel("Package Name").fill(packageName);
      // Description uses a rich-text area; the `textarea[name=description]` is
      // the actual editable; fall back to filling via role so both renderings pass.
      await addPkgDialog
         .locator("textarea[name=description]")
         .fill(packageDescription);
      await addPkgDialog.getByLabel("Location").fill(packageLocation);
      await addPkgDialog.getByRole("button", { name: "Save Changes" }).click();

      // Git clone can take a while; wait up to 60s for the tile to appear.
      const pkgTile = page.getByText(packageName, { exact: true });
      await expect(pkgTile).toBeVisible({ timeout: 60_000 });
      // Dialog closes when the server responds; don't race it against the tile.
      await expect(addPkgDialog).toBeHidden({ timeout: 60_000 });

      // --- 4. Open the package ---
      await pkgTile.click();
      await expect(page).toHaveURL(new RegExp(`/${envName}/${packageName}/?$`));

      // --- 5. Go back to the environment ---
      await page.goBack();
      await expect(page).toHaveURL(new RegExp(`/${envName}/?$`));
      await expect(
         page.getByRole("heading", {
            name: `${envName} packages`,
            level: 6,
         }),
      ).toBeVisible();

      // --- 6. Delete the package ---
      await page
         .getByRole("button", { name: `Package actions for ${packageName}` })
         .dispatchEvent("click");
      await page.getByRole("menuitem", { name: "Delete" }).click();
      const deletePkgDialog = page.getByRole("dialog", {
         name: "Delete Package",
      });
      await expect(deletePkgDialog).toContainText(packageName);
      await deletePkgDialog.getByRole("button", { name: "Delete" }).click();
      await expect(deletePkgDialog).toBeHidden();
      await expect(page.getByText(packageName, { exact: true })).toHaveCount(0);

      // --- 7. Go back to Home and delete the environment ---
      await page.goto("/");
      await expect(
         page.getByRole("heading", { name: envName, level: 6 }),
      ).toBeVisible();
      await page
         .getByRole("button", { name: `Environment actions for ${envName}` })
         .dispatchEvent("click");
      await page.getByRole("menuitem", { name: "Delete" }).click();
      const deleteEnvDialog = page.getByRole("dialog", {
         name: "Delete Environment",
      });
      await expect(deleteEnvDialog).toContainText(envName);
      await deleteEnvDialog.getByRole("button", { name: "Delete" }).click();
      await expect(deleteEnvDialog).toBeHidden();
      await expect(
         page.getByRole("heading", { name: envName, level: 6 }),
      ).toHaveCount(0);
   });
});

test.describe("packages — mutability parity with /api/v0/status", () => {
   test("Add Package renders iff the publisher reports mutable", async ({
      page,
   }, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      const expected = mutable ? 1 : 0;

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await expect(
         page.getByRole("button", { name: "Add Package" }),
      ).toHaveCount(expected);
   });
});

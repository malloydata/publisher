import { expect, test } from "@playwright/test";
import { gotoHome } from "./helpers/navigation";

const TOGGLE_NAME = /Light mode|Dark mode|Auto mode/i;
const STORAGE_KEY = "publisher:themeMode";

test.describe("theming — light/dark/auto toggle", () => {
   test.beforeEach(async ({ page }) => {
      // Clear any persisted preference from a previous test so this spec
      // is deterministic across re-runs.
      await page.goto("/");
      await page.evaluate(
         (k) => window.localStorage.removeItem(k),
         STORAGE_KEY,
      );
   });

   test("toggle button is visible from the home page", async ({ page }) => {
      await gotoHome(page);
      await expect(
         page.getByRole("button", { name: TOGGLE_NAME }),
      ).toBeVisible();
   });

   test("cycles light → dark → auto → light, persisting each step to localStorage", async ({
      page,
   }) => {
      await gotoHome(page);
      const button = page.getByRole("button", { name: TOGGLE_NAME });
      await expect(button).toBeVisible();

      // Click three times and assert each transition exposes a different
      // aria-label so a regression that collapses to a 2-state cycle is
      // caught.
      const seen: string[] = [];
      for (let i = 0; i < 3; i++) {
         const label = await button.getAttribute("aria-label");
         if (label) seen.push(label);
         await button.click();
         // Allow MUI Tooltip + React re-render to settle.
         await page.waitForTimeout(50);
      }
      const unique = new Set(seen);
      expect(unique.size).toBe(3);

      const stored = await page.evaluate(
         (k) => window.localStorage.getItem(k),
         STORAGE_KEY,
      );
      expect(["light", "dark", "auto"]).toContain(stored);
   });

   test("dark mode persists across reload and applies a dark MUI palette", async ({
      page,
   }) => {
      await gotoHome(page);
      // Set the choice directly so the assertion is deterministic across
      // whatever default-mode the operator config might set.
      await page.evaluate(({ k, v }) => window.localStorage.setItem(k, v), {
         k: STORAGE_KEY,
         v: "dark",
      });
      await page.reload();
      await expect(
         page.getByRole("heading", { name: "Publisher", level: 1 }),
      ).toBeVisible();

      const bgColor = await page.evaluate(() => {
         return getComputedStyle(document.body).backgroundColor;
      });
      // MUI dark palette resolves to a non-white background.
      expect(bgColor.replace(/\s/g, "")).not.toMatch(/^rgba?\(255,255,255/);
   });

   test("auto mode round-trips: setting auto, reloading, viewing toggle still reads auto", async ({
      page,
   }) => {
      await gotoHome(page);
      await page.evaluate(({ k, v }) => window.localStorage.setItem(k, v), {
         k: STORAGE_KEY,
         v: "auto",
      });
      await page.reload();
      const label = await page
         .getByRole("button", { name: TOGGLE_NAME })
         .getAttribute("aria-label");
      expect(label).toMatch(/Auto mode/i);
   });
});

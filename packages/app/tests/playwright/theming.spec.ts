// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

   /**
    * The primary button's own label has to be readable in both modes.
    *
    * This is asserted rather than eyeballed because the failure is silent and
    * it has already happened once: moving the primary onto the palette's blue
    * put white-on-blue at 3.7:1 in dark mode, under the 4.5:1 a label needs,
    * with a HOVER state at 2.5:1 — worse than its resting state. A screenshot
    * of that looks like a blue button.
    */
   test("a primary button's label clears 4.5:1 against its fill, in both modes", async ({
      page,
   }) => {
      const contrast = () =>
         page.evaluate(() => {
            const button = [...document.querySelectorAll("button")].find((b) =>
               b.className.includes("MuiButton-contained"),
            );
            if (!button) return null;
            const style = getComputedStyle(button);
            const parse = (c: string) =>
               (c.match(/[\d.]+/g) ?? []).slice(0, 3).map(Number);
            const lum = (rgb: number[]) => {
               const ch = rgb.map((v) => {
                  const s = v / 255;
                  return s <= 0.03928
                     ? s / 12.92
                     : Math.pow((s + 0.055) / 1.055, 2.4);
               });
               return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2];
            };
            const a = lum(parse(style.color));
            const b2 = lum(parse(style.backgroundColor));
            const [hi, lo] = a > b2 ? [a, b2] : [b2, a];
            return (hi + 0.05) / (lo + 0.05);
         });

      for (const mode of ["light", "dark"]) {
         await page.goto("/");
         await page.evaluate(({ k, v }) => window.localStorage.setItem(k, v), {
            k: STORAGE_KEY,
            v: mode,
         });
         await page.reload();
         await expect(
            page.getByRole("heading", { name: "Publisher", level: 1 }),
         ).toBeVisible();
         const ratio = await contrast();
         expect(ratio, `${mode} mode primary button`).not.toBeNull();
         expect(ratio!, `${mode} mode primary button`).toBeGreaterThanOrEqual(
            4.5,
         );
      }
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

test.describe("theming — allowUserToggle and frozenConfig", () => {
   test("allowUserToggle:false hides the viewer mode toggle", async ({
      page,
   }) => {
      // Serve a status payload whose theme disables the viewer toggle. The
      // header toggle is gated on theme.allowUserToggle, so it should not
      // render at all.
      await page.route("**/api/v0/status", async (route) => {
         const res = await route.fetch();
         const body = await res.json();
         await route.fulfill({
            response: res,
            json: {
               ...body,
               theme: { ...(body.theme ?? {}), allowUserToggle: false },
            },
         });
      });
      await gotoHome(page);
      await expect(
         page.getByRole("heading", { name: "Publisher", level: 1 }),
      ).toBeVisible();
      await expect(page.getByRole("button", { name: TOGGLE_NAME })).toHaveCount(
         0,
      );
   });

   test("frozenConfig renders the Theme Editor read-only", async ({ page }) => {
      // Serve a frozen status so the editor should show its read-only warning
      // and disable every mutation control.
      await page.route("**/api/v0/status", async (route) => {
         const res = await route.fetch();
         const body = await res.json();
         await route.fulfill({
            response: res,
            json: { ...body, frozenConfig: true },
         });
      });
      await page.goto("/settings/theme");

      await expect(
         page.getByText(/can.?t be edited from this page/i),
      ).toBeVisible();
      await expect(
         page.getByRole("button", { name: /Reset to defaults/i }),
      ).toBeDisabled();
      await expect(
         page
            .getByRole("group", { name: /Edit colors for mode/i })
            .getByRole("button", { name: "Light mode" }),
      ).toBeDisabled();
   });
});

import { Page, expect } from "@playwright/test";
import { DEFAULT_ENV } from "./fixtures";

export async function gotoHome(page: Page): Promise<void> {
   await page.goto("/");
   await expect(
      page.getByRole("heading", { name: "Publisher", level: 1 }),
   ).toBeVisible();
}

export async function openEnvironment(
   page: Page,
   name: string = DEFAULT_ENV,
): Promise<void> {
   // Redesigned env cards on Home are themselves the click target — no
   // separate "Open Environment" button. Click the env's heading.
   const heading = page.getByRole("heading", { name, level: 6 });
   await expect(heading).toBeVisible();
   await heading.click();
   await expect(page).toHaveURL(new RegExp(`/${name}/?$`));
}

export async function openPackage(
   page: Page,
   env: string,
   pkg: string,
): Promise<void> {
   // Redesigned env page renders the env name as h4 and the section header
   // as h6 "Packages" (separate, not concatenated).
   await expect(
      page.getByRole("heading", { name: "Packages", level: 6 }),
   ).toBeVisible();
   await page.getByText(pkg, { exact: true }).first().click();
   await expect(page).toHaveURL(new RegExp(`/${env}/${pkg}/?$`));
}

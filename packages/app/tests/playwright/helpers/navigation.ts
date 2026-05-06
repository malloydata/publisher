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
   await expect(page.getByRole("heading", { name, level: 6 })).toBeVisible();
   await page.getByRole("button", { name: "Open Environment" }).first().click();
   await expect(page).toHaveURL(new RegExp(`/${name}/?$`));
}

export async function openPackage(
   page: Page,
   env: string,
   pkg: string,
): Promise<void> {
   // Scope to the packages heading's section; the word can otherwise match
   // descriptions, breadcrumbs, or env card text.
   await expect(
      page.getByRole("heading", { name: `${env} packages`, level: 6 }),
   ).toBeVisible();
   await page.getByText(pkg, { exact: true }).first().click();
   await expect(page).toHaveURL(new RegExp(`/${env}/${pkg}/?$`));
}

import { test, expect, Page } from '@playwright/test';
const BASE_URL = 'http://localhost:4000/';
test.setTimeout(60_000);

async function gotoStartPage(page:Page, timeout = 45_000){
  await page.goto(BASE_URL,{timeout});
}

// Test Case 1.
test.describe('Create New Project Flow', () => {
  
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('should open the dialog and submit the form', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('Demo Project');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('Demo Description Tester');  
    await page.locator('form#project-form').press('Enter');  
    await expect(page.getByRole('heading',{ name : 'Demo Project' })).toBeVisible({ timeout: 10000 });
  });

});

// Test Case 2.
// Navigation of the top right button
test.describe('Header Navigation', () => {
  
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  //  Malloy Docs navigation
  test('should navigate to Malloy Docs page', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    await page.waitForSelector(
      'a[href="https://docs.malloydata.dev/documentation/"]',
      { state: 'visible', timeout: 60000 }
    );
    await page.getByRole('link', { name: /Malloy Docs/i }).click();
    await expect(page).toHaveURL('https://docs.malloydata.dev/documentation/', { timeout: 60000 });
  });

  // Publisher doc navigation
  test('should navigate to Publisher Docs page', async ({ page }) => {
  await page.goto(BASE_URL, { timeout: 60000 });
  await page.waitForLoadState('domcontentloaded');

  await page.waitForSelector('a[href="https://github.com/malloydata/publisher/blob/main/README.md"]', {
    state: 'visible',
    timeout: 60000,
  });

  await Promise.all([
    page.waitForURL('https://github.com/malloydata/publisher/blob/main/README.md', { timeout: 60000 }),
    page.getByRole('link', { name: /Publisher Docs/i }).click(),
  ]);

  await expect(page).toHaveURL('https://github.com/malloydata/publisher/blob/main/README.md');
  });

  // Publisher Api navigation
  test('should navigate to Publisher API (local page)', async ({ page }) => {
    
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });

    const apiLink = page.getByRole('link', { name: /Publisher API/i });
    await apiLink.waitFor({ state: 'visible' });

    await Promise.all([
      page.waitForURL('**/api-doc.html', { waitUntil: 'load', timeout: 60000 }),
      apiLink.click(),
    ]);

    await expect(page).toHaveURL('http://localhost:4000/api-doc.html');
  });

});

// Test Case 3.
test.describe('enter project details and perform different operations', () => {

  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('enter project details and click cancel', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    const dialog = page.getByRole('dialog', { name: 'Create New Project' });
    await expect(dialog).toBeVisible();
    await page.getByLabel('Project Name').fill('my Project');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards')
      .fill('My Project description');
    await page.getByRole('button', { name: 'Cancel' }).click();
    await expect(dialog).toBeHidden(); 
  });

  test('enter invalid project details and click create project', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('!@#$%^&*()');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('!@#$%^&*()');  
    await page.locator('form#project-form').press('Enter');  
    await expect(page.getByRole('heading',{ name : '!@#$%^&*()' })).toBeVisible();
  });

  test('leave project name empty and click create project', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('');  
    await page.locator('form#project-form').press('Enter');  
  });
  
});
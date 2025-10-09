import { test, expect } from '@playwright/test';
// Test Case 1.
test.describe('Create New Project Flow', () => {

  test.setTimeout(60_000);
  
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:4000/', { timeout: 45_000 });
  });

  test('should open the dialog and submit the form', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('Demo Project');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('Demo Description Tester');  
    await page.locator('form#project-form').press('Enter');  
    await expect(page.getByRole('heading',{ name : 'Demo Project' })).toBeVisible();
    // await expect(page.locator('h6',{hasText: 'Demo Project' })).toBeVisible();
    // await expect(page.locator('placeholder',{hasText : 'Demo Description Tester' })).toBeVisible();
  });

});

// Test Case 2.
// Navigation of the top right button
test.describe('Header Navigation', () => {

  test.setTimeout(60_000);
  
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:4000/', { timeout: 45_000 });
  });

  // Molly Docs navigation
  test('should navigate to Malloy Docs page', async ({ page }) => {
    await page.goto('http://localhost:4000/', { timeout: 60000 });
    await page.waitForLoadState('domcontentloaded');

    await page.waitForSelector(
      'a[href="https://docs.malloydata.dev/documentation/"]',
      { state: 'visible', timeout: 60000 }
    );

    await Promise.all([
      page.waitForURL('https://docs.malloydata.dev/documentation/', { timeout: 60000 }),
      page.getByRole('link', { name: /Malloy Docs/i }).click(),
    ]);

    await expect(page).toHaveURL('https://docs.malloydata.dev/documentation/', { timeout: 60000 });
  });

  // Publisher doc navigation
  test('should navigate to Publisher Docs page', async ({ page }) => {
  await page.goto('http://localhost:4000/', { timeout: 60000 });
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
    
    await page.goto('http://localhost:4000/', { waitUntil: 'domcontentloaded' });

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

  test.setTimeout(60_000);

  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:4000/', { timeout: 45_000 });
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
    // await expect(page.locator('h6',{hasText: 'Demo Project' })).toBeVisible();
    // await expect(page.locator('placeholder',{hasText : '!@#$%^&*()' })).toBeVisible();
  });

  test('leave project name empty and click create project', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('');  
    await page.locator('form#project-form').press('Enter');  
  });
  
});
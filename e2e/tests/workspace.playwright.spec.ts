import { expect, Page, test } from '@playwright/test';
import { spawn } from 'child_process';
import { execSync } from 'child_process';
const BASE_URL = 'http://localhost:4000/';
test.setTimeout(60_000);

async function gotoStartPage(page: Page, timeout = 45_000) {
  await page.goto(BASE_URL, { timeout });
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
    await expect(page.getByRole('heading', { name: 'Demo Project' })).toBeVisible({ timeout: 10000 });
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
    await page.getByLabel('Project Name').fill('Tester-info');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('@#$%^&*');
    await page.locator('form#project-form').press('Enter');
    await expect(page.getByRole('heading', { name: 'Tester-info' })).toBeVisible();
  });

  test('leave project name empty and click create project', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('');
    await page.locator('form#project-form').press('Enter');
  });

});

// Test Case 4.
test.describe('Add Package and Verify Notebook Display', () => {
 
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });
 
  test('should add a new package and open notebook correctly', async ({ page }) => {
    const openProjectButtons = page.getByRole('button', { name: 'Open Project' });
    const count = await openProjectButtons.count();
    console.log(`Found ${count} "Open Project" buttons`);
    if (count > 1) {
      for (let i = 0; i < count; i++) {
        const containerText = await openProjectButtons.nth(i).locator('..').innerText();
        console.log(`Button[${i}] context:`, containerText.substring(0, 120));
      }
    }
    const firstVisible = openProjectButtons.first();
    await firstVisible.waitFor({ state: 'visible', timeout: 15_000 });
    await firstVisible.click({ timeout: 10_000 });
    console.log('✅ Clicked first visible "Open Project" successfully');
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 30_000 });
    await page.waitForLoadState('domcontentloaded');
    const addPackageBtn = page.getByRole('button', { name: 'Add Package' });
    await addPackageBtn.waitFor({ state: 'visible', timeout: 30_000 });
    await addPackageBtn.click();
    const timestamp = Date.now();
    const packageName = `jelly test ${timestamp}`;
    await page.getByLabel('Package Name').fill(packageName);
    await page.getByLabel('Description').fill('Automated test package for notebook verification');
    await page.getByLabel('Location').fill('git@github.com:jellysaini/credible-data-package.git');
    await page.getByRole('button', { name: 'Save Changes' }).click();
    await page.waitForSelector(`text=${packageName}`, { state: 'visible', timeout: 60_000 });
    await expect(page.locator(`text=${packageName}`)).toBeVisible();
    await page.locator(`text=${packageName}`).click();
    await expect(page).toHaveURL(new RegExp(`malloy-samples/${encodeURIComponent(packageName)}`), { timeout: 30_000 });
    await page.waitForLoadState('networkidle', { timeout: 30_000 });
  });
 
});
 
// Test Case 5.
test.describe('Verify Semantic Models are Displayed', () => {
 
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });
 
  test('should navigate to a package and display semantic models correctly', async ({ page }) => {
    const openProjectButtons = page.getByRole('button', { name: 'Open Project' });
    await openProjectButtons.first().waitFor({ state: 'visible', timeout: 20_000 });
    console.log(`Found ${await openProjectButtons.count()} "Open Project" buttons — clicking the first`);
    await openProjectButtons.first().click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 20_000 });
    await page.waitForLoadState('domcontentloaded');
    const packageName = 'ecommerce';
    const packageLocator = page.locator(`.MuiTypography-overline:has-text("${packageName.toUpperCase()}")`);
    await expect(packageLocator.first()).toBeVisible({ timeout: 20_000 });
    await packageLocator.first().click();
    await expect(page).toHaveURL(new RegExp(`${packageName}`, 'i'), { timeout: 30_000 });
    await page.waitForLoadState('networkidle');
    console.log('Waiting for Semantic Models navigation control...');
    const possibleLocators = [
      page.getByRole('button', { name: /semantic models/i }),
      page.locator('role=tab[name=/semantic models/i]'),
      page.locator('text=/semantic models/i')
    ];
 
    let clicked = false;
    for (const locator of possibleLocators) {
      if (await locator.count() > 0) {
        console.log(`Found Semantic Models element via selector: ${locator.toString()}`);
        await locator.first().waitFor({ state: 'visible', timeout: 30_000 });
        await locator.first().click({ timeout: 10_000 });
        clicked = true;
        break;
      }
    }
 
    if (!clicked) {
      const debugButtons = await page.locator('button, [role=tab]').allInnerTexts();
      console.log('Available navigation controls:', debugButtons);
      throw new Error('❌ Could not find any element labeled "Semantic Models"');
    }
    const modelFile = page.locator(`text=${packageName}.malloy`);
    await modelFile.first().waitFor({ state: 'visible', timeout: 30_000 });
    await expect(modelFile.first()).toBeVisible();
    await expect(page.locator('text=Semantic Models')).toBeVisible();
  });
});
 
// Test Case 6.
test.describe('Start Local Server and Open Application', () => {
 
  test('Run "bun run start" and verify localhost:4000 opens', async ({ page }) => {
    console.log(' Starting the local server using "bun run start"...');
 
    const serverProcess = spawn('bun', ['run', 'start'], {
      shell: true,
      detached: true,
      stdio: 'inherit',
    });
 
    console.log(' Waiting for server to start...');
    await page.waitForTimeout(10_000); // adjust as needed
 
    await gotoStartPage(page);
    console.log(' Opened:', BASE_URL);
 
    await expect(page).toHaveURL(BASE_URL);
    await expect(page.locator('body')).toBeVisible();
 
   
  });
 
});


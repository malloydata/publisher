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
    await page.getByLabel('Project Name').fill('!@#$%^&*()');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('!@#$%^&*()');
    await page.locator('form#project-form').press('Enter');
    await expect(page.getByRole('heading', { name: '!@#$%^&*()' })).toBeVisible();
  });

  test('leave project name empty and click create project', async ({ page }) => {
    await page.getByRole('button', { name: 'Create New Project' }).click();
    await expect(page.getByRole('dialog', { name: 'Create New Project' })).toBeVisible();
    await page.getByLabel('Project Name').fill('');
    await page.getByPlaceholder('Explore semantic models, run queries, and build dashboards').fill('');
    await page.locator('form#project-form').press('Enter');
  });

});

//Test case 4.
// Test case 4.
test.describe('Add packages', () => {
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('leave package blank page', async ({ page }) => {
    // Wait for the "Open Project" buttons to appear
    await page.waitForSelector('button:has-text("Open Project")', { timeout: 60000 });

    // Click specifically the "Demo Project" one
    const demoProjectCard = page.locator('text=Demo Project').first();
    await demoProjectCard.locator('button:has-text("Open Project")').click();

    // Wait for the Add Package button to appear
    await page.waitForSelector('button:has-text("Add Package")', { timeout: 60000 });

    // Click Add Package to open popup
    await page.getByRole('button', { name: 'Add Package' }).click();

    // Ensure dialog is visible
    await expect(page.getByRole('dialog', { name: 'Add Package' })).toBeVisible();

    // Fill other fields but keep Package Name empty
    await page.getByLabel('Package Name').fill('');
    await page.getByLabel('Description').fill('Demo Description');
    await page
      .getByPlaceholder('E.g. s3://my-bucket/my-package.zip')
      .fill('Demo Placeholder');

    // Try to submit the form
    await page.locator('form#package-form').press('Enter');

    // Expect browser validation popup ("Please fill out this field")
    // It triggers a native HTML5 validation, so we can verify the input validity
    const packageNameInput = page.getByLabel('Package Name');
    await expect(packageNameInput).toHaveJSProperty('validationMessage', 'Please fill out this field.');
  });
});

//Test case 5.

test.describe('Add Package and Verify Notebook Display', () => {
 
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });
 
  test('should add a new package and open notebook correctly', async ({ page }) => {
    await page.getByRole('button', { name: 'Open Project' }).click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 30_000 });
    await page.getByRole('button', { name: 'Add Package' }).waitFor({ state: 'visible', timeout: 30_000 });
    await page.getByRole('button', { name: 'Add Package' }).click();
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
 
// Test Case 6.
test.describe('Verify Semantic Models are Displayed', () => {
 
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });
 
  test('should navigate to ecommerce semantic models and display them correctly', async ({ page }) => {
    await page.waitForSelector('button:has-text("Open Project")', { timeout: 60_000 });
    await page.getByRole('button', { name: 'Open Project' }).click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 60_000 });
    await page.waitForLoadState('domcontentloaded');
    await page.waitForSelector('text=ecommerce', { timeout: 60_000 });
    await page.locator('text=ecommerce').click();
    await expect(page).toHaveURL(/ecommerce/, { timeout: 60_000 });
    await page.waitForLoadState('networkidle');
    await page.waitForSelector('button:has-text("Semantic Models")', { timeout: 60_000 });
    await page.getByRole('button', { name: 'Semantic Models' }).click();
    await page.waitForSelector('text=ecommerce.malloy', { timeout: 60_000 });
    // await page.locator('text=ECOMMERCE.malloy').click();
    await expect(page).toHaveURL(/ecommerce.malloy/, { timeout: 60_000 });
    await page.waitForSelector('.semantic-models, text=Semantic Models', { timeout: 60_000 });
    await expect(page.locator('text=Semantic Models')).toBeVisible();
    console.log(' Verify Semantic Models are Displayed — Test Passed Successfully');
  });
});
 
// Test Case 7.
test.describe('Run Query from Semantic Models in a Project', () => {
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });
 
  test('should open project, navigate to semantic model, add group by, and run successfully', async ({ page }) => {
    await expect(page).toHaveURL(BASE_URL, { timeout: 30_000 });
    await page.getByRole('button', { name: 'Open Project' }).waitFor({ state: 'visible', timeout: 30_000 });
    await page.getByRole('button', { name: 'Open Project' }).click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 45_000 });
    await page.waitForLoadState('networkidle', { timeout: 45_000 });
    const firstComponent = page.locator('.component-card, .package-list-item').first();
    await firstComponent.waitFor({ state: 'visible', timeout: 45_000 });
    const componentName = await firstComponent.textContent();
    await firstComponent.click();
    await expect(page).toHaveURL(/malloy-samples\/.+/, { timeout: 45_000 });
    await page.getByRole('button', { name: /Semantic Models/i }).waitFor({ state: 'visible', timeout: 45_000 });
    await page.getByRole('button', { name: /Semantic Models/i }).click();
    const malloyFile = page.locator('text=.malloy').first();
    await malloyFile.waitFor({ state: 'visible', timeout: 45_000 });
    await malloyFile.click();
    await expect(page).toHaveURL(/\.malloy/, { timeout: 45_000 });
    await page.waitForLoadState('networkidle', { timeout: 45_000 });
    await page.getByRole('button', { name: /\+/ }).waitFor({ state: 'visible', timeout: 30_000 });
    await page.getByRole('button', { name: /\+/ }).click();
    await page.getByRole('menuitem', { name: /Add Group By/i }).waitFor({ state: 'visible', timeout: 30_000 });
    await page.getByRole('menuitem', { name: /Add Group By/i }).click();
    const firstGroupByField = page.locator('.group-by-selector option, .dropdown-item, .menu-item').first();
    await firstGroupByField.waitFor({ state: 'visible', timeout: 30_000 });
    await firstGroupByField.click({ force: true });
    const runButton = page.getByRole('button', { name: /^Run$/i });
    await runButton.waitFor({ state: 'visible', timeout: 30_000 });
    await runButton.click();
    await page.waitForSelector('.result-table, .chart-container, text=Rows returned', { timeout: 60_000 });
    await expect(page.locator('.result-table, .chart-container')).toBeVisible({ timeout: 60_000 });
    console.log(` Query executed successfully for component: ${componentName?.trim() || 'Unknown'}`);
  });
});
 
// Test Case 8.
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


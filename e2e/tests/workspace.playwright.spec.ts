import { test, expect, Page } from '@playwright/test';
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

test.describe('Verify Semantic Models are Displayed', () => {
  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('should navigate to a package and display semantic models correctly', async ({ page }) => {
    const openProjectButtons = page.getByRole('button', { name: 'Open Project' });
    await openProjectButtons.first().waitFor({ state: 'visible', timeout: 20_000 });
    await openProjectButtons.first().click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 20_000 });
    await page.waitForLoadState('domcontentloaded');
    const packageName = 'ecommerce';
    const packageLocator = page.locator(`.MuiTypography-overline:has-text("${packageName.toUpperCase()}")`);
    await expect(packageLocator.first()).toBeVisible({ timeout: 20_000 });
    await packageLocator.first().click();
    await expect(page).toHaveURL(new RegExp(`${packageName}`, 'i'), { timeout: 30_000 });
    await page.waitForLoadState('networkidle');
    const possibleLocators = [
      page.getByRole('button', { name: /semantic models/i }),
      page.locator('role=tab[name=/semantic models/i]'),
      page.locator('text=/semantic models/i')
    ];

    let clicked = false;
    for (const locator of possibleLocators) {
      if (await locator.count() > 0) {
        await locator.first().waitFor({ state: 'visible', timeout: 30_000 });
        await locator.first().click({ timeout: 10_000 });
        clicked = true;
        break;
      }
    }

    if (!clicked) {
      const debugButtons = await page.locator('button, [role=tab]').allInnerTexts();

      throw new Error(' Could not find any element labeled "Semantic Models"');
    }
    const modelFile = page.locator(`text=${packageName}.malloy`);
    await modelFile.first().waitFor({ state: 'visible', timeout: 30_000 });
    await expect(modelFile.first()).toBeVisible();

    const dbConnections = page.locator('text=Database Connections');
    await expect(dbConnections).toBeVisible({ timeout: 20_000 });
    await dbConnections.click();

    const bigQueryConnection = page.locator('text=BigQuery');
    await bigQueryConnection.first().waitFor({ state: 'visible', timeout: 20_000 });

    const bigQueryRow = page.locator('tr:has-text("BigQuery"), div:has-text("BigQuery")');
    await bigQueryRow.first().waitFor({ state: 'visible', timeout: 30_000 });

    const deleteIcon = bigQueryRow.locator('button[aria-label*="delete" i], svg[aria-label*="delete" i], [data-testid*="delete" i]');

    const deleteIconCount = await deleteIcon.count();
    if (deleteIconCount === 0) {
      const debugButtons = await page.locator('button, svg, [role=button]').allInnerTexts();

      throw new Error(' No delete icon found for BigQuery connection. Check selector.');
    }

    await deleteIcon.first().scrollIntoViewIfNeeded();
    await deleteIcon.first().hover({ timeout: 5_000 });
    await deleteIcon.first().click({ timeout: 10_000 });

  const confirmDialog = page.getByRole('dialog', { name: /Delete Connection/i });
  await expect(confirmDialog).toBeVisible({ timeout: 20_000 });

  const confirmDeleteBtn = confirmDialog.getByRole('button', { name: /^Delete$/i });
  await expect(confirmDeleteBtn).toBeVisible({ timeout: 10_000 });
  await confirmDeleteBtn.click();

});
});

// Test Case 5. high
test.describe('Verify user can add a new database connection show list', () => {

  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('should add a new Postgres connection successfully', async ({ page }) => {

    const openProjectBtn = page.getByRole('button', { name: /Open Project/i });
    await openProjectBtn.first().waitFor({ state: 'visible', timeout: 20_000 });
    await openProjectBtn.first().click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 20_000 });

    const packageLocator = page.locator('.MuiTypography-overline', { hasText: 'ECOMMERCE' });
    await expect(packageLocator.first()).toBeVisible({ timeout: 10_000 });
    await packageLocator.first().click();
    await expect(page).toHaveURL(/ecommerce/i, { timeout: 20_000 });
    await page.waitForLoadState('networkidle');

    const addConnectionBtn = page.getByRole('button', { name: /Add Connection/i });
    await addConnectionBtn.waitFor({ state: 'visible', timeout: 10_000 });
    await addConnectionBtn.click();

    const dialog = page.getByRole('dialog', { name: /Create New Connection/i });
    await expect(dialog).toBeVisible({ timeout: 10_000 });

    await dialog.getByLabel('Connection Name').fill('QA-testing');
    await dialog.getByLabel('Connection Type').click();
    await page.getByRole('option', { name: /Postgres/i }).click();

    await dialog.getByLabel('Host').fill('134.192.232.134');
    await dialog.getByLabel('Port').fill('5432');
    await dialog.getByLabel('Database Name').fill('raavi_picking_system');
    await dialog.getByLabel('User Name').fill('postgres');
    await dialog.getByLabel('Password').fill('postgres123');


    const createBtn = dialog.getByRole('button', { name: /^Create Connection$/i });
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();

    const newConnection = page.locator('text=QA-testing');
    await expect(newConnection.first()).toBeVisible({ timeout: 20_000 });

    const connectionType = page.locator('tr:has-text("QA-testing") >> text=postgres');
    await expect(connectionType).toBeVisible();

  });
});

// Test Case 6. high

test.describe('Verify system behavior with invalid connection details', () => {

  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('should show an error when user enters invalid DuckDB connection name', async ({ page }) => {

    const openProjectBtn = page.getByRole('button', { name: /Open Project/i });
    await openProjectBtn.first().waitFor({ state: 'visible', timeout: 20_000 });
    await openProjectBtn.first().click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 20_000 });

    const packageLocator = page.locator('.MuiTypography-overline', { hasText: 'ECOMMERCE' });
    await expect(packageLocator.first()).toBeVisible({ timeout: 10_000 });
    await packageLocator.first().click();
    await expect(page).toHaveURL(/ecommerce/i, { timeout: 20_000 });
    await page.waitForLoadState('networkidle');

    const addConnectionBtn = page.getByRole('button', { name: /Add Connection/i });
    await addConnectionBtn.waitFor({ state: 'visible', timeout: 10_000 });
    await addConnectionBtn.click();

    const dialog = page.getByRole('dialog', { name: /Create New Connection/i });
    await expect(dialog).toBeVisible({ timeout: 10_000 });

    const typeDropdown = dialog.getByLabel('Connection Type');
    await typeDropdown.click();
    await page.getByRole('option', { name: /DuckDB/i }).click();

    const invalidName = '!@#$%^&&&&&*';
    const nameField = dialog.getByLabel('Connection Name');
    await nameField.fill(invalidName);

    const createBtn = dialog.getByRole('button', { name: /^Create Connection$/i });
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();

    const errorMsg = page.locator('text=/invalid|missing|error|configuration/i');
    await expect(errorMsg.first()).toBeVisible({ timeout: 15_000 });

    const invalidConnRow = page.locator(`text=${invalidName}`);
    await expect(invalidConnRow).toHaveCount(0);
  });
});

// Test Case 7. high
test.describe('Verify user can add a new database connection ', () => {

  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('should add a new Postgres connection successfully ', async ({ page }) => {

    const openProjectBtn = page.getByRole('button', { name: /Open Project/i });
    await openProjectBtn.first().waitFor({ state: 'visible', timeout: 20_000 });
    await openProjectBtn.first().click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 20_000 });

    const packageLocator = page.locator('.MuiTypography-overline', { hasText: 'ECOMMERCE' });
    await expect(packageLocator.first()).toBeVisible({ timeout: 10_000 });
    await packageLocator.first().click();
    await expect(page).toHaveURL(/ecommerce/i, { timeout: 20_000 });
    await page.waitForLoadState('networkidle');

    const addConnectionBtn = page.getByRole('button', { name: /Add Connection/i });
    await addConnectionBtn.waitFor({ state: 'visible', timeout: 10_000 });
    await addConnectionBtn.click();

    const dialog = page.getByRole('dialog', { name: /Create New Connection/i });
    await expect(dialog).toBeVisible({ timeout: 10_000 });

    await dialog.getByLabel('Connection Name').fill('QA-testing');
    await dialog.getByLabel('Connection Type').click();
    await page.getByRole('option', { name: /Postgres/i }).click();

    await dialog.getByLabel('Host').fill('134.192.232.134');
    await dialog.getByLabel('Port').fill('5432');
    await dialog.getByLabel('Database Name').fill('raavi_picking_system');
    await dialog.getByLabel('User Name').fill('postgres');
    await dialog.getByLabel('Password').fill('postgres123');


    const createBtn = dialog.getByRole('button', { name: /^Create Connection$/i });
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();



  });
});



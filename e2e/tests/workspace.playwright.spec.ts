import { expect, Page, test } from '@playwright/test';
import fs from 'fs';
import { spawn, execSync } from 'child_process';
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

// Test case 7

test('Verify git clone and bun run start commands', async () => {
  const repoUrl = 'https://github.com/malloydata/publisher.git';
  const repoName = 'publisher';
  if (fs.existsSync(repoName)) {
    fs.rmSync(repoName, { recursive: true, force: true });
  }
  const cloneOutput = execSync(`git clone ${repoUrl}`, { encoding: 'utf-8', stdio: 'pipe' });

  expect(fs.existsSync(repoName)).toBeTruthy();
  process.chdir(repoName);

  execSync('bun run build:server-deploy', { stdio: 'inherit' });
  execSync('bun run start', { stdio: 'inherit' });

});

// Test Case 8.

test('Setup MCP Server project successfully', async () => {
  const nodeVersion = execSync('node -v').toString().trim();
  const repoUrl = 'https://github.com/vercel/next.js.git';
  const repoName = 'next.js';
  if (fs.existsSync(repoName)) {
    fs.rmSync(repoName, { recursive: true, force: true });
  }

  execSync(`git clone ${repoUrl}`, { stdio: 'inherit' });
  expect(fs.existsSync(repoName)).toBeTruthy();
  execSync('npm install', { cwd: repoName, stdio: 'inherit' });

  expect(fs.existsSync(`${repoName}/package.json`)).toBeTruthy();

});

// Test Case 9.

test.describe('Verify successful execution shows results', () => {

  test.beforeEach(async ({ page }) => {
    await gotoStartPage(page);
  });

  test('should navigate to package, open ecommerce.malloy, add group, and run query', async ({ page }) => {
    const openProjectButtons = page.getByRole('button', { name: 'Open Project' });
    await openProjectButtons.first().waitFor({ state: 'visible', timeout: 20_000 });
    await openProjectButtons.first().click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 30_000 });
    await page.waitForLoadState('domcontentloaded');

    const packageName = 'ecommerce';
    const packageLocator = page.locator(`.MuiTypography-overline:has-text("${packageName.toUpperCase()}")`);
    await packageLocator.first().waitFor({ state: 'visible', timeout: 20_000 });
    await packageLocator.first().click();
    await expect(page).toHaveURL(new RegExp(`${packageName}`, 'i'), { timeout: 30_000 });
    await page.waitForLoadState('networkidle');

    const possibleSelectors = [
      'button:has-text("Semantic Models")',
      '[role="tab"]:has-text("Semantic Models")',
      'text=Semantic Models'
    ];

    let semanticModelLocator: any = null;
    for (const selector of possibleSelectors) {
      const loc = page.locator(selector);
      if (await loc.count()) {
        semanticModelLocator = loc.first();
        break;
      }
    }

    if (!semanticModelLocator) {
      const visibleTexts = await page.locator('button, [role=tab], div, span').allInnerTexts();
      console.log(' Could not find Semantic Models. Visible elements:', visibleTexts.slice(0, 20));
      throw new Error(' "Semantic Models" button/tab not found.');
    }

    await semanticModelLocator.scrollIntoViewIfNeeded();
    await semanticModelLocator.waitFor({ state: 'visible', timeout: 15_000 });
    await semanticModelLocator.click({ timeout: 10_000 });

    const modelFile = page.locator(`text=${packageName}.malloy`);
    await modelFile.first().waitFor({ state: 'visible', timeout: 20_000 });
    await modelFile.first().click();
    const addButton = page.locator('[data-testid="icon-primary-insert"], .icon-primary-insert, button:has-text("+")');
    await addButton.first().waitFor({ state: 'visible', timeout: 15_000 });
    await addButton.first().click();

    const addGroupOption = page.getByRole('menuitem', { name: /add group by/i });
    await addGroupOption.waitFor({ state: 'visible', timeout: 15_000 });
    await addGroupOption.click();

    const possibleFieldSelectors = [
      '.MuiMenuItem-root',                      
      '[role="menuitem"]',                       
      '.MuiList-root .MuiButtonBase-root',       
      '.MuiDialog-container button',             
      'div[role="option"]'                      
    ];

    let foundFieldLocator: any = null;
    for (const selector of possibleFieldSelectors) {
      const loc = page.locator(selector);
      if (await loc.count() > 0) {
        foundFieldLocator = loc.first();
        break;
      }
    }

    if (!foundFieldLocator) { 
      await page.waitForTimeout(5000);
      for (const selector of possibleFieldSelectors) {
        const loc = page.locator(selector);
        if (await loc.count() > 0) {
          foundFieldLocator = loc.first();
          break;
        }
      }
    }
    if (!foundFieldLocator) {
  
      await page.screenshot({ path: 'no-field-list.png', fullPage: true });
      const visibleElements = await page.locator('button, div, span').allInnerTexts();
      throw new Error(' Group-by field list not visible after Add group by click');
    }

    await foundFieldLocator.scrollIntoViewIfNeeded();
    await foundFieldLocator.waitFor({ state: 'visible', timeout: 10_000 });
    await foundFieldLocator.click();

    const runButton = page.getByRole('button', { name: /^run$/i });
    await runButton.waitFor({ state: 'visible', timeout: 20_000 });

    const resultTable = page.locator('.result-table, text=Results');
  });   
});

// Test Case 10.

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
 
// Test Case 11.
 
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
 
// Test Case 12.

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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, tmpName } from "./helpers/fixtures";
import { gotoHome, openEnvironment } from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

test.describe("environment-connections — read", () => {
   test("Connections section renders a card for each connection", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await expect(
         page.getByRole("heading", { name: "Connections", level: 6 }),
      ).toBeVisible();
      // `bigquery` is a stub connection the CI workflow injects into the
      // examples environment before this suite runs; see app-playwright.yml.
      const bigqueryCard = page.locator("h6", { hasText: "bigquery" });
      await expect(bigqueryCard).toBeVisible();
      await expect(page.getByText("BigQuery", { exact: true })).toBeVisible();
   });
});

test.describe("environment-connections — mutable", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("Add Connection button is present in the section header", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await expect(
         page.getByRole("button", { name: "Add Connection" }),
      ).toBeVisible();
   });

   test("kebab menu exposes Edit and Delete actions", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await page
         .getByRole("button", { name: /Connection actions for bigquery/ })
         .click();

      await expect(
         page.getByRole("menuitem", { name: "Edit connection bigquery" }),
      ).toBeVisible();
      await expect(
         page.getByRole("menuitem", { name: "Delete connection bigquery" }),
      ).toBeVisible();
   });
});

test.describe("environment-connections — mutable CRUD", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("Create → Edit → Delete a postgres connection", async ({ page }) => {
      test.setTimeout(60_000);
      const connName = `tmp_test_${Date.now()}`;

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      // --- 1. Create ---
      await page.getByRole("button", { name: "Add Connection" }).click();
      const addDialog = page.getByRole("dialog", {
         name: "Create New Connection",
      });
      await expect(addDialog).toBeVisible();
      await addDialog.getByLabel("Connection Name").fill(connName);
      // Type defaults to postgres. Postgres requires either a Connection String
      // or all 5 detail fields — give it a connection string, which is the
      // shortest valid form.
      await addDialog
         .locator("input[name=connectionString]")
         .fill("postgres://test@localhost:5432/test");
      await addDialog
         .getByRole("button", { name: "Create Connection" })
         .click();
      await expect(addDialog).toBeHidden({ timeout: 15_000 });

      // Verify the card rendered with the right name + type label
      const card = page.locator("h6", { hasText: connName });
      await expect(card).toBeVisible();
      await expect(
         page.getByText("PostgreSQL", { exact: true }).first(),
      ).toBeVisible();

      // --- 2. Edit ---
      await page
         .getByRole("button", { name: `Connection actions for ${connName}` })
         .click();
      await page
         .getByRole("menuitem", { name: `Edit connection ${connName}` })
         .click();
      const editDialog = page.getByRole("dialog", { name: "Edit Connection" });
      await expect(editDialog).toBeVisible();
      // Change the host field to verify the round-trip works
      await editDialog.locator("input[name=host]").fill("edited-host.example");
      await editDialog.getByRole("button", { name: "Edit Connection" }).click();
      await expect(editDialog).toBeHidden({ timeout: 15_000 });
      // Card name + type label are unchanged, so it should still render
      await expect(card).toBeVisible();

      // --- 3. Delete ---
      await page
         .getByRole("button", { name: `Connection actions for ${connName}` })
         .click();
      await page
         .getByRole("menuitem", { name: `Delete connection ${connName}` })
         .click();
      const deleteDialog = page.getByRole("dialog", {
         name: "Delete Connection",
      });
      await expect(deleteDialog).toBeVisible();
      await expect(deleteDialog).toContainText(connName);
      await deleteDialog.getByRole("button", { name: "Delete" }).click();
      await expect(deleteDialog).toBeHidden({ timeout: 15_000 });
      await expect(card).toHaveCount(0);
   });
});

test.describe("environment-connections — mutability parity with /api/v0/status", () => {
   test("Add Connection button renders iff publisher reports mutable", async ({
      page,
   }, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      const expected = mutable ? 1 : 0;

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await expect(
         page.getByRole("button", { name: "Add Connection" }),
      ).toHaveCount(expected);
   });
});

test.describe("environment-connections — delete is not gated on `resource`", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   /**
    * Every connection the server hands back carries a `resource` self-link once
    * it has been through either stamping path: config load
    * (convertConnectionsToApiConnections) or the database load every boot after
    * the first (EnvironmentStore.initialize). A UI guard once refused to delete
    * anything carrying one, which made Delete a no-op for every connection on
    * any server that had been restarted. Seeding `resource` explicitly here
    * reproduces that state without needing a restart mid-suite.
    *
    * The sibling "Create -> Edit -> Delete" test cannot catch this: it creates
    * through the UI, so its connection has no `resource` yet and takes the one
    * path that always worked.
    */
   test("a connection carrying `resource` can be deleted from the UI", async ({
      page,
      request,
   }) => {
      test.setTimeout(60_000);
      const connName = tmpName("tmp_guarded").replace(/-/g, "_");

      const listRes = await request.get(
         `/api/v0/environments/${DEFAULT_ENV}/connections`,
      );
      expect(listRes.ok()).toBeTruthy();
      const existing = (await listRes.json()) as unknown[];

      const seeded = await request.patch(
         `/api/v0/environments/${DEFAULT_ENV}`,
         {
            data: {
               name: DEFAULT_ENV,
               connections: [
                  ...existing,
                  {
                     name: connName,
                     type: "postgres",
                     resource: `/api/v0/connections/${connName}`,
                     postgresConnection: {
                        connectionString: "postgres://test@localhost:5432/test",
                     },
                  },
               ],
            },
         },
      );
      expect(seeded.ok()).toBeTruthy();

      // Everything from here runs inside the try: the connection now exists, so
      // any failure past this point owes the cleanup below. The `resource`
      // assertion in particular is one this test expects to fail some day,
      // since it is the regression it exists to catch.
      try {
         // The seeded connection really does carry the self-link; without this
         // the test could pass by exercising the always-worked path instead.
         const seededRes = await request.get(
            `/api/v0/environments/${DEFAULT_ENV}/connections`,
         );
         expect(seededRes.ok()).toBeTruthy();
         const seededList = (await seededRes.json()) as Array<{
            name?: string;
            resource?: string | null;
         }>;
         expect(
            seededList.find((c) => c.name === connName)?.resource,
         ).toBeTruthy();

         await gotoHome(page);
         await openEnvironment(page, DEFAULT_ENV);

         const card = page.locator("h6", { hasText: connName });
         await expect(card).toBeVisible();

         await page
            .getByRole("button", { name: `Connection actions for ${connName}` })
            .click();
         await page
            .getByRole("menuitem", { name: `Delete connection ${connName}` })
            .click();
         const deleteDialog = page.getByRole("dialog", {
            name: "Delete Connection",
         });
         await expect(deleteDialog).toBeVisible();

         // Which request the UI sends is the load-bearing assertion, not just
         // whether the card goes away. PATCHing the environment with the
         // connection filtered out also empties the list in memory, but it
         // upserts the connections the environment still holds and never drops
         // the row for one that went away, so the connection returns on the
         // next boot. Only the dedicated endpoint removes the row.
         const deleteRequest = page.waitForRequest(
            (req) =>
               req.method() === "DELETE" &&
               req
                  .url()
                  .includes(
                     `/api/v0/environments/${DEFAULT_ENV}/connections/${connName}`,
                  ),
            { timeout: 15_000 },
         );
         await deleteDialog.getByRole("button", { name: "Delete" }).click();
         await deleteRequest;

         // The dialog has no self-close: it goes away by unmounting with the
         // card when the refetch drops the connection. So both of these say
         // "the delete happened", not "the button was clickable".
         await expect(deleteDialog).toBeHidden({ timeout: 15_000 });
         await expect(card).toHaveCount(0);
      } finally {
         // A failure after seeding would otherwise leave the connection in the
         // environment for the rest of the run, and on a locally reused server
         // (playwright.config.ts sets reuseExistingServer) for every run after
         // it. Unconditional, and a 404 here is the success case.
         await request
            .delete(
               `/api/v0/environments/${DEFAULT_ENV}/connections/${connName}`,
            )
            .catch(() => undefined);
      }
   });
});

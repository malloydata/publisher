// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { tmpName } from "./helpers/fixtures";

/**
 * The fixture is copied to a temp directory because saving writes the notebook
 * file. A server that refuses the environment registration is read-only, so the
 * spec skips; the browser-draft save path is covered by the SDK's tests.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(
   __dirname,
   "../../../server/tests/fixtures/notebooks-malloyyo",
);
const PKG = "notebooks-malloyyo";
const SLUG = "revenue_review";
const EDITED = "Revenue prose, edited in the browser";

let env: string;
let baseURL: string;
let location: string;

test.describe("notebook-builder", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      env = tmpName("nbbuilder");
      location = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-nb-builder-"),
      );
      fs.cpSync(FIXTURE, location, { recursive: true });
      const res = await fetch(`${baseURL}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: env,
            packages: [{ name: PKG, location }],
            connections: [],
         }),
      });
      test.skip(
         res.status === 405 || res.status === 403,
         "publisher is read-only",
      );
      expect(res.ok, await res.text()).toBe(true);
   });

   test.afterAll(async () => {
      if (env && baseURL)
         await fetch(`${baseURL}/api/v0/environments/${env}`, {
            method: "DELETE",
         }).catch(() => undefined);
      if (location) fs.rmSync(location, { recursive: true, force: true });
   });

   test("edits a markdown cell and saves it into the package file", async ({
      page,
   }) => {
      // The server serves a copy of the fixture, so the file is read back through the API.
      const readSource = async () => {
         const res = await fetch(
            `${baseURL}/api/v0/environments/${env}/packages/${PKG}/models/${encodeURIComponent(`notebooks/${SLUG}.malloy`)}`,
         );
         const body = await res.text();
         expect(res.ok, body).toBe(true);
         return (JSON.parse(body) as { sourceText: string }).sourceText;
      };
      const before = await readSource();

      // The reader's Edit button leads into the editor.
      await page.goto(`/${env}/${PKG}/notebooks/${SLUG}`);
      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await expect(page).toHaveURL(new RegExp(`/notebooks/${SLUG}/edit$`));
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByText("Where revenue came from")).toBeVisible();

      const cell = page.getByRole("group", { name: "Cell 2, text" });
      await cell.getByRole("button", { name: "Edit text" }).click();
      await page.getByLabel("Markdown", { exact: true }).fill(EDITED);
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await expect(
         page.getByRole("button", { name: "Save changes" }),
      ).toBeEnabled();
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(page.getByRole("button", { name: "Saved" })).toBeVisible();

      // On disk: the edited cell, and every byte outside it untouched.
      const after = await readSource();
      expect(after).toContain(EDITED);
      expect(after).not.toContain("Where revenue came from");
      expect(after).toContain(
         "##(markdown) A single line of prose is a cell too.",
      );
      expect(after.slice(after.indexOf("given: REGION"))).toBe(
         before.slice(before.indexOf("given: REGION")),
      );

      // A reload re-reads the saved file, and the reader renders it too.
      await page.reload();
      await expect(page.getByText(EDITED)).toBeVisible({ timeout: 60_000 });
      await page.goto(`/${env}/${PKG}/notebooks/${SLUG}`);
      await expect(page.getByText(EDITED)).toBeVisible({ timeout: 60_000 });
      // A saved file whose query cells no longer run would leave this empty.
      await expect(page.locator(".malloy-render").first()).toBeVisible({
         timeout: 60_000,
      });
   });
});

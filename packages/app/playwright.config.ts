// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright config for the Publisher React app.
 *
 * Expects a publisher server running at BASE_URL (default
 * http://localhost:4000). The global-setup step polls /api/v0/status until
 * `operationalState === "serving"` before any spec runs, so packages have
 * finished loading.
 *
 * Run the server yourself (e.g. `npm run start:init` from repo root) OR let
 * the `webServer` block below spawn it.
 */

const BASE_URL = process.env.PUBLISHER_URL ?? "http://localhost:4000";
const USE_WEB_SERVER = process.env.PLAYWRIGHT_USE_WEBSERVER !== "0";
const IS_CI = !!process.env.CI;

export default defineConfig({
   testDir: "./tests/playwright",
   timeout: 60_000,
   expect: { timeout: 15_000 },
   fullyParallel: false,
   // One worker, because `fullyParallel: false` only serializes tests WITHIN a
   // file; separate files still run concurrently, and several of these write to
   // the same shared state. `notebook-givens`, `notebook-authorize` and
   // `notebook-readme-links` each write fixture files into
   // `publisher_data/examples/storefront` and POST `?reload=true` on that
   // package; they are the only three specs that do either. A second kind of
   // contention is environment churn: `packages.spec`, `environments.spec` and
   // `package-dashboards.spec` each create and delete a whole environment,
   // which appears and vanishes from the listings other specs navigate
   // through. All three, not just the first: anyone relaxing `workers: 1`
   // reads this comment to find out what contends, so a short list is worse
   // than none. Observed: a `notebook-givens` navigation timed out
   // waiting for the storefront tile while another file was mid-reload. It
   // passes 9/9 in isolation, which is the tell that it is contention rather
   // than a broken assertion.
   workers: 1,
   retries: IS_CI ? 1 : 0,
   reporter: IS_CI
      ? [
           ["list"],
           ["html", { outputFolder: "playwright-report", open: "never" }],
        ]
      : [["list"]],
   use: {
      baseURL: BASE_URL,
      trace: IS_CI ? "retain-on-failure" : "on-first-retry",
      screenshot: "only-on-failure",
      video: IS_CI ? "retain-on-failure" : "off",
   },
   projects: [
      {
         name: "chromium",
         use: { ...devices["Desktop Chrome"] },
      },
   ],
   globalSetup: "./tests/playwright/global-setup.ts",
   webServer: USE_WEB_SERVER
      ? {
           // Start the publisher from the repo root so `npm run start:init`
           // resolves to the server's init-mode start (loads config,
           // downloads fixture packages, marks ready when done).
           command: "npm run start:init",
           cwd: "../../",
           url: `${BASE_URL}/api/v0/status`,
           reuseExistingServer: true,
           timeout: 300_000,
           stdout: "pipe",
           stderr: "pipe",
        }
      : undefined,
});

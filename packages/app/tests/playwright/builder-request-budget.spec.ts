// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * How many API requests an editor costs. A budget only means something with
 * the same counting rules each time: every fetch/xhr the page makes (static
 * assets excluded) from the moment the navigation starts until the page has
 * been quiet, then a further quiet stretch to prove nothing keeps polling.
 */

const FIRST_LOAD_MAX = 32;
const NAVIGATION_MAX = 15;
const IDLE_SECONDS = 5;

interface Counted {
   total: number;
   repeats: string[];
   idle: number;
   requests: string[];
}

function watch(page: Page) {
   const seen: string[] = [];
   page.on("request", (request) => {
      if (!["fetch", "xhr"].includes(request.resourceType())) return;
      seen.push(
         `${request.method()} ${new URL(request.url()).pathname}${new URL(request.url()).search} ${request.postData() ?? ""}`,
      );
   });
   return {
      mark: () => seen.length,
      since: (from: number) => seen.slice(from),
   };
}

async function quiet(
   page: Page,
   watcher: ReturnType<typeof watch>,
   ms: number,
) {
   let last = watcher.mark();
   let stable = 0;
   const step = 250;
   // Quiet means no new request for `ms` in a row, bounded so a chatty page fails the idle count rather than hanging.
   for (let waited = 0; waited < 30_000; waited += step) {
      await page.waitForTimeout(step);
      const now = watcher.mark();
      if (now === last) stable += step;
      else {
         stable = 0;
         last = now;
      }
      if (stable >= ms) return;
   }
}

async function measure(
   page: Page,
   action: () => Promise<void>,
   ready: () => Promise<void>,
): Promise<Counted> {
   const watcher = watch(page);
   const start = watcher.mark();
   await action();
   await ready();
   await quiet(page, watcher, 1_500);
   const settled = watcher.mark();
   await quiet(page, watcher, IDLE_SECONDS * 1_000);
   const requests = watcher.since(start);
   const counts = new Map<string, number>();
   for (const r of requests.slice(0, settled - start))
      counts.set(r, (counts.get(r) ?? 0) + 1);
   return {
      total: settled - start,
      repeats: [...counts]
         .filter(([, n]) => n > 1)
         .map(([r, n]) => `${n}x ${r}`),
      idle: watcher.mark() - settled,
      requests: requests.map((r) => r.slice(0, 160)),
   };
}

let pe: PackageEnv;

test.describe("builder request budget", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "budget",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   const editors = [
      {
         name: "dashboard",
         path: "dashboards/overview",
         ready: (page: Page) =>
            expect(page.locator("[data-malloy-render-as]").first()).toBeVisible(
               {
                  timeout: 60_000,
               },
            ),
         link: /Overview|Storefront overview/,
         // Measured over budget: the model file is fetched twice by two
         // callers on open, and reader-to-editor costs 18 against 15 because
         // the five tiles run once unbound and again with the document's
         // bindings. Expected to fail until those are fixed.
         overBudget:
            "dashboard editor fetches its model twice and re-runs every tile on open",
      },
      {
         name: "notebook",
         path: "notebooks/category-review",
         ready: (page: Page) =>
            expect(page.locator("[data-malloy-render-as]").first()).toBeVisible(
               {
                  timeout: 60_000,
               },
            ),
         link: /Category review/,
         overBudget: undefined as string | undefined,
      },
   ];

   for (const editor of editors) {
      const report = (label: string, counted: Counted) => {
         console.log(
            `[budget] ${editor.name} ${label}: total=${counted.total} repeats=${counted.repeats.length} idle(${IDLE_SECONDS}s)=${counted.idle}`,
         );
         if (counted.repeats.length || counted.idle)
            console.log(
               `[budget] ${editor.name} ${label} detail:\n${counted.repeats.join("\n")}\n--\n${counted.requests.join("\n")}`,
            );
      };

      test(`${editor.name} editor: first load`, async ({ page }) => {
         if (editor.overBudget) test.fail(true, editor.overBudget);
         const counted = await measure(
            page,
            () =>
               page
                  .goto(`/${pe.env}/${pe.pkg}/${editor.path}/edit`)
                  .then(() => undefined),
            () => editor.ready(page),
         );
         report("first load", counted);
         expect(counted.total).toBeLessThanOrEqual(FIRST_LOAD_MAX);
         expect(counted.repeats).toEqual([]);
         expect(counted.idle).toBe(0);
      });

      test(`${editor.name} editor: navigation from the package page`, async ({
         page,
      }) => {
         if (editor.overBudget) test.fail(true, editor.overBudget);
         await page.goto(`/${pe.env}/${pe.pkg}`);
         await expect(
            page.getByRole("button", { name: editor.link }).first(),
         ).toBeVisible({ timeout: 60_000 });
         // Route into the reader first, so Edit is a client-side navigation.
         await page.getByRole("button", { name: editor.link }).first().click();
         await expect(
            page.locator("[data-malloy-render-as]").first(),
         ).toBeVisible({
            timeout: 60_000,
         });
         const counted = await measure(
            page,
            () =>
               page.getByRole("button", { name: "Edit", exact: true }).click(),
            () =>
               expect(page.getByText("Editing", { exact: true })).toBeVisible({
                  timeout: 60_000,
               }),
         );
         report("navigation (reader to editor)", counted);
         expect(counted.total).toBeLessThanOrEqual(NAVIGATION_MAX);
         expect(counted.repeats).toEqual([]);
         expect(counted.idle).toBe(0);
      });
   }
});

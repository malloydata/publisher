import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";

/**
 * The app's route for a model is `/<env>/<pkg>/<file>`, and a package's own
 * static files are served from `/environments/<env>/packages/<pkg>/<file>`. The
 * two are easy to confuse, and the catch-all used to answer the wrong one with
 * the app shell and HTTP 200, which reads as success: the page then reported
 * "Unrecognized file type: index.html", blaming the file for a wrong path.
 *
 * These assert the status code and the served document rather than what the app
 * renders, because a 200 carrying the shell was the whole defect.
 */
test.describe("package asset URLs", () => {
   test("a package file addressed on the app route reaches the file", async ({
      request,
   }) => {
      const wrong = `/${DEFAULT_ENV}/${PACKAGES.dataApp}/index.html`;
      const right = `/environments/${DEFAULT_ENV}/packages/${PACKAGES.dataApp}/index.html`;

      const redirect = await request.get(wrong, { maxRedirects: 0 });
      expect(redirect.status()).toBe(302);
      expect(redirect.headers()["location"]).toBe(right);

      // Following it lands on the same document the correct URL serves, which is
      // the measurement the defect was reported with: same status, same title.
      const [followed, direct] = await Promise.all([
         request.get(wrong),
         request.get(right),
      ]);
      expect(followed.status()).toBe(200);
      expect(direct.status()).toBe(200);
      const title = (body: string) => /<title>([^<]*)<\/title>/.exec(body)?.[1];
      expect(title(await followed.text())).toBe(title(await direct.text()));
      expect(title(await followed.text())).not.toBe("Malloy Publisher");
   });

   test("the redirect keeps the query string", async ({ request }) => {
      // An embedded page carries ?embed_token=..., so sending it to the right
      // path without its query would swap one broken result for another.
      const redirect = await request.get(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/index.html?embed_token=abc&x=1`,
         { maxRedirects: 0 },
      );
      expect(redirect.status()).toBe(302);
      expect(redirect.headers()["location"]).toBe(
         `/environments/${DEFAULT_ENV}/packages/${PACKAGES.dataApp}/index.html?embed_token=abc&x=1`,
      );
   });

   test("a package file that does not exist says so instead of returning 200", async ({
      request,
   }) => {
      const response = await request.get(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/no-such-page.html`,
      );
      expect(response.status()).toBe(404);
   });

   test("a guessed `public/` segment reaches the file", async ({ request }) => {
      // The 07-28 near-miss: the files live in `public/`, so a reader guesses
      // that segment. The static route does not strip it, so the redirect has
      // to. Asserted end to end because the unit test can only pin the target.
      const guess = `/${DEFAULT_ENV}/${PACKAGES.dataApp}/public/index.html`;
      const redirect = await request.get(guess, { maxRedirects: 0 });
      expect(redirect.status()).toBe(302);
      expect(redirect.headers()["location"]).toBe(
         `/environments/${DEFAULT_ENV}/packages/${PACKAGES.dataApp}/index.html`,
      );
      const followed = await request.get(guess);
      expect(followed.status()).toBe(200);
      expect(await followed.text()).toContain("<title>");
   });

   test("a path shaped like a package file but under no environment 404s here", async ({
      request,
   }) => {
      // `/assets/foo/bar.js` has the same shape as `/<env>/<pkg>/<file>`.
      // Redirecting it would answer with JSON naming an internal resolution
      // failure and echo the segment back, so the guess only stands if the
      // environment is real.
      const response = await request.get("/assets/foo/bar.js", {
         maxRedirects: 0,
      });
      expect(response.status()).toBe(404);
      expect(response.headers()["content-type"]).toContain("text/html");
      const body = await response.text();
      expect(body).toContain("public/");
      expect(body).not.toContain("could not be resolved");
   });

   test("an unknown API endpoint answers JSON, not the app shell", async ({
      request,
   }) => {
      const response = await request.get("/api/v0/bogus-endpoint");
      expect(response.status()).toBe(404);
      expect(response.headers()["content-type"]).toContain("application/json");
      expect((await response.json()).code).toBe(404);
   });

   test("model and notebook URLs still serve the app", async ({ request }) => {
      // The guard decides by extension, so these are the routes it must not
      // divert: they are how every model and notebook in the product is opened.
      for (const path of [
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront.malloy`,
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront.malloynb`,
         `/${DEFAULT_ENV}/${PACKAGES.storefront}`,
         `/${DEFAULT_ENV}`,
      ]) {
         const response = await request.get(path, { maxRedirects: 0 });
         expect(response.status(), `${path} should serve the app`).toBe(200);
         expect(await response.text()).toContain("<title>Malloy Publisher");
      }
   });

   test("the in-app page viewer route is not diverted", async ({ request }) => {
      // `pages/<file>.html` ends in an asset extension but is an app route: the
      // package view links to it and ModelPage iframes the standalone URL.
      const response = await request.get(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/pages/index.html`,
         { maxRedirects: 0 },
      );
      expect(response.status()).toBe(200);
      expect(await response.text()).toContain("<title>Malloy Publisher");
   });

   test("the app names the path when it is not a model", async ({ page }) => {
      // What a reader sees for a path with no servable extension: the old text
      // named the file's type, which was a confident answer to the wrong
      // question. This one names the path and points at what does serve it.
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.storefront}/not-a-model`);
      await expect(
         page.getByRole("heading", { name: "Nothing to open at this path" }),
      ).toBeVisible();
      // Scoped to the message: the path is in the breadcrumb too.
      await expect(
         page.getByText(/is not a model or notebook in package/),
      ).toContainText("not-a-model");
      await expect(page.getByText("Unrecognized file type")).toHaveCount(0);
   });

   test("the app offers the static URL when the path names a file", async ({
      page,
   }) => {
      // Reachable for a file extension the server does not divert, and in
      // development, where Vite serves the shell instead of the catch-all.
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.dataApp}/report.yaml`);
      const link = page.getByRole("link", {
         name: new RegExp(
            `/environments/${DEFAULT_ENV}/packages/${PACKAGES.dataApp}/report\\.yaml$`,
         ),
      });
      await expect(link).toBeVisible();
   });
});

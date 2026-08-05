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
 *
 * REQUIRES A SERVER RUNNING THE BUILT APP. The server-side half of this lives
 * behind `if (!isDevelopment)`, and a dev server hands every unmatched path to
 * Vite before any of it runs, so against `make dev` the redirect and 404 cases
 * below fail for a reason that has nothing to do with the code. Note the config
 * sets `reuseExistingServer`, so a dev server already listening on the port is
 * adopted whether or not you pass `PLAYWRIGHT_USE_WEBSERVER=0`. If these fail
 * together, check what is on the port before reading the diff.
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
      // The body is the point: this route used to answer with an empty 404, so a
      // status-only assertion would pass against the version this replaced.
      const body = await response.text();
      expect(body).toContain("public/");
      expect(body).toContain("does not include it");
   });

   test("an asset under a real environment is not rescued into the app shell", async ({
      request,
   }) => {
      // The short-path rescue exists so a package legally named `report.html`
      // still serves its page. It must not extend to any asset request that
      // happens to sit under a real environment name, which would put back the
      // 200-plus-shell answer this whole file is about.
      const response = await request.get(`/${DEFAULT_ENV}/style.css`, {
         maxRedirects: 0,
      });
      expect(response.status()).toBe(404);
      expect(await response.text()).not.toContain("<title>Malloy Publisher");
   });

   test("a real environment with an unknown package does not reflect the name back", async ({
      request,
   }) => {
      // Redirecting this landed on the static route, which answers an
      // unresolvable name with JSON naming an internal failure and echoing the
      // segment, which the adjacent route's own 404s deliberately avoid.
      const response = await request.get(
         `/${DEFAULT_ENV}/no-such-package/index.html`,
         { maxRedirects: 0 },
      );
      expect(response.status()).toBe(404);
      expect(response.headers()["content-type"]).toContain("text/html");
      const body = await response.text();
      // The explanatory page, not the static route's error JSON. It does echo the
      // path the caller typed, escaped, which is their own input; what must not
      // appear is an internal resolution failure naming the package.
      expect(body).toContain("public/");
      expect(body).not.toContain("not found");
      expect(body).not.toContain('"code"');
   });

   test("a structured query parameter survives the redirect intact", async ({
      request,
   }) => {
      // Rebuilding the query from Express's parsed object turned `filter[a]=1`
      // into `filter=[object Object]`, so the page got a corrupted parameter
      // rather than an intact one.
      const response = await request.get(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/index.html?filter%5Bregion%5D=west`,
         { maxRedirects: 0 },
      );
      expect(response.status()).toBe(302);
      expect(response.headers()["location"]).toContain(
         "filter%5Bregion%5D=west",
      );
      expect(response.headers()["location"]).not.toContain("object");
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

   test("the in-app data app viewer route is not diverted", async ({
      request,
   }) => {
      // `data-apps/<file>.html` ends in an asset extension but is an app route:
      // the package view links to it and ModelPage iframes the standalone URL.
      const response = await request.get(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/data-apps/index.html`,
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
      // Scoped to the message: the path is in the breadcrumb too. The wording
      // claims only what the component checked, the extension, rather than
      // asserting the file does not exist, which it never asked the server.
      await expect(
         page.getByText(/does not name a .*file in package/),
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

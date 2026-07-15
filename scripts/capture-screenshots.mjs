// One-off screenshot capture for the docs. Requires a Publisher server running
// on localhost:4000 serving the `examples` environment.
//
//   node scripts/capture-screenshots.mjs
//
// Writes PNGs into docs/screenshots/. Playwright + chromium ship with the repo
// dev deps (see packages/app Playwright tests).
import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";

const BASE = process.env.PUBLISHER_BASE || "http://localhost:4000";
const OUT = "docs/screenshots";

const SHOTS = [
  {
    file: "storefront-data-app.png",
    url: `${BASE}/environments/examples/packages/storefront/index.html`,
    viewport: { width: 1200, height: 900 },
    waitUntil: "domcontentloaded",   // page holds an SSE live-reload stream, so networkidle never fires
    waitFor: "#byCategory",
    settle: 5000,
    fullPage: true,
  },
  {
    file: "storefront-dashboard.png",
    url: `${BASE}/examples/storefront/storefront.malloynb`,
    viewport: { width: 1440, height: 1400 },
    waitFor: "svg, canvas",
    settle: 6000,
    fullPage: false,
  },
  {
    // README hero — the storefront Business overview dashboard, with the
    // dashboard cell expanded past its internal scroll so every chart shows.
    file: "../malloy-publisher-demo.png",
    url: `${BASE}/examples/storefront/storefront.malloynb`,
    viewport: { width: 1600, height: 1440 },
    waitFor: "svg, canvas",
    settle: 6000,
    expandDashboard: true,
    scrollY: 292,
    fullPage: false,
  },
  {
    file: "html-data-app-dashboard.png",
    url: `${BASE}/environments/examples/packages/html-data-app/index.html`,
    viewport: { width: 1200, height: 900 },
    waitUntil: "domcontentloaded", // SSE live-reload stream, so networkidle never fires
    waitFor: "#mrrByMonth",
    settle: 5000,
    fullPage: false,
  },
  {
    file: "givens-parameters-panel.png",
    url: `${BASE}/examples/governed-analytics/orders.malloynb`,
    viewport: { width: 1440, height: 900 },
    waitFor: "input",
    settle: 5000,
    fullPage: false,
  },
  {
    file: "publisher-app.png",
    url: `${BASE}/examples/storefront`,
    viewport: { width: 1440, height: 900 },
    waitFor: "body",
    settle: 3000,
    fullPage: false,
  },
  {
    file: "api-explorer.png",
    url: `${BASE}/api-doc.html`,
    viewport: { width: 1440, height: 1000 },
    waitFor: ".swagger-ui",
    settle: 3000,
    fullPage: false,
  },
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });
let ok = 0;
for (const s of SHOTS) {
  const page = await browser.newPage({ viewport: s.viewport, deviceScaleFactor: 2 });
  try {
    await page.goto(s.url, { waitUntil: s.waitUntil || "networkidle", timeout: 30000 });
    if (s.waitFor) {
      await page.waitForSelector(s.waitFor, { timeout: 15000 }).catch(() => {});
    }
    await sleep(s.settle);
    if (s.expandDashboard) {
      // Dashboard cells cap their height and scroll internally; lift the cap
      // on the cell and its ancestors so the whole dashboard is visible.
      await page.evaluate(() => {
        let el = document.querySelector(".malloy-dashboard");
        for (let i = 0; i < 7 && el; i++) {
          el.style.setProperty("height", "auto", "important");
          el.style.setProperty("max-height", "none", "important");
          el.style.setProperty("overflow", "visible", "important");
          el = el.parentElement;
        }
      });
      await sleep(1500);
    }
    if (s.scrollY) {
      // Some pages scroll an inner container, so scroll via the mouse wheel
      // over the page center rather than window.scrollTo.
      await page.mouse.move(s.viewport.width / 2, s.viewport.height / 2);
      await page.mouse.wheel(0, s.scrollY);
      await sleep(1000);
    }
    await page.screenshot({ path: `${OUT}/${s.file}`, fullPage: s.fullPage });
    console.log(`✓ ${s.file}`);
    ok++;
  } catch (e) {
    console.log(`✗ ${s.file}: ${e.message}`);
  } finally {
    await page.close();
  }
}
await browser.close();
console.log(`\n${ok}/${SHOTS.length} captured into ${OUT}/`);

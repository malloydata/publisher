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
    waitFor: "td.publisher-drill",
    settle: 5000,
    fullPage: true,
  },
  {
    file: "storefront-dashboard.png",
    url: `${BASE}/examples/storefront/storefront.malloynb`,
    viewport: { width: 1440, height: 1400 },
    waitFor: "svg, canvas",
    settle: 6000,
    anchor: "h2:has-text('Business overview')",
    fullPage: false,
  },
  {
    // README hero — the storefront Business overview dashboard. No
    // `expandDashboard`: the dashboard fits this viewport on its own now, and
    // lifting the height caps reflows the category tile until its axis labels
    // clip.
    file: "../malloy-publisher-demo.png",
    url: `${BASE}/examples/storefront/storefront.malloynb`,
    viewport: { width: 1600, height: 1440 },
    waitFor: "svg, canvas",
    settle: 6000,
    anchor: "h2:has-text('Business overview')",
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
    // Tall enough to reach Materializations, the last of the six content
    // sections: the shot is what docs/console.md points at when it says every
    // kind of content has its own icon and color, and four of six does not
    // show that.
    file: "console.png",
    url: `${BASE}/examples/storefront`,
    viewport: { width: 1440, height: 1440 },
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

// Name one or more files to capture just those, leaving the other PNGs (and
// their binary diffs) alone: `node scripts/capture-screenshots.mjs console.png`.
const only = process.argv.slice(2);
const shots = only.length ? SHOTS.filter((s) => only.includes(s.file)) : SHOTS;

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });
let ok = 0;
for (const s of shots) {
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
    // Scrolling to an element rather than a pixel offset, so editing the prose
    // above a shot does not silently reframe it.
    const scrollBy = s.anchor
      ? await page
          .locator(s.anchor)
          .first()
          .boundingBox()
          .then((box) => (box ? box.y - (s.anchorOffset ?? 24) : 0))
          .catch(() => 0)
      : (s.scrollY ?? 0);
    if (scrollBy) {
      // Some pages scroll an inner container, so scroll via the mouse wheel
      // over the page center rather than window.scrollTo.
      await page.mouse.move(s.viewport.width / 2, s.viewport.height / 2);
      await page.mouse.wheel(0, scrollBy);
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
console.log(`\n${ok}/${shots.length} captured into ${OUT}/`);

// Short screen recordings for the docs, captured with Playwright and converted
// to optimized GIFs with ffmpeg. Requires a Publisher server running on
// localhost:4000 serving the `examples` environment, plus `ffmpeg` on PATH.
//
//   node scripts/capture-recordings.mjs
//
// Writes GIFs into docs/screenshots/. Each recording is a self-contained clip
// that shows a feature in motion — the kind of thing a still can't sell. The
// initial page-load frames are trimmed so the clip opens on populated data.
import { chromium } from "playwright";
import { mkdir, mkdtemp, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

const BASE = process.env.PUBLISHER_BASE || "http://localhost:4000";
const OUT = "docs/screenshots";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function ffmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", ["-y", ...args], { stdio: "ignore" });
    p.on("error", reject);
    p.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`ffmpeg exited ${code}`)),
    );
  });
}

// Turn the single .webm Playwright writes into an optimized, looping GIF,
// skipping `trimStart` seconds of leading page-load footage.
async function toGif(webmDir, outFile, { fps = 10, width = 820, trimStart = 0 } = {}) {
  const files = (await readdir(webmDir)).filter((f) => f.endsWith(".webm"));
  if (!files.length) throw new Error("no video recorded");
  const webm = join(webmDir, files[0]);
  const pre = trimStart > 0 ? ["-ss", String(trimStart)] : [];
  await ffmpeg([
    ...pre,
    "-i",
    webm,
    "-vf",
    `fps=${fps},scale=${width}:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=96[p];[s1][p]paletteuse=dither=bayer`,
    "-loop",
    "0",
    `${OUT}/${outFile}`,
  ]);
}

// Record one clip: wait until the page is populated, hold on it briefly, run
// `steps(page)` while Playwright captures video, then convert to a GIF and
// trim the leading load. Failures are logged and skipped, never fatal.
async function record(
  browser,
  { name, url, viewport, ready, gif, steps, trimStart = 2, width = 820 },
) {
  const dir = await mkdtemp(join(tmpdir(), "publisher-rec-"));
  const context = await browser.newContext({
    viewport,
    recordVideo: { dir, size: viewport },
  });
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await ready(page); // resolves only once real data is on screen
    await sleep(1400); // hold on the populated baseline before acting
    await steps(page);
    await sleep(1200);
    await page.close();
    await context.close(); // flushes the video file
    await toGif(dir, gif, { trimStart, width });
    console.log(`✓ ${gif}`);
    return 1;
  } catch (e) {
    console.log(`✗ ${name}: ${e.message}`);
    try {
      await context.close();
    } catch {}
    return 0;
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

// Name one or more recordings to re-record just those, leaving the other GIFs
// (and their binary diffs) alone: `node scripts/capture-recordings.mjs givens-live`.
const only = process.argv.slice(2);
const wanted = (name) => only.length === 0 || only.includes(name);

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });
let ok = 0;
let attempted = 0;

const maybeRecord = async (options) => {
  if (!wanted(options.name)) return;
  attempted++;
  ok += await record(browser, options);
};

// 1) HTML data app — filtering re-queries every tile, and a cell drills.
await maybeRecord({
  name: "data-app-filtering",
  url: `${BASE}/environments/examples/packages/storefront/index.html`,
  viewport: { width: 1100, height: 760 },
  gif: "data-app-filtering.gif",
  width: 820,
  trimStart: 1.0,
  ready: async (page) => {
    await page.waitForSelector("td.publisher-drill", { timeout: 20000 });
    await page.waitForFunction(
      () =>
        /^\$[\d,]/.test(
          document.querySelector(".panel:not([hidden]) [data-kpi='total_sales'] .kpi-value")
            ?.textContent || "",
        ),
      { timeout: 20000 },
    );
  },
  steps: async (page) => {
    await page.selectOption("#given-REGION", { label: "West" });
    await sleep(1800);
    // Then a drill: a category cell offers its destinations, and choosing one
    // switches tab with the value seeded.
    await page.locator('[data-tile="categories"] td.publisher-drill').first().click();
    await sleep(1200);
    await page.locator(".drill-menu-item", { hasText: "Category" }).click();
    await sleep(2000);
    await page.click("#reset");
    await sleep(1400);
  },
});

// 2) Givens Parameters panel — change a control, every notebook cell re-runs.
await maybeRecord({
  name: "givens-live",
  url: `${BASE}/examples/governed-analytics/orders.malloynb`,
  viewport: { width: 1280, height: 820 },
  gif: "givens-live.gif",
  width: 900,
  trimStart: 1.6,
  ready: async (page) => {
    await page.waitForSelector('[role="combobox"]', { timeout: 20000 });
    await page.waitForSelector("svg, canvas", { timeout: 25000 }); // dashboard rendered
  },
  steps: async (page) => {
    // The controls are what the declarations asked for: a Region dropdown, a
    // Status multi-select, and a Minimum-amount slider.
    const region = page.locator('[role="combobox"]').first();
    await region.click();
    await sleep(900);
    await page.locator('[role="option"]', { hasText: "us-east" }).first().click();
    await sleep(3200);

    const status = page.locator('[role="combobox"]').nth(1);
    await status.click();
    await sleep(900);
    await page.locator('[role="option"]', { hasText: "Complete" }).first().click();
    await page.keyboard.press("Escape");
    await sleep(3200);

    const slider = page.locator('.MuiSlider-root input[type="range"]').first();
    await slider.focus();
    for (let i = 0; i < 12; i++) await page.keyboard.press("ArrowRight");
    await sleep(3200);
  },
});

await browser.close();
console.log(`\n${ok}/${attempted} recordings written into ${OUT}/`);

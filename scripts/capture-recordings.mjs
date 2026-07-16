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

const browser = await chromium.launch();
await mkdir(OUT, { recursive: true });
let ok = 0;

// 1) SaaS HTML data app — live filtering updates KPIs, charts, and the table.
ok += await record(browser, {
  name: "saas-app-filtering",
  url: `${BASE}/environments/examples/packages/html-data-app/index.html`,
  viewport: { width: 1100, height: 760 },
  gif: "html-data-app-filtering.gif",
  width: 820,
  trimStart: 1.0,
  ready: async (page) => {
    await page.waitForSelector("#mrrByMonth", { timeout: 20000 });
    await page.waitForFunction(
      () => /^\$[\d,]/.test(document.getElementById("kpi-mrr")?.textContent || ""),
      { timeout: 20000 },
    );
  },
  steps: async (page) => {
    await page.selectOption("#f-plan", { label: "Enterprise" });
    await sleep(1700);
    await page.selectOption("#f-industry", { label: "Healthcare" });
    await sleep(1700);
    await page.click("#reset");
    await sleep(1400);
  },
});

// 2) Givens Parameters panel — change a control, every notebook cell re-runs.
ok += await record(browser, {
  name: "givens-live",
  url: `${BASE}/examples/governed-analytics/orders.malloynb`,
  viewport: { width: 1280, height: 820 },
  gif: "givens-live.gif",
  width: 900,
  trimStart: 1.6,
  ready: async (page) => {
    await page.waitForSelector("input", { timeout: 20000 });
    await page.waitForSelector("svg, canvas", { timeout: 25000 }); // dashboard rendered
  },
  steps: async (page) => {
    const inputs = page.locator("input");
    const region = inputs.nth(0);
    const minAmount = inputs.nth(1);
    await minAmount.click();
    await minAmount.fill("5000");
    await minAmount.press("Enter");
    await sleep(3000);
    await region.click();
    await region.fill("us-east");
    await region.press("Enter");
    await sleep(3200);
  },
});

await browser.close();
console.log(`\n${ok}/2 recordings written into ${OUT}/`);

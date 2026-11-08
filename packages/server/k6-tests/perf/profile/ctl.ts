// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Drive profiling windows against a Publisher instrumented with hook.mjs.
// Profiling only; see docs/performance-profile.md for the workflow.
//
//   bun perf/profile/ctl.ts serve   <dir> [KEY=VALUE ...]
//       Generate the medium and large packages into <dir>/pkgs (once), copy
//       storefront beside them, and run Publisher in the foreground on
//       :4200/:4240 with the hook preloaded. Extra KEY=VALUE pairs go into the
//       server's environment (e.g. PERF_PROFILE_TRACE=compile,duckdb).
//   bun perf/profile/ctl.ts loads   <dir> <register|reload> <pkg> <n> [--prof]
//       Two warm-up operations, then a window around n sequential operations.
//       pkg: storefront | genm | genl (register) or gen | storefront (reload).
//   bun perf/profile/ctl.ts queries <dir> <storefront|gen> <vus> <seconds> [--prof]
//       Run the harness's queries.ts with k6 and open a window over the middle
//       of the run; prints process CPU (cores used), k6 per-op results and the
//       hook's event-loop and DuckDB summaries.
//   bun perf/profile/ctl.ts phases  <dir> <window>
//       The server's own package-load phase timings inside a window
//       (publisher_package_load_phase_duration_ms, plus the install and reload
//       totals), from /metrics snapshots taken when the window opened and
//       closed, and the event-loop summary. To see which code a stall is in,
//       use the window's CPU profile with analyze.ts --focus.
//
// Output lands in <dir>/out; a window's name is printed when it closes.
import { spawn, spawnSync } from "bun";
import {
   appendFileSync,
   cpSync,
   existsSync,
   mkdirSync,
   readFileSync,
   rmSync,
   writeFileSync,
} from "fs";
import path from "path";
import { generatePackage } from "../gen-package.ts";

const K6_DIR = path.resolve(import.meta.dir, "..", "..");
const SERVER_DIR = path.resolve(K6_DIR, "..");
const REPO = path.resolve(SERVER_DIR, "..", "..");
const BASE = "http://127.0.0.1:4200";
const PKGS = `${BASE}/api/v0/environments/perf/packages`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const [cmd, dirArg, ...rest] = process.argv.slice(2);
if (!cmd || !dirArg) {
   console.error(
      readFileSync(import.meta.path, "utf8")
         .split("\n")
         .slice(3, 25)
         .join("\n"),
   );
   process.exit(2);
}
const DIR = path.resolve(dirArg);
const OUT = path.join(DIR, "out");
const CTL = path.join(DIR, "ctl");
const prof = rest.includes("--prof");
const args = rest.filter((a) => a !== "--prof");

async function waitServing(): Promise<void> {
   for (let i = 0; i < 1200; i++) {
      try {
         const s = (await (await fetch(`${BASE}/api/v0/status`)).json()) as {
            operationalState?: string;
         };
         if (s.operationalState === "serving") return;
      } catch {
         /* not up yet */
      }
      await sleep(250);
   }
   throw new Error("server on :4200 never reached serving");
}

let seq = Date.now();
async function window(line: string, expect: string): Promise<void> {
   appendFileSync(CTL, `${++seq} ${line}\n`);
   for (let i = 0; i < 100; i++) {
      await sleep(100);
      const log = existsSync(`${OUT}/hook.log`)
         ? readFileSync(`${OUT}/hook.log`, "utf8")
         : "";
      if (log.includes(expect)) return;
   }
   throw new Error(
      `hook did not acknowledge "${line}" (is the server running with the hook?)`,
   );
}
/** Snapshot /metrics as a window opens or closes, for `phases`. */
async function snapshotMetrics(name: string, at: "start" | "stop") {
   const res = await fetch(`${BASE}/metrics`);
   if (res.ok) writeFileSync(`${OUT}/${name}.metrics.${at}`, await res.text());
}
const open = async (name: string) => {
   await snapshotMetrics(name, "start");
   await window(
      `start ${name} ${prof ? "prof" : "noprof"}`,
      `main start ${name} `,
   );
   await sleep(300); // let the workers' pollers see it too
};
const close = async (name: string) => {
   await window("stop", `main stop ${name} `);
   await sleep(300);
   await snapshotMetrics(name, "stop");
};

async function timed(
   label: string,
   f: () => Promise<Response>,
): Promise<number> {
   const t = performance.now();
   const r = await f();
   const body = await r.text();
   if (!r.ok) throw new Error(`${label} -> ${r.status} ${body.slice(0, 300)}`);
   return performance.now() - t;
}

function serverPid(): string {
   return spawnSync(["lsof", "-nP", "-t", "-iTCP:4200", "-sTCP:LISTEN"])
      .stdout.toString()
      .trim();
}
function cpuSeconds(pid: string): number {
   const t = spawnSync(["ps", "-o", "cputime=", "-p", pid])
      .stdout.toString()
      .trim();
   return t.split(":").reduce((acc, part) => acc * 60 + Number(part), 0);
}

if (cmd === "serve") {
   const pkgs = path.join(DIR, "pkgs");
   for (const [name, preset] of [
      ["genm", "medium"],
      ["genl", "large"],
   ] as const) {
      if (!existsSync(path.join(pkgs, name, "publisher.json"))) {
         await generatePackage({
            dir: path.join(pkgs, name),
            preset,
            rows: 1_000_000,
            seed: 42,
            name: "gen",
         });
      }
   }
   const sf = path.join(pkgs, "storefront");
   if (!existsSync(sf)) {
      cpSync(path.join(REPO, "examples", "storefront"), sf, {
         recursive: true,
         filter: (src) => !/[/\\](evals|tests|node_modules)([/\\]|$)/.test(src),
      });
   }
   const root = path.join(DIR, "root");
   rmSync(root, { recursive: true, force: true });
   rmSync(OUT, { recursive: true, force: true });
   mkdirSync(root, { recursive: true });
   mkdirSync(OUT, { recursive: true });
   writeFileSync(CTL, "");
   const config = path.join(DIR, "publisher.config.json");
   writeFileSync(
      config,
      JSON.stringify({
         frozenConfig: false,
         environments: [
            {
               name: "perf",
               packages: [
                  { name: "storefront", location: sf },
                  { name: "gen", location: path.join(pkgs, "genm") },
               ],
               connections: [],
            },
         ],
      }),
   );
   const extra = Object.fromEntries(
      args.map((kv) => [
         kv.slice(0, kv.indexOf("=")),
         kv.slice(kv.indexOf("=") + 1),
      ]),
   );
   const proc = spawn(
      [
         "bun",
         "--preload",
         path.join(import.meta.dir, "hook.mjs"),
         path.join(SERVER_DIR, "dist", "server.mjs"),
         "--server_root",
         root,
         "--config",
         config,
         "--port",
         "4200",
         "--mcp_port",
         "4240",
         "--host",
         "127.0.0.1",
         "--no-mcp-config",
         "--init",
      ],
      {
         cwd: root,
         env: {
            ...process.env,
            NODE_ENV: "production",
            DISABLE_RESPONSE_LOGGING: "true",
            LOG_LEVEL: "warn",
            PUBLISHER_NO_MCP_CONFIG: "1",
            PERF_PROFILE_CTL: CTL,
            PERF_PROFILE_OUT: OUT,
            ...extra,
         },
         stdout: Bun.file(path.join(DIR, "server.log")),
         stderr: Bun.file(path.join(DIR, "server.err.log")),
      },
   );
   const stop = () => proc.kill("SIGTERM");
   process.on("SIGINT", stop);
   process.on("SIGTERM", stop);
   console.log(
      `serving on :4200 (pid ${proc.pid}); Ctrl-C to stop. Output: ${OUT}`,
   );
   process.exit(await proc.exited);
} else if (cmd === "loads") {
   const [op, pkg, nArg] = args;
   const n = Number(nArg ?? 5);
   await waitServing();
   const name = `${op}-${pkg}-${n}-${prof ? "prof" : "noprof"}`;
   const times: number[] = [];
   const once = async (i: number, record: boolean) => {
      const keep = (t: number) => record && times.push(t);
      if (op === "register") {
         const pkgName = `p${record ? "" : "w"}${i}x${Date.now() % 100000}`;
         keep(
            await timed("register", () =>
               fetch(PKGS, {
                  method: "POST",
                  headers: { "content-type": "application/json" },
                  body: JSON.stringify({
                     name: pkgName,
                     location: path.join(DIR, "pkgs", pkg!),
                  }),
               }),
            ),
         );
         await timed("delete", () =>
            fetch(`${PKGS}/${pkgName}`, { method: "DELETE" }),
         );
      } else {
         keep(await timed("reload", () => fetch(`${PKGS}/${pkg}?reload=true`)));
      }
   };
   for (let i = 0; i < 2; i++) await once(i, false);
   await sleep(500);
   await open(name);
   const t0 = performance.now();
   for (let i = 0; i < n; i++) await once(i, true);
   const wall = performance.now() - t0;
   await close(name);
   console.log(
      JSON.stringify({
         window: name,
         wallMs: Math.round(wall),
         opMs: times.map(Math.round),
      }),
   );
} else if (cmd === "queries") {
   const [target, vusArg, secArg] = args;
   const vus = Number(vusArg);
   const seconds = Number(secArg ?? 20);
   await waitServing();
   const pkgs = path.join(DIR, "pkgs");
   const sources = [1, 2, 3, 4, 5, 6, 7, 8].map((m) => `sales_${m}_1`);
   const workload =
      target === "storefront"
         ? {
              package: "storefront",
              location: path.join(pkgs, "storefront"),
              modelPath: "storefront.malloy",
              queries: {
                 query_cheap: [
                    "run: order_items -> { aggregate: order_item_count, total_sales }",
                 ],
                 query_medium: [
                    "run: order_items -> sales_by_month",
                    "run: order_items -> by_status",
                 ],
                 query_heavy: [
                    "run: order_items -> business_overview",
                    "run: order_items -> category_performance",
                 ],
              },
              reports: [
                 "overview.malloy",
                 "overview-story.malloy",
                 "category-review.malloy",
              ],
           }
         : {
              package: "gen",
              location: path.join(pkgs, "genm"),
              modelPath: "index.malloy",
              queries: {
                 query_cheap: sources.map((s) => `run: ${s} -> kpis`),
                 query_medium: sources.map(
                    (s) => `run: ${s} -> by_month_status`,
                 ),
                 query_heavy: sources.flatMap((s) => [
                    `run: ${s} -> overview`,
                    `run: ${s} -> by_category_region`,
                 ]),
              },
              reports: ["overview.malloy", "story.malloy"],
           };
   const name = `queries-${target}-c${vus}-${prof ? "prof" : "noprof"}`;
   const summary = path.join(OUT, `${name}.k6.json`);
   const k6 = spawn(
      [
         "k6",
         "run",
         "--quiet",
         "--no-color",
         "--summary-export",
         summary,
         "--summary-trend-stats",
         "avg,min,med,max,p(90),p(95),p(99)",
         path.join(K6_DIR, "perf", "queries.ts"),
      ],
      {
         cwd: K6_DIR,
         env: {
            ...process.env,
            K6_PUBLISHER_URL: BASE,
            PERF_ENV: "perf",
            PERF_WARMUP_ROUNDS: "1",
            PERF_MAX_ERROR_RATE: "0.05",
            PERF_RUN_ID: Date.now().toString(36),
            K6_NO_USAGE_REPORT: "true",
            PERF_WORKLOAD: JSON.stringify(workload),
            PERF_VUS: String(vus),
            PERF_DURATION: `${seconds}s`,
         },
         stdout: "ignore",
         stderr: "ignore",
      },
   );
   const pid = serverPid();
   await sleep(4000); // past setup()'s warm-up
   await open(name);
   const cpu0 = cpuSeconds(pid);
   const t0 = Date.now();
   await sleep((seconds - 6) * 1000);
   const cores = (cpuSeconds(pid) - cpu0) / ((Date.now() - t0) / 1000);
   await close(name);
   await k6.exited;
   console.log(
      `${name}: server process used ${cores.toFixed(1)} cores on average`,
   );
   const k = JSON.parse(readFileSync(summary, "utf8"));
   const win = k.metrics.perf_measure_window_ms?.max as number;
   for (const [key, v] of Object.entries<Record<string, number>>(k.metrics)) {
      const m = /^http_req_duration\{.*op:([a-z_]+).*\}$/.exec(key);
      if (!m || !key.includes("phase:measure")) continue;
      const reqs = k.metrics[key.replace("http_req_duration", "http_reqs")]
         ?.count as number;
      console.log(
         `  ${m[1]!.padEnd(13)} ${(reqs / (win / 1000)).toFixed(1).padStart(6)} req/s  avg ${v.avg!.toFixed(1)}  p95 ${v["p(95)"]!.toFixed(1)}  p99 ${v["p(99)"]!.toFixed(1)}  max ${v.max!.toFixed(1)}`,
      );
   }
   for (const f of ["eld", "duckdb"]) {
      const p = path.join(OUT, `${name}.${f}.json`);
      if (existsSync(p)) {
         const {
            series100ms: _s,
            stalls: _t,
            ...summaryFields
         } = JSON.parse(readFileSync(p, "utf8"));
         console.log(`  ${f}: ${JSON.stringify(summaryFields)}`);
      }
   }
} else if (cmd === "phases") {
   const name = args[0]!;
   const read = (at: "start" | "stop") =>
      histograms(readFileSync(`${OUT}/${name}.metrics.${at}`, "utf8"));
   const [before, after] = [read("start"), read("stop")];
   console.log(`window ${name}: per-load phase time (server metrics)`);
   const rows = [...after.entries()]
      .map(([key, a]) => {
         const b = before.get(key) ?? { sum: 0, count: 0 };
         return { key, n: a.count - b.count, total: a.sum - b.sum };
      })
      .filter((r) => r.n > 0)
      .sort((x, y) => y.total - x.total);
   for (const r of rows) {
      console.log(
         `  ${r.key.padEnd(44)} n=${String(r.n).padStart(4)}  total=${r.total.toFixed(0).padStart(7)} ms  avg=${(r.total / r.n).toFixed(1).padStart(7)} ms`,
      );
   }
   const eldPath = `${OUT}/${name}.eld.json`;
   if (existsSync(eldPath)) {
      const eld = JSON.parse(readFileSync(eldPath, "utf8"));
      console.log(
         `event loop: p50 ${eld.p50.toFixed(2)}  p99 ${eld.p99.toFixed(1)}  p99.9 ${eld.p999.toFixed(1)}  max ${eld.max.toFixed(1)} ms; >50 ms ${eld.over50ms}, >100 ms ${eld.over100ms}; blocked ${(100 * eld.blockedFrac).toFixed(0)}%`,
      );
   }
} else {
   console.error(`unknown command ${cmd}`);
   process.exit(2);
}

/** The package-load duration histograms in a Prometheus text scrape: sum and count per series. */
function histograms(text: string): Map<string, { sum: number; count: number }> {
   const out = new Map<string, { sum: number; count: number }>();
   const names = [
      "publisher_package_load_phase_duration_ms",
      "publisher_package_install_duration_ms",
      "publisher_package_reload_duration_ms",
   ];
   for (const line of text.split("\n")) {
      const m = /^([a-z_]+)_(sum|count)(\{[^}]*\})? ([0-9.eE+-]+)$/.exec(line);
      if (!m || !names.includes(m[1]!)) continue;
      const labels = (m[3] ?? "")
         .replace(/[{}"]/g, "")
         .split(",")
         .filter((l) => /^(phase|outcome)=/.test(l))
         .join(",");
      const key = `${m[1]!.replace("publisher_package_", "")}${labels ? `{${labels}}` : ""}`;
      const e = out.get(key) ?? { sum: 0, count: 0 };
      e[m[2] as "sum" | "count"] += Number(m[4]);
      out.set(key, e);
   }
   return out;
}

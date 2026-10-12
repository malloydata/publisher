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
//       Sum phases.log (patch-dist.ts timers) inside a window, and attribute
//       each event-loop stall > 20 ms to the innermost phase it fell in.
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
const open = async (name: string) => {
   await window(
      `start ${name} ${prof ? "prof" : "noprof"}`,
      `main start ${name} `,
   );
   await sleep(300); // let the workers' pollers see it too
};
const close = async (name: string) => {
   await window("stop", `main stop ${name} `);
   await sleep(300);
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
   const hook = readFileSync(`${OUT}/hook.log`, "utf8").split("\n");
   const at = (kind: string) =>
      Number(
         hook.find((l) => l.startsWith(`main ${kind} ${name} `))!.split(" ")[3],
      );
   const [t0, t1] = [at("start"), at("stop")];
   const phases = readFileSync(`${OUT}/phases.log`, "utf8")
      .trim()
      .split("\n")
      .map((l) => l.split(" "))
      .map(([n, s, d]) => ({
         n: n!,
         s: Number(s),
         e: Number(s) + Number(d),
         d: Number(d),
      }))
      .filter((p) => p.s >= t0 && p.s <= t1);
   const by = new Map<string, number[]>();
   for (const p of phases) by.set(p.n, [...(by.get(p.n) ?? []), p.d]);
   console.log(`window ${name}: ${t1 - t0} ms`);
   for (const [n, ds] of [...by.entries()].sort(
      (a, b) => sum(b[1]) - sum(a[1]),
   )) {
      ds.sort((a, b) => a - b);
      console.log(
         `  ${n.padEnd(20)} n=${String(ds.length).padStart(4)}  total=${sum(ds).toFixed(0).padStart(7)} ms  avg=${(sum(ds) / ds.length).toFixed(1).padStart(7)}  max=${ds[ds.length - 1]!.toFixed(1)}`,
      );
   }
   const eldPath = `${OUT}/${name}.eld.json`;
   if (existsSync(eldPath)) {
      const eld = JSON.parse(readFileSync(eldPath, "utf8"));
      console.log(
         `event loop: p50 ${eld.p50.toFixed(2)}  p99 ${eld.p99.toFixed(1)}  p99.9 ${eld.p999.toFixed(1)}  max ${eld.max.toFixed(1)} ms; >50 ms ${eld.over50ms}, >100 ms ${eld.over100ms}; blocked ${(100 * eld.blockedFrac).toFixed(0)}%`,
      );
      const attrib = new Map<string, { n: number; ms: number }>();
      for (const [start, d] of eld.stalls as Array<[number, number]>) {
         const inner = phases
            .filter((p) => p.s <= start + 1 && p.e >= start + d - 2)
            .sort((a, b) => a.e - a.s - (b.e - b.s))[0];
         const e = attrib.get(inner?.n ?? "(none)") ?? { n: 0, ms: 0 };
         e.n++;
         e.ms += d;
         attrib.set(inner?.n ?? "(none)", e);
      }
      console.log("stalls > 20 ms by innermost phase:");
      for (const [n, v] of [...attrib.entries()].sort(
         (a, b) => b[1].ms - a[1].ms,
      )) {
         console.log(`  ${n.padEnd(20)} n=${v.n}  ${v.ms} ms`);
      }
   }
} else {
   console.error(`unknown command ${cmd}`);
   process.exit(2);
}

function sum(xs: number[]): number {
   return xs.reduce((a, b) => a + b, 0);
}

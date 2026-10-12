// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Perf harness runner: boots a dedicated Publisher from this checkout's
// production build, generates the workload, runs the selected k6 scenario(s)
// at each concurrency level, scrapes /metrics around every run, stops the
// server, removes its temp dir and writes a results directory + report.md.
//
//   bun perf/run.ts <loads|queries|mixed|all> [options]
//
// It builds nothing: run `bun run build` at the repo root first. See
// docs/performance.md for the options and how to read the output.

import { spawnSync, type Subprocess } from "bun";
import {
   cpSync,
   existsSync,
   mkdirSync,
   mkdtempSync,
   readdirSync,
   rmSync,
   statSync,
   writeFileSync,
} from "fs";
import os from "os";
import path from "path";
import {
   DEFAULT_ROWS,
   DEFAULT_SEED,
   generatePackage,
   PRESETS,
   type GeneratedPackage,
} from "./gen-package.ts";
import { writeReport, type RunEntry, type RunContext } from "./report.ts";
import type { Workload } from "./lib/k6.ts";

const K6_DIR = path.resolve(import.meta.dir, "..");
const SERVER_DIR = path.resolve(K6_DIR, "..");
const REPO_ROOT = path.resolve(SERVER_DIR, "..", "..");
const SERVER_ENTRY = path.join(SERVER_DIR, "dist", "server.mjs");
const ENV_NAME = "perf";
const GEN_PACKAGE = "gen";
const STOREFRONT_DIR = path.join(REPO_ROOT, "examples", "storefront");

// Ports other runs on this machine own; the harness never binds or hits them.
const RESERVED_PORTS = new Set([4000, 4040, 4100, 4140]);

const SCENARIOS = ["loads", "queries", "mixed"] as const;
type Scenario = (typeof SCENARIOS)[number];
type Target = "storefront" | "gen";

const TREND_STATS = "avg,min,med,max,p(90),p(95),p(99)";

// ── CLI ───────────────────────────────────────────────────────────────────

interface Options {
   scenarios: Scenario[];
   preset: string;
   rows: number;
   seed: number;
   targets: Target[];
   durationS: number;
   warmupRounds: number;
   loadOps: Array<"register" | "reload">;
   loadConcurrency: number[];
   queryConcurrency: number[];
   rate: number;
   loaderVus: number;
   weights?: string;
   port: number;
   mcpPort: number;
   out: string;
   keepTemp: boolean;
   serverEnv: Record<string, string>;
   maxErrorRate: string;
}

const USAGE = `usage: bun perf/run.ts <loads|queries|mixed|all> [options]

  --preset small|medium|large   generated package size (default small)
  --rows N                      fact rows in the generated data (default ${DEFAULT_ROWS})
  --seed S                      data seed (default ${DEFAULT_SEED})
  --targets storefront,gen      packages to measure (default storefront,gen)
  --duration S                  seconds per measured run; mixed uses S per window (default 20)
  --warmup-rounds N             warm-up rounds before each measured run (default 1)
  --load-ops register,reload    which load operations \`loads\` measures (default both)
  --load-concurrency 1,2,4      VUs for \`loads\` (default 1,2,4)
  --query-concurrency 1,4,16,32 VUs for \`queries\` (default 1,4,16,32)
  --rate R                      \`mixed\` query arrival rate, req/s (default 10)
  --loader-vus N                \`mixed\` VUs per loader scenario (default 1)
  --weights a,b,c,d,e           query mix: cheap,medium,heavy,report_list,report_get
  --max-error-rate R            sanity threshold on http_req_failed (default 0.05)
  --port P / --mcp-port P       server ports (default 4200 / 4240)
  --server-env KEY=VALUE        extra server env, repeatable (e.g. PACKAGE_LOAD_WORKERS=2)
  --out DIR                     results directory (default results/perf-<timestamp>)
  --keep-temp                   keep the temp server root and generated package
`;

function parseArgs(argv: string[]): Options {
   const positional: string[] = [];
   const flags = new Map<string, string[]>();
   for (let i = 0; i < argv.length; i++) {
      const a = argv[i]!;
      if (a === "--help" || a === "-h") {
         console.log(USAGE);
         process.exit(0);
      }
      if (a === "--keep-temp") {
         flags.set("keep-temp", ["true"]);
      } else if (a.startsWith("--")) {
         const value = argv[++i];
         if (value === undefined) throw new Error(`${a} needs a value`);
         flags.set(a.slice(2), [...(flags.get(a.slice(2)) ?? []), value]);
      } else {
         positional.push(a);
      }
   }
   const one = (k: string): string | undefined => flags.get(k)?.at(-1);
   const list = (k: string, d: string): string[] =>
      (one(k) ?? d)
         .split(",")
         .map((s) => s.trim())
         .filter(Boolean);
   const ints = (k: string, d: string): number[] =>
      list(k, d).map((s) => {
         const n = Number(s);
         if (!Number.isInteger(n) || n < 1) {
            throw new Error(`--${k}: "${s}" is not a positive integer`);
         }
         return n;
      });

   const which = positional[0];
   if (!which) throw new Error(USAGE);
   const scenarios: Scenario[] =
      which === "all"
         ? [...SCENARIOS]
         : which.split(",").map((s) => {
              if (!(SCENARIOS as readonly string[]).includes(s)) {
                 throw new Error(`unknown scenario "${s}"\n\n${USAGE}`);
              }
              return s as Scenario;
           });

   const preset = one("preset") ?? "small";
   if (!PRESETS[preset]) throw new Error(`unknown --preset ${preset}`);
   const targets = list("targets", "storefront,gen").map((t) => {
      if (t !== "storefront" && t !== "gen") {
         throw new Error(`--targets: unknown target "${t}"`);
      }
      return t as Target;
   });
   const loadOps = list("load-ops", "register,reload").map((o) => {
      if (o !== "register" && o !== "reload") {
         throw new Error(`--load-ops: unknown op "${o}"`);
      }
      return o as "register" | "reload";
   });
   const serverEnv: Record<string, string> = {};
   for (const kv of flags.get("server-env") ?? []) {
      const eq = kv.indexOf("=");
      if (eq < 1) throw new Error(`--server-env expects KEY=VALUE, got ${kv}`);
      serverEnv[kv.slice(0, eq)] = kv.slice(eq + 1);
   }
   const port = Number(one("port") ?? 4200);
   const mcpPort = Number(one("mcp-port") ?? 4240);
   for (const p of [port, mcpPort]) {
      if (RESERVED_PORTS.has(p)) {
         throw new Error(
            `port ${p} is reserved for other servers on this machine; pick another`,
         );
      }
   }
   const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
   return {
      scenarios,
      preset,
      rows: Number(one("rows") ?? DEFAULT_ROWS),
      seed: Number(one("seed") ?? DEFAULT_SEED),
      targets,
      durationS: Number(one("duration") ?? 20),
      warmupRounds: Number(one("warmup-rounds") ?? 1),
      loadOps,
      loadConcurrency: ints("load-concurrency", "1,2,4"),
      queryConcurrency: ints("query-concurrency", "1,4,16,32"),
      rate: Number(one("rate") ?? 10),
      loaderVus: Number(one("loader-vus") ?? 1),
      weights: one("weights"),
      port,
      mcpPort,
      out: path.resolve(
         one("out") ?? path.join(K6_DIR, "results", `perf-${stamp}`),
      ),
      keepTemp: flags.has("keep-temp"),
      serverEnv,
      maxErrorRate: one("max-error-rate") ?? "0.05",
   };
}

// ── Server ────────────────────────────────────────────────────────────────

interface Server {
   baseUrl: string;
   proc: Subprocess;
   stop(): Promise<void>;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function portInUse(port: number): Promise<boolean> {
   try {
      const server = Bun.listen({
         hostname: "127.0.0.1",
         port,
         socket: { data() {} },
      });
      server.stop(true);
      return false;
   } catch {
      return true;
   }
}

async function startServer(
   opts: Options,
   serverRoot: string,
   configPath: string,
   logFile: string,
): Promise<Server> {
   for (const p of [opts.port, opts.mcpPort]) {
      if (await portInUse(p)) {
         throw new Error(
            `port ${p} is already in use — is a previous perf server still running?`,
         );
      }
   }
   const baseUrl = `http://127.0.0.1:${opts.port}`;
   const log = Bun.file(logFile).writer();
   const proc = Bun.spawn(
      [
         "bun",
         "run",
         SERVER_ENTRY,
         "--server_root",
         serverRoot,
         "--config",
         configPath,
         "--port",
         String(opts.port),
         "--mcp_port",
         String(opts.mcpPort),
         "--host",
         "127.0.0.1",
         "--no-mcp-config",
         "--init",
      ],
      {
         cwd: serverRoot,
         env: {
            ...process.env,
            NODE_ENV: "production",
            DISABLE_RESPONSE_LOGGING: "true",
            LOG_LEVEL: "warn",
            PUBLISHER_NO_MCP_CONFIG: "1",
            ...opts.serverEnv,
         },
         stdout: "pipe",
         stderr: "pipe",
      },
   );

   let exited = false;
   void proc.exited.then(() => {
      exited = true;
   });
   let ready: "ready" | "failed" | undefined;
   const pump = async (stream: ReadableStream<Uint8Array>): Promise<void> => {
      let tail = "";
      for await (const chunk of stream) {
         log.write(chunk);
         tail = (tail + new TextDecoder().decode(chunk)).slice(-4096);
         if (tail.includes("PUBLISHER_READY")) ready ??= "ready";
         if (tail.includes("PUBLISHER_INIT_FAILED")) ready ??= "failed";
      }
      await log.flush();
   };
   void pump(proc.stdout);
   void pump(proc.stderr);

   const stop = async (): Promise<void> => {
      if (!exited) {
         proc.kill("SIGTERM");
         const done = await Promise.race([
            proc.exited.then(() => true),
            sleep(15_000).then(() => false),
         ]);
         if (!done) proc.kill("SIGKILL");
         await proc.exited;
      }
      await log.flush();
      await log.end();
   };

   const deadline = Date.now() + 300_000;
   while (ready === undefined) {
      if (exited) {
         await stop();
         throw new Error(`server exited during startup; see ${logFile}`);
      }
      if (Date.now() > deadline) {
         await stop();
         throw new Error(`server not ready after 300s; see ${logFile}`);
      }
      await sleep(200);
   }
   if (ready === "failed") {
      await stop();
      throw new Error(`server printed PUBLISHER_INIT_FAILED; see ${logFile}`);
   }
   return { baseUrl, proc, stop };
}

async function getJson<T>(url: string): Promise<T> {
   const res = await fetch(url);
   if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
   return (await res.json()) as T;
}

// ── Workloads ─────────────────────────────────────────────────────────────

function dirBytes(dir: string): number {
   let total = 0;
   for (const e of readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
         // Not part of what Publisher serves for this harness's purposes.
         if (["evals", "tests", "node_modules"].includes(e.name)) continue;
         total += dirBytes(full);
      } else total += statSync(full).size;
   }
   return total;
}

function storefrontWorkload(location: string): Workload {
   return {
      package: "storefront",
      location,
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
   };
}

function genWorkload(gen: GeneratedPackage): Workload {
   // Spread queries over up to 8 sources from different model files, so the
   // mix is not one cached source.
   const pool = gen.exportedSources;
   const step = Math.max(1, Math.floor(pool.length / 8));
   const sources = pool.filter((_, i) => i % step === 0).slice(0, 8);
   return {
      package: GEN_PACKAGE,
      location: gen.dir,
      modelPath: "index.malloy",
      queries: {
         query_cheap: sources.map((s) => `run: ${s} -> kpis`),
         query_medium: sources.map((s) => `run: ${s} -> by_month_status`),
         query_heavy: sources.flatMap((s) => [
            `run: ${s} -> overview`,
            `run: ${s} -> by_category_region`,
         ]),
      },
      reports: gen.reports,
   };
}

// ── k6 ────────────────────────────────────────────────────────────────────

interface K6Run {
   scenario: Scenario;
   target: Target;
   label: string;
   script: string;
   concurrency: number;
   env: Record<string, string>;
}

function runK6(
   run: K6Run,
   opts: Options,
   server: Server,
   outDir: string,
): { exitCode: number; summaryPath: string; logPath: string } {
   const base = `${run.scenario}-${run.target}-${run.label}`;
   const summaryPath = path.join(outDir, `${base}.summary.json`);
   const logPath = path.join(outDir, `${base}.k6.log`);
   const env: Record<string, string> = {
      ...(process.env as Record<string, string>),
      K6_PUBLISHER_URL: server.baseUrl,
      PERF_ENV: ENV_NAME,
      PERF_WARMUP_ROUNDS: String(opts.warmupRounds),
      PERF_MAX_ERROR_RATE: opts.maxErrorRate,
      PERF_RUN_ID: `${Date.now().toString(36)}`,
      K6_NO_USAGE_REPORT: "true",
      ...(opts.weights ? { PERF_QUERY_WEIGHTS: opts.weights } : {}),
      ...run.env,
   };
   const args = [
      "k6",
      "run",
      "--quiet",
      "--no-color",
      "--summary-export",
      summaryPath,
      "--summary-trend-stats",
      TREND_STATS,
      path.join(import.meta.dir, run.script),
   ];
   const res = spawnSync(args, {
      cwd: K6_DIR,
      env,
      stdout: "pipe",
      stderr: "pipe",
   });
   writeFileSync(
      logPath,
      `$ ${args.join(" ")}\n\n${res.stdout.toString()}\n${res.stderr.toString()}`,
   );
   return { exitCode: res.exitCode ?? -1, summaryPath, logPath };
}

function k6Version(): string {
   const r = spawnSync(["k6", "version"], { stdout: "pipe" });
   return r.stdout.toString().trim().replace(/^k6 /, "");
}

function cmdOut(cmd: string[], cwd = REPO_ROOT): string {
   try {
      const r = spawnSync(cmd, { cwd, stdout: "pipe", stderr: "pipe" });
      return r.exitCode === 0 ? r.stdout.toString().trim() : "";
   } catch {
      return "";
   }
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
   const opts = parseArgs(process.argv.slice(2));
   if (!existsSync(SERVER_ENTRY)) {
      throw new Error(
         `${SERVER_ENTRY} not found — run \`bun run build\` at the repo root first`,
      );
   }
   mkdirSync(opts.out, { recursive: true });
   const tmp = mkdtempSync(path.join(os.tmpdir(), "publisher-perf-"));
   const serverRoot = path.join(tmp, "server-root");
   mkdirSync(serverRoot, { recursive: true });
   let server: Server | undefined;

   const cleanup = async (): Promise<void> => {
      if (server) {
         const s = server;
         server = undefined;
         await s.stop();
      }
      if (!opts.keepTemp) rmSync(tmp, { recursive: true, force: true });
   };
   const onSignal = (): void => {
      console.error("\ninterrupted — stopping the server and cleaning up");
      void cleanup().finally(() => process.exit(130));
   };
   process.on("SIGINT", onSignal);
   process.on("SIGTERM", onSignal);

   try {
      console.log(`[perf] temp dir ${tmp}`);
      console.log(
         `[perf] generating package preset=${opts.preset} rows=${opts.rows} seed=${opts.seed}`,
      );
      const genStart = Date.now();
      const gen = await generatePackage({
         dir: path.join(tmp, "packages", GEN_PACKAGE),
         preset: opts.preset,
         rows: opts.rows,
         seed: opts.seed,
         name: GEN_PACKAGE,
      });
      console.log(`[perf] generated in ${Date.now() - genStart}ms`);
      // A private copy of storefront, so nothing in the repo is ever touched.
      const storefront = path.join(tmp, "packages", "storefront");
      cpSync(STOREFRONT_DIR, storefront, {
         recursive: true,
         filter: (src) =>
            !/^(evals|tests|node_modules)([/\\]|$)/.test(
               path.relative(STOREFRONT_DIR, src),
            ),
      });

      const workloads: Record<Target, Workload> = {
         storefront: storefrontWorkload(storefront),
         gen: genWorkload(gen),
      };
      const configPath = path.join(tmp, "publisher.config.json");
      writeFileSync(
         configPath,
         JSON.stringify(
            {
               frozenConfig: false,
               environments: [
                  {
                     name: ENV_NAME,
                     packages: [
                        { name: "storefront", location: storefront },
                        { name: GEN_PACKAGE, location: gen.dir },
                     ],
                     connections: [],
                  },
               ],
            },
            null,
            2,
         ),
      );

      console.log(
         `[perf] starting Publisher on :${opts.port} (mcp :${opts.mcpPort})`,
      );
      const bootStart = Date.now();
      server = await startServer(
         opts,
         serverRoot,
         configPath,
         path.join(opts.out, "server.log"),
      );
      const bootMs = Date.now() - bootStart;
      const status = await getJson<{
         version?: string;
         loadErrors?: unknown[];
      }>(`${server.baseUrl}/api/v0/status`);
      if (status.loadErrors && status.loadErrors.length > 0) {
         throw new Error(
            `packages failed to load: ${JSON.stringify(status.loadErrors)}`,
         );
      }
      console.log(
         `[perf] serving (version ${status.version}) after ${bootMs}ms`,
      );

      const context: RunContext = {
         startedAt: new Date().toISOString(),
         command: `bun perf/run.ts ${process.argv.slice(2).join(" ")}`,
         publisherVersion: status.version ?? "unknown",
         gitSha: cmdOut(["git", "rev-parse", "HEAD"]),
         gitBranch: cmdOut(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
         gitDirty: cmdOut(["git", "status", "--porcelain"]) !== "",
         machine: {
            cpu: os.cpus()[0]?.model ?? "unknown",
            cores: os.cpus().length,
            memGiB: Math.round((os.totalmem() / 2 ** 30) * 10) / 10,
            os: `${os.type()} ${os.release()} ${os.arch()}`,
            loadAvgStart: os.loadavg().map((n) => Math.round(n * 100) / 100),
         },
         runtimes: {
            bun: Bun.version,
            node: cmdOut(["node", "--version"]),
            k6: k6Version(),
         },
         serverEnv: {
            PACKAGE_LOAD_WORKERS: "1 (default)",
            PUBLISHER_MAX_CONCURRENT_QUERIES: "32 (default)",
            ...opts.serverEnv,
         },
         bootMs,
         options: {
            durationS: opts.durationS,
            warmupRounds: opts.warmupRounds,
            loadConcurrency: opts.loadConcurrency,
            queryConcurrency: opts.queryConcurrency,
            rate: opts.rate,
            loaderVus: opts.loaderVus,
            weights: opts.weights ?? "40,35,20,2.5,2.5",
         },
         packages: {
            storefront: {
               files: readdirSync(storefront).filter((f) =>
                  f.endsWith(".malloy"),
               ).length,
               bytes: dirBytes(storefront),
               note: "examples/storefront, ~25k order lines",
            },
            gen: {
               preset: gen.preset,
               shape: gen.shape,
               rows: gen.rows,
               seed: gen.seed,
               modelFiles: gen.modelFiles,
               sources: gen.sources,
               malloyBytes: gen.malloyBytes,
               dataBytes: gen.dataBytes,
            },
         },
      };

      const runs: K6Run[] = [];
      for (const scenario of opts.scenarios) {
         for (const target of opts.targets) {
            const w = workloads[target];
            const workloadEnv = { PERF_WORKLOAD: JSON.stringify(w) };
            if (scenario === "loads") {
               for (const op of opts.loadOps) {
                  for (const c of opts.loadConcurrency) {
                     runs.push({
                        scenario,
                        target,
                        label: `${op}-c${c}`,
                        script: "loads.ts",
                        concurrency: c,
                        env: {
                           ...workloadEnv,
                           PERF_LOAD_OP: op,
                           PERF_VUS: String(c),
                           PERF_DURATION: `${opts.durationS}s`,
                        },
                     });
                  }
               }
            } else if (scenario === "queries") {
               for (const c of opts.queryConcurrency) {
                  runs.push({
                     scenario,
                     target,
                     label: `c${c}`,
                     script: "queries.ts",
                     concurrency: c,
                     env: {
                        ...workloadEnv,
                        PERF_VUS: String(c),
                        PERF_DURATION: `${opts.durationS}s`,
                     },
                  });
               }
            } else {
               const other = workloads[target === "gen" ? "storefront" : "gen"];
               runs.push({
                  scenario,
                  target,
                  label: `r${opts.rate}`,
                  script: "mixed.ts",
                  concurrency: opts.rate,
                  env: {
                     ...workloadEnv,
                     PERF_OTHER_WORKLOAD: JSON.stringify(other),
                     PERF_DURATION_S: String(opts.durationS),
                     PERF_RATE: String(opts.rate),
                     PERF_LOADER_VUS: String(opts.loaderVus),
                  },
               });
            }
         }
      }

      const entries: RunEntry[] = [];
      for (const [i, run] of runs.entries()) {
         const tag = `${run.scenario}/${run.target}/${run.label}`;
         console.log(`[perf] (${i + 1}/${runs.length}) ${tag}`);
         const before = await (await fetch(`${server.baseUrl}/metrics`)).text();
         const loadAvgBefore = os.loadavg()[0]!;
         const started = Date.now();
         const k6 = runK6(run, opts, server, opts.out);
         const wallMs = Date.now() - started;
         const after = await (await fetch(`${server.baseUrl}/metrics`)).text();
         const base = `${run.scenario}-${run.target}-${run.label}`;
         const metricsBefore = path.join(
            opts.out,
            `${base}.metrics-before.prom`,
         );
         const metricsAfter = path.join(opts.out, `${base}.metrics-after.prom`);
         writeFileSync(metricsBefore, before);
         writeFileSync(metricsAfter, after);
         const post = await getJson<{ loadErrors?: unknown[] }>(
            `${server.baseUrl}/api/v0/status`,
         );
         entries.push({
            scenario: run.scenario,
            target: run.target,
            label: run.label,
            concurrency: run.concurrency,
            loadOp: run.env.PERF_LOAD_OP,
            durationS: opts.durationS,
            wallMs,
            k6ExitCode: k6.exitCode,
            summary: path.basename(k6.summaryPath),
            k6Log: path.basename(k6.logPath),
            metricsBefore: path.basename(metricsBefore),
            metricsAfter: path.basename(metricsAfter),
            loadAvg1m: [loadAvgBefore, os.loadavg()[0]!].map(
               (n) => Math.round(n * 100) / 100,
            ),
            loadErrorsAfter: post.loadErrors?.length ?? 0,
         });
         const note =
            k6.exitCode === 0
               ? "ok"
               : k6.exitCode === 99
                 ? "THRESHOLD CROSSED (error rate)"
                 : `k6 exit ${k6.exitCode}`;
         console.log(`[perf]     ${note} in ${(wallMs / 1000).toFixed(1)}s`);
      }

      context.machine.loadAvgEnd = os
         .loadavg()
         .map((n) => Math.round(n * 100) / 100);
      context.finishedAt = new Date().toISOString();
      writeFileSync(
         path.join(opts.out, "context.json"),
         JSON.stringify(context, null, 2),
      );
      writeFileSync(
         path.join(opts.out, "runs.json"),
         JSON.stringify(entries, null, 2),
      );
      const report = writeReport(opts.out);
      console.log(`\n${report}`);
      console.log(`[perf] results in ${opts.out}`);
      if (entries.some((e) => e.k6ExitCode !== 0)) process.exitCode = 1;
   } finally {
      await cleanup();
   }
}

main().catch((err) => {
   console.error(`[perf] ${err instanceof Error ? err.message : err}`);
   process.exit(1);
});

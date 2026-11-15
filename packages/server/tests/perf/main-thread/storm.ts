// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One storm-lite run against one built server: what the control plane sends a
 * worker during a fleet reload, scaled to a CI runner. `run.ts` drives it
 * once per side per round; run it directly to explore one build.
 *
 *   bun tests/perf/main-thread/storm.ts --dist <packages/server/dist> \
 *       --fixtures <dir>/fixtures.json --out <result.json> [--preflight]
 *       [--ops 160] [--concurrency 8] [--envs 4] [--seed 1] [--port 4941]
 *       [--only name,name]
 *
 * The server runs from the build's own `dist/server.mjs` under `bun`, as the
 * image runs it, with package versioning on, materialization on, four load
 * workers and the memory governor ticking, and with `probe.ts` preloaded on
 * its main thread (see that file). One run:
 *
 *  1. Creates `--envs` environments and places every fixture version in each.
 *     Each materializing version is published once, so its build plan names
 *     the sources a manifest binds, and two manifests are written for it
 *     (`a`: two sources, `b`: all four).
 *  2. Resets the probe and drives `--ops` operations, `--concurrency` at a
 *     time, chosen by a seeded generator: publishes (a materializing version
 *     with its `manifestLocation`), version unloads, reloads, manifest binds
 *     that change the binding, re-binds of the identical manifest, and
 *     environment patches. Two pollers read `/status?includeLoading=true`
 *     throughout, as the control plane's heartbeat does.
 *  3. Reads the probe: main-thread CPU, the longest event-loop block, load
 *     dispatches.
 *  4. Two quiet checks, each in its own probe window: three identical
 *     re-binds of a bound version (expect no main-thread Malloy compile), and
 *     ten `/status` polls after unloading versions (expect no load dispatch).
 *
 * `--preflight` instead publishes each fixture's first version once and
 * reports which ones this build loads, so `run.ts` can keep the A/B to the
 * packages both builds accept.
 */

import { spawn, type Subprocess } from "bun";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import os from "os";
import path from "path";
import type { FixtureSet } from "./fixtures";

// ── CLI ───────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
function flag(name: string): string | undefined;
function flag(name: string, dflt: string): string;
function flag(name: string, dflt?: string): string | undefined {
   const i = args.indexOf(`--${name}`);
   return i >= 0 ? args[i + 1] : dflt;
}

export interface StormOptions {
   dist: string;
   fixtures: string;
   out: string;
   preflight: boolean;
   ops: number;
   concurrency: number;
   envs: number;
   seed: number;
   port: number;
   only?: string[];
   label: string;
   logDir: string;
   /** Write the server's main-thread `--cpu-prof` profile into `logDir`. */
   cpuProf: boolean;
}

// ── Result shape (read by run.ts and verdict.ts) ──────────────────────────
export interface ProbeSnapshot {
   windowMs: number;
   mainThreadCpuMs: number;
   processCpuMs: number;
   rssBytes: number;
   maxBlockMs: number;
   blocksOver: Record<string, number>;
   blockedMsOver50: number;
   eventLoopDelay: { maxMs: number; p99Ms: number };
   dispatches: Record<string, number>;
   compiles: number;
   compilesByCaller: Record<string, number>;
   materializations: number;
   hooks: Record<
      "postMessage" | "compile" | "getModel" | "queryModel",
      boolean
   >;
   queryModelBuilds: number;
   queryModelMs: number;
   queryModelByCaller: Record<string, number>;
}

export interface StormResult {
   label: string;
   dist: string;
   version?: string;
   options: Omit<StormOptions, "out" | "logDir" | "cpuProf">;
   wallMs: number;
   ops: Record<string, { ok: number; failed: Record<string, number> }>;
   completedOps: number;
   mainThreadCpuMs: number;
   processCpuMs: number;
   cpuPerOpMs: number;
   status: {
      n: number;
      p50: number;
      p90: number;
      max: number;
      timeouts: number;
   };
   maxBlockMs: number;
   blocksOver: Record<string, number>;
   eventLoopDelay: { maxMs: number; p99Ms: number };
   loadDispatches: number;
   rebind: {
      rebinds: number;
      ok: boolean;
      compiles: number;
      compilesByCaller: Record<string, number>;
      dispatches: Record<string, number>;
   };
   statusLoads: {
      polls: number;
      unloaded: number;
      loadDispatches: number;
      dispatches: Record<string, number>;
   };
   /** Which probe hooks took (probe.ts); a counter whose hook failed counts nothing. */
   hooks: ProbeSnapshot["hooks"];
   /** How long each quiet check waited for quiet, ms; -1 when it never came. */
   quietMs: number[];
   /** Positive controls: an ad-hoc query's compiles, which must be at least 1. */
   control: { queryStatus: number; queryCompiles: number };
   /** Main-thread Malloy QueryModel builds over the storm (ms2data/service#7641). */
   queryModel: { builds: number; ms: number; byCaller: Record<string, number> };
   /**
    * One quiet load of the indexed fixture's version, from unloaded: the
    * main-thread QueryModel builds it took, against its #(index) count.
    */
   indexedLoad: {
      ok: boolean;
      indexedDimensions: number;
      builds: number;
      ms: number;
      byCaller: Record<string, number>;
      /** The same, for three `GET …/entities` reads after it (404 before 0.12.1). */
      entitiesOk: boolean;
      entitiesBuilds: number;
      entitiesMs: number;
   };
   maxRssBytes: number;
}

export interface PreflightResult {
   label: string;
   version?: string;
   loaded: string[];
   refused: Record<string, string>;
}

// ── Server ────────────────────────────────────────────────────────────────
const SERVER_ENV: Record<string, string> = {
   NODE_ENV: "production",
   PUBLISHER_PACKAGE_VERSIONING: "on",
   // Dev and staging's setting, where the bind path regressed (ms2data/service#7795).
   PUBLISHER_MATERIALIZE: "true",
   PACKAGE_LOAD_WORKERS: "4",
   // The governor only runs with a limit; prod ticks it every 2 s. The limit
   // is far above what the storm holds, so it never refuses a load.
   PUBLISHER_MAX_MEMORY_BYTES: String(32 * 1024 ** 3),
   PUBLISHER_MEMORY_CHECK_INTERVAL_MS: "2000",
   PUBLISHER_NO_MCP_CONFIG: "1",
   LOG_LEVEL: "info",
   // Stop promptly when the run ends; there is no traffic left to drain.
   SHUTDOWN_DRAIN_DURATION_SECONDS: "0",
   SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "5",
   DISABLE_RESPONSE_LOGGING: "true",
   OTEL_TRACES_EXPORTER: "none",
   OTEL_METRICS_EXPORTER: "none",
   OTEL_LOGS_EXPORTER: "none",
};

const PROBE = path.join(import.meta.dir, "probe.ts");

interface Server {
   proc: Subprocess;
   api: string;
   mcp: string;
   probe: string;
   root: string;
}

async function startServer(o: StormOptions): Promise<Server> {
   const root = mkdtempSync(path.join(os.tmpdir(), "mtr-root-"));
   writeFileSync(
      path.join(root, "publisher.config.json"),
      JSON.stringify({ frozenConfig: false, environments: [] }),
   );
   const entry = path.join(path.resolve(o.dist), "server.mjs");
   const probePort = o.port + 2;
   mkdirSync(o.logDir, { recursive: true });
   const log = path.join(o.logDir, `mtr_${o.label}_server`);
   const prof = o.cpuProf
      ? [
           "--cpu-prof",
           `--cpu-prof-dir=${path.join(o.logDir, `prof-${o.label}`)}`,
        ]
      : [];
   const proc = spawn(
      [
         "bun",
         ...prof,
         "--preload",
         PROBE,
         entry,
         "--port",
         String(o.port),
         "--mcp_port",
         String(o.port + 1),
         "--host",
         "127.0.0.1",
         "--server_root",
         root,
      ],
      {
         cwd: root,
         env: {
            ...process.env,
            ...SERVER_ENV,
            MTR_PROBE_PORT: String(probePort),
            MTR_SERVER_ENTRY: entry,
         },
         stdout: Bun.file(`${log}.log`),
         stderr: Bun.file(`${log}.err`),
      },
   );
   const api = `http://127.0.0.1:${o.port}/api/v0`;
   const deadline = Date.now() + 120_000;
   while (Date.now() < deadline) {
      if (proc.exitCode !== null) {
         throw new Error(
            `server exited ${proc.exitCode} at boot; see ${log}.err`,
         );
      }
      try {
         const r = await fetch(`${api}/status`, {
            signal: AbortSignal.timeout(2000),
         });
         if (r.ok) {
            const s = (await r.json()) as { operationalState?: string };
            if (s.operationalState === "serving") {
               return {
                  proc,
                  api,
                  mcp: `http://127.0.0.1:${o.port + 1}/mcp`,
                  probe: `http://127.0.0.1:${probePort}/probe`,
                  root,
               };
            }
         }
      } catch {
         // not up yet
      }
      await Bun.sleep(250);
   }
   proc.kill("SIGKILL");
   throw new Error(`server did not reach serving in 120 s; see ${log}.err`);
}

async function stopServer(s: Server): Promise<void> {
   s.proc.kill("SIGTERM");
   const exited = await Promise.race([
      s.proc.exited.then(() => true),
      Bun.sleep(30_000).then(() => false),
   ]);
   if (!exited) {
      s.proc.kill("SIGKILL");
      await s.proc.exited;
   }
   rmSync(s.root, { recursive: true, force: true });
}

/**
 * Wait until the main thread is quiet: two consecutive one-second windows
 * with no Malloy compile, no QueryModel build and no load dispatched, so
 * background work the storm started (a retrieval index build reading
 * #(index) values, a queued load) cannot land in a quiet check's window.
 * Returns how long it took, or -1 when the server never went quiet.
 */
async function waitQuiet(s: Server, limitMs = 120_000): Promise<number> {
   const start = Date.now();
   let quietFor = 0;
   await readProbe(s, true);
   while (Date.now() - start < limitMs) {
      await Bun.sleep(1000);
      const w = await readProbe(s, true);
      const busy =
         w.compiles +
         w.queryModelBuilds +
         (w.dispatches["load-package"] ?? 0) +
         (w.dispatches["load-package+manifest"] ?? 0);
      quietFor = busy === 0 ? quietFor + 1 : 0;
      if (quietFor >= 2) return Date.now() - start;
   }
   return -1;
}

async function readProbe(s: Server, reset = false): Promise<ProbeSnapshot> {
   const r = await fetch(reset ? `${s.probe}/reset` : s.probe, {
      method: reset ? "POST" : "GET",
      signal: AbortSignal.timeout(120_000),
   });
   return (await r.json()) as ProbeSnapshot;
}

async function serverVersion(s: Server): Promise<string | undefined> {
   try {
      const r = await fetch(`${s.api}/status`);
      return ((await r.json()) as { version?: string }).version;
   } catch {
      return undefined;
   }
}

// ── HTTP ──────────────────────────────────────────────────────────────────
interface Answer {
   status: number;
   body: string;
   ms: number;
}

async function call(
   method: string,
   url: string,
   body?: unknown,
   timeoutMs = 300_000,
   headers: Record<string, string> = {},
): Promise<Answer> {
   const start = performance.now();
   try {
      const r = await fetch(url, {
         method,
         headers: {
            ...(body ? { "content-type": "application/json" } : {}),
            ...headers,
         },
         body: body ? JSON.stringify(body) : undefined,
         signal: AbortSignal.timeout(timeoutMs),
      });
      const text = await r.text();
      return { status: r.status, body: text, ms: performance.now() - start };
   } catch (e) {
      return {
         status: 0,
         body: (e as Error).name,
         ms: performance.now() - start,
      };
   }
}

/** Deterministic [0, 1) generator (mulberry32), so both sides run one script. */
function rng(seed: number): () => number {
   let a = seed >>> 0;
   return () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
   };
}

export function percentile(xs: number[], p: number): number {
   if (xs.length === 0) return 0;
   const s = [...xs].sort((a, b) => a - b);
   return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
}

// ── Preflight ─────────────────────────────────────────────────────────────
async function preflight(o: StormOptions, set: FixtureSet): Promise<void> {
   const s = await startServer(o);
   const result: PreflightResult = {
      label: o.label,
      version: await serverVersion(s),
      loaded: [],
      refused: {},
   };
   try {
      await call("POST", `${s.api}/environments`, {
         name: "mtr-preflight",
         packages: [],
         connections: [],
      });
      for (const f of set.fixtures) {
         const v = f.versions[0];
         const a = await call(
            "POST",
            `${s.api}/environments/mtr-preflight/packages`,
            { name: f.name, location: v.dir },
         );
         if (a.status >= 200 && a.status < 300) result.loaded.push(f.name);
         else result.refused[f.name] = `${a.status} ${a.body.slice(0, 300)}`;
      }
   } finally {
      await stopServer(s);
   }
   writeFileSync(o.out, JSON.stringify(result, null, 2));
   console.log(
      `[${o.label}] preflight ${result.version}: loads ${result.loaded.join(", ")}` +
         (Object.keys(result.refused).length
            ? `; refuses ${Object.keys(result.refused).join(", ")}`
            : ""),
   );
}

// ── The storm ─────────────────────────────────────────────────────────────
interface Placed {
   env: string;
   pkg: string;
   version: string;
   dir: string;
   materializing: boolean;
   /** The fixture the quiet re-bind check binds (fixtures.ts). */
   rebindProbe: boolean;
   /** Manifests written for a materializing version; `bound` is the last sent. */
   manifests?: { a: string; b: string; bound: "a" | "b" };
}

type Op =
   | "load"
   | "unload"
   | "reload"
   | "bind"
   | "rebind"
   | "entities"
   | "context"
   | "patchEnv";
const MIX: Array<[Op, number]> = [
   ["load", 0.38],
   ["unload", 0.2],
   ["reload", 0.1],
   ["bind", 0.1],
   ["rebind", 0.1],
   // What an indexer and an agent read: the package's entities, and
   // get_context (MCP only; it has no REST form).
   ["entities", 0.07],
   // Few: each starts a background index build of its package (reading its
   // #(index) values) that runs on past the storm, as in the fleet.
   ["context", 0.02],
   ["patchEnv", 0.03],
];

function pickOp(r: number): Op {
   let acc = 0;
   for (const [op, w] of MIX) {
      acc += w;
      if (r < acc) return op;
   }
   return "load";
}

interface BuildPlanSource {
   name?: string;
   sourceID?: string;
   sourceEntityId?: string;
}

/** Write manifests `a` and `b` for one materializing version from its plan. */
async function writeManifests(
   s: Server,
   p: Placed,
   tables: string[],
   dir: string,
): Promise<boolean> {
   const r = await call(
      "GET",
      `${s.api}/environments/${p.env}/packages/${p.pkg}?versionId=${p.version}`,
   );
   if (r.status !== 200) return false;
   const plan = (
      JSON.parse(r.body) as {
         buildPlan?: { sources?: Record<string, BuildPlanSource> };
      }
   ).buildPlan;
   const persisted = Object.values(plan?.sources ?? {})
      .map((src) => ({
         name: src.name ?? String(src.sourceID ?? "").split("@")[0],
         id: src.sourceEntityId,
      }))
      .filter((src) => src.id && tables.includes(`data/${src.name}.parquet`))
      .sort((x, y) => x.name.localeCompare(y.name));
   if (persisted.length === 0) return false;
   // fixtures.ts writes each rollup's table as data/<source name>.parquet.
   const tableFor = (name: string): string =>
      tables.find((t) => t === `data/${name}.parquet`) ?? tables[0];
   const manifest = (count: number) => ({
      builtAt: "2026-11-15T00:00:00.000Z",
      strict: false,
      entries: Object.fromEntries(
         persisted.slice(0, count).map((src) => [
            src.id,
            {
               sourceEntityId: src.id,
               sourceName: src.name,
               physicalTableName: tableFor(src.name),
               connectionName: "duckdb",
            },
         ]),
      ),
   });
   const base = path.join(dir, `${p.env}-${p.pkg}-${p.version}`);
   writeFileSync(`${base}-a.json`, JSON.stringify(manifest(2)));
   writeFileSync(`${base}-b.json`, JSON.stringify(manifest(persisted.length)));
   p.manifests = { a: `${base}-a.json`, b: `${base}-b.json`, bound: "a" };
   return true;
}

/** MCP `get_context` for a few concepts in one package (stateless HTTP). */
async function getContext(s: Server, p: Placed): Promise<Answer> {
   const a = await call(
      "POST",
      s.mcp,
      {
         jsonrpc: "2.0",
         id: 1,
         method: "tools/call",
         params: {
            name: "get_context",
            arguments: {
               search_targets: [
                  { target_type: "source", search_text: "orders by region" },
                  { target_type: "dimension", search_text: "brand" },
                  { target_type: "measure", search_text: "revenue" },
               ],
               scopes: [{ environment: p.env, package: p.pkg }],
            },
         },
      },
      300_000,
      { accept: "application/json, text/event-stream" },
   );
   // A tool error is a 200 carrying `isError`; count it as failed.
   return a.status === 200 && a.body.includes('"isError":true')
      ? { ...a, status: 599 }
      : a;
}

async function storm(o: StormOptions, set: FixtureSet): Promise<void> {
   const fixtures = set.fixtures.filter(
      (f) => !o.only || o.only.includes(f.name),
   );
   const tablesOf = new Map(fixtures.map((f) => [f.name, f.tables ?? []]));
   const envs = Array.from(
      { length: o.envs },
      (_, i) => `mtr-env-${String(i + 1).padStart(2, "0")}`,
   );
   const placement: Placed[] = envs.flatMap((env) =>
      fixtures.flatMap((f) =>
         f.versions.map((v) => ({
            env,
            pkg: f.name,
            version: v.version,
            dir: v.dir,
            materializing: f.kind === "materializing",
            rebindProbe: f.rebindProbe === true,
         })),
      ),
   );
   const manifestDir = mkdtempSync(path.join(os.tmpdir(), "mtr-manifests-"));

   const s = await startServer(o);
   const version = await serverVersion(s);
   let maxRss = 0;
   try {
      // 1. Environments, and a manifest pair per materializing version.
      for (const env of envs) {
         await call("POST", `${s.api}/environments`, {
            name: env,
            packages: [],
            connections: [],
         });
      }
      for (const p of placement.filter((x) => x.materializing)) {
         const a = await call(
            "POST",
            `${s.api}/environments/${p.env}/packages`,
            {
               name: p.pkg,
               location: p.dir,
            },
         );
         if (a.status !== 200 && a.status !== 201) {
            throw new Error(
               `setup publish ${p.env}/${p.pkg}@${p.version}: ${a.status} ${a.body.slice(0, 300)}`,
            );
         }
         if (
            !(await writeManifests(
               s,
               p,
               tablesOf.get(p.pkg) ?? [],
               manifestDir,
            ))
         ) {
            throw new Error(
               `setup: ${p.env}/${p.pkg}@${p.version} has no persist sources in its build plan`,
            );
         }
      }

      // 2. The storm.
      const ops: StormResult["ops"] = {};
      const record = (op: Op, a: Answer): void => {
         const o2 = (ops[op] ??= { ok: 0, failed: {} });
         if (a.status >= 200 && a.status < 300) o2.ok++;
         else
            o2.failed[String(a.status)] =
               (o2.failed[String(a.status)] ?? 0) + 1;
      };
      const pkgUrl = (p: Placed) =>
         `${s.api}/environments/${p.env}/packages/${p.pkg}`;
      const random = rng(o.seed);
      // The whole script is drawn up front, so it never depends on timing.
      const script = Array.from({ length: o.ops }, () => {
         const op = pickOp(random());
         const pool =
            op === "bind" || op === "rebind"
               ? placement.filter((x) => x.materializing)
               : placement;
         return { op, p: pool[Math.floor(random() * pool.length)] };
      });

      const statusMs: number[] = [];
      let statusTimeouts = 0;
      let stormDone = false;
      const poller = async () => {
         while (!stormDone) {
            const a = await call(
               "GET",
               `${s.api}/status?includeLoading=true`,
               undefined,
               60_000,
            );
            if (a.status === 200) statusMs.push(a.ms);
            else {
               statusTimeouts++;
               statusMs.push(a.ms);
            }
            await Bun.sleep(200);
         }
      };
      const rssWatch = async () => {
         while (!stormDone) {
            await Bun.sleep(5000);
            try {
               maxRss = Math.max(maxRss, (await readProbe(s)).rssBytes);
            } catch {
               // busy; next time
            }
         }
      };

      const before = await readProbe(s, true);
      const started = performance.now();
      let next = 0;
      const driver = async () => {
         while (next < script.length) {
            const { op, p } = script[next++];
            let a: Answer;
            switch (op) {
               case "load":
                  a = await call(
                     "POST",
                     `${s.api}/environments/${p.env}/packages`,
                     {
                        name: p.pkg,
                        location: p.dir,
                        ...(p.manifests
                           ? {
                                manifestLocation:
                                   p.manifests[p.manifests.bound],
                             }
                           : {}),
                     },
                  );
                  break;
               case "unload":
                  a = await call(
                     "DELETE",
                     `${pkgUrl(p)}/versions/${p.version}`,
                  );
                  break;
               case "reload":
                  a = await call("GET", `${pkgUrl(p)}?reload=true`);
                  break;
               case "bind":
               case "rebind": {
                  const m = p.manifests!;
                  if (op === "bind") m.bound = m.bound === "a" ? "b" : "a";
                  a = await call(
                     "PUT",
                     `${pkgUrl(p)}/versions/${p.version}/manifest`,
                     {
                        manifestLocation: m[m.bound],
                     },
                  );
                  break;
               }
               case "entities":
                  a = await call("GET", `${pkgUrl(p)}/entities`);
                  break;
               case "context":
                  a = await getContext(s, p);
                  break;
               case "patchEnv":
                  a = await call("PATCH", `${s.api}/environments/${p.env}`, {
                     name: p.env,
                     readme: `mtr ${next}`,
                  });
                  break;
            }
            record(op, a);
         }
      };
      const pollers = [poller(), poller(), rssWatch()];
      await Promise.all(Array.from({ length: o.concurrency }, driver));
      const wallMs = performance.now() - started;
      stormDone = true;
      await Promise.all(pollers);
      const after = await readProbe(s);
      maxRss = Math.max(maxRss, after.rssBytes);

      // 3. Quiet check: identical re-binds of a bound version.
      const target =
         placement.find((x) => x.rebindProbe) ??
         placement.find((x) => x.materializing)!;
      const m = target.manifests!;
      await call("POST", `${s.api}/environments/${target.env}/packages`, {
         name: target.pkg,
         location: target.dir,
         manifestLocation: m[m.bound],
      });
      const first = await call(
         "PUT",
         `${pkgUrl(target)}/versions/${target.version}/manifest`,
         { manifestLocation: m[m.bound] },
      );
      const quietMs: number[] = [await waitQuiet(s)];
      // Positive control first: one ad-hoc query must count as a compile, or
      // a zero in the re-bind window means nothing.
      await readProbe(s, true);
      const q = await call(
         "POST",
         `${pkgUrl(target)}/models/mat.malloy/query`,
         {
            query: "run: orders -> { aggregate: order_count }",
            versionId: target.version,
         },
      );
      const control = await readProbe(s, true);
      const REBINDS = 3;
      let rebindOk = first.status === 200;
      for (let i = 0; i < REBINDS; i++) {
         const a = await call(
            "PUT",
            `${pkgUrl(target)}/versions/${target.version}/manifest`,
            { manifestLocation: m[m.bound] },
         );
         rebindOk &&= a.status === 200;
      }
      await Bun.sleep(2000);
      const rebind = await readProbe(s);

      // 4. Quiet check: /status after unloads starts no load.
      const toUnload = envs
         .map((env) => placement.find((x) => x.env === env)!)
         .slice(0, 3);
      for (const p of toUnload) {
         await call("POST", `${s.api}/environments/${p.env}/packages`, {
            name: p.pkg,
            location: p.dir,
         });
         await call("DELETE", `${pkgUrl(p)}/versions/${p.version}`);
      }
      quietMs.push(await waitQuiet(s));
      await readProbe(s, true);
      const POLLS = 10;
      for (let i = 0; i < POLLS; i++) {
         await call("GET", `${s.api}/status?includeLoading=true`);
         await call("GET", `${s.api}/status`);
      }
      await Bun.sleep(1000);
      const statusLoads = await readProbe(s);

      // 5. Quiet check: one load of the indexed package from unloaded, then
      // three reads of its entities, each in its own probe window.
      const indexed = fixtures.find((f) => f.kind === "indexed");
      const ip = indexed
         ? placement.find((x) => x.pkg === indexed.name)
         : undefined;
      let indexedLoad: StormResult["indexedLoad"] = {
         ok: false,
         indexedDimensions: indexed?.indexedDimensions ?? 0,
         builds: 0,
         ms: 0,
         byCaller: {},
         entitiesOk: false,
         entitiesBuilds: 0,
         entitiesMs: 0,
      };
      if (ip) {
         await call("DELETE", `${pkgUrl(ip)}/versions/${ip.version}`);
         quietMs.push(await waitQuiet(s));
         await readProbe(s, true);
         const a = await call(
            "POST",
            `${s.api}/environments/${ip.env}/packages`,
            { name: ip.pkg, location: ip.dir },
         );
         await Bun.sleep(1000);
         const loaded = await readProbe(s, true);
         let entitiesOk = true;
         for (let i = 0; i < 3; i++) {
            const e = await call("GET", `${pkgUrl(ip)}/entities`);
            entitiesOk &&= e.status === 200;
         }
         const read = await readProbe(s);
         indexedLoad = {
            ...indexedLoad,
            ok: a.status >= 200 && a.status < 300,
            entitiesOk,
            builds: loaded.queryModelBuilds,
            ms: loaded.queryModelMs,
            byCaller: loaded.queryModelByCaller,
            entitiesBuilds: read.queryModelBuilds,
            entitiesMs: read.queryModelMs,
         };
      }

      const loadsIn = (d: Record<string, number>) =>
         (d["load-package"] ?? 0) + (d["load-package+manifest"] ?? 0);
      const completedOps = Object.values(ops).reduce(
         (n, x) =>
            n + x.ok + Object.values(x.failed).reduce((a, b) => a + b, 0),
         0,
      );
      const cpu = after.mainThreadCpuMs - before.mainThreadCpuMs;
      const result: StormResult = {
         label: o.label,
         dist: o.dist,
         version,
         options: {
            dist: o.dist,
            fixtures: o.fixtures,
            preflight: false,
            ops: o.ops,
            concurrency: o.concurrency,
            envs: o.envs,
            seed: o.seed,
            port: o.port,
            only: o.only,
            label: o.label,
         },
         wallMs: Math.round(wallMs),
         ops,
         completedOps,
         mainThreadCpuMs: Math.round(cpu),
         processCpuMs: Math.round(after.processCpuMs - before.processCpuMs),
         cpuPerOpMs: Math.round((cpu / Math.max(1, completedOps)) * 10) / 10,
         status: {
            n: statusMs.length,
            p50: Math.round(percentile(statusMs, 50)),
            p90: Math.round(percentile(statusMs, 90)),
            max: Math.round(Math.max(0, ...statusMs)),
            timeouts: statusTimeouts,
         },
         maxBlockMs: after.maxBlockMs,
         blocksOver: after.blocksOver,
         eventLoopDelay: after.eventLoopDelay,
         loadDispatches: loadsIn(after.dispatches),
         rebind: {
            rebinds: REBINDS,
            ok: rebindOk,
            compiles: rebind.compiles,
            compilesByCaller: rebind.compilesByCaller,
            dispatches: rebind.dispatches,
         },
         statusLoads: {
            polls: POLLS * 2,
            unloaded: toUnload.length,
            loadDispatches: loadsIn(statusLoads.dispatches),
            dispatches: statusLoads.dispatches,
         },
         hooks: after.hooks,
         quietMs,
         control: { queryStatus: q.status, queryCompiles: control.compiles },
         queryModel: {
            builds: after.queryModelBuilds,
            ms: after.queryModelMs,
            byCaller: after.queryModelByCaller,
         },
         indexedLoad,
         maxRssBytes: maxRss,
      };
      writeFileSync(o.out, JSON.stringify(result, null, 2));
      console.log(
         `[${o.label}] ${version}: ${completedOps} ops in ${(wallMs / 1000).toFixed(0)} s, ` +
            `main-thread CPU ${result.cpuPerOpMs} ms/op, /status p50/p90/max ` +
            `${result.status.p50}/${result.status.p90}/${result.status.max} ms (n=${result.status.n}), ` +
            `longest block ${result.maxBlockMs} ms, re-bind compiles ${rebind.compiles}, ` +
            `status-started loads ${result.statusLoads.loadDispatches}, ` +
            `main-thread QueryModel builds ${after.queryModelBuilds} (${after.queryModelMs} ms), ` +
            `indexed load ${indexedLoad.builds} builds for ${indexedLoad.indexedDimensions} #(index)`,
      );
   } finally {
      await stopServer(s);
      rmSync(manifestDir, { recursive: true, force: true });
   }
}

export function parseOptions(): StormOptions {
   const dist = flag("dist");
   const fixtures = flag("fixtures");
   const out = flag("out");
   if (!dist || !fixtures || !out) {
      console.error(
         "usage: bun storm.ts --dist <dir> --fixtures <fixtures.json> --out <result.json> [--preflight] [--ops N] [--concurrency N] [--envs N] [--seed N] [--port N] [--label L] [--log-dir D] [--cpu-prof]",
      );
      process.exit(2);
   }
   return {
      dist,
      fixtures,
      out,
      preflight: args.includes("--preflight"),
      ops: Number(flag("ops", "160")),
      concurrency: Number(flag("concurrency", "8")),
      envs: Number(flag("envs", "4")),
      seed: Number(flag("seed", "1")),
      port: Number(flag("port", "4941")),
      only: flag("only")?.split(",").filter(Boolean),
      label: flag("label", "run"),
      logDir: flag("log-dir", path.dirname(path.resolve(out))),
      cpuProf: args.includes("--cpu-prof"),
   };
}

if (import.meta.main) {
   const o = parseOptions();
   const set = (await Bun.file(o.fixtures).json()) as FixtureSet;
   if (o.preflight) await preflight(o, set);
   else await storm(o, set);
   process.exit(0);
}

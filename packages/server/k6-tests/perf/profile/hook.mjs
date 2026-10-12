// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Profiling hook for a Publisher under load. NOT product code: it is loaded
// into the server only by perf/profile/ctl.ts (`bun --preload` on the main
// thread, and an import that perf/profile/patch-dist.ts prepends to
// dist/package_load_worker.mjs for each package-load worker). It does nothing
// unless PERF_PROFILE_CTL and PERF_PROFILE_OUT are set.
//
// A profiling window is opened and closed by appending a line to the control
// file (PERF_PROFILE_CTL), which every thread polls every 100 ms:
//
//   <seq> start <name> <prof|noprof>
//   <seq> stop
//
// On stop it writes, into PERF_PROFILE_OUT:
//   <name>.<main|workerN>.cpuprofile   CPU profile of that thread (prof only),
//                                     via node:inspector's Profiler domain
//   <name>.eld.json                   main thread: 1 ms setInterval drift
//                                     (event-loop delay) percentiles, the
//                                     100 ms max series, and every stall > 20 ms
//   <name>.compile-sites.json         PERF_PROFILE_TRACE has "compile": every
//                                     Malloy.compile on the main thread, grouped
//                                     by its (async) caller stack
//   <name>.duckdb.json                PERF_PROFILE_TRACE has "duckdb": time in
//                                     DuckDBConnection queries and how many were
//                                     in flight at once
// and appends "<thread> start|stop <name> <epoch ms>" to hook.log.
import {
   appendFileSync,
   existsSync,
   readFileSync,
   writeFileSync,
} from "node:fs";
import inspector from "node:inspector";
import { createRequire } from "node:module";
import { isMainThread, threadId } from "node:worker_threads";

const CTL = process.env.PERF_PROFILE_CTL;
const OUT = process.env.PERF_PROFILE_OUT;
const TRACE = new Set((process.env.PERF_PROFILE_TRACE ?? "").split(","));

if (CTL && OUT) {
   const who = isMainThread ? "main" : `worker${threadId}`;
   const log = (line) => appendFileSync(`${OUT}/hook.log`, `${who} ${line}\n`);
   const session = new inspector.Session();
   session.connect();
   const post = (method, params = {}) =>
      new Promise((resolve, reject) =>
         session.post(method, params, (err, res) =>
            err ? reject(err) : resolve(res),
         ),
      );
   const quantile = (sorted, p) =>
      sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] ?? 0;
   // Resolved from this file, which finds the same node_modules copy the
   // server's externalized imports resolve to.
   const req = createRequire(import.meta.url);

   let current = null; // { name, prof, t0 }

   // ── Event-loop delay probe (main thread) ──
   let eld = null;
   let lastTick = performance.now();
   if (isMainThread) {
      setInterval(() => {
         const now = performance.now();
         if (eld) {
            const d = Math.max(0, now - lastTick - 1);
            eld.delays.push(d);
            const bucket = Math.floor((now - eld.t0) / 100);
            eld.series[bucket] = Math.max(eld.series[bucket] ?? 0, d);
            if (d > 20)
               eld.stalls.push([Math.round(Date.now() - d), Math.round(d)]);
         }
         lastTick = now;
      }, 1);
   }

   // ── Malloy.compile call sites (main thread) ──
   const compileSites = new Map();
   if (isMainThread && TRACE.has("compile")) {
      const { Malloy } = req("@malloydata/malloy");
      const original = Malloy.compile;
      Error.stackTraceLimit = 60;
      Malloy.compile = async function (request) {
         const site = (new Error().stack ?? "")
            .split("\n")
            .slice(2)
            .map((l) => l.trim().replace(/^at /, ""))
            .filter(
               (l) => !/node_modules\/@malloydata|profile\/hook\.mjs/.test(l),
            )
            .slice(0, 6)
            .map((l) =>
               l.replace(
                  /\(?(?:file:\/\/)?\/\S*\/dist\/server\.mjs:(\d+):\d+\)?/,
                  "server.mjs:$1",
               ),
            )
            .join(" < ");
         const t = performance.now();
         try {
            return await original.call(this, request);
         } finally {
            if (current) {
               const e = compileSites.get(site) ?? { n: 0, ms: 0 };
               e.n++;
               e.ms += performance.now() - t;
               compileSites.set(site, e);
            }
         }
      };
   }

   // ── DuckDB query time and concurrency (main thread) ──
   let db = null;
   let inflight = 0;
   let lastChange = performance.now();
   const dbChange = (delta) => {
      const now = performance.now();
      if (db) {
         db.weighted += inflight * (now - lastChange);
         if (inflight > 0) db.busyMs += now - lastChange;
      }
      lastChange = now;
      inflight += delta;
      if (db) db.maxInflight = Math.max(db.maxInflight, inflight);
   };
   if (isMainThread && TRACE.has("duckdb")) {
      const proto = req("@malloydata/db-duckdb").DuckDBConnection.prototype;
      const done = (t) => {
         if (db) db.durations.push(performance.now() - t);
      };
      const run = proto.runDuckDBQuery;
      proto.runDuckDBQuery = async function (sql) {
         dbChange(1);
         const t = performance.now();
         try {
            return await run.call(this, sql);
         } finally {
            done(t);
            dbChange(-1);
         }
      };
      const stream = proto.runSQLStream;
      proto.runSQLStream = async function* (sql, options) {
         dbChange(1);
         const t = performance.now();
         try {
            yield* stream.call(this, sql, options);
         } finally {
            done(t);
            dbChange(-1);
         }
      };
   }

   const start = async (name, prof) => {
      current = { name, prof, t0: Date.now() };
      if (prof) {
         await post("Profiler.enable");
         await post("Profiler.start");
      }
      if (isMainThread) {
         eld = { t0: performance.now(), delays: [], series: [], stalls: [] };
         lastTick = performance.now();
         if (TRACE.has("duckdb")) {
            db = {
               maxInflight: inflight,
               busyMs: 0,
               weighted: 0,
               durations: [],
            };
            lastChange = performance.now();
         }
      }
      log(`start ${name} ${Date.now()}`);
   };

   const stop = async () => {
      const c = current;
      current = null;
      const wallMs = Date.now() - c.t0;
      if (c.prof) {
         const { profile } = await post("Profiler.stop");
         writeFileSync(
            `${OUT}/${c.name}.${who}.cpuprofile`,
            JSON.stringify(profile),
         );
      }
      if (isMainThread) {
         const e = eld;
         eld = null;
         const s = [...e.delays].sort((a, b) => a - b);
         const blocked = e.delays.reduce((a, d) => a + (d > 2 ? d : 0), 0);
         writeFileSync(
            `${OUT}/${c.name}.eld.json`,
            JSON.stringify({
               wallMs,
               ticks: s.length,
               p50: quantile(s, 0.5),
               p99: quantile(s, 0.99),
               p999: quantile(s, 0.999),
               max: s[s.length - 1] ?? 0,
               over10ms: s.filter((d) => d > 10).length,
               over50ms: s.filter((d) => d > 50).length,
               over100ms: s.filter((d) => d > 100).length,
               // Sum of the delays above 2 ms over the window: roughly the share
               // of the window the loop could not run a 1 ms timer on time.
               blockedFrac: blocked / wallMs,
               series100ms: e.series.map((v) => Math.round((v ?? 0) * 10) / 10),
               stalls: e.stalls,
            }),
         );
         if (compileSites.size > 0) {
            writeFileSync(
               `${OUT}/${c.name}.compile-sites.json`,
               JSON.stringify(
                  [...compileSites.entries()]
                     .sort((a, b) => b[1].ms - a[1].ms)
                     .map(([site, v]) => ({
                        site,
                        n: v.n,
                        wallMs: Math.round(v.ms),
                     })),
                  null,
                  1,
               ),
            );
            compileSites.clear();
         }
         if (db) {
            dbChange(0);
            const d = db;
            db = null;
            const s2 = d.durations.sort((a, b) => a - b);
            writeFileSync(
               `${OUT}/${c.name}.duckdb.json`,
               JSON.stringify({
                  wallMs,
                  queries: s2.length,
                  avgMs: s2.reduce((a, b) => a + b, 0) / Math.max(1, s2.length),
                  p50: quantile(s2, 0.5),
                  p95: quantile(s2, 0.95),
                  p99: quantile(s2, 0.99),
                  maxInflight: d.maxInflight,
                  busyFrac: d.busyMs / wallMs,
                  avgInflight: d.weighted / wallMs,
               }),
            );
         }
      }
      log(`stop ${c.name} ${Date.now()}`);
   };

   let lastSeq = "";
   const poll = async () => {
      if (!existsSync(CTL)) return;
      const lines = readFileSync(CTL, "utf8").trim().split("\n");
      const [seq, cmd, name, mode] = (lines[lines.length - 1] ?? "").split(
         /\s+/,
      );
      if (!seq || seq === lastSeq) return;
      lastSeq = seq;
      try {
         if (cmd === "start" && !current) await start(name, mode !== "noprof");
         else if (cmd === "stop" && current) await stop();
      } catch (err) {
         log(`error ${err?.message ?? err}`);
      }
   };
   setInterval(() => void poll(), 100).unref?.();
}

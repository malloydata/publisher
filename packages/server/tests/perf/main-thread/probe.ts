// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * TEST-ONLY. A `bun --preload` module the main-thread regression harness
 * (`storm.ts`) loads into the server it measures. It changes nothing the
 * server does and is never shipped: the server bundle does not import it, and
 * it does nothing unless `MTR_PROBE_PORT` is set, which only the harness sets.
 *
 * It runs on the server's main thread, before the bundle, and measures from
 * there what cannot be seen from outside the process:
 *
 *  - main-thread CPU (`process.threadCpuUsage`), not the load workers';
 *  - the event loop's longest single block, from a 10 ms heartbeat timer's
 *    lateness and from `perf_hooks.monitorEventLoopDelay`;
 *  - jobs posted to the package-load worker pool (`Worker.postMessage` with a
 *    `type: "load-package"` message), so a `/status` poll that starts a load
 *    shows up as a dispatch;
 *  - Malloy translations on the main thread (the static `Malloy.compile` in
 *    the `@malloydata/malloy` the bundle resolves, which the bundle keeps
 *    external), each attributed to the bundle function that asked for it,
 *    and model materializations (`getModel`) beside them;
 *  - Malloy QueryModel builds on the main thread
 *    (`QueryModelImpl.loadModelFromDef`), with their time and callers.
 *
 * `hooks` in every snapshot says which of these took, so the harness can
 * refuse a run whose counters could not have counted anything.
 *
 * Because it hooks only the runtime and the external Malloy package, it works
 * unchanged against any build, an old release included, which is what lets
 * the A/B compare a baseline that predates the harness.
 *
 * It answers on its own port, `GET /probe` (a JSON snapshot) and
 * `POST /probe/reset` (answer with the window so far, then start a new one). Both are served
 * by the main thread, so read them when the server is quiet.
 */

import { createRequire } from "module";
import path from "path";
import { monitorEventLoopDelay } from "perf_hooks";
import { isMainThread, Worker as NodeWorker } from "worker_threads";

const PORT = Number(process.env.MTR_PROBE_PORT ?? "");
const SERVER_ENTRY = process.env.MTR_SERVER_ENTRY ?? "";

if (isMainThread && PORT > 0) install();

interface Window {
   startedAt: number;
   maxBlockMs: number;
   blocksOver: Record<string, number>;
   blockedMsOver50: number;
   dispatches: Record<string, number>;
   compiles: number;
   materializations: number;
   queryModelBuilds: number;
   queryModelMs: number;
   queryModelByCaller: Record<string, number>;
   compilesByCaller: Record<string, number>;
}

function install(): void {
   const THRESHOLDS = [50, 100, 250, 500, 1000];
   const fresh = (): Window => ({
      startedAt: Date.now(),
      maxBlockMs: 0,
      blocksOver: Object.fromEntries(THRESHOLDS.map((t) => [String(t), 0])),
      blockedMsOver50: 0,
      dispatches: {},
      compiles: 0,
      compilesByCaller: {},
      materializations: 0,
      queryModelBuilds: 0,
      queryModelMs: 0,
      queryModelByCaller: {},
   });
   let win = fresh();
   // Totals since boot, never reset: the harness diffs them per window.
   // Which hooks took: a hook that silently failed would read as "no work".
   const hooks = {
      postMessage: false,
      compile: false,
      getModel: false,
      queryModel: false,
   };
   const total = {
      dispatches: {} as Record<string, number>,
      compiles: 0,
      queryModelBuilds: 0,
   };

   // ── Event-loop blocking ──────────────────────────────────────────────
   // A timer due every TICK ms; whatever it is late by, the loop was busy.
   const TICK = 10;
   let last = performance.now();
   const beat = setInterval(() => {
      const now = performance.now();
      const block = now - last - TICK;
      last = now;
      if (block <= 0) return;
      if (block > win.maxBlockMs) win.maxBlockMs = block;
      for (const t of THRESHOLDS) if (block >= t) win.blocksOver[String(t)]++;
      if (block >= 50) win.blockedMsOver50 += block;
   }, TICK);
   beat.unref?.();
   const eld = monitorEventLoopDelay({ resolution: TICK });
   eld.enable();

   // ── Load-pool dispatches ─────────────────────────────────────────────
   const countPost = (msg: unknown): void => {
      const type =
         msg && typeof msg === "object" && "type" in msg
            ? String((msg as { type: unknown }).type)
            : "other";
      const key =
         type === "load-package" &&
         (msg as { buildManifest?: unknown }).buildManifest
            ? "load-package+manifest"
            : type;
      win.dispatches[key] = (win.dispatches[key] ?? 0) + 1;
      total.dispatches[key] = (total.dispatches[key] ?? 0) + 1;
   };
   for (const W of new Set([NodeWorker, globalThis.Worker].filter(Boolean))) {
      const proto = (W as unknown as { prototype: Record<string, unknown> })
         .prototype;
      const post = proto.postMessage as (...a: unknown[]) => unknown;
      if (typeof post !== "function" || (post as { __mtr?: true }).__mtr) {
         continue;
      }
      const wrapped = function (this: unknown, ...a: unknown[]): unknown {
         countPost(a[0]);
         return post.apply(this, a);
      };
      (wrapped as { __mtr?: true }).__mtr = true;
      proto.postMessage = wrapped;
      hooks.postMessage = true;
   }

   // ── Main-thread Malloy compiles ──────────────────────────────────────
   // Resolve @malloydata/malloy from the server bundle's directory, so this
   // is the very module instance the bundle imports.
   try {
      const req = createRequire(
         SERVER_ENTRY || path.join(process.cwd(), "dist", "server.mjs"),
      );
      const malloy = req("@malloydata/malloy") as {
         Runtime?: { prototype: Record<string, unknown> };
         Malloy?: Record<string, unknown>;
      };
      // Translations: the static Malloy.compile, which every path that
      // translates Malloy text goes through (a model's getModel, a query's
      // run, validate, a prepared query), looked up on the class per call.
      const compile = malloy.Malloy?.compile as
         | ((...a: unknown[]) => unknown)
         | undefined;
      if (malloy.Malloy && compile) {
         malloy.Malloy.compile = function (this: unknown, ...a: unknown[]) {
            win.compiles++;
            total.compiles++;
            const caller = callerOf(new Error().stack ?? "");
            win.compilesByCaller[caller] =
               (win.compilesByCaller[caller] ?? 0) + 1;
            return compile.apply(this, a);
         };
         hooks.compile = true;
      } else {
         console.error("[mtr-probe] Malloy.compile not found");
      }
      // ModelMaterializer is not exported, so its prototype is taken from the
      // first one `Runtime.loadModel` returns, and its `getModel` (where the
      // translation runs) is counted from then on.
      const runtime = malloy.Runtime?.prototype;
      const loadModel = runtime?.loadModel as
         | ((...a: unknown[]) => unknown)
         | undefined;
      if (runtime && loadModel) {
         runtime.loadModel = function (this: unknown, ...a: unknown[]) {
            const materializer = loadModel.apply(this, a);
            hookMaterializer(materializer);
            return materializer;
         };
      } else {
         console.error("[mtr-probe] Runtime.loadModel not found");
      }
      const hookMaterializer = (m: unknown): void => {
         if (!m || typeof m !== "object") return;
         const proto = Object.getPrototypeOf(m) as Record<
            string,
            unknown
         > | null;
         const getModel = proto?.getModel as
            | ((...a: unknown[]) => unknown)
            | undefined;
         if (!proto || !getModel || (getModel as { __mtr?: true }).__mtr) {
            return;
         }
         const wrapped = function (this: unknown, ...a: unknown[]): unknown {
            win.materializations++;
            return getModel.apply(this, a);
         };
         (wrapped as { __mtr?: true }).__mtr = true;
         proto.getModel = wrapped;
         hooks.getModel = true;
      };
      // QueryModel builds: Malloy's whole-model pass (QueryStruct per source,
      // a UUID per field), which ms2data/service#7641's 0.12.1-0.12.5 ran on
      // the main thread once per #(index) dimension. It is synchronous, so
      // the wall time around it is main-thread time.
      const impl = req(
         path.join(
            path.dirname(req.resolve("@malloydata/malloy")),
            "model",
            "query_model_impl.js",
         ),
      ) as { QueryModelImpl?: { prototype: Record<string, unknown> } };
      const qm = impl.QueryModelImpl?.prototype;
      const loadModelFromDef = qm?.loadModelFromDef as
         | ((...a: unknown[]) => unknown)
         | undefined;
      if (qm && loadModelFromDef) {
         hooks.queryModel = true;
         qm.loadModelFromDef = function (this: unknown, ...a: unknown[]) {
            const start = performance.now();
            try {
               return loadModelFromDef.apply(this, a);
            } finally {
               win.queryModelBuilds++;
               total.queryModelBuilds++;
               win.queryModelMs += performance.now() - start;
               const caller = callerOf(new Error().stack ?? "");
               win.queryModelByCaller[caller] =
                  (win.queryModelByCaller[caller] ?? 0) + 1;
            }
         };
      } else {
         console.error("[mtr-probe] QueryModelImpl.loadModelFromDef not found");
      }
   } catch (e) {
      console.error(`[mtr-probe] could not hook Malloy: ${String(e)}`);
   }

   // ── The probe endpoint ───────────────────────────────────────────────
   const snapshot = () => {
      const t = process.threadCpuUsage();
      const p = process.cpuUsage();
      return {
         now: Date.now(),
         windowMs: Date.now() - win.startedAt,
         mainThreadCpuMs: (t.user + t.system) / 1000,
         processCpuMs: (p.user + p.system) / 1000,
         rssBytes: process.memoryUsage.rss(),
         maxBlockMs: Math.round(win.maxBlockMs),
         blocksOver: win.blocksOver,
         blockedMsOver50: Math.round(win.blockedMsOver50),
         eventLoopDelay: {
            maxMs: Math.round(eld.max / 1e6),
            p99Ms: Math.round(eld.percentile(99) / 1e6),
         },
         dispatches: win.dispatches,
         compiles: win.compiles,
         materializations: win.materializations,
         hooks,
         queryModelBuilds: win.queryModelBuilds,
         queryModelMs: Math.round(win.queryModelMs),
         queryModelByCaller: win.queryModelByCaller,
         compilesByCaller: win.compilesByCaller,
         totals: total,
      };
   };
   Bun.serve({
      port: PORT,
      hostname: "127.0.0.1",
      fetch(req) {
         const url = new URL(req.url);
         if (url.pathname === "/probe/reset" && req.method === "POST") {
            // Answer with the window that just closed, then open a new one.
            const closed = snapshot();
            win = fresh();
            eld.reset();
            last = performance.now();
            return Response.json(closed);
         }
         if (url.pathname.startsWith("/probe")) {
            return Response.json(snapshot());
         }
         return new Response("not found", { status: 404 });
      },
   });
   console.error(`[mtr-probe] listening on ${PORT}`);
}

/**
 * The first bundle frame below Malloy's in a stack: the server function that
 * asked for the compile. Bun does not minify the bundle, so its names are
 * the source's.
 */
function callerOf(stack: string): string {
   const frames = stack
      .split("\n")
      .slice(1)
      .map((l) => l.trim())
      .filter((l) => l.startsWith("at "));
   for (const f of frames) {
      if (f.includes("probe.ts")) continue;
      if (f.includes("@malloydata") || f.includes("node_modules")) continue;
      const m = f.match(/^at (?:async )?([^\s(]+)/);
      if (m && m[1] !== "<anonymous>") return m[1];
   }
   return "unknown";
}

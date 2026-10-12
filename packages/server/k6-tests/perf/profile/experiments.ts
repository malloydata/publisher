// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Standalone experiments behind docs/performance-profile.md. Each isolates one
// mechanism outside Publisher, against the installed @malloydata packages, to
// size an optimization before anyone writes it. Profiling only.
//
//   bun perf/profile/experiments.ts compile-cache <pkgDir> [rounds]
//       Compile every .malloy file of a package the way the load worker does
//       (a fresh Runtime per file) vs. one Runtime with a shared CacheManager.
//   bun perf/profile/experiments.ts query-model <pkgDir> <model.malloy> <query> [n]
//       Per-query loadQuery().getPreparedResult() cost, with and without the
//       base Model's QueryModel built once and shared.
//   bun perf/profile/experiments.ts duckdb-conns <dataDir> [gen|sf]
//       DuckDB-only throughput at 1..16 concurrent clients, over one connection
//       vs. several, and with fewer threads per instance.
//   bun perf/profile/experiments.ts namespace
//       Cost of one `new GlobalNameSpace()` (built for every Malloy document).
import { createRequire } from "node:module";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

/* eslint-disable @typescript-eslint/no-explicit-any */
const req = createRequire(import.meta.path);
const malloy: any = req("@malloydata/malloy");
const [cmd, ...args] = process.argv.slice(2);

function duckdbFor(dir: string): any {
   const { DuckDBConnection } = req("@malloydata/db-duckdb");
   return new DuckDBConnection("duckdb", ":memory:", path.resolve(dir));
}
const reader = {
   readURL: async (u: URL) => {
      const p = new URL(u).pathname;
      // The invalidation key is what lets CacheManager cache a file at all.
      return {
         contents: readFileSync(p, "utf8"),
         invalidationKey: String(statSync(p).mtimeMs),
      };
   },
};
const median = (xs: number[]) =>
   [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;

if (cmd === "compile-cache") {
   const pkg = path.resolve(args[0]!);
   const rounds = Number(args[1] ?? 5);
   const files = readdirSync(pkg).filter((f) => f.endsWith(".malloy"));
   const conn = duckdbFor(pkg);
   const connections = { lookupConnection: async () => conn };
   const url = (f: string) => pathToFileURL(path.join(pkg, f));
   const compile = (rt: any, f: string) =>
      rt.loadModel(url(f), { importBaseURL: new URL(".", url(f)) }).getModel();
   const variants: Array<[string, () => Promise<unknown>]> = [
      [
         "fresh Runtime per file (worker today)",
         async () => {
            for (const f of files)
               await compile(
                  new malloy.Runtime({ urlReader: reader, connections }),
                  f,
               );
         },
      ],
      [
         "shared CacheManager, one at a time",
         async () => {
            const cacheManager = new malloy.CacheManager(
               new malloy.InMemoryModelCache(),
            );
            const rt = new malloy.Runtime({
               urlReader: reader,
               connections,
               cacheManager,
            });
            for (const f of files) await compile(rt, f);
         },
      ],
      [
         "shared CacheManager, Promise.all",
         async () => {
            const cacheManager = new malloy.CacheManager(
               new malloy.InMemoryModelCache(),
            );
            const rt = new malloy.Runtime({
               urlReader: reader,
               connections,
               cacheManager,
            });
            await Promise.all(files.map((f) => compile(rt, f)));
         },
      ],
   ];
   for (const [name, fn] of variants) {
      await fn(); // schema cache + JIT
      const ts: number[] = [];
      for (let i = 0; i < rounds; i++) {
         const t = performance.now();
         await fn();
         ts.push(performance.now() - t);
      }
      console.log(
         `${name.padEnd(40)} ${files.length} files  median ${median(ts).toFixed(0)} ms`,
      );
   }
} else if (cmd === "query-model") {
   const [pkg, file, query, nArg] = args;
   const n = Number(nArg ?? 200);
   const conn = duckdbFor(pkg!);
   const rt = new malloy.Runtime({
      urlReader: reader,
      connections: { lookupConnection: async () => conn },
   });
   const url = pathToFileURL(path.resolve(pkg!, file!));
   for (const shared of [false, true]) {
      const mm = rt.loadModel(url, { importBaseURL: new URL(".", url) });
      const base = await mm.getModel();
      // A query compiled against an unmodified model reuses the base Model's
      // QueryModel only if one was already built (compile.js, existingQueryModel).
      if (shared) void base.queryModel;
      for (let i = 0; i < 20; i++)
         await mm.loadQuery(query).getPreparedResult();
      const t = performance.now();
      for (let i = 0; i < n; i++) await mm.loadQuery(query).getPreparedResult();
      console.log(
         `${file}: ${shared ? "shared QueryModel " : "QueryModel per query"}  ${((performance.now() - t) / n).toFixed(3)} ms per loadQuery+getPreparedResult`,
      );
   }
} else if (cmd === "duckdb-conns") {
   const { DuckDBInstance } = req("@duckdb/node-api");
   const dir = path.resolve(args[0]!);
   const F = `'${dir}/fact.parquet'`;
   const O = `'${dir}/order_items.parquet'`;
   const queries =
      args[1] === "sf"
         ? [
              `SELECT count(*) AS c, count(DISTINCT customer_id) AS u FROM ${O}`,
              `SELECT product_id, count(*) AS c, sum(sale_price) AS s FROM ${O} GROUP BY 1 ORDER BY 3 DESC LIMIT 20`,
           ]
         : [
              `SELECT count(*) AS row_count, count(order_id) AS order_count FROM ${F}`,
              `SELECT date_trunc('month', order_date) AS m, status, sum(quantity*unit_price*(1-discount)) AS revenue, count(order_id) AS oc FROM ${F} GROUP BY 1,2 ORDER BY 1 DESC LIMIT 500`,
              `SELECT p.category, s.region, sum(f.quantity*f.unit_price) AS rev, count(DISTINCT f.customer_id) AS cc FROM ${F} f LEFT JOIN '${dir}/products.parquet' p ON p.product_id=f.product_id LEFT JOIN '${dir}/stores.parquet' s ON s.store_id=f.store_id GROUP BY 1,2 ORDER BY 3 DESC`,
           ];
   const configs = [
      { conns: 1, clients: 1 },
      { conns: 1, clients: 4 },
      { conns: 1, clients: 16 },
      { conns: 4, clients: 4 },
      { conns: 16, clients: 16 },
      { threads: 4, conns: 4, clients: 4 },
      { threads: 4, conns: 16, clients: 16 },
      { threads: 1, conns: 16, clients: 16 },
   ];
   for (const { threads, conns, clients } of configs as Array<{
      threads?: number;
      conns: number;
      clients: number;
   }>) {
      const inst = await DuckDBInstance.create(
         ":memory:",
         threads ? { threads: String(threads) } : {},
      );
      const cs: any[] = [];
      for (let i = 0; i < conns; i++) cs.push(await inst.connect());
      for (const c of cs)
         for (const q of queries) await (await c.run(q)).getRowObjectsJson();
      const lat: number[] = [];
      const end = performance.now() + 6000;
      const t0 = performance.now();
      const cpu0 = process.cpuUsage();
      await Promise.all(
         Array.from({ length: clients }, async (_, k) => {
            for (let i = k; performance.now() < end; i++) {
               const t = performance.now();
               await (
                  await cs[k % conns].run(queries[i % queries.length])
               ).getRowObjectsJson();
               lat.push(performance.now() - t);
            }
         }),
      );
      const wall = (performance.now() - t0) / 1000;
      const cpu = process.cpuUsage(cpu0);
      lat.sort((a, b) => a - b);
      console.log(
         `threads=${String(threads ?? "default").padEnd(7)} conns=${String(conns).padStart(2)} clients=${String(clients).padStart(2)}  ${(lat.length / wall).toFixed(0).padStart(5)} q/s  p50 ${median(lat).toFixed(1)} ms  p95 ${lat[Math.floor(lat.length * 0.95)]!.toFixed(1)} ms  ${((cpu.user + cpu.system) / 1e6 / wall).toFixed(1)} cores`,
      );
      for (const c of cs) c.closeSync?.();
      inst.closeSync?.();
   }
} else if (cmd === "namespace") {
   const gns = path.join(
      path.dirname(req.resolve("@malloydata/malloy")),
      "lang",
      "ast",
      "types",
      "global-name-space.js",
   );
   const { GlobalNameSpace } = req(gns);
   for (let i = 0; i < 50; i++) new GlobalNameSpace();
   const n = 500;
   const t = performance.now();
   for (let i = 0; i < n; i++) new GlobalNameSpace();
   console.log(
      `new GlobalNameSpace(): ${((performance.now() - t) / n).toFixed(3)} ms each`,
   );
} else {
   console.error(
      readFileSync(import.meta.path, "utf8")
         .split("\n")
         .slice(3, 20)
         .join("\n"),
   );
   process.exit(2);
}
process.exit(0);

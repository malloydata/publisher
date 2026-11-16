// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import { execFileSync } from "child_process";
import fs from "fs";
import path from "path";
import {
   spawnServer,
   tempDir,
   type SpawnedServer,
} from "../../harness/spawn_server";
import { pollFor, TERMINAL_STATUSES } from "../materialization/polling";

/**
 * Churn does not grow a server's native memory or threads.
 *
 * Every DuckDB instance Publisher retires (a package's sandbox on unload, an
 * environment's storage destination on delete) used to keep its thread pool
 * and buffers until a JS GC finalized the node-api handle: an environment
 * delete/create loop with a sandbox package measured about 700 threads and
 * 2.7 GiB within 30 s. With the hard close (duckdb_hard_close.ts) the instance
 * is freed at retirement.
 *
 * Threads of a real server process, with no GC forced, so they count native
 * instances alone. A leaked sandbox holds three worker threads
 * (PUBLISHER_DUCKDB_THREADS=4) and a leaked destination at least one, until a
 * GC happens to run. RSS is logged, not asserted: without a forced GC it is
 * dominated by the JS heap.
 *
 * The destination loop needs a Postgres catalog: set
 * PUBLISHER_TEST_POSTGRES_URL to a server the test may create and drop
 * databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

setDefaultTimeout(600_000);

/** Environments churned at once in the peak loop, and cycles each. */
const LANES = 4;
const CYCLES = 10;
/**
 * Peak growth allowed. A lane holds at most one live instance (three worker
 * threads) at a time, so a server that frees what it retires peaks near
 * LANES x 3 threads; one that leaks peaks at up to LANES x CYCLES x 3.
 */
const THREAD_SLACK = 30;
/** Past the 30 s retirement drain of an unloaded package's connections. */
const DRAIN_WAIT_MS = 33_000;
/**
 * Growth allowed where nothing is in flight (between serial cycles, or after
 * the drain): none is expected, and each leaked instance holds at least one
 * worker thread.
 */
const QUIET_THREAD_SLACK = 8;

/** The OS thread count and resident MiB of `pid`, where they can be read. */
function usage(pid: number): { threads: number; rssMiB: number } | undefined {
   if (process.platform === "linux") {
      const status = fs.readFileSync(`/proc/${pid}/status`, "utf8");
      return {
         threads: Number(/^Threads:\s+(\d+)/m.exec(status)?.[1]),
         rssMiB: Number(/^VmRSS:\s+(\d+)/m.exec(status)?.[1]) / 1024,
      };
   }
   if (process.platform === "darwin") {
      const threads =
         execFileSync("ps", ["-M", "-p", String(pid)])
            .toString()
            .trim()
            .split("\n").length - 1;
      const rssKiB = Number(
         execFileSync("ps", ["-o", "rss=", "-p", String(pid)])
            .toString()
            .trim(),
      );
      return { threads, rssMiB: rssKiB / 1024 };
   }
   return undefined;
}

const measurable =
   process.platform === "linux" || process.platform === "darwin";

async function call(
   server: SpawnedServer,
   method: string,
   route: string,
   body?: unknown,
): Promise<{ status: number; body: Record<string, unknown> }> {
   const res = await fetch(`${server.baseUrl}/api/v0${route}`, {
      method,
      headers: { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
   });
   const text = await res.text();
   let parsed: Record<string, unknown> = {};
   try {
      parsed = JSON.parse(text);
   } catch {
      parsed = { text };
   }
   return { status: res.status, body: parsed };
}

async function ok(
   server: SpawnedServer,
   method: string,
   route: string,
   body?: unknown,
): Promise<Record<string, unknown>> {
   const res = await call(server, method, route, body);
   if (res.status >= 300) {
      throw new Error(
         `${method} ${route}: ${res.status} ${JSON.stringify(res.body).slice(0, 500)}`,
      );
   }
   return res.body;
}

/**
 * Run `cycle` on LANES lanes at once, a warm-up round and then CYCLES more,
 * sampling the process throughout. Returns the PEAK growth over the
 * post-warm-up baseline: a leaked instance lives until a GC happens to
 * finalize it, so a leak shows as the peak between collections, not as what
 * is left at the end.
 */
async function peakGrowth(
   server: SpawnedServer,
   cycle: (lane: number, i: number) => Promise<void>,
   { lanes: laneCount = LANES, cycles = CYCLES, betweenCycles = false } = {},
): Promise<{ threads: number; rssMiB: number }> {
   const lanes = Array.from({ length: laneCount }, (_, lane) => lane);
   await Promise.all(lanes.map((lane) => cycle(lane, 0)));
   const before = usage(server.proc.pid!)!;
   const peak = { ...before };
   const sample = () => {
      const now = usage(server.proc.pid!);
      if (now) {
         peak.threads = Math.max(peak.threads, now.threads);
         peak.rssMiB = Math.max(peak.rssMiB, now.rssMiB);
      }
   };
   let sampling = !betweenCycles;
   const sampler = (async () => {
      while (sampling) {
         sample();
         await new Promise((resolve) => setTimeout(resolve, 100));
      }
   })();
   try {
      await Promise.all(
         lanes.map(async (lane) => {
            for (let i = 1; i <= cycles; i++) {
               await cycle(lane, i);
               if (betweenCycles) sample();
            }
         }),
      );
   } finally {
      sampling = false;
      await sampler;
   }
   const grew = {
      threads: peak.threads - before.threads,
      rssMiB: Math.round(peak.rssMiB - before.rssMiB),
   };
   console.log(`churn peak growth ${JSON.stringify({ before, peak, grew })}`);
   return grew;
}

/** A query that starts every worker in the instance's pool. */
const PARALLEL_QUERY = "run: s -> { aggregate: n is count(), t is sum(x) }";

describe.skipIf(!measurable)("DuckDB sandbox churn", () => {
   let server: SpawnedServer;
   const pkg = tempDir("hard-close-pkg-");

   /** A copy of the sandbox package named `name`, written once. */
   function packageDir(name: string): string {
      const dir = path.join(pkg.dir, name);
      if (!fs.existsSync(dir)) {
         fs.mkdirSync(dir);
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name, version: "1.0.0" }),
         );
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            'source: s is duckdb.sql("SELECT x FROM range(5000000) t(x) WHERE x % 3 <> 0")\n',
         );
      }
      return dir;
   }

   beforeAll(async () => {
      server = await spawnServer({
         config: { frozenConfig: false, environments: [] },
         env: { PUBLISHER_DUCKDB_THREADS: "4" },
      });
      await server.waitUntilServing();
   });

   afterAll(async () => {
      await server?.stop();
      pkg.cleanup();
   });

   async function loadAndQuery(env: string, name = "sb"): Promise<void> {
      await ok(server, "POST", `/environments/${env}/packages`, {
         name,
         location: packageDir(name),
      });
      const body = await ok(
         server,
         "POST",
         `/environments/${env}/packages/${name}/models/m.malloy/query`,
         { query: PARALLEL_QUERY, compactJson: true },
      );
      expect(JSON.parse(body.result as string)[0].n).toBe(3333333);
   }

   it("unloading a package frees its sandbox once its drain ends", async () => {
      await ok(server, "POST", "/environments", {
         name: "unload",
         packages: [],
         connections: [],
      });
      // An unload retires the package's connections after a 30 s drain
      // (RETIRED_CONNECTION_DRAIN_MS), and a package reloaded at the same path
      // rejoins the same instance, so each cycle loads a package of its own
      // and the measurement waits out the drain.
      await loadAndQuery("unload", "warm");
      await ok(server, "DELETE", "/environments/unload/packages/warm");
      await new Promise((resolve) => setTimeout(resolve, DRAIN_WAIT_MS));
      const before = usage(server.proc.pid!)!;
      for (let i = 0; i < CYCLES; i++) {
         await loadAndQuery("unload", `sb${i}`);
         await ok(server, "DELETE", `/environments/unload/packages/sb${i}`);
      }
      await new Promise((resolve) => setTimeout(resolve, DRAIN_WAIT_MS));
      const after = usage(server.proc.pid!)!;
      console.log(`unload growth ${JSON.stringify({ before, after })}`);
      expect(after.threads - before.threads).toBeLessThan(QUIET_THREAD_SLACK);
      await ok(server, "DELETE", "/environments/unload");
   });

   it("deleting and creating an environment frees its packages' sandboxes", async () => {
      const grew = await peakGrowth(server, async (lane) => {
         await ok(server, "POST", "/environments", {
            name: `churn${lane}`,
            packages: [],
            connections: [],
         });
         await loadAndQuery(`churn${lane}`);
         await ok(server, "DELETE", `/environments/churn${lane}`);
      });
      expect(grew.threads).toBeLessThan(THREAD_SLACK);
   });
});

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

describe.skipIf(!measurable || !url)("DuckDB storage destination churn", () => {
   const token = Math.random().toString(36).slice(2, 8);
   const warehouse = `hc_wh_${token}`;
   const catalog = `hc_lake_${token}`;
   let admin: PostgresConnection;
   let server: SpawnedServer;
   let pg: Record<string, unknown>;
   const pkg = tempDir("hard-close-dest-pkg-");
   const lake = tempDir("hard-close-lake-");

   const environment = () => ({
      name: "dest",
      packages: [],
      connections: [
         {
            name: "wh",
            type: "postgres",
            postgresConnection: { ...pg, databaseName: warehouse },
         },
      ],
      storageDestinations: [
         {
            name: "lake",
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: { ...pg, databaseName: catalog },
               },
               storage: { bucketUrl: lake.dir },
            },
         },
      ],
   });

   beforeAll(async () => {
      admin = new PostgresConnection({ name: "admin", connectionString: url! });
      await admin.runSQL(`CREATE DATABASE ${warehouse}`);
      await admin.runSQL(`CREATE DATABASE ${catalog}`);
      const parsed = new URL(url!);
      pg = {
         host: parsed.hostname,
         port: Number(parsed.port || 5432),
         userName: decodeURIComponent(parsed.username),
         password: decodeURIComponent(parsed.password),
      };
      const wh = new PostgresConnection({
         name: "wh",
         connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
      });
      await wh.runSQL(
         "CREATE TABLE orders AS SELECT g AS id, (ARRAY['a','b','c'])[1 + g % 3] AS category, (g % 97)::float AS amount FROM generate_series(1, 20000) g",
      );
      await wh.close();
      fs.writeFileSync(
         path.join(pkg.dir, "publisher.json"),
         JSON.stringify({ name: "st", version: "1.0.0" }),
      );
      fs.writeFileSync(
         path.join(pkg.dir, "m.malloy"),
         [
            "##! experimental.persistence",
            "source: orders is wh.table('public.orders')",
            '#@ persist storage="lake" name="by_category"',
            "source: by_category is orders -> {",
            "  group_by: category",
            "  aggregate: n is count(), revenue is amount.sum()",
            "}",
            "",
         ].join("\n"),
      );
      server = await spawnServer({
         config: { frozenConfig: false, environments: [] },
         env: {
            PUBLISHER_DUCKDB_THREADS: "4",
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "on",
         },
      });
      await server.waitUntilServing();
   });

   afterAll(async () => {
      await server?.stop();
      for (const db of [warehouse, catalog]) {
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
            .catch(() => undefined);
      }
      await admin?.close().catch(() => undefined);
      pkg.cleanup();
      lake.cleanup();
   });

   it("deleting an environment frees its destination, opened and read", async () => {
      const api = "/environments/dest/packages/st";
      const grew = await peakGrowth(
         server,
         async () => {
            // Each environment starts with no build of its own, so each builds.
            await ok(server, "POST", "/environments", environment());
            await ok(server, "POST", "/environments/dest/packages", {
               name: "st",
               location: pkg.dir,
            });
            const run = (await ok(
               server,
               "POST",
               `${api}/materializations`,
               {},
            )) as {
               id: string;
            };
            const settled = await pollFor(
               async () =>
                  (await ok(
                     server,
                     "GET",
                     `${api}/materializations/${run.id}`,
                  )) as {
                     status: string;
                     error?: string | null;
                  },
               (m) => TERMINAL_STATUSES.includes(m.status),
               "the build did not settle",
               120_000,
            );
            expect(settled.error ?? null).toBeNull();
            // Served from the destination: this is what opens its instance.
            const res = await call(
               server,
               "POST",
               `${api}/models/m.malloy/query`,
               {
                  query: "run: by_category -> { aggregate: t is n.sum() }",
                  compactJson: true,
               },
            );
            expect(res.status).toBe(200);
            expect(
               (res.body.path as { servedBy?: { kind?: string } })?.servedBy
                  ?.kind,
            ).toBe("materialized");
            expect(JSON.parse(res.body.result as string)[0].t).toBe(20000);
            await ok(server, "DELETE", "/environments/dest");
         },
         // Serial, sampled between cycles: a build session and a query are
         // in flight within a cycle and would read as growth.
         { lanes: 1, cycles: 20, betweenCycles: true },
      );
      expect(grew.threads).toBeLessThan(QUIET_THREAD_SLACK);
   });
});

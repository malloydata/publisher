// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `GET /status` costs (ms2data/service#7795): the control plane's
 * heartbeat, polled against a real server holding E environments × P
 * packages × V published versions, with some versions registered but not
 * loaded (archived, then unarchived: rows with no copy in memory).
 *
 *   bun tests/perf/status_bench.ts [--server-dir <packages/server>] \
 *      [--envs 3] [--packages 8] [--versions 3] [--sources 30] \
 *      [--polls 300] [--seconds 30] [--port 4931]
 *
 * `--server-dir` picks the checkout whose `src/server.ts` is run, so one copy
 * of this script measures two branches alike. Two phases:
 *
 * - idle: `--polls` sequential polls with nothing else running. The server's
 *   CPU time over the phase (ps, all threads, of which only the main one is
 *   busy then) divided by the polls is the main-thread time per poll.
 * - under load: for `--seconds`, two loops publish fresh versions of other
 *   packages back to back (each a download, a compile and a commit) while
 *   `/status?includeLoading=true` is polled every 100 ms, as the control plane
 *   polls it.
 */

import { type ChildProcess, execFileSync, spawn } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";

function arg(name: string, fallback: string): string {
   const argv = process.argv;
   for (let i = 0; i < argv.length; i++) {
      if (argv[i] === `--${name}`) return argv[i + 1] ?? fallback;
      if (argv[i]?.startsWith(`--${name}=`)) {
         return argv[i]!.slice(name.length + 3);
      }
   }
   return fallback;
}

const SERVER_DIR = path.resolve(
   arg("server-dir", path.join(import.meta.dir, "../..")),
);
const ENVS = Number(arg("envs", "3"));
const PACKAGES = Number(arg("packages", "8"));
const VERSIONS = Number(arg("versions", "3"));
const SOURCES = Number(arg("sources", "30"));
const POLLS = Number(arg("polls", "300"));
const SECONDS = Number(arg("seconds", "30"));
const PORT = Number(arg("port", "4931"));
const BASE = `http://127.0.0.1:${PORT}/api/v0`;

/** A model with `SOURCES` sources, each with dimensions, measures and a view. */
function model(): string {
   const lines: string[] = [];
   for (let s = 0; s < SOURCES; s++) {
      lines.push(
         `source: s${s} is duckdb.sql("SELECT ${s} AS id, 'x' AS name, 1.5 AS amount, DATE '2024-01-01' AS d") extend {`,
         `  primary_key: id`,
         ...Array.from(
            { length: 6 },
            (_, i) => `  dimension: dim${i} is concat(name, '${i}')`,
         ),
         ...Array.from(
            { length: 4 },
            (_, i) => `  measure: m${i} is amount.sum() * ${i + 1}`,
         ),
         `  view: by_name is { group_by: name; aggregate: m0, m1 }`,
         `}`,
      );
   }
   return lines.join("\n") + "\n";
}

function writePackage(dir: string, name: string, version: string): string {
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name, version, description: `${name} ${version}` }),
   );
   fs.writeFileSync(path.join(dir, "model.malloy"), model());
   return dir;
}

async function json(
   method: string,
   url: string,
   body?: unknown,
): Promise<{ status: number; body: unknown }> {
   const response = await fetch(url, {
      method,
      headers: { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
   });
   const text = await response.text();
   let parsed: unknown = text;
   try {
      parsed = JSON.parse(text);
   } catch {
      // Not JSON; kept as text.
   }
   return { status: response.status, body: parsed };
}

function cpuSeconds(pid: number): number {
   // [[dd-]hh:]mm:ss.cc
   const raw = execFileSync("ps", ["-o", "time=", "-p", String(pid)])
      .toString()
      .trim();
   const [clock, days] = raw.includes("-")
      ? [raw.split("-")[1]!, Number(raw.split("-")[0])]
      : [raw, 0];
   const parts = clock.split(":").map(Number);
   let seconds = 0;
   for (const part of parts) seconds = seconds * 60 + part;
   return seconds + days * 86_400;
}

function percentile(sorted: number[], p: number): number {
   if (sorted.length === 0) return NaN;
   return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]!;
}

function summary(samples: number[]): string {
   const sorted = [...samples].sort((a, b) => a - b);
   return `n=${sorted.length} p50=${percentile(sorted, 0.5).toFixed(1)}ms p90=${percentile(sorted, 0.9).toFixed(1)}ms max=${sorted[sorted.length - 1]?.toFixed(1)}ms`;
}

async function poll(includeLoading: boolean): Promise<number> {
   const started = performance.now();
   const response = await fetch(
      `${BASE}/status${includeLoading ? "?includeLoading=true" : ""}`,
   );
   await response.arrayBuffer();
   if (response.status !== 200) throw new Error(`/status ${response.status}`);
   return performance.now() - started;
}

async function main(): Promise<void> {
   const root = fs.mkdtempSync(path.join(os.tmpdir(), "status-bench-"));
   const sources = path.join(root, "sources");
   const environments = Array.from({ length: ENVS }, (_, e) => `env${e}`);
   const config = {
      frozenConfig: false,
      // A seed package each, published as a version at boot: an environment
      // the config gives no package has no directory to publish into.
      environments: environments.map((name) => ({
         name,
         packages: [
            {
               name: "seed",
               location: writePackage(
                  path.join(sources, name, "seed"),
                  "seed",
                  "1.0.0",
               ),
            },
         ],
         connections: [],
      })),
   };
   fs.writeFileSync(
      path.join(root, "publisher.config.json"),
      JSON.stringify(config),
   );

   const server: ChildProcess = spawn(
      "bun",
      [path.join(SERVER_DIR, "src", "server.ts")],
      {
         cwd: root,
         env: {
            ...process.env,
            PUBLISHER_ROOT: root,
            PUBLISHER_HOST: "127.0.0.1",
            PUBLISHER_PORT: String(PORT),
            PUBLISHER_MCP_PORT: String(PORT + 1),
            PUBLISHER_MCP_CONFIG_FILE_ENABLED: "false",
            PUBLISHER_PACKAGE_VERSIONING: "on",
            PUBLISHER_LOGGING_LEVEL: "error",
         },
         stdio: ["ignore", "ignore", "pipe"],
      },
   );
   let stderr = "";
   server.stderr?.on("data", (chunk) => (stderr += String(chunk)));
   try {
      for (let i = 0; ; i++) {
         if (i > 600) throw new Error(`server never served:\n${stderr}`);
         try {
            const status = await json("GET", `${BASE}/status`);
            if (
               (status.body as { operationalState?: string })
                  .operationalState === "serving"
            )
               break;
         } catch {
            // Not listening yet.
         }
         await Bun.sleep(200);
      }

      const setupStarted = performance.now();
      for (const env of environments) {
         for (let p = 0; p < PACKAGES; p++) {
            const name = `pkg${p}`;
            for (let v = 0; v < VERSIONS; v++) {
               const version = `1.0.${v}`;
               const location = writePackage(
                  path.join(sources, env, `${name}-${version}`),
                  name,
                  version,
               );
               const published = await json(
                  "POST",
                  `${BASE}/environments/${env}/packages`,
                  { name, location },
               );
               if (published.status >= 300) {
                  throw new Error(
                     `publish ${env}/${name}@${version}: ${JSON.stringify(published.body)}`,
                  );
               }
            }
            // The oldest version: a row with no copy in memory.
            if (VERSIONS > 1) {
               for (const archiveStatus of ["archive", "unarchive"]) {
                  const changed = await json(
                     "PATCH",
                     `${BASE}/environments/${env}/packages/${name}/versions/1.0.0`,
                     { archiveStatus },
                  );
                  if (changed.status >= 300) {
                     throw new Error(
                        `${archiveStatus} ${env}/${name}: ${JSON.stringify(changed.body)}`,
                     );
                  }
               }
            }
         }
      }
      const status = (await json("GET", `${BASE}/status`)).body as {
         version?: string;
         environments: Array<{ packages?: Array<{ loaded?: boolean }> }>;
      };
      const entries = status.environments.flatMap((e) => e.packages ?? []);
      console.log(
         `server ${status.version} at ${SERVER_DIR}\n` +
            `${ENVS} envs x ${PACKAGES} packages x ${VERSIONS} versions, ` +
            `${SOURCES} sources each: ${entries.length} entries, ` +
            `${entries.filter((e) => e.loaded === false).length} not loaded ` +
            `(setup ${((performance.now() - setupStarted) / 1000).toFixed(1)}s)`,
      );
      const body = await (
         await fetch(`${BASE}/status?includeLoading=true`)
      ).arrayBuffer();
      console.log(`/status body: ${(body.byteLength / 1024).toFixed(0)} KiB`);

      // Idle: main-thread time per poll.
      for (let i = 0; i < 20; i++) await poll(true);
      const pid = server.pid!;
      const cpuBefore = cpuSeconds(pid);
      const idle: number[] = [];
      for (let i = 0; i < POLLS; i++) idle.push(await poll(true));
      const cpuPerPoll = ((cpuSeconds(pid) - cpuBefore) * 1000) / POLLS;
      console.log(
         `idle:       ${summary(idle)} cpu/poll=${cpuPerPoll.toFixed(2)}ms`,
      );

      // Under load: two loops publish fresh versions while /status is polled.
      let stop = false;
      let published = 0;
      const churn = async (loop: number) => {
         for (let n = 0; !stop; n++) {
            const env = environments[(loop + n) % environments.length]!;
            const name = `churn${loop}`;
            const version = `2.${loop}.${n}`;
            const location = writePackage(
               path.join(sources, env, `${name}-${version}`),
               name,
               version,
            );
            const result = await json(
               "POST",
               `${BASE}/environments/${env}/packages`,
               { name, location },
            );
            if (result.status < 300) published += 1;
         }
      };
      const loops = [churn(0), churn(1)];
      const loaded: number[] = [];
      const deadline = performance.now() + SECONDS * 1000;
      while (performance.now() < deadline) {
         const tick = performance.now();
         loaded.push(await poll(true));
         const rest = 100 - (performance.now() - tick);
         if (rest > 0) await Bun.sleep(rest);
      }
      stop = true;
      await Promise.all(loops);
      console.log(
         `under load: ${summary(loaded)} (${published} versions published meanwhile)`,
      );
   } finally {
      server.kill("SIGKILL");
      fs.rmSync(root, { recursive: true, force: true });
   }
}

await main();

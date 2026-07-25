// Phase A spike for per-scenario isolation: measure the two unknowns before
// designing around them.
//
//   1. What does a fresh DuckLake catalog cost? (CREATE DATABASE + the metadata
//      tables the ducklake extension creates on first attach — the "migration".)
//   2. Does server boot time scale with the NUMBER OF ENVIRONMENTS, or with the
//      number of packages? Per-scenario isolation trades one env holding N packages
//      for N envs holding one each, and the 22 boots in a full run multiply whatever
//      that difference is.
//
// Throwaway. `bun hammer/spike-isolation-cost.ts`

import path from "path";
import { mkdirSync, rmSync } from "fs";
import os from "os";
import {
   ducklakeDest,
   postgresSource,
   writeConfig,
   type EnvSpec,
} from "./lib/config";
import { runLakeSql } from "./lib/ducklake";
import { writePackage } from "./lib/packages";
import { startPostgres } from "./lib/postgres";
import { buildServerIfNeeded, startServer } from "./lib/server";

const REPO_ROOT = path.resolve(import.meta.dir, "..");
const ms = (t: number): string => `${Math.round(t)}ms`;

async function main(): Promise<void> {
   const workdir = path.join(os.tmpdir(), `hammer-spike-${Date.now()}`);
   mkdirSync(workdir, { recursive: true });
   console.log(`workdir: ${workdir}\n`);

   await buildServerIfNeeded(REPO_ROOT);
   const pg = await startPostgres({ reuse: true });
   const sourceDb = "spike_src";
   await pg.resetDb(sourceDb);
   await pg.sql(sourceDb, "CREATE TABLE t AS SELECT 1 AS id, 100 AS amount");

   // ── 1. catalog cost ──
   console.log("── DuckLake catalog cost (CREATE DATABASE + first-attach bootstrap)");
   const N_CAT = 5;
   const createTimes: number[] = [];
   const attachTimes: number[] = [];
   for (let i = 0; i < N_CAT; i++) {
      const catDb = `spike_cat_${i}`;
      const store = path.join(workdir, `store_${i}`);
      mkdirSync(store, { recursive: true });

      let t0 = performance.now();
      await pg.resetDb(catDb);
      createTimes.push(performance.now() - t0);

      // First attach initializes the catalog's metadata tables.
      t0 = performance.now();
      await runLakeSql(
         {
            host: pg.host,
            port: pg.hostPort,
            user: pg.user,
            password: pg.password,
            catalogDb: catDb,
            storageDir: store,
         },
         "CREATE TABLE IF NOT EXISTS probe AS SELECT 1 AS x",
      );
      attachTimes.push(performance.now() - t0);
   }
   const avg = (a: number[]) => a.reduce((x, y) => x + y, 0) / a.length;
   console.log(`   CREATE DATABASE  avg ${ms(avg(createTimes))}  (n=${N_CAT})`);
   console.log(
      `   first attach     avg ${ms(avg(attachTimes))}  min ${ms(Math.min(...attachTimes))}  max ${ms(Math.max(...attachTimes))}`,
   );
   console.log(
      `   => 60 catalogs ≈ ${((avg(createTimes) + avg(attachTimes)) * 60 / 1000).toFixed(1)}s one-time\n`,
   );

   // ── 2. boot cost: N envs × 1 package  vs  1 env × N packages ──
   console.log("── server boot: N envs x 1 package  vs  1 env x N packages");
   const pkgRoot = path.join(workdir, "packages");
   mkdirSync(pkgRoot, { recursive: true });
   const model = (n: string) => ({
      path: `${n}.malloy`,
      text: `source: t${n} is orders_pg.table('public.t')\n`,
   });

   const bootOnce = async (environments: EnvSpec[], label: string) => {
      const configPath = path.join(workdir, `config-${label}.json`);
      await writeConfig({ configPath, environments });
      const serverRoot = path.join(workdir, `root-${label}`);
      mkdirSync(serverRoot, { recursive: true });
      const t0 = performance.now();
      const server = await startServer({
         repoRoot: REPO_ROOT,
         serverRoot,
         configPath,
         port: 14900,
         mcpPort: 14940,
         mode: "on",
         logFile: path.join(workdir, `server-${label}.log`),
      });
      const elapsed = performance.now() - t0;
      await server.stop();
      return elapsed;
   };

   for (const n of [1, 10, 60]) {
      const conns = (env: string) => [postgresSource("orders_pg", pg, sourceDb)];
      // shape A: n envs, one package each
      const envsA: EnvSpec[] = [];
      for (let i = 0; i < n; i++) {
         const name = `envA${i}_${n}`;
         const loc = await writePackage(pkgRoot, {
            name: `pkgA${i}_${n}`,
            models: [model(`a${i}_${n}`)],
         });
         envsA.push({
            name,
            connections: conns(name),
            packages: [{ name: `pkgA${i}_${n}`, location: loc }],
         });
      }
      // shape B: one env, n packages
      const pkgsB = [];
      for (let i = 0; i < n; i++) {
         const loc = await writePackage(pkgRoot, {
            name: `pkgB${i}_${n}`,
            models: [model(`b${i}_${n}`)],
         });
         pkgsB.push({ name: `pkgB${i}_${n}`, location: loc });
      }
      const envsB: EnvSpec[] = [
         { name: `envB_${n}`, connections: conns(`envB_${n}`), packages: pkgsB },
      ];

      const a = await bootOnce(envsA, `A${n}`);
      const b = await bootOnce(envsB, `B${n}`);
      console.log(
         `   n=${String(n).padStart(2)}   ${n} envs x1 pkg: ${ms(a).padStart(7)}` +
            `    1 env x${n} pkg: ${ms(b).padStart(7)}`,
      );
   }

   console.log("\n(22 boots per full run today — multiply the delta by that.)");
   await pg.stop();
   rmSync(workdir, { recursive: true, force: true });
}

main().catch((err) => {
   console.error("SPIKE FAILED:", err instanceof Error ? err.stack : err);
   process.exit(1);
});

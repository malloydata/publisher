#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The mixed-version test: during a rolling deploy the last release's workers
// read the manifest this checkout's workers write. This checkout builds the
// fixture, and the last published `@malloy-publisher/server`, bound to the
// manifest that build wrote, must serve every stored table from it.
//
//   bun hammer/mixed-version/run.ts                  # against npm's `latest`
//   bun hammer/mixed-version/run.ts --from 0.17.2    # against a given release
//   bun hammer/mixed-version/run.ts --keep           # leave the workdir and Postgres up
//
// 1. Install the released server into its own directory (`npm view` resolves
//    `latest` first, so the report names what ran).
// 2. Start this checkout's build on a fresh server root, build the fixture
//    package (fixture/mixed), and write the run's manifest entries to a file:
//    the manifest a host would distribute, with this checkout's new fields
//    (`contract`, `builtAt`, `builtFrom`).
// 3. Change the source data, so a stored answer and a live one differ, and
//    stop this checkout's server.
// 4. Start the released server on a server root of its own, with the same
//    connections and destination, and bind the package to that manifest
//    (`manifestLocation`).
// 5. Assert: it reaches `serving` with no load errors, binds the manifest, and
//    answers every probe from its stored table on the expected tier with the
//    rows this checkout's server answered before the data changed (a live
//    answer would show the change).
//
// The in-process counterpart, run on every pull request, is
// packages/server/tests/component/mixed_version_manifest.component.spec.ts:
// it runs such a manifest through a frozen copy of the release's loader.
//
// What it needs: Docker, for a throwaway Postgres that is both the source
// warehouse and the DuckLake catalog, and npm. No warehouse credentials.
//
// With HAMMER_LOG_DIR set (CI sets it), writes mixed-version-report.json (the
// upgrade report's shape, rendered by `bun hammer/upgrade/summary.ts`), both
// servers' logs and the manifest there.

import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { cp } from "fs/promises";
import os from "os";
import path from "path";
import { writeConfig } from "../lib/config";
import { PortAllocator } from "../lib/ports";
import { startPostgres } from "../lib/postgres";
import { Rest } from "../lib/rest";
import { buildServerIfNeeded, startServer } from "../lib/server";
import {
   isReleaseVersion,
   type ProbeRecord,
   type UpgradeCheck,
   type UpgradeReport,
} from "../lib/upgrade_report";
import { log, runOrThrow } from "../lib/util";

const PACKAGE = "mixed";
const MODEL = "mixed.malloy";
const ENV = "mixed";

const SEED = `
CREATE TABLE public.mv_orders (
   order_id integer, order_date date, region text, category text, amount numeric
);
INSERT INTO public.mv_orders VALUES
   (1, '2026-01-01', 'US', 'books', 100),
   (2, '2026-01-01', 'EU', 'books', 50),
   (3, '2026-01-02', 'US', 'tools', 200),
   (4, '2026-01-02', 'EU', 'tools', 25);
CREATE TABLE public.mv_items (item_id integer, order_id integer);
INSERT INTO public.mv_items VALUES (10, 1), (11, 1), (12, 3);
`;
const MUTATION = `
INSERT INTO public.mv_orders VALUES (99, '2026-01-01', 'US', 'books', 1000);
INSERT INTO public.mv_items VALUES (13, 99);
`;

interface Probe {
   id: string;
   route: string;
   /** The tier it must be answered from. */
   tier: "storage" | "database";
   query: string;
   /** The same answer computed live, from the unannotated source. */
   live: string;
}

const PROBES: Probe[] = [
   {
      id: "persist-database",
      route: "#@ persist (database tier, Postgres)",
      tier: "database",
      query: "run: pg_daily -> { select: order_date, total_amount; order_by: order_date asc }",
      live: "run: live_orders -> { group_by: order_date; aggregate: total_amount is amount.sum(); order_by: order_date asc }",
   },
   {
      id: "materialize-database",
      route: "#(materialize) (database tier, Postgres)",
      tier: "database",
      query: "run: pg_by_category -> { select: category, total_amount; order_by: category asc }",
      live: "run: live_orders -> { group_by: category; aggregate: total_amount is amount.sum(); order_by: category asc }",
   },
   {
      id: "persist-storage",
      route: "#@ persist storage=lake",
      tier: "storage",
      query: "run: lake_daily -> { select: order_date, total_amount; order_by: order_date asc }",
      live: "run: live_orders -> { group_by: order_date; aggregate: total_amount is amount.sum(); order_by: order_date asc }",
   },
   {
      id: "materialize-storage",
      route: '#(materialize) storage="lake"',
      tier: "storage",
      query: "run: lake_by_region -> { select: region, total_amount; order_by: region asc }",
      live: "run: live_orders -> { group_by: region; aggregate: total_amount is amount.sum(); order_by: region asc }",
   },
   {
      id: "view-over-pair-table",
      route: "#@ persist view over a #(materialize) pair table (database tier)",
      tier: "database",
      query: "run: pair_views -> by_region -> { select: region, total_amount, item_count; order_by: region asc }",
      live: "run: live_orders -> { group_by: region; aggregate: total_amount is amount.sum(); order_by: region asc }",
   },
];

const FETCH_TIMEOUT_MS = 60_000;

const same = (a: unknown, b: unknown): boolean =>
   JSON.stringify(a) === JSON.stringify(b);

interface Args {
   from?: string;
   keep: boolean;
}

function parseArgs(argv: string[]): Args {
   const a: Args = { keep: false };
   for (let i = 0; i < argv.length; i++) {
      if (argv[i] === "--from") a.from = argv[++i];
      else if (argv[i] === "--keep") a.keep = true;
      else {
         console.error(
            "Usage: bun hammer/mixed-version/run.ts [--from <version>] [--keep]",
         );
         process.exit(2);
      }
   }
   return a;
}

async function queryProbe(rest: Rest, malloy: string): Promise<ProbeRecord> {
   const out = await rest.query(PACKAGE, MODEL, { query: malloy });
   return { rows: out.rows, servedFrom: out.servedFrom ?? null };
}

async function statusOf(rest: Rest): Promise<{
   loadErrors?: unknown[];
   environments?: unknown;
}> {
   return (await (
      await fetch(`${rest.baseUrl}/api/v0/status`, {
         signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      })
   ).json()) as { loadErrors?: unknown[] };
}

async function main(): Promise<void> {
   const args = parseArgs(process.argv.slice(2));
   if (args.from !== undefined && !isReleaseVersion(args.from)) {
      console.error(
         `--from must be a release version (x.y.z or x.y.z-pre), got ${JSON.stringify(args.from)}`,
      );
      process.exit(2);
   }
   const started = new Date();
   const repoRoot = path.resolve(import.meta.dir, "..", "..");
   const logDir = process.env.HAMMER_LOG_DIR;
   const workdir = mkdtempSync(path.join(os.tmpdir(), "publisher-mixed-"));
   log.info(`workdir: ${workdir}`);

   const checks: UpgradeCheck[] = [];
   const notes: string[] = [];
   const probes: UpgradeReport["probes"] = Object.fromEntries(
      PROBES.map((p) => [p.id, { route: p.route }]),
   );
   const check = (
      phase: string,
      name: string,
      ok: boolean,
      detail?: string,
   ): boolean => {
      checks.push({ phase, name, ok, ...(ok || !detail ? {} : { detail }) });
      (ok ? log.ok : log.err)(
         `[${phase}] ${name}${ok || !detail ? "" : ` — ${detail}`}`,
      );
      return ok;
   };

   const builderVersion = (
      (await Bun.file(
         path.join(repoRoot, "packages", "server", "package.json"),
      ).json()) as { version: string }
   ).version;
   let releasedVersion = args.from ?? "";
   let harnessError: string | undefined;
   const stops: (() => Promise<void>)[] = [];
   const manifestPath = path.join(workdir, "manifest.json");

   try {
      // 1. The released server, at an exact version, run as itself.
      if (!releasedVersion) {
         const r = await runOrThrow(
            ["npm", "view", "@malloy-publisher/server@latest", "version"],
            { timeoutMs: 60_000 },
         );
         const latest = r.stdout.trim();
         if (!isReleaseVersion(latest))
            throw new Error(
               `npm view returned ${JSON.stringify(latest)}, not a release version`,
            );
         releasedVersion = latest;
      }
      log.step(`installing @malloy-publisher/server@${releasedVersion}`);
      const releasedDir = path.join(workdir, "released");
      mkdirSync(releasedDir, { recursive: true });
      writeFileSync(path.join(releasedDir, "package.json"), '{"private":true}');
      await runOrThrow(
         [
            "npm",
            "install",
            "--no-audit",
            "--no-fund",
            "--loglevel=error",
            `@malloy-publisher/server@${releasedVersion}`,
         ],
         { cwd: releasedDir, timeoutMs: 10 * 60_000 },
      );
      const releasedEntry = path.join(
         releasedDir,
         "node_modules",
         "@malloy-publisher",
         "server",
         "dist",
         "server.mjs",
      );
      if (!existsSync(releasedEntry))
         throw new Error(`the install has no ${releasedEntry}`);

      await buildServerIfNeeded(repoRoot);

      // The data both servers read: one Postgres (the warehouse and the
      // DuckLake catalog) and the lake's data directory. Each server has a
      // server root of its own: only the manifest passes between them.
      const pg = await startPostgres({
         keep: args.keep,
         ...(process.env.MIXED_VERSION_PG_CONTAINER
            ? { containerName: process.env.MIXED_VERSION_PG_CONTAINER }
            : {}),
         ...(process.env.MIXED_VERSION_PG_PASSWORD
            ? { password: process.env.MIXED_VERSION_PG_PASSWORD }
            : {}),
      });
      stops.push(() => (args.keep ? Promise.resolve() : pg.stop()));
      await pg.resetDbs(["mv_source", "mv_catalog"]);
      await pg.sql("mv_source", SEED);
      const dataDir = path.join(workdir, "data");
      const lakeDir = path.join(dataDir, "lake");
      mkdirSync(lakeDir, { recursive: true });
      const pkgDir = path.join(dataDir, "packages", PACKAGE);
      await cp(path.join(import.meta.dir, "fixture", PACKAGE), pkgDir, {
         recursive: true,
      });
      const pgConn = {
         host: pg.host,
         port: pg.hostPort,
         databaseName: "mv_source",
         userName: pg.user,
         password: pg.password,
      };
      const configPath = path.join(dataDir, "publisher.config.json");
      await writeConfig({
         configPath,
         environments: [
            {
               name: ENV,
               packages: [{ name: PACKAGE, location: pkgDir }],
               connections: [
                  {
                     name: "orders_pg",
                     type: "postgres",
                     postgresConnection: pgConn,
                  },
               ],
               storageDestinations: [
                  {
                     name: "lake",
                     type: "ducklake",
                     ducklakeConnection: {
                        catalog: {
                           postgresConnection: {
                              ...pgConn,
                              databaseName: "mv_catalog",
                           },
                        },
                        storage: { bucketUrl: lakeDir },
                     },
                  },
               ],
            },
         ],
      });

      const ports = new PortAllocator();
      const help = await runOrThrow(["node", releasedEntry, "--help"], {
         cwd: workdir,
         timeoutMs: 60_000,
      });
      const legacyFlags = !/--root\b/.test(help.stdout + help.stderr);
      const boot = async (label: string, released: boolean) => {
         const [port, mcpPort] = await ports.allocate(2);
         const legacy = released && legacyFlags;
         const serverRoot = path.join(workdir, `root-${label}`);
         mkdirSync(serverRoot, { recursive: true });
         const server = await startServer({
            repoRoot,
            serverRoot,
            configPath,
            port,
            mcpPort,
            mode: "on",
            init: true,
            extraEnv: {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_NO_MCP_CONFIG: "1",
               ...(legacy ? { PERSIST_STORAGE_MODE: "on" } : {}),
            },
            logFile: path.join(workdir, `${label}.log`),
            ...(released
               ? { command: ["node", releasedEntry], cwd: workdir }
               : {}),
            legacyFlags: legacy,
         });
         stops.push(server.stop);
         return {
            server,
            rest: new Rest(server.baseUrl, ENV),
            ports: [port, mcpPort],
         };
      };

      // 2-3. This checkout builds, writes the manifest, and records the
      // answers its tables give; then the data changes.
      const builder = await boot("builder", false);
      const phase1 = `build (${builderVersion} from this checkout)`;
      const builderErrors = (await statusOf(builder.rest)).loadErrors ?? [];
      check(
         phase1,
         "starts with no load errors",
         same(builderErrors, []),
         JSON.stringify(builderErrors),
      );
      await builder.rest.build(PACKAGE);
      const entries = await builder.rest.latestManifestEntries(PACKAGE);
      const built = Object.values(entries) as Record<string, unknown>[];
      check(
         phase1,
         "writes a manifest with the new fields on every entry",
         built.length > 0 &&
            built.every(
               (e) =>
                  !e.error &&
                  typeof e.builtAt === "string" &&
                  (e.contract as { v?: unknown } | undefined)?.v === 1,
            ),
         JSON.stringify(built.map((e) => Object.keys(e))),
      );
      check(
         phase1,
         "builds the view over the pair table from that table",
         built.some(
            (e) =>
               e.sourceName === "pair_views__by_region__view" &&
               e.upstreamReuse === "reused",
         ),
         JSON.stringify(
            built.map((e) => [e.sourceName, e.upstreamReuse ?? null]),
         ),
      );
      writeFileSync(manifestPath, `${JSON.stringify({ entries }, null, 2)}\n`);
      for (const p of PROBES) {
         probes[p.id].before = await queryProbe(builder.rest, p.query);
      }
      await builder.server.stop();
      ports.release(builder.ports);
      await pg.sql("mv_source", MUTATION);

      // 4-5. The released server, bound to that manifest.
      const released = await boot("released", true);
      const phase2 = `serve (${releasedVersion}, released)`;
      const releasedErrors = (await statusOf(released.rest)).loadErrors ?? [];
      check(
         phase2,
         "starts with no load errors",
         same(releasedErrors, []),
         JSON.stringify(releasedErrors),
      );
      const bound = await released.rest
         .patchPackage(PACKAGE, { manifestLocation: manifestPath })
         .then(
            () => undefined,
            (e: Error) => e.message,
         );
      check(
         phase2,
         "binds the manifest this checkout wrote",
         bound === undefined,
         bound,
      );
      for (const p of PROBES) {
         const rec: ProbeRecord = await queryProbe(
            released.rest,
            p.query,
         ).catch((e: Error) => ({
            rows: [],
            servedFrom: null,
            error: e.message,
         }));
         const live = await queryProbe(released.rest, p.live).catch(() => null);
         probes[p.id].after = rec;
         const before = probes[p.id].before!;
         check(
            phase2,
            `${p.id}: answered from its stored table, as this checkout's server answered`,
            same(rec.rows, before.rows),
            rec.error
               ? `query failed: ${rec.error}`
               : `released ${JSON.stringify(rec.rows)}, built ${JSON.stringify(before.rows)}`,
         );
         if (live && p.id !== "view-over-pair-table")
            check(
               phase2,
               `${p.id}: not the live answer (the data changed after the build)`,
               !same(rec.rows, live.rows),
               `stored and live both ${JSON.stringify(rec.rows)}`,
            );
         if (rec.servedFrom === null)
            notes.push(`${p.id}: ${releasedVersion} reported no servedFrom`);
         else
            check(
               phase2,
               `${p.id}: servedFrom names ${p.tier}`,
               rec.servedFrom === p.tier,
               `servedFrom ${rec.servedFrom}`,
            );
      }
   } catch (e) {
      harnessError = (e as Error).message;
      log.err(`harness error: ${harnessError}`);
   } finally {
      for (const stop of stops.reverse()) await stop().catch(() => {});
      if (logDir) {
         mkdirSync(logDir, { recursive: true });
         for (const f of ["builder.log", "released.log", "manifest.json"]) {
            const src = path.join(workdir, f);
            if (existsSync(src))
               await cp(src, path.join(logDir, `mixed-version-${f}`)).catch(
                  (e: Error) => log.err(`could not keep ${f}: ${e.message}`),
               );
         }
      }
      if (args.keep) log.info(`--keep: left ${workdir}`);
      else
         try {
            rmSync(workdir, { recursive: true, force: true });
         } catch (e) {
            log.err(`could not remove ${workdir}: ${(e as Error).message}`);
         }
   }

   const report: UpgradeReport = {
      version: 1,
      ok: !harnessError && checks.length > 0 && checks.every((c) => c.ok),
      // The released server serves; this checkout built.
      fromVersion: releasedVersion,
      toVersion: builderVersion,
      startedAt: started.toISOString(),
      durationMs: Date.now() - started.getTime(),
      ...(harnessError ? { harnessError } : {}),
      checks,
      probes,
      notes,
   };
   if (logDir)
      writeFileSync(
         path.join(logDir, "mixed-version-report.json"),
         `${JSON.stringify(report, null, 2)}\n`,
      );
   const failed = checks.filter((c) => !c.ok);
   console.log(
      `\nmixed version: built by ${builderVersion}, served by ${releasedVersion}: ` +
         `${report.ok ? "PASSED" : "FAILED"} (${checks.length - failed.length}/${checks.length} checks` +
         `${harnessError ? `; harness error: ${harnessError}` : ""})`,
   );
   process.exit(report.ok ? 0 : 1);
}

void main();

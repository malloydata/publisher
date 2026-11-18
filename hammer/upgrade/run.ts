#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The upgrade test: the last released Publisher builds materializations, and
// this checkout's build, started on the same server root and the same data,
// must keep serving them.
//
//   bun hammer/upgrade/run.ts                  # from npm's `latest`
//   bun hammer/upgrade/run.ts --from 0.14.2    # from a given release
//   bun hammer/upgrade/run.ts --keep           # leave the workdir and Postgres up
//
// 1. Install the released server (`npm view` resolves `latest` to a version
//    first, so the report names what ran) and start it on a fresh server root.
// 2. Build the fixture package (fixture/upgrade): every route there writes a
//    table, into the `lake` DuckLake destination or the source's own DuckDB.
// 3. Change the source data, so a stored answer and a live one differ, and
//    record each probe's answer and which tier served it. Stop the server.
// 4. Start this checkout's build on the SAME server root, config and data,
//    without re-initializing storage.
// 5. Assert: it reaches `serving` with no load errors; every probe is answered
//    from the same tier with the same rows (a live answer would show the
//    change, so equal rows mean the stored table bound); every run record and
//    its manifest still reads.
// 6. Build again on the new server, then force a rebuild, and check both
//    succeed and the forced one serves the changed data from the same tiers.
//
// What it needs: Docker, for a throwaway Postgres that is both the source
// warehouse and the DuckLake catalog (a DuckLake catalog is a Postgres, and a
// `storage=` build reads only from a warehouse it can pass a query through to,
// which DuckDB is not). No warehouse credentials, nothing from the network but
// npm.
//
// Assertions are on behaviour, never on a wire value a release may change on
// purpose. See KNOWN_DIFFERENCES below; add to it when a release renames one.
//
// With HAMMER_LOG_DIR set (CI sets it), writes upgrade-report.json and both
// servers' logs there; `bun hammer/upgrade/summary.ts` renders the report.

import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { cp } from "fs/promises";
import os from "os";
import path from "path";
import { writeConfig } from "../lib/config";
import { PortAllocator } from "../lib/ports";
import { reapStaleContainers, startPostgres } from "../lib/postgres";
import { Rest } from "../lib/rest";
import { buildServerIfNeeded, startServer } from "../lib/server";
import { log, runOrThrow } from "../lib/util";

const PACKAGE = "upgrade";
const MODEL = "upgrade.malloy";
const ENV = "upgrade";

/**
 * Wire values a release changed on purpose, mapped to today's spelling before
 * any comparison, so a rename is not read as a regression. Each entry names the
 * release that made the change.
 */
const KNOWN_DIFFERENCES = {
   /** `servedFrom` (0.15.0 renamed the colocated tier to database, #1859). */
   servedFrom: { colocated: "database" } as Record<string, string>,
};

/**
 * Differences this test deliberately does not assert on, because a release
 * may change them without breaking an upgrade:
 *
 * - table names and `sourceEntityId`s (content addresses): a release may name
 *   tables differently (0.15.0's readable names) or hash a source differently,
 *   which makes its next build write a new table rather than carry the old one
 *   forward. The test asserts the old tables still SERVE after the upgrade, and
 *   that a build succeeds; whether that build carried tables forward is
 *   reported, not asserted.
 * - a manifest's `origin` and other bookkeeping fields (0.15.0 changed
 *   manifest origins and host-owned versions);
 * - the path report's shape, beyond `servedBy.tier`, which is read when both
 *   versions report it.
 *
 * And differences in how the released server is driven, handled rather than
 * asserted:
 *
 * - flag and variable names: releases before 0.15.0 read `--server_root`,
 *   `--mcp_port`, `--init` and `PERSIST_STORAGE_MODE`, and ignore the derived
 *   names (`--root`, `--mcp-port`, `--initialize-storage`,
 *   `PUBLISHER_PERSIST_STORAGE_MODE`). Which a release reads is asked of its
 *   `--help`.
 * - `servedFrom` on the released server: some releases report no tier for a
 *   database table, so before the upgrade a stored answer is proven by its
 *   rows (the data changed after the build) and `servedFrom` is checked only
 *   when reported. After the upgrade both are asserted.
 *
 * The fixture is written for 0.15.0 and later: `#(rollup)` is 0.15.0's name
 * (earlier releases read `#(preaggregate)`), so `--from` an older release
 * fails the rollup probe before the upgrade, by design.
 */

/** The data each build reads, and the change made after the first build. */
const SEED = `
CREATE TABLE public.up_orders (
   order_id integer, order_date date, region text, category text, amount numeric
);
INSERT INTO public.up_orders VALUES
   (1, '2026-01-01', 'US', 'books', 100),
   (2, '2026-01-01', 'EU', 'books', 50),
   (3, '2026-01-02', 'US', 'tools', 200),
   (4, '2026-01-02', 'EU', 'tools', 25);
`;
const MUTATION = `INSERT INTO public.up_orders VALUES (99, '2026-01-01', 'US', 'books', 1000);`;

interface Probe {
   id: string;
   /** The route the probe reads, for the report. */
   route: string;
   /** The tier it must be answered from, in today's spelling. */
   tier: "storage" | "database";
   query: string;
   /** The same answer computed live, from the unannotated source. */
   live: string;
}

const PROBES: Probe[] = [
   {
      id: "persist-storage",
      route: "#@ persist storage=lake",
      tier: "storage",
      query: "run: daily_persist -> { select: order_date, total_amount; order_by: order_date asc }",
      live: "run: live_orders -> { group_by: order_date; aggregate: total_amount is amount.sum(); order_by: order_date asc }",
   },
   {
      id: "materialize-storage",
      route: '#(materialize) storage="lake"',
      tier: "storage",
      query: "run: by_region -> { select: region, total_amount; order_by: region asc }",
      live: "run: live_orders -> { group_by: region; aggregate: total_amount is amount.sum(); order_by: region asc }",
   },
   {
      id: "rollup-storage",
      route: '#(rollup) grain=["category"] storage="lake"',
      tier: "storage",
      query: "run: orders -> { group_by: category; aggregate: total; order_by: category asc }",
      live: "run: live_orders -> { group_by: category; aggregate: total is amount.sum(); order_by: category asc }",
   },
   {
      id: "materialize-database-duckdb",
      route: "#(materialize) on DuckDB (database tier)",
      tier: "database",
      query: "run: duck_by_category -> { select: category, total_amount; order_by: category asc }",
      live: "run: live_orders -> { group_by: category; aggregate: total_amount is amount.sum(); order_by: category asc }",
   },
   {
      id: "persist-database-duckdb",
      route: "#@ persist on DuckDB (database tier)",
      tier: "database",
      query: "run: duck_daily -> { select: order_date, total_amount; order_by: order_date asc }",
      live: "run: live_orders -> { group_by: order_date; aggregate: total_amount is amount.sum(); order_by: order_date asc }",
   },
];

export interface Check {
   phase: string;
   name: string;
   ok: boolean;
   detail?: string;
}

export interface ProbeRecord {
   rows: Record<string, unknown>[];
   servedFrom: string | null;
   pathTier?: string;
   /** Set when the query failed. */
   error?: string;
}

export interface UpgradeReport {
   version: 1;
   ok: boolean;
   fromVersion: string;
   toVersion: string;
   startedAt: string;
   durationMs: number;
   harnessError?: string;
   checks: Check[];
   probes: Record<
      string,
      {
         route: string;
         before?: ProbeRecord;
         after?: ProbeRecord;
         afterRebuild?: ProbeRecord;
      }
   >;
   /** Informational: what this test saw but does not assert (see the header). */
   notes: string[];
}

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
            "Usage: bun hammer/upgrade/run.ts [--from <version>] [--keep]",
         );
         process.exit(2);
      }
   }
   return a;
}

const normalizeTier = (t: string | null | undefined): string | null =>
   t ? (KNOWN_DIFFERENCES.servedFrom[t] ?? t) : null;

/** Rows compared as JSON, so a number and its string spelling differ. */
const same = (a: unknown, b: unknown): boolean =>
   JSON.stringify(a) === JSON.stringify(b);

async function queryProbe(rest: Rest, malloy: string): Promise<ProbeRecord> {
   const out = await rest.query(PACKAGE, MODEL, { query: malloy });
   let pathTier: string | undefined;
   try {
      const raw = await fetch(
         `${rest.baseUrl}/api/v0/environments/${ENV}/packages/${PACKAGE}/models/${MODEL}/query`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ query: malloy, compactJson: true }),
         },
      );
      const json = (await raw.json()) as {
         path?: { servedBy?: { tier?: string } };
      };
      pathTier = json.path?.servedBy?.tier;
   } catch {
      pathTier = undefined;
   }
   return {
      rows: out.rows,
      servedFrom: out.servedFrom ?? null,
      ...(pathTier ? { pathTier } : {}),
   };
}

/** A call's value, or its error's message, without a throw. */
async function attempt<T>(
   fn: () => Promise<T>,
): Promise<{ ok: true; value: T } | { ok: false; error: string }> {
   try {
      return { ok: true, value: await fn() };
   } catch (e) {
      return { ok: false, error: (e as Error).message };
   }
}

async function statusErrors(rest: Rest): Promise<unknown[]> {
   const s = (await (await fetch(`${rest.baseUrl}/api/v0/status`)).json()) as {
      loadErrors?: unknown[];
   };
   return s.loadErrors ?? [];
}

async function main(): Promise<void> {
   const args = parseArgs(process.argv.slice(2));
   const started = new Date();
   const repoRoot = path.resolve(import.meta.dir, "..", "..");
   const logDir = process.env.HAMMER_LOG_DIR;
   const workdir = mkdtempSync(path.join(os.tmpdir(), "publisher-upgrade-"));
   log.info(`workdir: ${workdir}`);

   const checks: Check[] = [];
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

   const toVersion = (
      (await Bun.file(
         path.join(repoRoot, "packages", "server", "package.json"),
      ).json()) as { version: string }
   ).version;
   let fromVersion = args.from ?? "";
   let harnessError: string | undefined;
   const stops: (() => Promise<void>)[] = [];

   try {
      // 1. The released server, installed into its own directory rather than
      // run through npx's shared cache, so the version is exact and the
      // process is the server itself (npx would sit between it and SIGTERM).
      if (!fromVersion) {
         const r = await runOrThrow([
            "npm",
            "view",
            "@malloy-publisher/server@latest",
            "version",
         ]);
         fromVersion = r.stdout.trim();
      }
      log.step(`installing @malloy-publisher/server@${fromVersion}`);
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
            `@malloy-publisher/server@${fromVersion}`,
         ],
         { cwd: releasedDir },
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

      // The data both servers share: one Postgres (the source warehouse and
      // the DuckLake catalog), the lake's data directory, the server root.
      await reapStaleContainers();
      const pg = await startPostgres({ keep: args.keep });
      stops.push(() => (args.keep ? Promise.resolve() : pg.stop()));
      await pg.resetDbs(["up_source", "up_catalog"]);
      await pg.sql("up_source", SEED);
      const dataDir = path.join(workdir, "data");
      const lakeDir = path.join(dataDir, "lake");
      const serverRoot = path.join(workdir, "server-root");
      mkdirSync(lakeDir, { recursive: true });
      mkdirSync(serverRoot, { recursive: true });
      const pkgDir = path.join(dataDir, "packages", PACKAGE);
      await cp(path.join(import.meta.dir, "fixture", PACKAGE), pkgDir, {
         recursive: true,
      });
      const pgConn = {
         host: pg.host,
         port: pg.hostPort,
         databaseName: "up_source",
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
                  {
                     name: "duck",
                     type: "duckdb",
                     duckdbConnection: {
                        attachedDatabases: [
                           {
                              name: "pgsrc",
                              type: "postgres",
                              postgresConnection: pgConn,
                           },
                        ],
                     },
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
                              databaseName: "up_catalog",
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
      // A release before 0.15.0 reads only the old flag and variable names
      // (KNOWN_DIFFERENCES); asked of its --help, not of its version number.
      const help = await runOrThrow(["node", releasedEntry, "--help"], {
         cwd: workdir,
      });
      const legacyFlags = !/--root\b/.test(help.stdout + help.stderr);
      if (legacyFlags)
         notes.push(
            `${fromVersion} reads the pre-0.15.0 flag names; started with them`,
         );
      const boot = async (label: string, released: boolean, init: boolean) => {
         const [port, mcpPort] = await ports.allocate(2);
         const legacy = released && legacyFlags;
         const server = await startServer({
            repoRoot,
            serverRoot,
            configPath,
            port,
            mcpPort,
            mode: "on",
            init,
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

      // 2-3. The released server builds, the data changes, the answers are
      // recorded.
      const before = await boot("released", true, true);
      const phase1 = `before (${fromVersion})`;
      check(
         phase1,
         "starts with no load errors",
         same(await statusErrors(before.rest), []),
         JSON.stringify(await statusErrors(before.rest)),
      );
      const firstBuild = await before.rest.build(PACKAGE);
      check(phase1, "builds the package", true);
      await pg.sql("up_source", MUTATION);
      for (const p of PROBES) {
         const rec = await queryProbe(before.rest, p.query);
         const live = await queryProbe(before.rest, p.live);
         probes[p.id].before = rec;
         // The evidence a table answered, and the precondition the comparison
         // after the upgrade rests on: the data changed after the build, so
         // only a stored table can still give the old answer, and were the two
         // equal, equal rows after the upgrade would prove nothing.
         check(
            phase1,
            `${p.id}: answered from its stored table (not live)`,
            !same(rec.rows, live.rows),
            `stored and live both ${JSON.stringify(rec.rows)}`,
         );
         // `servedFrom` is checked when the release reports it; some older
         // releases report no tier for a database table.
         if (rec.servedFrom === null)
            notes.push(`${p.id}: ${fromVersion} reported no servedFrom`);
         else
            check(
               phase1,
               `${p.id}: servedFrom names ${p.tier}`,
               normalizeTier(rec.servedFrom) === p.tier,
               `servedFrom ${rec.servedFrom}`,
            );
      }
      const runsBefore = await before.rest.listMaterializations(PACKAGE);
      check(
         phase1,
         "the build's run record lists",
         runsBefore.some((r) => r.id === firstBuild.id),
         `runs: ${JSON.stringify(runsBefore.map((r) => r.id))}`,
      );
      await before.server.stop();
      ports.release(before.ports);

      // 4-5. This checkout's build on the same root and data.
      const after = await boot("current", false, false);
      const phase2 = `after (${toVersion} from this checkout)`;
      const errs = await statusErrors(after.rest);
      check(
         phase2,
         "starts with no load errors",
         same(errs, []),
         JSON.stringify(errs),
      );
      for (const p of PROBES) {
         const rec: ProbeRecord = await queryProbe(after.rest, p.query).catch(
            (e: Error) => ({ rows: [], servedFrom: null, error: e.message }),
         );
         probes[p.id].after = rec;
         const before = probes[p.id].before!;
         check(
            phase2,
            `${p.id}: still served from ${p.tier}`,
            normalizeTier(rec.servedFrom) === p.tier,
            rec.error
               ? `query failed: ${rec.error}`
               : `servedFrom ${rec.servedFrom}`,
         );
         check(
            phase2,
            `${p.id}: same answer as before the upgrade`,
            same(rec.rows, before.rows),
            `before ${JSON.stringify(before.rows)}, after ${JSON.stringify(rec.rows)}`,
         );
         if (before.pathTier && rec.pathTier)
            check(
               phase2,
               `${p.id}: path report names the same tier`,
               normalizeTier(rec.pathTier) === normalizeTier(before.pathTier),
               `before ${before.pathTier}, after ${rec.pathTier}`,
            );
      }
      const runsAfter = await after.rest.listMaterializations(PACKAGE);
      for (const r of runsBefore) {
         const id = String(r.id);
         const listed = runsAfter.find((x) => x.id === r.id);
         check(phase2, `run ${id}: listed`, !!listed, `not in the list after`);
         const read = await attempt(() =>
            after.rest.getMaterialization(PACKAGE, id),
         );
         const rec = read.ok ? read.value : {};
         check(
            phase2,
            `run ${id}: reads, with its status`,
            read.ok && rec.status === r.status,
            read.ok
               ? `status ${String(rec.status)}, was ${String(r.status)}`
               : read.error,
         );
         const entries = (rec as { manifest?: { entries?: object } }).manifest
            ?.entries;
         const entriesBefore = (r as { manifest?: { entries?: object } })
            .manifest?.entries;
         if (r.status === "MANIFEST_FILE_READY")
            check(
               phase2,
               `run ${id}: manifest reads, with every entry`,
               !!entries &&
                  Object.keys(entries).length ===
                     Object.keys(entriesBefore ?? {}).length,
               `entries ${JSON.stringify(entries && Object.keys(entries))}`,
            );
      }

      // 6. Build on the new server: the ordinary build (skip-if-unchanged),
      // then a forced one, which must store the changed data.
      const rebuild = await attempt(() => after.rest.build(PACKAGE));
      check(
         "rebuild",
         "an ordinary build succeeds",
         rebuild.ok,
         rebuild.ok ? undefined : rebuild.error,
      );
      // Whether that build carried each old table forward or wrote a new one
      // is the content address's business (see the header): reported only.
      for (const p of PROBES) {
         const rec = await queryProbe(after.rest, p.query).catch(() => null);
         if (!rec) continue;
         notes.push(
            `${p.id}: the ordinary build ${
               same(rec.rows, probes[p.id].before!.rows)
                  ? "answers as before it (the table carried forward)"
                  : "answers differently (a new table)"
            }`,
         );
      }
      const forced = await attempt(() =>
         after.rest.build(PACKAGE, { forceRefresh: true }),
      );
      check(
         "rebuild",
         "a forced build succeeds",
         forced.ok,
         forced.ok ? undefined : forced.error,
      );
      for (const p of PROBES) {
         const rec = await queryProbe(after.rest, p.query);
         const live = await queryProbe(after.rest, p.live);
         probes[p.id].afterRebuild = rec;
         check(
            "rebuild",
            `${p.id}: served from ${p.tier}`,
            normalizeTier(rec.servedFrom) === p.tier,
            `servedFrom ${rec.servedFrom}`,
         );
         check(
            "rebuild",
            `${p.id}: answer is the changed data`,
            same(rec.rows, live.rows),
            `stored ${JSON.stringify(rec.rows)}, live ${JSON.stringify(live.rows)}`,
         );
      }
   } catch (e) {
      harnessError = (e as Error).message;
      log.err(`harness error: ${harnessError}`);
   } finally {
      for (const stop of stops.reverse()) await stop().catch(() => {});
      if (logDir) {
         mkdirSync(logDir, { recursive: true });
         for (const f of ["released.log", "current.log"]) {
            const src = path.join(workdir, f);
            if (existsSync(src))
               await cp(src, path.join(logDir, `upgrade-${f}`));
         }
      }
      if (args.keep) log.info(`--keep: left ${workdir}`);
      else rmSync(workdir, { recursive: true, force: true });
   }

   const report: UpgradeReport = {
      version: 1,
      ok: !harnessError && checks.length > 0 && checks.every((c) => c.ok),
      fromVersion,
      toVersion,
      startedAt: started.toISOString(),
      durationMs: Date.now() - started.getTime(),
      ...(harnessError ? { harnessError } : {}),
      checks,
      probes,
      notes,
   };
   if (logDir)
      writeFileSync(
         path.join(logDir, "upgrade-report.json"),
         `${JSON.stringify(report, null, 2)}\n`,
      );
   const failed = checks.filter((c) => !c.ok);
   console.log(
      `\nupgrade ${fromVersion} -> ${toVersion}: ${report.ok ? "PASSED" : "FAILED"} ` +
         `(${checks.length - failed.length}/${checks.length} checks` +
         `${harnessError ? `; harness error: ${harnessError}` : ""})`,
   );
   process.exit(report.ok ? 0 : 1);
}

void main();

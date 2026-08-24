#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Hammer harness entrypoint. One command: brings up Postgres (DuckLake catalog +
// source warehouse), builds + boots the real Publisher server (on demand, per
// mode) and runs the selected scenarios against it, prints a report, tears
// everything down.
//
//   bun hammer/run.ts                     # all scenarios
//   bun hammer/run.ts --scenarios D0      # just D0
//   bun hammer/run.ts --keep              # leave pg + workdir up for inspection
//   bun hammer/run.ts --rebuild           # force a fresh server build

import { mkdirSync, mkdtempSync, rmSync } from "fs";
import os from "os";
import path from "path";
import {
   ducklakeDest,
   duckdbConn,
   postgresSource,
   writeConfig,
   type ConnectionConfig,
   type EnvSpec,
   type PackageRef,
} from "./lib/config";
import { writePackage } from "./lib/packages";
import { runLakeSql } from "./lib/ducklake";
import { startPostgres } from "./lib/postgres";
import {
   buildServerIfNeeded,
   startServer,
   type PersistStorageMode,
   type ServerHandle,
} from "./lib/server";
import { Rest } from "./lib/rest";
import { log, setQuiet } from "./lib/util";
import {
   Assert,
   ATTENTION_TAG,
   KNOWN_RED_TAG,
   type Scenario,
   type ScenarioContext,
} from "./scenarios/framework";
import { loadScenarios } from "./scenarios/index";

interface Args {
   scenarios?: string[];
   tags?: string[];
   attentionOlderThan?: number;
   keep: boolean;
   rebuild: boolean;
   reusePg: boolean;
   quiet: boolean;
   pgPort: number;
   port: number;
   mcpPort: number;
   /**
    * Scenarios to run concurrently. Each worker owns its own publishers and a
    * config holding only its own scenarios' environments — see parseArgs for the
    * measured default.
    */
   workers: number;
}

function parseArgs(argv: string[]): Args {
   const a: Args = {
      keep: false,
      rebuild: false,
      reusePg: false,
      quiet: false,
      pgPort: 55432,
      port: 14000,
      mcpPort: 14040,
      // 6 measured fastest on a 10-core laptop: ~51s, against ~57s at 4 and flat at
      // ~51-54s for 8, 10 and 12. The run is CPU-SATURATED — not memory- or
      // IO-bound, verified by re-running the whole sweep with 61% of RAM free and
      // getting the same curve. At 6 workers it burns 323 CPU-seconds over 51s wall
      // (6.3 cores); 12 workers burns 368 for the same wall, which is pure
      // overhead. The ceiling is therefore total CPU work over usable cores, and
      // workers past saturation only add context switching.
      //
      // So scale this with CORES, not memory. For sizing: a boot costs ~1.6 CPU-s
      // — 0.97 fixed, plus a ~0.67 step that appears once a config names any
      // package (the package-load worker; connections and environment count are
      // lazy and cost nothing) — so the run's 76 boots are ~125 of those CPU
      // seconds and the scenarios' own work is the rest.
      workers: 6,
   };
   for (let i = 0; i < argv.length; i++) {
      const arg = argv[i];
      const next = (): string => argv[++i];
      if (arg === "--scenarios")
         a.scenarios = next()
            .split(",")
            .map((s) => s.trim());
      else if (arg === "--tags")
         a.tags = next()
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
      else if (arg === "--attention-older-than")
         a.attentionOlderThan = Number(next());
      else if (arg === "--keep") a.keep = true;
      else if (arg === "--rebuild") a.rebuild = true;
      else if (arg === "--reuse-pg") a.reusePg = true;
      else if (arg === "--quiet") a.quiet = true;
      else if (arg === "--pg-port") a.pgPort = Number(next());
      else if (arg === "--port") a.port = Number(next());
      else if (arg === "--mcp-port") a.mcpPort = Number(next());
      else if (arg === "--workers") a.workers = Math.max(1, Number(next()));
      else if (arg === "--help" || arg === "-h") {
         console.log(
            "Usage: bun hammer/run.ts [--scenarios a,b] [--tags security,orchestration] [--attention-older-than DAYS] [--keep] [--rebuild] [--reuse-pg] [--pg-port N] [--port N] [--mcp-port N] [--workers N] [--quiet]",
         );
         process.exit(0);
      }
   }
   return a;
}

interface ScenarioResult {
   scenario: Scenario;
   assert: Assert;
   error?: string;
   /** Set when the scenario was skipped (unmet `requires`); reason for the report. */
   skipped?: string;
   /** Wall-clock the scenario took, and how much of it was server boots. */
   ms?: number;
   bootMs?: number;
   boots?: number;
}

/** Server-boot accounting, so a slow scenario can be attributed to boots or to work. */
const bootStats = { count: 0, ms: 0, stopMs: 0, startMs: 0 };

/**
 * The publishers ONE scenario runs against: named processes sharing that
 * scenario's config, so they share its source warehouse + DuckLake tier. Each
 * name is a distinct server on its own ports and server-root; `use(name, …)`
 * boots it on first reference and restarts it when the mode changes. Most
 * scenarios touch only the "default" publisher; a multi-publisher scenario
 * addresses several (`p1`, `p2`, …) to model the real stateless-worker
 * topology — build on one, bind the manifest to the others.
 *
 * A cluster lives and dies with its scenario. That is what makes a scenario a
 * clean room: the publisher it talks to was started for it, from a config
 * naming only its own environments and packages, and is torn down with its
 * storage afterwards. Sharing one publisher across scenarios would be cheaper,
 * but every scenario would then be read against a server that had already
 * loaded sixty other scenarios' packages — which is not what the markdown says
 * is happening, and the point of the markdown is that you can trust it.
 */
class PublisherCluster {
   private pubs = new Map<
      string,
      { server: ServerHandle; rest: Rest; mode: PersistStorageMode }
   >();
   private ports = new Map<string, { port: number; mcpPort: number }>();
   private nextIdx = 0;
   /** Every server root this cluster created, so teardown can remove them. */
   private roots: string[] = [];

   constructor(
      private readonly cfg: {
         repoRoot: string;
         workdir: string;
         configPath: string;
         env: string;
         basePort: number;
         baseMcpPort: number;
         /**
          * Prefix for this cluster's server roots and log files. Scenarios run
          * concurrently and each owns its own publishers, so a shared root would
          * mean two servers wiping each other's storage on `--init`.
          */
         tag: string;
      },
   ) {}

   /**
    * Stable, distinct ports per publisher name (assigned on first sighting).
    * The per-index stride (100) must exceed the base REST↔MCP gap so the two
    * ranges never overlap: with a stride of 10 and bases 40 apart, the 5th
    * publisher's REST port lands on the 1st publisher's MCP port — and since
    * publishers stay running across the whole suite, that collision makes the new
    * server exit early ("port in use"). A wide stride keeps every REST/MCP port
    * distinct no matter how many named publishers a run accumulates.
    */
   private portsFor(name: string): { port: number; mcpPort: number } {
      let p = this.ports.get(name);
      if (!p) {
         const i = this.nextIdx++;
         p = {
            port: this.cfg.basePort + i * 100,
            mcpPort: this.cfg.baseMcpPort + i * 100,
         };
         this.ports.set(name, p);
      }
      return p;
   }

   /** Get-or-start `name` at `mode`; restart (fresh) if the mode changed. */
   async use(
      name: string,
      mode: PersistStorageMode,
      opts: { init?: boolean; extraEnv?: Record<string, string> } = {},
   ): Promise<Rest> {
      const existing = this.pubs.get(name);
      // A provided extraEnv forces a fresh boot: those flags are fixed at process
      // start, so a scenario declaring them means "(re)start with them set".
      if (
         existing &&
         existing.mode === mode &&
         opts.init === undefined &&
         opts.extraEnv === undefined
      ) {
         return existing.rest;
      }
      return this.boot(name, mode, opts.init ?? true, opts.extraEnv);
   }

   /** Force a restart of `name` (default preserves the store — no --init). */
   async reboot(
      name: string,
      mode: PersistStorageMode,
      opts: { init?: boolean; extraEnv?: Record<string, string> } = {},
   ): Promise<Rest> {
      return this.boot(name, mode, opts.init ?? false, opts.extraEnv);
   }

   private async boot(
      name: string,
      mode: PersistStorageMode,
      init: boolean,
      extraEnv?: Record<string, string>,
   ): Promise<Rest> {
      const existing = this.pubs.get(name);
      if (existing) {
         const stopStart = performance.now();
         await existing.server.stop();
         bootStats.stopMs += performance.now() - stopStart;
         this.pubs.delete(name);
      }
      const { port, mcpPort } = this.portsFor(name);
      const serverRoot = path.join(
         this.cfg.workdir,
         `server-${this.cfg.tag}${name}`,
      );
      mkdirSync(serverRoot, { recursive: true });
      if (!this.roots.includes(serverRoot)) this.roots.push(serverRoot);
      const bootStart = performance.now();
      const server = await startServer({
         repoRoot: this.cfg.repoRoot,
         serverRoot,
         configPath: this.cfg.configPath,
         port,
         mcpPort,
         mode,
         init,
         extraEnv,
         logFile: path.join(
            this.cfg.workdir,
            `server-${this.cfg.tag}${name}.log`,
         ),
      });
      bootStats.count++;
      bootStats.startMs += performance.now() - bootStart;
      const rest = new Rest(server.baseUrl, this.cfg.env);
      const st = await rest.status();
      bootStats.ms = bootStats.startMs + bootStats.stopMs;
      if (st.loadErrors)
         log.warn(
            `[${this.cfg.tag}${name}] load errors: ${JSON.stringify(st.loadErrors)}`,
         );
      this.pubs.set(name, { server, rest, mode });
      return rest;
   }

   /** The Rest of an already-running publisher (throws if not started). */
   restOf(name: string): Rest {
      const p = this.pubs.get(name);
      if (!p) throw new Error(`publisher "${name}" is not running`);
      return p.rest;
   }

   /**
    * Stop every publisher and delete its storage. `keepRoots` leaves the trees in
    * place for `--keep`; otherwise they go, because a run of sixty scenarios
    * otherwise leaves sixty `publisher_data` trees on disk.
    */
   async stop(keepRoots = false): Promise<void> {
      for (const { server } of this.pubs.values()) await server.stop();
      this.pubs.clear();
      if (!keepRoots) {
         for (const root of this.roots) {
            rmSync(root, { recursive: true, force: true });
         }
      }
      this.roots = [];
   }
}

/**
 * Per-scenario physical resources. Scenarios used to share one Postgres source
 * database, one DuckLake catalog, and one environment, so any two that picked the
 * same package name or seeded the same table silently corrupted each other — the
 * symptom being a mystery failure in an unrelated scenario. Each scenario now gets
 * its own environment, source database, and catalog, addressed through the SAME
 * connection names (`orders_pg`, `lake`) so scenario markdown is unchanged.
 *
 * One Postgres server is still shared — a warehouse legitimately pre-exists a
 * scenario — so the physical database names carry the scenario slug. The
 * environment name does too, which is what lets a scenario's own config be
 * selected out of the whole set by owner.
 */
interface ScenarioResources {
   slug: string;
   sourceDb: string;
   /** Logical env name -> physical env name. */
   envFor(logical: string): string;
   /** Connection name -> its catalog database (ducklake connections only). */
   catalogDbFor(conn: string): string;
   storageDirFor(conn: string): string;
}

/** Postgres-safe, collision-free stem for a scenario id. */
function slugOf(id: string, index: number): string {
   const base = id
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "");
   // Index-suffixed so truncation of two long ids cannot converge.
   return `${base.slice(0, 40)}_${index}`;
}

function resourcesFor(
   id: string,
   index: number,
   workdir: string,
): ScenarioResources {
   const slug = slugOf(id, index);
   return {
      slug,
      sourceDb: `src_${slug}`,
      envFor: (logical) => `${slug}__${logical}`,
      catalogDbFor: (conn) => `cat_${slug}__${conn}`,
      storageDirFor: (conn) => path.join(workdir, `store_${slug}__${conn}`),
   };
}

/** Run `tasks` with at most `limit` in flight (provisioning is IO-bound). */
async function pooled<T>(
   items: T[],
   limit: number,
   fn: (item: T) => Promise<void>,
): Promise<void> {
   let next = 0;
   const worker = async (): Promise<void> => {
      while (next < items.length) {
         const i = next++;
         await fn(items[i]);
      }
   };
   await Promise.all(
      Array.from({ length: Math.min(limit, items.length) }, () => worker()),
   );
}

async function main(): Promise<void> {
   const args = parseArgs(process.argv.slice(2));
   setQuiet(args.quiet);

   const repoRoot = path.resolve(import.meta.dir, "..");
   const workdir = mkdtempSync(path.join(os.tmpdir(), "publisher-hammer-"));
   const PRIMARY = "default";
   const packagesDir = path.join(workdir, "packages");
   log.info(`workdir: ${workdir}`);

   let scenarios = await loadScenarios(args.scenarios, args.tags);
   if (scenarios.length === 0) {
      log.err("no scenarios selected");
      process.exit(2);
   }
   // Execution ORDER is a performance lever, not a semantic one: per-scenario
   // isolation means scenarios cannot influence each other, so they are free to
   // reorder. PERSIST_STORAGE_MODE is fixed at publisher start, so every switch
   // costs a full server boot (~2.3s), and boots are the single largest line item
   // in a run. Interleaved by directory number, ~6 non-`on` scenarios among 53
   // `on` ones cost TWELVE boots — each excursion pays to leave and to come back,
   // once per worker. Grouped, that collapses to one transition per mode.
   //
   // Scenarios that switch mode internally sort last within their group: they will
   // boot regardless, so they should not fragment a run of single-mode ones.
   const MODE_RANK: Record<string, number> = { on: 0, "write-only": 1, off: 2 };
   const orderKey = (s: Scenario): [number, number, string] => [
      MODE_RANK[s.modes?.[0] ?? "on"] ?? 0,
      new Set(s.modes ?? []).size > 1 ? 1 : 0,
      s.id,
   ];
   const declaredOrder = new Map(scenarios.map((s, i) => [s.id, i]));
   scenarios = [...scenarios].sort((a, b) => {
      const [am, ax, aid] = orderKey(a);
      const [bm, bx, bid] = orderKey(b);
      return am - bm || ax - bx || aid.localeCompare(bid);
   });
   log.info(`scenarios: ${scenarios.map((s) => s.id).join(", ")}`);

   // Capabilities this run provides. A scenario whose `requires` names a token
   // outside this set is SKIPPED (e.g. a `dialect:snowflake` scenario with no
   // Snowflake connection wired). The harness wires exactly these two
   // connections (see the `connections` array below), so these are the defaults
   // every scenario can assume; add to this set when the harness wires more.
   const available = new Set<string>([
      "connection:orders_pg",
      "dialect:postgres",
      "connection:lake",
      "dialect:duckdb",
      "dialect:ducklake",
   ]);

   await buildServerIfNeeded(repoRoot, args.rebuild);

   const pg = await startPostgres({
      hostPort: args.pgPort,
      reuse: args.reusePg,
   });
   const results: ScenarioResult[] = [];
   let hardError: Error | null = null;
   // One set of physical resources per scenario, addressed through the same
   // connection names its markdown already uses.
   const resources = new Map<string, ScenarioResources>();
   scenarios.forEach((s, i) =>
      resources.set(s.id, resourcesFor(s.id, i, workdir)),
   );

   try {
      // Fresh databases each run (matters when reusing the pg container): a stale
      // DuckLake catalog would carry old data-path bindings and table metadata.
      // Every database in ONE psql session — a `docker exec psql` per database was
      // ~100ms of spawn overhead each, several times the statement's own cost.
      const lakesOf = (s: Scenario): string[] => [
         "lake",
         ...(s.connections ?? [])
            .filter((c) => c.kind === "ducklake")
            .map((c) => c.name),
      ];
      const dbs: string[] = [];
      for (const s of scenarios) {
         const r = resources.get(s.id)!;
         dbs.push(r.sourceDb);
         for (const name of lakesOf(s)) {
            dbs.push(r.catalogDbFor(name));
            mkdirSync(r.storageDirFor(name), { recursive: true });
         }
      }
      await pg.resetDbs(dbs);
      // Seeding is per-database, so it still fans out.
      await pooled(scenarios, 8, async (s) => {
         const r = resources.get(s.id)!;
         for (const st of s.sourceTables ?? []) {
            await pg.sql(st.db ?? r.sourceDb, st.sql);
         }
      });
      log.ok(
         `provisioned ${scenarios.length} isolated scenario environment(s)`,
      );

      // A package belongs to an environment (default = the primary `env`). Write
      // each (env, package) to packagesDir/<env>/<pkg> and register it under its
      // environment — a package name may recur across environments with a
      // different model. The config gets one entry per distinct environment.
      const envs = new Set<string>();
      const pkgsByEnv = new Map<string, PackageRef[]>();
      const seen = new Set<string>();
      for (const s of scenarios) {
         const r = resources.get(s.id)!;
         // Every scenario has its own primary environment even if it declares no
         // package there, so its connections always have a home.
         envs.add(r.envFor(PRIMARY));
         for (const p of s.packages) {
            const pEnv = r.envFor(p.env ?? PRIMARY);
            envs.add(pEnv);
            const key = `${pEnv} ${p.name}`;
            if (seen.has(key)) continue;
            seen.add(key);
            const dir = await writePackage(path.join(packagesDir, pEnv), p);
            if (!pkgsByEnv.has(pEnv)) pkgsByEnv.set(pEnv, []);
            pkgsByEnv.get(pEnv)!.push({ name: p.name, location: dir });
         }
      }

      // Scenario-declared connections (`## Connection <name> (type=…)`), collected
      // per environment. A declared `postgres` reuses the run's source warehouse;
      // a declared `ducklake` gets its OWN catalog db + storage dir so it is a
      // genuinely separate destination — and lands in the DESTINATION list, not
      // the connection list, because that is the only list `storage=` resolves
      // through. Deduped by (env, name); each declared ducklake catalog is reset
      // like the default one. Names become available capability tokens so a
      // `requires: connection:<name>` scenario doesn't skip.
      const declaredByEnv = new Map<string, ConnectionConfig[]>();
      const declaredDestinationsByEnv = new Map<string, ConnectionConfig[]>();
      const declaredSeen = new Set<string>();
      for (const s of scenarios) {
         const r = resources.get(s.id)!;
         for (const c of s.connections ?? []) {
            const cEnv = r.envFor(c.env ?? PRIMARY);
            envs.add(cEnv);
            const key = `${cEnv} ${c.name}`;
            if (declaredSeen.has(key)) continue;
            declaredSeen.add(key);
            if (c.kind === "ducklake") {
               // Catalog + storage already provisioned above.
               const dest = ducklakeDest(
                  c.name,
                  pg,
                  r.catalogDbFor(c.name),
                  r.storageDirFor(c.name),
               );
               if (!declaredDestinationsByEnv.has(cEnv))
                  declaredDestinationsByEnv.set(cEnv, []);
               declaredDestinationsByEnv.get(cEnv)!.push(dest);
            } else {
               const conn =
                  c.kind === "duckdb"
                     ? duckdbConn(c.name, pg, r.sourceDb)
                     : postgresSource(c.name, pg, r.sourceDb);
               if (!declaredByEnv.has(cEnv)) declaredByEnv.set(cEnv, []);
               declaredByEnv.get(cEnv)!.push(conn);
            }
            available.add(`connection:${c.name}`);
         }
      }

      // Every environment gets the SAME base connection set — the same source
      // warehouse and, deliberately, the SAME DuckLake catalog + storage (so a
      // cross-environment same-name collision is observable against a SHARED
      // destination) — plus any connections that env's scenarios declared.
      // A physical env name is `<scenarioSlug>__<logicalEnv>`, so the owning
      // scenario's resources are recoverable from it. Both of a two-environment
      // scenario's envs therefore share ONE catalog — which is what keeps
      // cross-environment-same-name's premise (a SHARED destination) intact.
      const ownerOf = new Map<string, ScenarioResources>();
      for (const s of scenarios) {
         const r = resources.get(s.id)!;
         for (const e of envs) {
            if (e.startsWith(`${r.slug}__`)) ownerOf.set(e, r);
         }
      }
      const destinationsFor = (envName: string): ConnectionConfig[] => {
         const r = ownerOf.get(envName);
         if (!r) throw new Error(`no scenario owns environment "${envName}"`);
         return [
            ducklakeDest(
               "lake",
               pg,
               r.catalogDbFor("lake"),
               r.storageDirFor("lake"),
            ),
            ...(declaredDestinationsByEnv.get(envName) ?? []),
         ];
      };

      /**
       * A destination is not reachable by name from a model or the connection
       * endpoints, so a scenario that needs to LOOK at a lake — count its tables,
       * check a name was not re-quoted — gets a connection of its own pointing at
       * the same catalog and storage, named `<dest>_probe`. It is an ordinary user
       * connection: proof, incidentally, that the two lists address the same
       * warehouse independently.
       */
      const probeFor = (dest: ConnectionConfig): ConnectionConfig => ({
         ...dest,
         name: `${dest.name}_probe`,
      });

      const connectionsFor = (envName: string): ConnectionConfig[] => {
         const r = ownerOf.get(envName);
         if (!r) throw new Error(`no scenario owns environment "${envName}"`);
         return [
            postgresSource("orders_pg", pg, r.sourceDb),
            ...destinationsFor(envName).map(probeFor),
            ...(declaredByEnv.get(envName) ?? []),
         ];
      };

      const environments: EnvSpec[] = [...envs].map((name) => ({
         name,
         connections: connectionsFor(name),
         storageDestinations: destinationsFor(name),
         packages: pkgsByEnv.get(name) ?? [],
      }));

      // ONE CONFIG PER SCENARIO, naming only that scenario's own environments,
      // connections and packages. Nothing another scenario declared is present, so
      // the publisher a scenario talks to has never seen another scenario's
      // packages — the markdown describes the whole world the server knows about.
      //
      // Concurrency does not have to be paid for with fidelity here: N publishers
      // booting at once with a one-environment config each measured 1.36s for two,
      // 1.62s for four and 2.19s for six, against 1.36s for one. Boots overlap
      // almost freely; what degrades past ~6 is host memory (each publisher is
      // ~450MB), not any lock.
      const scenarioConfigPath = new Map<string, string>();
      await Promise.all(
         scenarios.map(async (s) => {
            const r = resources.get(s.id)!;
            const mine = environments.filter(
               (e) => ownerOf.get(e.name)!.slug === r.slug,
            );
            const configPath = path.join(workdir, `config-${r.slug}.json`);
            await writeConfig({ configPath, environments: mine });
            scenarioConfigPath.set(s.id, configPath);
         }),
      );

      // Bound per scenario below: the caller names a LOGICAL env, the file lands in
      // that scenario's physical env directory.
      const editPackageModelFor =
         (r: ScenarioResources) =>
         async (
            pkg: string,
            modelPath: string,
            text: string,
            pkgEnv: string = PRIMARY,
         ): Promise<void> => {
            await Bun.write(
               path.join(packagesDir, r.envFor(pkgEnv), pkg, modelPath),
               text,
            );
         };

      // Act as the orchestrator's manifest store: write a build manifest to a
      // local file and hand back a file:// URI the publisher can fetch. The
      // caller then PATCHes the package's `manifestLocation` to that URI, so the
      // publisher binds the orchestrator-authoritative manifest (the production serve path)
      // rather than its own local-store rebind.
      const manifestsDir = path.join(workdir, "manifests");
      mkdirSync(manifestsDir, { recursive: true });
      let manifestSeq = 0;
      const writeManifest = async (
         name: string,
         entries: Record<string, unknown>,
      ): Promise<string> => {
         const file = path.join(manifestsDir, `${name}-${manifestSeq++}.json`);
         await Bun.write(file, JSON.stringify({ entries }));
         return `file://${file}`;
      };

      // Statements psql must be asked for as a RESULT rather than as a command.
      // `EXPLAIN` and `VALUES` return rows without selecting; `RETURNING` turns a
      // write into one. Anchored per statement, so a leading comment or whitespace
      // does not hide the keyword.
      const RETURNS_ROWS =
         /^\s*(select|with|show|table|explain|values)\b|\breturning\b/i;

      // Operator DDL: provisioning a scenario does out-of-band, as an operator
      // would — never through the publisher, whose own paths are deliberately
      // narrower (the serve attach is read-only, and the connection SQL endpoint
      // cannot run a statement that returns no rows).
      //
      // Two backends, because a persist target can live in either: a DuckLake
      // destination is reached by a read-write attach, and a source warehouse is
      // reached by psql against the scenario's own database. A colocated persist
      // whose `name=` carries a schema needs that schema to exist first, and only
      // this path can create it.
      const operatorSqlFor =
         (r: ScenarioResources, lakes: Set<string>, warehouses: Set<string>) =>
         async (
            conn: string,
            sql: string,
         ): Promise<Record<string, string>[]> => {
            if (!lakes.has(conn)) {
               // A name this scenario never declared is a typo, and running it
               // anyway is worse than failing: the statement lands in the source
               // warehouse, the scenario goes green, and the step guards nothing.
               if (!warehouses.has(conn))
                  throw new Error(
                     `operatorSql: connection "${conn}" is not one this scenario declared ` +
                        `(warehouses: ${[...warehouses].join(", ")}; ` +
                        `ducklake destinations: ${[...lakes].join(", ")})`,
                  );
               // A source warehouse. Its named connections all resolve to the one
               // scenario database, so the name is a label here rather than a
               // lookup key.
               //
               // `query` for a statement that returns rows, `sql` otherwise: psql
               // errors on a no-result statement asked to produce tuples, and DDL
               // is the common case here.
               return RETURNS_ROWS.test(sql)
                  ? await pg.query(r.sourceDb, sql)
                  : (await pg.sql(r.sourceDb, sql), []);
            }
            await runLakeSql(
               {
                  host: pg.host,
                  port: pg.hostPort,
                  user: pg.user,
                  password: pg.password,
                  catalogDb: r.catalogDbFor(conn),
                  storageDir: r.storageDirFor(conn),
               },
               sql,
            );
            // The lake arm provisions; asserting on a destination's contents is
            // what `## Connection <lake>_probe` is for.
            return [];
         };

      // A worker's port block. Publisher NAMES stride by 100 within a cluster, so
      // workers stride by 1000 to keep every REST/MCP port distinct. Clusters are
      // per scenario but a worker only ever runs one at a time, so its block is
      // free to be reused by its next scenario.
      const clusterFor = (s: Scenario, w: number): PublisherCluster =>
         new PublisherCluster({
            repoRoot,
            workdir,
            configPath: scenarioConfigPath.get(s.id)!,
            env: PRIMARY,
            basePort: args.port + w * 1000,
            baseMcpPort: args.mcpPort + w * 1000,
            tag: `${resources.get(s.id)!.slug}-`,
         });
      if (args.workers > 1) {
         log.info(`running ${args.workers} scenarios concurrently`);
      }

      const runOne = async (
         s: Scenario,
         mgr: PublisherCluster,
      ): Promise<void> => {
         const missing = s.requires.filter((r) => !available.has(r));
         if (missing.length) {
            const reason = `requires ${missing.join(", ")}`;
            log.info(`[${s.id}] SKIPPED — ${reason}`);
            results.push({
               scenario: s,
               assert: new Assert(s.id),
               skipped: reason,
            });
            return;
         }
         log.step(`[${s.id}] ${s.title}`);
         const assert = new Assert(s.id);
         let error: string | undefined;
         const started = performance.now();
         const bootsBefore = { ...bootStats };
         const r = resources.get(s.id)!;
         const lakes = new Set<string>([
            "lake",
            ...(s.connections ?? [])
               .filter((c) => c.kind === "ducklake")
               .map((c) => c.name),
         ]);
         // `orders_pg` is wired into every scenario's config; anything else has to
         // have been declared by this scenario to be addressable.
         const warehouses = new Set<string>([
            "orders_pg",
            ...(s.connections ?? [])
               .filter((c) => c.kind !== "ducklake")
               .map((c) => c.name),
         ]);
         // A scenario may address a non-primary environment of its own, so every
         // Rest is bound to the environment the step meant.
         const bind = (rest: Rest): Rest =>
            rest.env === r.envFor(PRIMARY)
               ? rest
               : new Rest(rest.baseUrl, r.envFor(PRIMARY));
         const ctx: ScenarioContext = {
            pg,
            env: r.envFor(PRIMARY),
            envFor: (logical) => r.envFor(logical),
            sourceDb: r.sourceDb,
            catalogDbFor: (conn) => r.catalogDbFor(conn),
            usePublisher: async (name, mode, opts) =>
               bind(await mgr.use(name, mode, opts)),
            // Backward-compat shorthand for the single "default" publisher (hooks).
            use: async (mode, opts) =>
               bind(await mgr.use("default", mode, opts)),
            reboot: async (opts) =>
               bind(
                  await mgr.reboot(
                     opts?.name ?? "default",
                     opts?.mode ?? "on",
                     {
                        init: opts?.init,
                     },
                  ),
               ),
            restOf: (name) => bind(mgr.restOf(name)),
            editPackageModel: editPackageModelFor(r),
            operatorSql: operatorSqlFor(r, lakes, warehouses),
            writeManifest,
         };
         try {
            await s.run(ctx, assert);
         } catch (e) {
            error = (e as Error).stack ?? (e as Error).message;
            assert.fail("scenario threw", (e as Error).message);
         }
         const result: ScenarioResult = {
            scenario: s,
            assert,
            error,
            ms: performance.now() - started,
            // Boots are global, so a concurrent worker's boot can be misattributed;
            // the aggregate at the bottom of the report is the reliable figure.
            bootMs: bootStats.ms - bootsBefore.ms,
            boots: bootStats.count - bootsBefore.count,
         };
         results.push(result);
         printScenario(result);
      };

      // Workers pull from one queue, so a slow scenario does not idle the others.
      // Each takes a scenario, starts a publisher FOR it, runs it, and tears the
      // publisher and its storage down before taking the next one.
      let nextIdx = 0;
      await Promise.all(
         Array.from({ length: args.workers }, async (_unused, w) => {
            for (;;) {
               const i = nextIdx++;
               if (i >= scenarios.length) break;
               const s = scenarios[i];
               const mgr = clusterFor(s, w);
               try {
                  await runOne(s, mgr);
               } finally {
                  await mgr.stop(args.keep);
               }
            }
         }),
      );
   } catch (e) {
      hardError = e as Error;
      log.err(`harness error: ${hardError.message}`);
      if (hardError.stack) log.info(hardError.stack);
   } finally {
      if (!args.keep) {
         await pg.stop();
         rmSync(workdir, { recursive: true, force: true });
      } else {
         log.info(
            `--keep: left workdir ${workdir} and container ${pg.containerName} up`,
         );
      }
   }

   // Report in declared (directory) order: execution order is now mode-grouped and,
   // with workers, completion order is nondeterministic — neither should show up as
   // a shuffled summary.
   results.sort(
      (a, b) =>
         (declaredOrder.get(a.scenario.id) ?? 0) -
         (declaredOrder.get(b.scenario.id) ?? 0),
   );
   const failed = summarize(results, args.attentionOlderThan) || !!hardError;
   process.exit(failed ? 1 : 0);
}

function printScenario(r: ScenarioResult): void {
   const bad = r.assert.checks.filter((c) => !c.ok);
   if (bad.length === 0) {
      log.ok(`[${r.scenario.id}] ${r.assert.checks.length} checks passed`);
   } else {
      log.err(
         `[${r.scenario.id}] ${bad.length}/${r.assert.checks.length} checks FAILED`,
      );
      for (const c of bad)
         log.err(`    ✗ ${c.name}${c.detail ? ` — ${c.detail}` : ""}`);
      if (r.error) log.info(`    (${r.error.split("\n")[0]})`);
   }
}

/** Whole days between a YYYY-MM-DD date and today (null if unparseable). */
function ageInDays(since: string | undefined): number | null {
   if (!since) return null;
   const then = Date.parse(since);
   if (Number.isNaN(then)) return null;
   return Math.floor((Date.now() - then) / 86_400_000);
}

/** Prints the final table + a needs-attention block; returns true if anything failed. */
function summarize(
   results: ScenarioResult[],
   attentionOlderThan?: number,
): boolean {
   console.log("\n──────────── hammer summary ────────────");
   let anyFail = false;
   for (const r of results) {
      if (r.skipped) {
         console.log(
            `  \x1b[33mSKIP\x1b[0m  ${r.scenario.id.padEnd(28)}  ${r.skipped}`,
         );
         continue;
      }
      const bad = r.assert.checks.filter((c) => !c.ok).length;
      const total = r.assert.checks.length;
      const knownRed = r.scenario.tags.includes(KNOWN_RED_TAG);
      // A known-red is expected to fail, so failing is not a run failure. Passing
      // IS: the rule now holds and the tag has become a lie — which is exactly how
      // a fix lands without anyone remembering to retire the scenario's tag.
      const mark =
         bad === 0
            ? knownRed
               ? "\x1b[31mFIXED\x1b[0m"
               : "\x1b[32mPASS\x1b[0m"
            : knownRed
              ? "\x1b[33mKRED\x1b[0m"
              : "\x1b[31mFAIL\x1b[0m";
      const tags = r.scenario.tags.join(",");
      const timing =
         r.ms === undefined
            ? ""
            : ` \x1b[2m${(r.ms / 1000).toFixed(1)}s${r.boots ? `/${r.boots}b` : ""}\x1b[0m`;
      console.log(
         `  ${mark}  ${r.scenario.id.padEnd(28)} ${String(total - bad).padStart(2)}/${total}${timing}  ${r.scenario.title}${tags ? `  \x1b[2m[${tags}]\x1b[0m` : ""}`,
      );
      if (knownRed ? bad === 0 : bad > 0) anyFail = true;
   }
   console.log("─────────────────────────────────────────");

   // Where the wall clock went. Scenario time SUMS across workers, so with N
   // workers it exceeds the elapsed run; the ratio of boot to work is the point.
   const timed = results.filter((r) => r.ms !== undefined);
   if (timed.length) {
      const total = timed.reduce((n, r) => n + (r.ms ?? 0), 0);
      console.log(
         `  \x1b[2mscenario time ${(total / 1000).toFixed(1)}s summed across workers; ` +
            `${bootStats.count} server boots costing ${(bootStats.ms / 1000).toFixed(1)}s ` +
            `(${Math.round((bootStats.ms / total) * 100)}%) — ` +
            `${(bootStats.startMs / 1000).toFixed(1)}s starting, ` +
            `${(bootStats.stopMs / 1000).toFixed(1)}s stopping the old one\x1b[0m`,
      );
      const slow = [...timed]
         .sort((a, b) => (b.ms ?? 0) - (a.ms ?? 0))
         .slice(0, 5)
         .map((r) => `${r.scenario.id} ${((r.ms ?? 0) / 1000).toFixed(1)}s`);
      console.log(`  \x1b[2mslowest: ${slow.join(", ")}\x1b[0m`);
   }

   // Known-reds get their own block, ages included. A rule the Publisher does not
   // meet yet is a debt, and debt that nobody re-reads stops being a decision and
   // becomes furniture. `--attention-older-than N` filters this the same way it
   // filters the callouts below, so "what have we been red on for a month?" is one
   // flag away. Age comes from the scenario's `## Note (since=…)`; an undated one
   // always shows, because it cannot be proven fresh.
   let reds = results.filter(
      (r) => !r.skipped && r.scenario.tags.includes(KNOWN_RED_TAG),
   );
   if (attentionOlderThan !== undefined) {
      reds = reds.filter((r) => {
         const age = ageInDays(r.scenario.note?.since);
         return age === null || age >= attentionOlderThan;
      });
   }
   if (reds.length) {
      const header =
         attentionOlderThan !== undefined
            ? `● known-red (declared ≥ ${attentionOlderThan}d ago)`
            : "● known-red — rules not met yet";
      console.log(`\n\x1b[33m${header}\x1b[0m`);
      for (const r of reds) {
         const age = ageInDays(r.scenario.note?.since);
         const since = r.scenario.note?.since;
         const when = since
            ? ` \x1b[2m(declared ${since}${age !== null ? `, ${age}d ago` : ""})\x1b[0m`
            : " \x1b[2m(undated)\x1b[0m";
         const passed = r.assert.checks.every((c) => c.ok);
         console.log(
            `  • ${r.scenario.id}${when}${passed ? "  \x1b[31m← PASSES NOW; retire the tag\x1b[0m" : ""}`,
         );
         console.log(`      ${r.scenario.title}`);
      }
      console.log("─────────────────────────────────────────");
   }

   // Self-documenting follow-ups: any scenario tagged `needs-attention` or
   // carrying an author `## Note` is surfaced here so open questions don't get
   // lost in a sea of green. These do NOT affect the exit code. With
   // `--attention-older-than N`, only callouts raised at least N days ago are
   // shown (undated ones always show — they can't be proven fresh), so stale
   // follow-ups can be triaged on their own.
   let attn = results.filter(
      (r) => r.scenario.tags.includes(ATTENTION_TAG) || !!r.scenario.note,
   );
   if (attentionOlderThan !== undefined) {
      attn = attn.filter((r) => {
         const age = ageInDays(r.scenario.note?.since);
         return age === null || age >= attentionOlderThan;
      });
   }
   if (attn.length) {
      const header =
         attentionOlderThan !== undefined
            ? `⚠ needs attention (raised ≥ ${attentionOlderThan}d ago)`
            : "⚠ needs attention";
      console.log(`\n\x1b[33m${header}\x1b[0m`);
      for (const r of attn) {
         const age = ageInDays(r.scenario.note?.since);
         const since = r.scenario.note?.since;
         const when = since
            ? ` \x1b[2m(raised ${since}${age !== null ? `, ${age}d ago` : ""})\x1b[0m`
            : "";
         console.log(`  • ${r.scenario.id}${when}`);
         if (r.scenario.note) {
            for (const line of r.scenario.note.text.split("\n")) {
               if (line.trim()) console.log(`      ${line}`);
            }
         }
      }
      console.log("─────────────────────────────────────────");
   }
   return anyFail;
}

void main();

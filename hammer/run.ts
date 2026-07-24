#!/usr/bin/env bun
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
import { Assert, ATTENTION_TAG, type Scenario, type ScenarioContext } from "./scenarios/framework";
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
   };
   for (let i = 0; i < argv.length; i++) {
      const arg = argv[i];
      const next = (): string => argv[++i];
      if (arg === "--scenarios") a.scenarios = next().split(",").map((s) => s.trim());
      else if (arg === "--tags") a.tags = next().split(",").map((s) => s.trim()).filter(Boolean);
      else if (arg === "--attention-older-than") a.attentionOlderThan = Number(next());
      else if (arg === "--keep") a.keep = true;
      else if (arg === "--rebuild") a.rebuild = true;
      else if (arg === "--reuse-pg") a.reusePg = true;
      else if (arg === "--quiet") a.quiet = true;
      else if (arg === "--pg-port") a.pgPort = Number(next());
      else if (arg === "--port") a.port = Number(next());
      else if (arg === "--mcp-port") a.mcpPort = Number(next());
      else if (arg === "--help" || arg === "-h") {
         console.log(
            "Usage: bun hammer/run.ts [--scenarios a,b] [--tags security,orchestration] [--attention-older-than DAYS] [--keep] [--rebuild] [--reuse-pg] [--pg-port N] [--port N] [--mcp-port N] [--quiet]",
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
}

/**
 * A cluster of named publisher processes sharing one config (so they share the
 * source warehouse + DuckLake tier). Each name is a distinct server on its own
 * ports and server-root; `use(name, …)` boots it on first reference and restarts
 * it when the mode changes. Most scenarios touch only the "default" publisher; a
 * multi-publisher scenario addresses several (`p1`, `p2`, …) to model the real
 * stateless-worker topology — build on one, bind the manifest to the others.
 */
class PublisherCluster {
   private pubs = new Map<
      string,
      { server: ServerHandle; rest: Rest; mode: PersistStorageMode }
   >();
   private ports = new Map<string, { port: number; mcpPort: number }>();
   private nextIdx = 0;

   constructor(
      private readonly cfg: {
         repoRoot: string;
         workdir: string;
         configPath: string;
         env: string;
         basePort: number;
         baseMcpPort: number;
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
         await existing.server.stop();
         this.pubs.delete(name);
      }
      const { port, mcpPort } = this.portsFor(name);
      const serverRoot = path.join(this.cfg.workdir, `server-${name}`);
      mkdirSync(serverRoot, { recursive: true });
      const server = await startServer({
         repoRoot: this.cfg.repoRoot,
         serverRoot,
         configPath: this.cfg.configPath,
         port,
         mcpPort,
         mode,
         init,
         extraEnv,
         logFile: path.join(this.cfg.workdir, `server-${name}.log`),
      });
      const rest = new Rest(server.baseUrl, this.cfg.env);
      const st = await rest.status();
      if (st.loadErrors)
         log.warn(`[${name}] load errors: ${JSON.stringify(st.loadErrors)}`);
      this.pubs.set(name, { server, rest, mode });
      return rest;
   }

   /** The Rest of an already-running publisher (throws if not started). */
   restOf(name: string): Rest {
      const p = this.pubs.get(name);
      if (!p) throw new Error(`publisher "${name}" is not running`);
      return p.rest;
   }

   async stop(): Promise<void> {
      for (const { server } of this.pubs.values()) await server.stop();
      this.pubs.clear();
   }
}

async function main(): Promise<void> {
   const args = parseArgs(process.argv.slice(2));
   setQuiet(args.quiet);

   const repoRoot = path.resolve(import.meta.dir, "..");
   const workdir = mkdtempSync(path.join(os.tmpdir(), "publisher-hammer-"));
   const env = "default";
   const sourceDb = "hammer_src";
   const catalogDb = "ducklake_catalog";
   const packagesDir = path.join(workdir, "packages");
   log.info(`workdir: ${workdir}`);

   const scenarios = await loadScenarios(args.scenarios, args.tags);
   if (scenarios.length === 0) {
      log.err("no scenarios selected");
      process.exit(2);
   }
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

   const pg = await startPostgres({ hostPort: args.pgPort, reuse: args.reusePg });
   const results: ScenarioResult[] = [];
   let hardError: Error | null = null;
   const mgr = new PublisherCluster({
      repoRoot,
      workdir,
      configPath: path.join(workdir, "publisher.config.json"),
      env,
      basePort: args.port,
      baseMcpPort: args.mcpPort,
   });

   try {
      // Fresh databases each run (matters when reusing the pg container): a stale
      // DuckLake catalog would carry old data-path bindings and table metadata.
      await pg.resetDb(sourceDb);
      await pg.resetDb(catalogDb);

      for (const s of scenarios) {
         for (const t of s.sourceTables ?? []) await pg.sql(t.db ?? sourceDb, t.sql);
      }
      log.ok("source tables seeded");

      const storageDir = path.join(workdir, "lake-storage");
      mkdirSync(storageDir, { recursive: true });

      // A package belongs to an environment (default = the primary `env`). Write
      // each (env, package) to packagesDir/<env>/<pkg> and register it under its
      // environment — a package name may recur across environments with a
      // different model. The config gets one entry per distinct environment.
      const envs = new Set<string>([env]);
      const pkgsByEnv = new Map<string, PackageRef[]>();
      const seen = new Set<string>();
      for (const s of scenarios) {
         for (const p of s.packages) {
            const pEnv = p.env ?? env;
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
      // genuinely separate destination. Deduped by (env, name); each declared
      // ducklake catalog is reset like the default one. Names become available
      // capability tokens so a `requires: connection:<name>` scenario doesn't skip.
      const declaredByEnv = new Map<string, ConnectionConfig[]>();
      const declaredSeen = new Set<string>();
      for (const s of scenarios) {
         for (const c of s.connections ?? []) {
            const cEnv = c.env ?? env;
            envs.add(cEnv);
            const key = `${cEnv} ${c.name}`;
            if (declaredSeen.has(key)) continue;
            declaredSeen.add(key);
            let conn: ConnectionConfig;
            if (c.kind === "ducklake") {
               const catDb = `ducklake_catalog_${c.name}`;
               const store = path.join(workdir, `lake-storage-${c.name}`);
               mkdirSync(store, { recursive: true });
               await pg.resetDb(catDb);
               conn = ducklakeDest(c.name, pg, catDb, store);
            } else if (c.kind === "duckdb") {
               conn = duckdbConn(c.name, pg, sourceDb);
            } else {
               conn = postgresSource(c.name, pg, sourceDb);
            }
            if (!declaredByEnv.has(cEnv)) declaredByEnv.set(cEnv, []);
            declaredByEnv.get(cEnv)!.push(conn);
            available.add(`connection:${c.name}`);
         }
      }

      // Every environment gets the SAME base connection set — the same source
      // warehouse and, deliberately, the SAME DuckLake catalog + storage (so a
      // cross-environment same-name collision is observable against a SHARED
      // destination) — plus any connections that env's scenarios declared.
      const connectionsFor = (envName: string): ConnectionConfig[] => [
         postgresSource("orders_pg", pg, sourceDb),
         ducklakeDest("lake", pg, catalogDb, storageDir),
         ...(declaredByEnv.get(envName) ?? []),
      ];

      const environments: EnvSpec[] = [...envs].map((name) => ({
         name,
         connections: connectionsFor(name),
         packages: pkgsByEnv.get(name) ?? [],
      }));
      await writeConfig({
         configPath: path.join(workdir, "publisher.config.json"),
         environments,
      });

      const editPackageModel = async (
         pkg: string,
         modelPath: string,
         text: string,
         pkgEnv: string = env,
      ): Promise<void> => {
         await Bun.write(path.join(packagesDir, pkgEnv, pkg, modelPath), text);
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

      // Operator DDL: the DuckLake destination `lake` is provisioned out-of-band
      // via a read-write attach (the publisher's serve attach is read-only).
      const operatorSql = async (conn: string, sql: string): Promise<void> => {
         if (conn !== "lake") {
            throw new Error(`operatorSql: no read-write path wired for connection "${conn}"`);
         }
         await runLakeSql(
            {
               host: pg.host,
               port: pg.hostPort,
               user: pg.user,
               password: pg.password,
               catalogDb,
               storageDir,
            },
            sql,
         );
      };

      for (const s of scenarios) {
         const missing = s.requires.filter((r) => !available.has(r));
         if (missing.length) {
            const reason = `requires ${missing.join(", ")}`;
            log.info(`[${s.id}] SKIPPED — ${reason}`);
            results.push({ scenario: s, assert: new Assert(s.id), skipped: reason });
            continue;
         }
         log.step(`[${s.id}] ${s.title}`);
         const assert = new Assert(s.id);
         let error: string | undefined;
         const ctx: ScenarioContext = {
            pg,
            env,
            sourceDb,
            usePublisher: (name, mode, opts) => mgr.use(name, mode, opts),
            // Backward-compat shorthand for the single "default" publisher (hooks).
            use: (mode, opts) => mgr.use("default", mode, opts),
            reboot: (opts) =>
               mgr.reboot(opts?.name ?? "default", opts?.mode ?? "on", {
                  init: opts?.init,
               }),
            restOf: (name) => mgr.restOf(name),
            editPackageModel,
            operatorSql,
            writeManifest,
         };
         try {
            await s.run(ctx, assert);
         } catch (e) {
            error = (e as Error).stack ?? (e as Error).message;
            assert.fail("scenario threw", (e as Error).message);
         }
         results.push({ scenario: s, assert, error });
         printScenario(results[results.length - 1]);
      }
   } catch (e) {
      hardError = e as Error;
      log.err(`harness error: ${hardError.message}`);
      if (hardError.stack) log.info(hardError.stack);
   } finally {
      await mgr.stop();
      if (!args.keep) {
         await pg.stop();
         rmSync(workdir, { recursive: true, force: true });
      } else {
         log.info(`--keep: left workdir ${workdir} and container ${pg.containerName} up`);
      }
   }

   const failed = summarize(results, args.attentionOlderThan) || !!hardError;
   process.exit(failed ? 1 : 0);
}

function printScenario(r: ScenarioResult): void {
   const bad = r.assert.checks.filter((c) => !c.ok);
   if (bad.length === 0) {
      log.ok(`[${r.scenario.id}] ${r.assert.checks.length} checks passed`);
   } else {
      log.err(`[${r.scenario.id}] ${bad.length}/${r.assert.checks.length} checks FAILED`);
      for (const c of bad) log.err(`    ✗ ${c.name}${c.detail ? ` — ${c.detail}` : ""}`);
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
function summarize(results: ScenarioResult[], attentionOlderThan?: number): boolean {
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
      const mark = bad === 0 ? "\x1b[32mPASS\x1b[0m" : "\x1b[31mFAIL\x1b[0m";
      const tags = r.scenario.tags.join(",");
      console.log(
         `  ${mark}  ${r.scenario.id.padEnd(28)} ${String(total - bad).padStart(2)}/${total}  ${r.scenario.title}${tags ? `  \x1b[2m[${tags}]\x1b[0m` : ""}`,
      );
      if (bad > 0) anyFail = true;
   }
   console.log("─────────────────────────────────────────");

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

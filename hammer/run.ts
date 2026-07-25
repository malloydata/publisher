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
import {
   Assert,
   ATTENTION_TAG,
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

/**
 * Per-scenario physical resources. Scenarios used to share one Postgres source
 * database, one DuckLake catalog, and one environment, so any two that picked the
 * same package name or seeded the same table silently corrupted each other — the
 * symptom being a mystery failure in an unrelated scenario. Each scenario now gets
 * its own environment, source database, and catalog, addressed through the SAME
 * connection names (`orders_pg`, `lake`) so scenario markdown is unchanged.
 *
 * The environment is what makes that possible: the generated config keys connections
 * by (environment, name), so sixty scenarios can each define their own `lake`.
 * Boot cost is indifferent to environment count — measured at ~2.4s for both 60
 * environments holding one package each and one environment holding sixty.
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
   const base = id.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
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

   const pg = await startPostgres({
      hostPort: args.pgPort,
      reuse: args.reusePg,
   });
   const results: ScenarioResult[] = [];
   let hardError: Error | null = null;
   const mgr = new PublisherCluster({
      repoRoot,
      workdir,
      configPath: path.join(workdir, "publisher.config.json"),
      env: PRIMARY,
      basePort: args.port,
      baseMcpPort: args.mcpPort,
   });

   // One set of physical resources per scenario, addressed through the same
   // connection names its markdown already uses.
   const resources = new Map<string, ScenarioResources>();
   scenarios.forEach((s, i) =>
      resources.set(s.id, resourcesFor(s.id, i, workdir)),
   );

   try {
      // Fresh databases each run (matters when reusing the pg container): a stale
      // DuckLake catalog would carry old data-path bindings and table metadata.
      // Provisioned concurrently — it is all IO against one Postgres, and serially
      // this is ~0.5s per scenario.
      await pooled(scenarios, 8, async (s) => {
         const r = resources.get(s.id)!;
         await pg.resetDb(r.sourceDb);
         // Every ducklake destination this scenario can reach: the implicit `lake`
         // plus any it declared.
         const lakes = [
            "lake",
            ...(s.connections ?? [])
               .filter((c) => c.kind === "ducklake")
               .map((c) => c.name),
         ];
         for (const name of lakes) {
            await pg.resetDb(r.catalogDbFor(name));
            mkdirSync(r.storageDirFor(name), { recursive: true });
         }
         for (const st of s.sourceTables ?? []) {
            await pg.sql(st.db ?? r.sourceDb, st.sql);
         }
      });
      log.ok(`provisioned ${scenarios.length} isolated scenario environment(s)`);

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
      // genuinely separate destination. Deduped by (env, name); each declared
      // ducklake catalog is reset like the default one. Names become available
      // capability tokens so a `requires: connection:<name>` scenario doesn't skip.
      const declaredByEnv = new Map<string, ConnectionConfig[]>();
      const declaredSeen = new Set<string>();
      for (const s of scenarios) {
         const r = resources.get(s.id)!;
         for (const c of s.connections ?? []) {
            const cEnv = r.envFor(c.env ?? PRIMARY);
            envs.add(cEnv);
            const key = `${cEnv} ${c.name}`;
            if (declaredSeen.has(key)) continue;
            declaredSeen.add(key);
            let conn: ConnectionConfig;
            if (c.kind === "ducklake") {
               // Catalog + storage already provisioned above.
               conn = ducklakeDest(
                  c.name,
                  pg,
                  r.catalogDbFor(c.name),
                  r.storageDirFor(c.name),
               );
            } else if (c.kind === "duckdb") {
               conn = duckdbConn(c.name, pg, r.sourceDb);
            } else {
               conn = postgresSource(c.name, pg, r.sourceDb);
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
      const connectionsFor = (envName: string): ConnectionConfig[] => {
         const r = ownerOf.get(envName);
         if (!r) throw new Error(`no scenario owns environment "${envName}"`);
         return [
            postgresSource("orders_pg", pg, r.sourceDb),
            ducklakeDest("lake", pg, r.catalogDbFor("lake"), r.storageDirFor("lake")),
            ...(declaredByEnv.get(envName) ?? []),
         ];
      };

      const environments: EnvSpec[] = [...envs].map((name) => ({
         name,
         connections: connectionsFor(name),
         packages: pkgsByEnv.get(name) ?? [],
      }));
      await writeConfig({
         configPath: path.join(workdir, "publisher.config.json"),
         environments,
      });

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

      // Operator DDL: the DuckLake destination `lake` is provisioned out-of-band
      // via a read-write attach (the publisher's serve attach is read-only).
      const operatorSqlFor =
         (r: ScenarioResources, lakes: Set<string>) =>
         async (conn: string, sql: string): Promise<void> => {
            if (!lakes.has(conn)) {
               throw new Error(
                  `operatorSql: no read-write path wired for connection "${conn}" ` +
                     `(this scenario's ducklake destinations: ${[...lakes].join(", ")})`,
               );
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
         };

      for (const s of scenarios) {
         const missing = s.requires.filter((r) => !available.has(r));
         if (missing.length) {
            const reason = `requires ${missing.join(", ")}`;
            log.info(`[${s.id}] SKIPPED — ${reason}`);
            results.push({
               scenario: s,
               assert: new Assert(s.id),
               skipped: reason,
            });
            continue;
         }
         log.step(`[${s.id}] ${s.title}`);
         const assert = new Assert(s.id);
         let error: string | undefined;
         const r = resources.get(s.id)!;
         const lakes = new Set<string>([
            "lake",
            ...(s.connections ?? [])
               .filter((c) => c.kind === "ducklake")
               .map((c) => c.name),
         ]);
         // Publishers are shared across scenarios, so every Rest handed to a
         // scenario is rebound to ITS environment.
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
            use: async (mode, opts) => bind(await mgr.use("default", mode, opts)),
            reboot: async (opts) =>
               bind(
                  await mgr.reboot(opts?.name ?? "default", opts?.mode ?? "on", {
                     init: opts?.init,
                  }),
               ),
            restOf: (name) => bind(mgr.restOf(name)),
            editPackageModel: editPackageModelFor(r),
            operatorSql: operatorSqlFor(r, lakes),
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
         log.info(
            `--keep: left workdir ${workdir} and container ${pg.containerName} up`,
         );
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

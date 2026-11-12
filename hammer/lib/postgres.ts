// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A throwaway Postgres container for the harness: it is BOTH the DuckLake
// catalog store AND a source warehouse (a `postgres` connection), mirroring the
// tutorial. One server, several databases.

import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import { log, run, runOrThrow, waitFor } from "./util";

/**
 * Labels a hammer run stamps on the container it creates. `hammer.run` is the
 * run's id; host, pid and the pid's start time say whether that run is still
 * alive, so a label left by a crash or a reboot never looks owned forever.
 */
const LABEL_RUN = "hammer.run";
const LABEL_HOST = "hammer.host";
const LABEL_PID = "hammer.pid";
const LABEL_PID_START = "hammer.pidstart";
/** Set on a container a `--keep` run created: it is left up on purpose. */
const LABEL_KEEP = "hammer.keep";

/**
 * Where the postgres image declares its data VOLUME. Mounting a tmpfs there
 * keeps Docker from creating an anonymous volume per container: a container
 * removed without `-v` left that volume behind, and a few hundred of them
 * filled Docker Desktop's disk.
 */
export const PG_DATA_DIR = "/var/lib/postgresql/data";

/** The name the harness used before containers were per run, still reusable. */
export const LEGACY_CONTAINER_NAME = "publisher-hammer-pg";

/** A short random id for one run. */
export function newRunId(): string {
   return randomBytes(4).toString("hex");
}

/**
 * The container name a run uses when none is given: one per run, so two runs
 * started at the same moment never pick the same name, whatever their ports.
 */
export function defaultContainerName(runId: string): string {
   return `${LEGACY_CONTAINER_NAME}-${runId}`;
}

/** Who created a container, from its labels. */
export interface ContainerOwner {
   runId?: string;
   host?: string;
   pid?: number;
   pidStart?: string;
   /** Created by a `--keep` run, which asked for it to outlive the run. */
   keep?: boolean;
}

/** What `docker inspect` said about a container that exists. */
export type OwnerReading =
   | { kind: "labelled"; owner: ContainerOwner }
   /** Made by hand or by an older harness: no run owns it. */
   | { kind: "unlabelled" }
   /** Inspect failed: ownership is unknown, so nothing may remove it. */
   | { kind: "unknown"; error: string };

/** Whether the run that created a container is still going. */
export type Liveness = "live" | "dead" | "other-host";

/** How this host checks whether a pid is still the process that labelled a container. */
export interface ProcessProbe {
   host: string;
   /** True when `pid` exists (EPERM counts: it is someone else's live process). */
   alive(pid: number): boolean;
   /** The pid's start time as the OS reports it, or undefined when unknown. */
   startTime(pid: number): Promise<string | undefined>;
}

/**
 * Whether `owner` is still running. Only a run on THIS host can be checked: one
 * on another host (a shared or remote Docker daemon) is reported as such, and
 * the caller refuses rather than guess. A pid that exists but started at a
 * different time is a reused pid, so the owner is dead.
 */
export async function ownerLiveness(
   owner: ContainerOwner,
   probe: ProcessProbe,
): Promise<Liveness> {
   if (owner.host !== undefined && owner.host !== probe.host) {
      return "other-host";
   }
   if (owner.pid === undefined || !probe.alive(owner.pid)) return "dead";
   if (owner.pidStart === undefined) return "live";
   const now = await probe.startTime(owner.pid);
   // Unknown start time (no `ps`): trust the pid rather than remove a live run's
   // database.
   if (now === undefined) return "live";
   return now === owner.pidStart ? "live" : "dead";
}

/** The state of a container a run was asked to use (by name) or found. */
export interface ContainerState {
   name: string;
   exists: boolean;
   running: boolean;
   owner?: OwnerReading;
   liveness?: Liveness;
}

export type ContainerAction =
   | { action: "create" }
   | { action: "reuse" }
   /** Remove the stale container (no live run owns it), then create. */
   | { action: "replace" }
   | { action: "refuse"; message: string };

/**
 * What to do with a named container. A container is reused only when it is
 * running and no live run owns it, and removed only when it is provably not a
 * live run's: an unreadable owner, a live owner, or an owner on another host
 * refuses, and never suggests sharing a live run's database.
 */
export function decideContainerAction(
   state: ContainerState,
   wantReuse: boolean,
): ContainerAction {
   if (!state.exists) return { action: "create" };
   const owner = state.owner ?? { kind: "unlabelled" };
   if (owner.kind === "unknown") {
      return {
         action: "refuse",
         message:
            `could not read who owns Postgres container ${state.name} ` +
            `(${owner.error}); leaving it alone. Inspect it with ` +
            `\`docker inspect ${state.name}\` or remove it yourself.`,
      };
   }
   if (owner.kind === "labelled" && state.liveness === "live") {
      const pid = owner.owner.pid;
      return {
         action: "refuse",
         message:
            `Postgres container ${state.name} belongs to a hammer run that is ` +
            `still going${pid === undefined ? "" : ` (pid ${pid})`}. Let it ` +
            `finish, or start this run without --pg-container so it gets its ` +
            `own database.`,
      };
   }
   if (owner.kind === "labelled" && state.liveness === "other-host") {
      return {
         action: "refuse",
         message:
            `Postgres container ${state.name} was created by a hammer run on ` +
            `${owner.owner.host}, which this host cannot check. If that run is ` +
            `over, remove it with \`docker rm -fv ${state.name}\`.`,
      };
   }
   if (wantReuse && state.running) return { action: "reuse" };
   return { action: "replace" };
}

/**
 * Whether the startup reaper removes a container: only one under a hammer
 * name, labelled by a hammer run, whose run on THIS host is provably over, and
 * that no `--keep` run asked to keep. Anything else (an unlabelled or
 * unreadable container, a live owner, another host's run, a kept container,
 * any other name) is left alone, so the reaper never touches a container
 * hammer did not create, nor one another run still needs.
 */
export function shouldReap(state: ContainerState): boolean {
   if (!state.exists || !state.name.startsWith(LEGACY_CONTAINER_NAME)) {
      return false;
   }
   if (state.owner?.kind !== "labelled") return false;
   if (state.owner.owner.runId === undefined) return false;
   if (state.owner.owner.keep) return false;
   return state.liveness === "dead";
}

/**
 * The `docker run` arguments for a run's container. Its data lives on a tmpfs
 * (no anonymous volume to leak), and unless `--keep` asked for it to outlive
 * the run it is started with `--rm`, so a container that stops for any reason
 * takes its filesystem with it.
 */
export function containerRunArgs(o: {
   name: string;
   runId: string;
   host: string;
   pid: number;
   pidStart: string;
   keep: boolean;
   user: string;
   password: string;
   hostPort?: number;
   image: string;
}): string[] {
   return [
      "docker",
      "run",
      "-d",
      ...(o.keep ? [] : ["--rm"]),
      "--name",
      o.name,
      "--label",
      `${LABEL_RUN}=${o.runId}`,
      "--label",
      `${LABEL_HOST}=${o.host}`,
      "--label",
      `${LABEL_PID}=${o.pid}`,
      "--label",
      `${LABEL_PID_START}=${o.pidStart}`,
      ...(o.keep ? ["--label", `${LABEL_KEEP}=1`] : []),
      "--tmpfs",
      PG_DATA_DIR,
      "-e",
      `POSTGRES_USER=${o.user}`,
      "-e",
      `POSTGRES_PASSWORD=${o.password}`,
      "-e",
      "POSTGRES_DB=postgres",
      // Docker assigns the host port when none is asked for: probing for a
      // free one and then binding it raced a second run started together.
      "-p",
      o.hostPort ? `127.0.0.1:${o.hostPort}:5432` : "127.0.0.1::5432",
      o.image,
      // No `max_connections` override: the default 100 is ample now that a
      // publisher lives for one scenario. Publisher leaks 2 idle backends per
      // materialization build and frees none until it exits, so a publisher
      // shared across a whole run climbed past 100 by itself — a peak of 127
      // measured serially. Per-scenario publishers bound that to one scenario's
      // builds, and a measured run peaks at 15 even with 8 workers. If that ever
      // regresses it shows up as "sorry, too many clients already" from a schema
      // fetch mid-scenario, which reads like a product failure rather than a
      // harness limit.
      //
      // A throwaway container: durability buys nothing and costs a lot. Creating
      // ~130 databases leaves enough dirty buffers that the FIRST `DROP DATABASE`
      // — which forces an immediate checkpoint — blocks for ~18s, landing on
      // whichever scenario happens to drop something first and reading as a
      // mystery slow scenario. With fsync off that drop is 0.2s, and provisioning
      // is ~20% quicker as well.
      "-c",
      "fsync=off",
      "-c",
      "full_page_writes=off",
      "-c",
      "synchronous_commit=off",
   ];
}

/**
 * A shell script that waits for `pid` to exit, then removes every container
 * labelled with `runId` and its volumes. It runs detached, so a hammer killed
 * with SIGKILL (no handler, no `finally`) still loses its container within
 * seconds instead of at the next run's reaper. It removes by the run's label,
 * never by name: a later run that reuses a `--pg-container` name has another
 * run id, so the watchdog cannot take its container.
 */
export function watchdogScript(runId: string, pid: number): string {
   if (!/^[A-Za-z0-9_.-]+$/.test(runId)) {
      throw new Error(`unsafe hammer run id: ${runId}`);
   }
   return (
      `while kill -0 ${pid} 2>/dev/null; do sleep 2; done; ` +
      `ids=$(docker ps -aq --filter label=${LABEL_RUN}=${runId}); ` +
      `[ -n "$ids" ] && docker rm -fv $ids >/dev/null 2>&1; exit 0`
   );
}

/** Start the detached watchdog for this run; its pid, to stop it at teardown. */
function spawnWatchdog(runId: string): number | undefined {
   const child = spawn("sh", ["-c", watchdogScript(runId, process.pid)], {
      detached: true,
      stdio: "ignore",
   });
   child.unref();
   return child.pid;
}

/**
 * Remove leftovers of earlier hammer runs that died without tearing down (a
 * SIGKILL, a crash, a closed laptop): every container {@link shouldReap}
 * accepts, with its anonymous volumes. Returns the names it removed.
 */
export async function reapStaleContainers(
   probe: ProcessProbe = hostProbe,
): Promise<string[]> {
   const r = await run([
      "docker",
      "ps",
      "-a",
      "--filter",
      `label=${LABEL_RUN}`,
      "--filter",
      `name=^/${LEGACY_CONTAINER_NAME}`,
      "--format",
      "{{.Names}}",
   ]);
   if (r.code !== 0) {
      log.warn(`could not list hammer containers to reap: ${r.stderr.trim()}`);
      return [];
   }
   const removed: string[] = [];
   for (const name of r.stdout.trim().split("\n").filter(Boolean)) {
      if (!shouldReap(await containerState(name, probe))) continue;
      const rm = await run(["docker", "rm", "-fv", name]);
      if (rm.code === 0) {
         log.info(`reaped Postgres container ${name} (its run is gone)`);
         removed.push(name);
      } else {
         log.warn(`could not reap ${name}: ${rm.stderr.trim()}`);
      }
   }
   return removed;
}

/** The real process probe: this host's name, `kill -0`, and `ps` for start times. */
export const hostProbe: ProcessProbe = {
   host: hostname(),
   alive(pid: number): boolean {
      try {
         process.kill(pid, 0);
         return true;
      } catch (err) {
         return (err as NodeJS.ErrnoException).code === "EPERM";
      }
   },
   async startTime(pid: number): Promise<string | undefined> {
      if (process.platform === "win32") return undefined;
      const r = await run(["ps", "-o", "lstart=", "-p", String(pid)]);
      const out = r.stdout.trim();
      return r.code === 0 && out !== "" ? out : undefined;
   },
};

export interface PostgresHandle {
   containerName: string;
   host: string;
   hostPort: number;
   user: string;
   password: string;
   /** Create (if absent) a database on the server. */
   createDb(name: string): Promise<void>;
   /** Drop a database if it exists (terminating connections), for a clean slate. */
   dropDb(name: string): Promise<void>;
   /** Drop then create — a guaranteed-empty database. */
   resetDb(name: string): Promise<void>;
   /**
    * Drop-then-create many databases in ONE psql session. Every `docker exec psql`
    * costs ~100ms of process spawn, which dominates the statement itself, so
    * resetting a scenario per database serially is mostly overhead.
    */
   resetDbs(names: string[]): Promise<void>;
   /** Run SQL against a database (via `psql`), throwing on error. */
   sql(db: string, statements: string): Promise<void>;
   /** Run a SELECT and return rows as objects (column name -> string value). */
   query(db: string, sql: string): Promise<Record<string, string>[]>;
   /** True if the given database currently exists. */
   dbExists(name: string): Promise<boolean>;
   stop(): Promise<void>;
}

export interface PostgresOptions {
   /**
    * A container to use by name. It is reused when it is running and no live
    * run owns it, replaced when it is stopped or stale, and refused when a live
    * run (or a run this host cannot check) owns it.
    */
   containerName?: string;
   /**
    * Publish Postgres on this host port. Unset lets Docker assign a free one, so
    * two runs started together never race for a port.
    */
   hostPort?: number;
   user?: string;
   password?: string;
   image?: string;
   /**
    * Without `containerName`: reuse a running hammer container no live run
    * owns (one a `--keep` run left, under any hammer name, including the old
    * fixed one), or start a new one when there is none.
    */
   reuse?: boolean;
   /** This run's id, for the default name and the owner label. */
   runId?: string;
   /**
    * `--keep`: the container outlives the run. It is created without `--rm`
    * and without a watchdog, and labelled so the reaper leaves it alone.
    */
   keep?: boolean;
}

/** Who owns `name`, read from its labels. A failed inspect is "unknown". */
async function readOwner(name: string): Promise<OwnerReading> {
   const r = await run([
      "docker",
      "inspect",
      "-f",
      "{{json .Config.Labels}}",
      name,
   ]);
   if (r.code !== 0) {
      return {
         kind: "unknown",
         error: (r.stderr || r.stdout).trim() || `exit ${r.code}`,
      };
   }
   let labels: Record<string, string> | null;
   try {
      labels = JSON.parse(r.stdout.trim()) as Record<string, string> | null;
   } catch {
      return {
         kind: "unknown",
         error: `unreadable labels: ${r.stdout.trim()}`,
      };
   }
   const pid = Number(labels?.[LABEL_PID]);
   const owner: ContainerOwner = {
      runId: labels?.[LABEL_RUN] || undefined,
      host: labels?.[LABEL_HOST] || undefined,
      pid: Number.isInteger(pid) && pid > 0 ? pid : undefined,
      pidStart: labels?.[LABEL_PID_START] || undefined,
      keep: labels?.[LABEL_KEEP] === "1" || undefined,
   };
   return owner.runId === undefined && owner.pid === undefined
      ? { kind: "unlabelled" }
      : { kind: "labelled", owner };
}

/** A named container's existence, state and owner. */
async function containerState(
   name: string,
   probe: ProcessProbe,
): Promise<ContainerState> {
   const r = await run([
      "docker",
      "ps",
      "-a",
      "--filter",
      `name=^/${name}$`,
      "--format",
      "{{.Names}} {{.State}}",
   ]);
   const [found, status] = r.stdout.trim().split(/\s+/);
   if (found !== name) return { name, exists: false, running: false };
   const owner = await readOwner(name);
   const liveness =
      owner.kind === "labelled"
         ? await ownerLiveness(owner.owner, probe)
         : undefined;
   return {
      name,
      exists: true,
      running: status === "running",
      owner,
      liveness,
   };
}

/**
 * A running hammer container this run may reuse: the newest one, under any
 * hammer name (the old fixed name included), that no live run owns.
 */
async function findReusableContainer(
   probe: ProcessProbe,
): Promise<string | undefined> {
   const r = await run([
      "docker",
      "ps",
      "--filter",
      `name=^/${LEGACY_CONTAINER_NAME}`,
      "--format",
      "{{.Names}}",
   ]);
   for (const name of r.stdout.trim().split("\n").filter(Boolean)) {
      const state = await containerState(name, probe);
      if (decideContainerAction(state, true).action === "reuse") return name;
   }
   return undefined;
}

export async function startPostgres(
   opts: PostgresOptions = {},
   probe: ProcessProbe = hostProbe,
): Promise<PostgresHandle> {
   const runId = opts.runId ?? newRunId();
   const user = opts.user ?? "hammer";
   const password = opts.password ?? "hammer";
   const image = opts.image ?? "postgres:16";

   let containerName: string;
   let action: ContainerAction;
   let reusable: string | undefined;
   if (opts.containerName) {
      containerName = opts.containerName;
      action = decideContainerAction(
         await containerState(containerName, probe),
         true,
      );
   } else if (opts.reuse && (reusable = await findReusableContainer(probe))) {
      containerName = reusable;
      action = { action: "reuse" };
   } else {
      containerName = defaultContainerName(runId);
      action = decideContainerAction(
         await containerState(containerName, probe),
         false,
      );
   }
   if (action.action === "refuse") throw new Error(action.message);
   const justCreated = action.action !== "reuse";
   let watchdogPid: number | undefined;

   if (action.action === "reuse") {
      log.ok(`reusing running Postgres container ${containerName}`);
   } else {
      if (action.action === "replace") {
         log.info(`removing stale container ${containerName}`);
         await run(["docker", "rm", "-fv", containerName]);
      }
      const pidStart = (await probe.startTime(process.pid)) ?? "";
      log.step(
         `starting Postgres (${image}) as ${containerName}` +
            (opts.hostPort ? ` on host port ${opts.hostPort}` : ""),
      );
      await runOrThrow(
         containerRunArgs({
            name: containerName,
            runId,
            host: probe.host,
            pid: process.pid,
            pidStart,
            keep: opts.keep ?? false,
            user,
            password,
            hostPort: opts.hostPort,
            image,
         }),
      );
      if (!opts.keep) watchdogPid = spawnWatchdog(runId);
   }

   // The port Docker published, read back: assigned when none was asked for,
   // and whatever a reused container publishes.
   const hostPort = await publishedPort(containerName);
   if (hostPort === undefined) {
      throw new Error(
         `Postgres container ${containerName} publishes no host port for 5432`,
      );
   }

   const psqlBase = (db: string): string[] => [
      "docker",
      "exec",
      "-i",
      "-e",
      `PGPASSWORD=${password}`,
      containerName,
      "psql",
      "-v",
      "ON_ERROR_STOP=1",
      "-U",
      user,
      "-d",
      db,
   ];

   const handle: PostgresHandle = {
      containerName,
      host: "127.0.0.1",
      hostPort,
      user,
      password,
      async createDb(name: string): Promise<void> {
         if (await this.dbExists(name)) return;
         // CREATE DATABASE can't run in a transaction/among other statements.
         await runOrThrow([
            ...psqlBase("postgres"),
            "-c",
            `CREATE DATABASE ${name}`,
         ]);
      },
      async dropDb(name: string): Promise<void> {
         await runOrThrow([
            ...psqlBase("postgres"),
            "-c",
            `DROP DATABASE IF EXISTS ${name} WITH (FORCE)`,
         ]);
      },
      async resetDb(name: string): Promise<void> {
         await this.dropDb(name);
         await this.createDb(name);
      },
      async resetDbs(names: string[]): Promise<void> {
         if (names.length === 0) return;
         // CREATE/DROP DATABASE cannot run inside a transaction, so no -1 here;
         // ON_ERROR_STOP still aborts the batch on the first failure.
         const script = names
            .flatMap((n) => [
               `DROP DATABASE IF EXISTS ${n} WITH (FORCE);`,
               `CREATE DATABASE ${n};`,
            ])
            .join("\n");
         await runOrThrow(psqlBase("postgres"), { stdin: script });
      },
      async dbExists(name: string): Promise<boolean> {
         const r = await run([
            ...psqlBase("postgres"),
            "-tAc",
            `SELECT 1 FROM pg_database WHERE datname = '${name}'`,
         ]);
         return r.code === 0 && r.stdout.trim() === "1";
      },
      async sql(db: string, statements: string): Promise<void> {
         await runOrThrow(psqlBase(db), { stdin: statements });
      },
      async query(db: string, sql: string): Promise<Record<string, string>[]> {
         const r = await runOrThrow([...psqlBase(db), "--csv", "-c", sql]);
         return parseCsv(r.stdout.trim());
      },
      async stop(): Promise<void> {
         // Only the container this run created: a reused one belongs to the
         // run (or person) that started it.
         if (!justCreated) {
            log.info(`leaving reused Postgres container ${containerName} up`);
            return;
         }
         log.info(`removing Postgres container ${containerName}`);
         // `-v`: a container's anonymous volumes outlive a plain `rm -f`.
         await run(["docker", "rm", "-fv", containerName]);
         if (watchdogPid !== undefined) {
            try {
               process.kill(watchdogPid);
            } catch {
               // Already gone.
            }
         }
      },
   };

   // The postgres image's entrypoint runs initdb against a TEMPORARY server that
   // listens only on the unix socket, then shuts it down and restarts the real
   // server. `pg_isready` reports the temp server as ready, so a bare readiness
   // check races the restart (a `psql` a moment later hits the gap). On a freshly
   // created container, wait until the "ready to accept connections" log line has
   // appeared TWICE (temp init server + real server) before trusting readiness.
   if (justCreated) {
      await waitFor(
         "Postgres init (real server up)",
         async () => {
            const r = await run(["docker", "logs", containerName]);
            const logs = r.stdout + r.stderr;
            const count = (logs.match(/ready to accept connections/g) ?? [])
               .length;
            return count >= 2;
         },
         { timeoutMs: 60_000, intervalMs: 500 },
      );
   }
   await waitFor(
      "Postgres readiness",
      async () => {
         const r = await run([
            "docker",
            "exec",
            containerName,
            "pg_isready",
            "-U",
            user,
            "-d",
            "postgres",
         ]);
         return r.code === 0;
      },
      { timeoutMs: 60_000, intervalMs: 500 },
   );
   log.ok(`Postgres ready on 127.0.0.1:${hostPort}`);
   return handle;
}

/** Minimal CSV parser (handles double-quoted fields with embedded commas/quotes). */
function parseCsv(text: string): Record<string, string>[] {
   if (!text) return [];
   const lines = text.split("\n");
   const parseLine = (line: string): string[] => {
      const out: string[] = [];
      let cur = "";
      let inQ = false;
      for (let i = 0; i < line.length; i++) {
         const c = line[i];
         if (inQ) {
            if (c === '"' && line[i + 1] === '"') {
               cur += '"';
               i++;
            } else if (c === '"') inQ = false;
            else cur += c;
         } else if (c === '"') inQ = true;
         else if (c === ",") {
            out.push(cur);
            cur = "";
         } else cur += c;
      }
      out.push(cur);
      return out;
   };
   const headers = parseLine(lines[0]);
   return lines.slice(1).map((l) => {
      const cells = parseLine(l);
      const row: Record<string, string> = {};
      headers.forEach((h, i) => (row[h] = cells[i] ?? ""));
      return row;
   });
}

/**
 * The host port in `docker port <name> 5432/tcp` output (`127.0.0.1:49153`,
 * `0.0.0.0:55432` then `[::]:55432`), or undefined when nothing is published.
 */
export function parsePublishedPort(stdout: string): number | undefined {
   const match = /:(\d+)\s*$/.exec(stdout.trim().split("\n")[0] ?? "");
   return match ? Number(match[1]) : undefined;
}

/** The host port a running container publishes for 5432, if any. */
async function publishedPort(name: string): Promise<number | undefined> {
   const r = await run(["docker", "port", name, "5432/tcp"]);
   return r.code === 0 ? parsePublishedPort(r.stdout) : undefined;
}

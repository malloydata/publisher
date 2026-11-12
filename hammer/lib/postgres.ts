// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A throwaway Postgres container for the harness: it is BOTH the DuckLake
// catalog store AND a source warehouse (a `postgres` connection), mirroring the
// tutorial. One server, several databases.

import { createServer } from "node:net";
import { log, run, runOrThrow, waitFor } from "./util";

/** The label a hammer run stamps on the container it creates: its own pid. */
const OWNER_LABEL = "hammer.pid";

/**
 * The container name a run uses when none is given: one per host port, so two
 * runs on different ports never share (or tear down) a database, and a later
 * `--reuse-pg` on the same port finds the container a `--keep` run left.
 */
export function defaultContainerName(hostPort: number): string {
   return `publisher-hammer-pg-${hostPort}`;
}

/** True when nothing on this host is listening on `port`. */
export function portIsFree(port: number): Promise<boolean> {
   return new Promise((resolve) => {
      const server = createServer();
      server.once("error", () => resolve(false));
      server.once("listening", () => server.close(() => resolve(true)));
      // No host: all interfaces, as `docker run -p` publishes.
      server.listen(port);
   });
}

/** The first free port at or above `from`, trying `attempts` ports. */
export async function firstFreePort(
   from: number,
   attempts = 100,
): Promise<number> {
   for (let port = from; port < from + attempts; port++) {
      if (await portIsFree(port)) return port;
   }
   throw new Error(`no free port in ${from}-${from + attempts - 1}`);
}

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
   containerName?: string;
   hostPort?: number;
   user?: string;
   password?: string;
   image?: string;
   /** Reuse a container of this name if it is already running. */
   reuse?: boolean;
}

/**
 * The pid that created `name`, from its owner label, or undefined for a
 * container with no label (made by an older harness, or by hand).
 */
async function ownerPid(name: string): Promise<number | undefined> {
   const r = await run([
      "docker",
      "inspect",
      "-f",
      `{{index .Config.Labels "${OWNER_LABEL}"}}`,
      name,
   ]);
   const pid = Number(r.stdout.trim());
   return r.code === 0 && Number.isInteger(pid) && pid > 0 ? pid : undefined;
}

function processAlive(pid: number): boolean {
   try {
      process.kill(pid, 0);
      return true;
   } catch (err) {
      // EPERM: alive, owned by someone else.
      return (err as NodeJS.ErrnoException).code === "EPERM";
   }
}

export async function startPostgres(
   opts: PostgresOptions = {},
): Promise<PostgresHandle> {
   const requestedPort = opts.hostPort ?? 55432;
   const containerName =
      opts.containerName ?? defaultContainerName(requestedPort);
   let hostPort = requestedPort;
   const user = opts.user ?? "hammer";
   const password = opts.password ?? "hammer";
   const image = opts.image ?? "postgres:16";

   const running = await run([
      "docker",
      "ps",
      "--filter",
      `name=^/${containerName}$`,
      "--format",
      "{{.Names}}",
   ]);
   const isUp = running.stdout.trim() === containerName;
   let justCreated = false;

   if (isUp && opts.reuse) {
      // A container named with --pg-container may publish another port.
      hostPort = (await publishedPort(containerName)) ?? requestedPort;
      log.ok(`reusing running Postgres container ${containerName}`);
   } else {
      justCreated = true;
      if (isUp || (await containerExists(containerName))) {
         // Replace a container only when no live run owns it: a running
         // container whose creating run is still alive is another run's
         // database, and removing it fails that run mid-scenario.
         const pid = await ownerPid(containerName);
         if (isUp && pid !== undefined && processAlive(pid)) {
            throw new Error(
               `Postgres container ${containerName} belongs to a hammer run that is ` +
                  `still going (pid ${pid}). Pass a different --pg-port or ` +
                  `--pg-container, or --reuse-pg to share it.`,
            );
         }
         log.info(`removing stale container ${containerName}`);
         await run(["docker", "rm", "-f", containerName]);
      }
      log.step(`starting Postgres (${image}) on host port ${hostPort}`);
      await runOrThrow([
         "docker",
         "run",
         "-d",
         "--name",
         containerName,
         "--label",
         `${OWNER_LABEL}=${process.pid}`,
         "-e",
         `POSTGRES_USER=${user}`,
         "-e",
         `POSTGRES_PASSWORD=${password}`,
         "-e",
         "POSTGRES_DB=postgres",
         "-p",
         `${hostPort}:5432`,
         image,
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
      ]);
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
         await run(["docker", "rm", "-f", containerName]);
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

/** The host port a running container publishes for 5432, if any. */
async function publishedPort(name: string): Promise<number | undefined> {
   const r = await run(["docker", "port", name, "5432/tcp"]);
   const match = /:(\d+)\s*$/m.exec(r.stdout.trim().split("\n")[0] ?? "");
   return match ? Number(match[1]) : undefined;
}

async function containerExists(name: string): Promise<boolean> {
   const r = await run([
      "docker",
      "ps",
      "-a",
      "--filter",
      `name=^/${name}$`,
      "--format",
      "{{.Names}}",
   ]);
   return r.stdout.trim() === name;
}

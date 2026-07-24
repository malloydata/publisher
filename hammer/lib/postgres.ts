// A throwaway Postgres container for the harness: it is BOTH the DuckLake
// catalog store AND a source warehouse (a `postgres` connection), mirroring the
// tutorial. One server, several databases.

import { log, run, runOrThrow, waitFor } from "./util";

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

export async function startPostgres(
   opts: PostgresOptions = {},
): Promise<PostgresHandle> {
   const containerName = opts.containerName ?? "publisher-hammer-pg";
   const hostPort = opts.hostPort ?? 55432;
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
      log.ok(`reusing running Postgres container ${containerName}`);
   } else {
      justCreated = true;
      if (isUp || (await containerExists(containerName))) {
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
         "-e",
         `POSTGRES_USER=${user}`,
         "-e",
         `POSTGRES_PASSWORD=${password}`,
         "-e",
         "POSTGRES_DB=postgres",
         "-p",
         `${hostPort}:5432`,
         image,
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

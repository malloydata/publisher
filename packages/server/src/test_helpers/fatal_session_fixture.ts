// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Run as a child process by materialization_build_session.spec.ts and
 * loads.integration.spec.ts: a case that aborts the whole process (exit 134)
 * when an isolated session does not contain it, so it cannot run in the test
 * process. Prints one `RESULT <json>` line and exits 0.
 *
 * - no argument: a fatal DuckDB error inside a DuckLake transaction on a local
 *   catalog. The trigger is DuckDB 1.5.5's second read of a cached encrypted
 *   Parquet file (DuckDB issue 24311).
 * - `postgres fatal <table>` | `postgres dead <table>`: the same on a table
 *   (named in full) of a Postgres-catalog DuckLake `lake`, or a session
 *   disposed with its transaction open after its metadata connection died. The
 *   conninfo is in the environment, out of `ps`.
 */
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { attachDuckLakeReadWrite } from "../service/connection";
import {
   createIsolatedBuildSession,
   inTransaction,
   type IsolatedSessionConnection,
   withIsolatedSession,
} from "../service/materialization/run/materialization_build_session";

async function count(
   session: IsolatedSessionConnection,
   table: string,
): Promise<number> {
   const { rows } = await session.runSQL(`SELECT count(*) AS n FROM ${table}`);
   return Number((rows[0] as { n: unknown }).n);
}

/** An error's first line: DuckDB's INTERNAL errors carry a native stack trace. */
const message = (e: unknown): string =>
   (e instanceof Error ? e.message : String(e)).split("\n")[0];

/** What a later statement on the session answers, and whether its cause is the fatal error. */
async function refusal(session: IsolatedSessionConnection) {
   const error = await session.runSQL("SELECT 1").then(
      () => undefined,
      (e) => e as Error,
   );
   return {
      message: error ? message(error) : null,
      causeIsInternal:
         error?.cause instanceof Error &&
         error.cause.message.includes("INTERNAL Error"),
   };
}

async function local() {
   const dir = mkdtempSync(join(tmpdir(), "fatal-session-"));
   const data = join(dir, "data");
   mkdirSync(data);

   async function attach(session: IsolatedSessionConnection): Promise<void> {
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await session.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${data}/', ENCRYPTED true, DATA_INLINING_ROW_LIMIT 0)`,
      );
   }

   async function scenario(swallow: boolean) {
      const out: Record<string, unknown> = {};
      await withIsolatedSession("fatal_fixture", async (session) => {
         await attach(session);
         await session.runSQL("CREATE TABLE IF NOT EXISTS lake.t(id INTEGER)");
         if ((await count(session, "lake.t")) === 0) {
            await session.runSQL("INSERT INTO lake.t VALUES (1), (2), (3)");
         }
         await session.runSQL("SET parquet_metadata_cache = true");
         const body = async () => {
            await session.runSQL("INSERT INTO lake.t VALUES (99)");
            await session.runSQL("SELECT * FROM lake.t");
            try {
               await session.runSQL("SELECT * FROM lake.t");
            } catch (e) {
               out.secondReadError = message(e);
               if (!swallow) throw e;
            }
            if (swallow) {
               try {
                  await session.runSQL("INSERT INTO lake.t VALUES (100)");
                  out.insertAfterFatal = "ran";
               } catch (e) {
                  out.insertAfterFatal = message(e);
               }
            }
         };
         try {
            await inTransaction(session, "lake.t", body);
            out.transaction = "committed";
         } catch (e) {
            out.transaction = message(e);
         }
         out.later = await refusal(session);
      });
      out.disposed = true;
      // Whether the catalog file kept its write-ahead log instead of checkpointing on close.
      out.walKept = existsSync(join(dir, "catalog.ducklake.wal"));
      return out;
   }

   try {
      const propagated = await scenario(false);
      const swallowed = await scenario(true);
      const unchanged = await withIsolatedSession(
         "fatal_fixture",
         async (s) => {
            await attach(s);
            return count(s, "lake.t");
         },
      );
      return { propagated, swallowed, unchanged };
   } finally {
      rmSync(dir, { recursive: true, force: true });
   }
}

/** A write session on the Postgres catalog `lake`, whose table the parent made. */
async function attachPostgres() {
   const connectionString = process.env.FATAL_FIXTURE_CONNINFO;
   if (!connectionString) throw new Error("FATAL_FIXTURE_CONNINFO is not set");
   const made = createIsolatedBuildSession("fatal_fixture_pg");
   try {
      await attachDuckLakeReadWrite(made.session, "lake", {
         catalog: {
            postgresConnection: { connectionString },
            metadataSchema: "lake",
         },
      } as Parameters<typeof attachDuckLakeReadWrite>[2]);
   } catch (error) {
      await made.dispose();
      throw error;
   }
   return made;
}

async function postgresFatal(table: string) {
   const { session, dispose } = await attachPostgres();
   const out: Record<string, unknown> = {};
   try {
      await session.runSQL("SET parquet_metadata_cache = true");
      try {
         await inTransaction(session, table, async () => {
            await session.runSQL(`INSERT INTO ${table} VALUES (99, 'x')`);
            await session.runSQL(`SELECT * FROM ${table}`);
            await session.runSQL(`SELECT * FROM ${table}`);
         });
         out.transaction = "committed";
      } catch (e) {
         out.transaction = message(e);
      }
   } finally {
      await dispose();
   }
   return out;
}

/** Leaves a transaction open, tells the parent, and disposes once it has killed the backends. */
async function postgresDead(table: string) {
   const { session, dispose } = await attachPostgres();
   await session.runSQL("BEGIN TRANSACTION");
   await session.runSQL(`INSERT INTO ${table} VALUES (99, 'x')`);
   console.log("OPEN");
   await new Promise<void>((resolve) =>
      process.stdin.once("data", () => resolve()),
   );
   await dispose();
   return { disposed: true };
}

const [mode, scenarioName, table] = process.argv.slice(2);
const result =
   mode === undefined
      ? await local()
      : mode === "postgres" && scenarioName === "fatal"
        ? await postgresFatal(table)
        : mode === "postgres" && scenarioName === "dead"
          ? await postgresDead(table)
          : (() => {
               throw new Error(
                  `Unknown fixture scenario: ${mode} ${scenarioName}`,
               );
            })();
console.log(`RESULT ${JSON.stringify(result)}`);
process.exit(0);

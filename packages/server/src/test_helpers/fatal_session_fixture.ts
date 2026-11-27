// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Run as a child process by materialization_build_session.spec.ts: a fatal
 * DuckDB error inside an isolated session's DuckLake transaction, which aborts
 * the whole process (exit 134) when the session is not contained. The trigger
 * is DuckDB 1.5.5's second read of a cached encrypted Parquet file
 * (duckdb#24311). Prints one `RESULT <json>` line and exits 0.
 */
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
   inTransaction,
   type IsolatedSessionConnection,
   withIsolatedSession,
} from "../service/materialization/run/materialization_build_session";

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

async function count(session: IsolatedSessionConnection): Promise<number> {
   const { rows } = await session.runSQL("SELECT count(*) AS n FROM lake.t");
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

async function scenario(swallow: boolean) {
   const out: Record<string, unknown> = {};
   await withIsolatedSession("fatal_fixture", async (session) => {
      await attach(session);
      await session.runSQL("CREATE TABLE IF NOT EXISTS lake.t(id INTEGER)");
      if ((await count(session)) === 0) {
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
   return out;
}

try {
   const propagated = await scenario(false);
   const swallowed = await scenario(true);
   const unchanged = await withIsolatedSession("fatal_fixture", async (s) => {
      await attach(s);
      return count(s);
   });
   console.log(
      `RESULT ${JSON.stringify({ propagated, swallowed, unchanged })}`,
   );
} finally {
   rmSync(dir, { recursive: true, force: true });
}
process.exit(0);

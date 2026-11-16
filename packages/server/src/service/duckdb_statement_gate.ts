// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One statement at a time per DuckDB connection.
 *
 * `@malloydata/db-duckdb` runs every statement on one native connection, and
 * DuckDB closes a connection's open streaming result when another statement
 * starts on it. A stream that loses its result ends with no error and the rows
 * it had not yet read, so two parallel `runSQLStream` calls (the raw SQL
 * endpoints stream) answer 200 with zero rows, and a stream that overlaps a
 * `runSQL` is cut short. This gate holds each statement, and each stream from
 * its first row to its end, behind the one before it on the same connection.
 * DuckDB already ran them one after another; only the silent truncation goes.
 *
 * Wraps the same two entry points as duckdb_hard_close.ts, and is installed
 * first so the hard close counts a queued statement as in flight too.
 *
 * Holds to know: a stream nobody finishes or returns holds the gate, and so does
 * a consumer that runs another statement on the same connection inside its
 * `for await` (its only consumer, stream_helpers.ts, drains with `for await`).
 * Calling `idle()` between a stream's `setup()` and its `acquire()` would
 * deadlock; Publisher only calls `shutdown("close")`.
 *
 * Upstream fix: `@malloydata/db-duckdb` (malloydata/malloy) should serialize
 * statements per native connection, or hold the connection for a stream's life,
 * and `runSQLStream` should throw on abort instead of returning quietly.
 * Delete this module, and its install in connection.ts, once a release does.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Mutex } from "async-mutex";
import { AsyncLocalStorage } from "node:async_hooks";
import { logger } from "../logger";
import { dbDuckDBHardCloseInternalsMatch } from "./duckdb_hard_close";

interface Internals {
   setup(): Promise<void>;
   runDuckDBQuery(sql: string): Promise<unknown>;
   runSQLStream(sql: string, options?: unknown): AsyncIterableIterator<unknown>;
}

const gates = new WeakMap<object, Mutex>();
/**
 * The connection whose gate the running async context holds, released once the
 * stream is done: a stream's own split-off statements run inside it and must not
 * queue behind it, but work it started without waiting inherits the context.
 */
const holding = new AsyncLocalStorage<{ connection: object; held: boolean }>();
let installed = false;

function gateOf(connection: object): Mutex {
   let gate = gates.get(connection);
   if (!gate) {
      gate = new Mutex();
      gates.set(connection, gate);
   }
   return gate;
}

/** Idempotent; installed once per process (connection.ts). */
export function installDuckDBStatementGate(): void {
   if (installed) return;
   installed = true;
   if (!dbDuckDBHardCloseInternalsMatch()) {
      logger.warn(
         "DuckDB statement gate is off (parallel statements on one connection can truncate a stream): @malloydata/db-duckdb's internals changed shape",
      );
      return;
   }
   const proto = DuckDBConnection.prototype as unknown as Internals;
   const runDuckDBQuery = proto.runDuckDBQuery;
   const runSQLStream = proto.runSQLStream;

   proto.runDuckDBQuery = function (this: Internals, sql: string) {
      const hold = holding.getStore();
      if (hold?.held && hold.connection === this)
         return runDuckDBQuery.call(this, sql);
      return gateOf(this).runExclusive(() => runDuckDBQuery.call(this, sql));
   };

   proto.runSQLStream = async function* (
      this: Internals,
      sql: string,
      options?: unknown,
   ): AsyncIterableIterator<unknown> {
      // Before the gate: the first statements run the connection's setup, which
      // would queue behind a stream that is waiting for it.
      await this.setup();
      const release = await gateOf(this).acquire();
      const hold = { connection: this, held: true };
      try {
         // A signal that fired while waiting: end without running the statement.
         if (
            (options as { abortSignal?: AbortSignal } | undefined)?.abortSignal
               ?.aborted
         )
            return;
         const inner = runSQLStream.call(this, sql, options);
         try {
            for (;;) {
               const step = await holding.run(hold, () => inner.next());
               if (step.done) return;
               yield step.value;
            }
         } finally {
            await holding.run(hold, () => inner.return?.(undefined));
         }
      } finally {
         hold.held = false;
         release();
      }
   };
}

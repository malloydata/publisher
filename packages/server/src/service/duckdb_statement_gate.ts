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
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Mutex } from "async-mutex";
import { AsyncLocalStorage } from "node:async_hooks";

interface Internals {
   setup(): Promise<void>;
   runDuckDBQuery(sql: string): Promise<unknown>;
   runSQLStream(sql: string, options?: unknown): AsyncIterableIterator<unknown>;
}

const gates = new WeakMap<object, Mutex>();
/**
 * The connection whose gate the running async context already holds: a stream's
 * own split-off statements run inside it and must not queue behind it.
 */
const holding = new AsyncLocalStorage<object>();
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
   const proto = DuckDBConnection.prototype as unknown as Internals;
   const runDuckDBQuery = proto.runDuckDBQuery;
   const runSQLStream = proto.runSQLStream;

   proto.runDuckDBQuery = function (this: Internals, sql: string) {
      if (holding.getStore() === this) return runDuckDBQuery.call(this, sql);
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
      const inner = runSQLStream.call(this, sql, options);
      try {
         for (;;) {
            const step = await holding.run(this, () => inner.next());
            if (step.done) return;
            yield step.value;
         }
      } finally {
         try {
            await holding.run(this, () => inner.return?.(undefined));
         } finally {
            release();
         }
      }
   };
}

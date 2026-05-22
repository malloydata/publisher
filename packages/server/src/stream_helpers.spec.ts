import type {
   QueryRecord,
   RunSQLOptions,
   StreamingConnection,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";

import { PayloadTooLargeError } from "./errors";
import { isStreamingConnection, streamSqlWithBudget } from "./stream_helpers";

/**
 * Build a fake StreamingConnection backed by a fixed row array. The
 * fake records the abort signal so tests can confirm
 * `streamSqlWithBudget` actually signaled the driver to stop —
 * client-side counting alone is not enough; the whole point of the
 * helper is to terminate fetching early.
 */
function fakeStreamingConnection(opts: {
   rows: QueryRecord[];
   /**
    * When true (the default), the fake honors `RunSQLOptions.rowLimit`
    * by slicing — same as real Postgres/DuckDB. Set false to drive
    * the helper's own overflow detection (the (cap+1)-th row check).
    */
   honorRowLimit?: boolean;
   /**
    * Test hook: set to `true` when the fake observes `signal.aborted`
    * flip via the listener it installed at the start of streaming.
    */
   abortObserved?: { value: boolean };
   /**
    * Test hook: captures the options the helper passed to
    * `runSQLStream`, so tests can assert it preserved caller-supplied
    * `rowLimit` (and the abortSignal it wired in).
    */
   capturedOptions?: { value: RunSQLOptions | undefined };
}): StreamingConnection {
   const { rows, honorRowLimit = true, abortObserved, capturedOptions } = opts;
   return {
      canStream(): true {
         return true;
      },
      async *runSQLStream(
         _sql: string,
         options?: RunSQLOptions,
      ): AsyncIterableIterator<QueryRecord> {
         if (capturedOptions) capturedOptions.value = options;
         if (abortObserved) {
            options?.abortSignal?.addEventListener("abort", () => {
               abortObserved.value = true;
            });
         }
         const limit =
            honorRowLimit && typeof options?.rowLimit === "number"
               ? options.rowLimit
               : rows.length;
         for (let i = 0; i < Math.min(rows.length, limit); i += 1) {
            yield rows[i];
         }
      },
   } as unknown as StreamingConnection;
}

describe("isStreamingConnection", () => {
   it("returns true for a connection whose canStream() returns true", () => {
      const conn = fakeStreamingConnection({ rows: [] });
      expect(isStreamingConnection(conn)).toBe(true);
   });

   it("returns false for a connection without canStream", () => {
      expect(isStreamingConnection({} as never)).toBe(false);
   });

   it("returns false for a connection whose canStream() returns false", () => {
      const conn = {
         canStream() {
            return false;
         },
      } as never;
      expect(isStreamingConnection(conn)).toBe(false);
   });
});

describe("streamSqlWithBudget", () => {
   it("returns all rows when both budgets are comfortably above the stream", async () => {
      const rows: QueryRecord[] = [{ a: 1 }, { a: 2 }, { a: 3 }];
      const conn = fakeStreamingConnection({ rows });
      const result = await streamSqlWithBudget(
         conn,
         "SELECT a FROM t",
         { rowLimit: 10 },
         { maxRows: 10, maxBytes: 1_000_000 },
      );
      expect(result.rows).toEqual(rows);
      expect(result.totalRows).toBe(3);
   });

   it("forwards caller-supplied runSQLOptions (rowLimit) to the driver", async () => {
      const captured = { value: undefined as RunSQLOptions | undefined };
      const conn = fakeStreamingConnection({
         rows: [{ a: 1 }, { a: 2 }],
         capturedOptions: captured,
      });
      await streamSqlWithBudget(
         conn,
         "SELECT 1",
         { rowLimit: 6 },
         { maxRows: 5, maxBytes: 0 },
      );
      expect(captured.value?.rowLimit).toBe(6);
      // abortSignal must be wired in so the helper can abort the
      // iterator on overflow.
      expect(captured.value?.abortSignal).toBeDefined();
   });

   it("composes the caller-supplied abortSignal with its internal cap-abort signal", async () => {
      // Step 5: the caller's signal is the query timeout. Composing
      // both sources means EITHER an external timeout OR an internal
      // cap overflow terminates the iterator. The combined signal
      // must be a fresh AbortSignal (not either input by reference);
      // aborting either input must mark the composed signal aborted.
      const captured = { value: undefined as RunSQLOptions | undefined };
      const callerAc = new AbortController();
      const conn = fakeStreamingConnection({
         rows: [{ a: 1 }],
         capturedOptions: captured,
      });
      await streamSqlWithBudget(
         conn,
         "SELECT 1",
         { rowLimit: 10, abortSignal: callerAc.signal },
         { maxRows: 5, maxBytes: 0 },
      );
      const observed = captured.value?.abortSignal;
      expect(observed).toBeInstanceOf(AbortSignal);
      // Composed signal is a new object (`AbortSignal.any` returns a
      // fresh signal), not the caller's signal by reference.
      expect(observed).not.toBe(callerAc.signal);
      expect(observed?.aborted).toBe(false);
   });

   it("composed signal aborts when the caller's signal aborts", async () => {
      // Drive the external (caller) signal manually and confirm the
      // composed signal that reached the driver tracks it. This is the
      // half of composition that runWithQueryTimeout depends on for
      // 504 to actually cancel an in-flight query.
      if (typeof AbortSignal.any !== "function") return;
      const captured = { value: undefined as RunSQLOptions | undefined };
      const callerAc = new AbortController();
      const conn = fakeStreamingConnection({
         rows: [{ a: 1 }],
         capturedOptions: captured,
      });
      await streamSqlWithBudget(
         conn,
         "SELECT 1",
         { rowLimit: 10, abortSignal: callerAc.signal },
         { maxRows: 5, maxBytes: 0 },
      );
      const observed = captured.value?.abortSignal;
      expect(observed?.aborted).toBe(false);
      callerAc.abort();
      expect(observed?.aborted).toBe(true);
   });

   it("throws PayloadTooLargeError and aborts the iterator on the (cap+1)-th row", async () => {
      const rows: QueryRecord[] = [
         { a: 1 },
         { a: 2 },
         { a: 3 },
         { a: 4 },
         { a: 5 },
      ];
      const abortObserved = { value: false };
      const conn = fakeStreamingConnection({
         rows,
         abortObserved,
         honorRowLimit: false,
      });
      await expect(
         streamSqlWithBudget(
            conn,
            "SELECT a FROM t",
            {},
            { maxRows: 2, maxBytes: 1_000_000 },
         ),
      ).rejects.toBeInstanceOf(PayloadTooLargeError);
      await expect(
         streamSqlWithBudget(
            conn,
            "SELECT a FROM t",
            {},
            { maxRows: 2, maxBytes: 1_000_000 },
         ),
      ).rejects.toThrow("more than 2 rows");
      // The helper must have fired the abort signal so a real driver
      // (pg-query-stream / duckdb) would stop producing rows server-
      // side, not just be discarded client-side.
      expect(abortObserved.value).toBe(true);
   });

   it("throws PayloadTooLargeError when summed JSON byte size exceeds the cap", async () => {
      const big = "x".repeat(40);
      const rows: QueryRecord[] = [{ s: big }, { s: big }, { s: big }];
      const abortObserved = { value: false };
      const conn = fakeStreamingConnection({
         rows,
         abortObserved,
         honorRowLimit: false,
      });
      await expect(
         streamSqlWithBudget(
            conn,
            "SELECT s FROM t",
            {},
            { maxRows: 100, maxBytes: 60 },
         ),
      ).rejects.toThrow("exceeded 60 bytes");
      expect(abortObserved.value).toBe(true);
   });

   it("returns all rows when the byte cap is disabled (maxBytes = 0)", async () => {
      const big = "x".repeat(10_000);
      const rows: QueryRecord[] = Array.from({ length: 3 }, () => ({ s: big }));
      const conn = fakeStreamingConnection({ rows, honorRowLimit: false });
      const result = await streamSqlWithBudget(
         conn,
         "SELECT s FROM t",
         {},
         { maxRows: 100, maxBytes: 0 },
      );
      expect(result.rows.length).toBe(3);
   });

   it("returns all rows when the row cap is disabled (maxRows = 0)", async () => {
      const rows: QueryRecord[] = Array.from({ length: 50 }, (_, i) => ({
         a: i,
      }));
      const conn = fakeStreamingConnection({ rows, honorRowLimit: false });
      const result = await streamSqlWithBudget(
         conn,
         "SELECT a FROM t",
         {},
         { maxRows: 0, maxBytes: 1_000_000 },
      );
      expect(result.rows.length).toBe(50);
   });

   it("returns rows when count equals the cap exactly (not an overflow)", async () => {
      const rows: QueryRecord[] = [{ a: 1 }, { a: 2 }, { a: 3 }];
      const conn = fakeStreamingConnection({ rows, honorRowLimit: false });
      const result = await streamSqlWithBudget(
         conn,
         "SELECT a FROM t",
         {},
         { maxRows: 3, maxBytes: 1_000_000 },
      );
      expect(result.rows.length).toBe(3);
   });

   it("re-throws non-overflow errors from the driver", async () => {
      const conn = {
         canStream(): true {
            return true;
         },
         // eslint-disable-next-line require-yield
         async *runSQLStream(): AsyncIterableIterator<QueryRecord> {
            throw new Error("connection reset");
         },
      } as unknown as StreamingConnection;
      await expect(
         streamSqlWithBudget(
            conn,
            "SELECT 1",
            {},
            { maxRows: 10, maxBytes: 1_000 },
         ),
      ).rejects.toThrow("connection reset");
   });
});

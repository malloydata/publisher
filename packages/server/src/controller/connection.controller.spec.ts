import type {
   Connection,
   QueryRecord,
   RunSQLOptions,
   StreamingConnection,
} from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";

import { BadRequestError, PayloadTooLargeError } from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import { ConnectionController } from "./connection.controller";

/**
 * Tests for the row-cap path inside
 * {@link ConnectionController.getConnectionQueryData}. Mocks the
 * underlying Malloy `Connection.runSQL` so the test runs without a
 * live database; the EnvironmentStore is similarly stubbed via
 * sinon — we only exercise the controller's request-shaping and
 * overflow-detection logic.
 */
/**
 * Build a controller whose `getMalloyConnection` resolves to a
 * fake `Connection` with a sinon-stubbed `runSQL`. We bypass the
 * normal EnvironmentStore lookup entirely so the test stays a
 * single-file unit test.
 *
 * `assertCanAdmitQuery` defaults to a no-op (controller is never
 * back-pressured); pass an overridden stub via
 * `{ assertCanAdmitQuery }` to drive the 503 path.
 *
 * Hoisted to module scope so the admission-gate describe can reuse it.
 */
function buildController(
   runSQL: sinon.SinonStub,
   opts: { assertCanAdmitQuery?: sinon.SinonStub } = {},
): {
   controller: ConnectionController;
   runSQL: sinon.SinonStub;
   assertCanAdmitQuery: sinon.SinonStub;
} {
   const fakeConnection = { runSQL } as unknown as Connection;
   const assertCanAdmitQuery =
      opts.assertCanAdmitQuery ?? sinon.stub().returns(undefined);
   const fakeEnv = { assertCanAdmitQuery };
   const fakeStore = {
      getEnvironment: sinon.stub().resolves(fakeEnv),
   } as unknown as EnvironmentStore;
   const controller = new ConnectionController(fakeStore);
   // `getMalloyConnection` is private; cast through `unknown` so we
   // can swap it without exposing internals on the public surface.
   sinon
      .stub(
         controller as unknown as {
            getMalloyConnection: (...args: unknown[]) => Promise<Connection>;
         },
         "getMalloyConnection",
      )
      .resolves(fakeConnection);
   return { controller, runSQL, assertCanAdmitQuery };
}

describe("ConnectionController.getConnectionQueryData row cap", () => {
   const originalEnv = process.env.PUBLISHER_MAX_QUERY_ROWS;

   afterEach(() => {
      sinon.restore();
      if (originalEnv === undefined) {
         delete process.env.PUBLISHER_MAX_QUERY_ROWS;
      } else {
         process.env.PUBLISHER_MAX_QUERY_ROWS = originalEnv;
      }
   });

   it("passes the SQL through verbatim and asks the connector for cap+1 rows", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "5";
      const runSQL = sinon.stub().resolves({ rows: [{ a: 1 }], totalRows: 1 });
      const { controller } = buildController(runSQL);

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT a FROM t",
         "",
      );

      expect(runSQL.calledOnce).toBe(true);
      expect(runSQL.firstCall.args[0]).toBe("SELECT a FROM t");
      const opts = runSQL.firstCall.args[1] as { rowLimit?: number };
      expect(opts.rowLimit).toBe(6);
      const parsed = JSON.parse(result.data ?? "") as {
         rows: Array<{ a: number }>;
         totalRows: number;
      };
      expect(parsed.rows).toEqual([{ a: 1 }]);
   });

   it("returns the result when row count equals the cap exactly", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "3";
      const rows = [{ a: 1 }, { a: 2 }, { a: 3 }];
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { controller } = buildController(runSQL);

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT a FROM t",
         "",
      );

      const parsed = JSON.parse(result.data ?? "") as { rows: unknown[] };
      expect(parsed.rows.length).toBe(3);
   });

   it("throws PayloadTooLargeError when the connection returns cap+1 rows", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "3";
      const rows = [{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }];
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { controller } = buildController(runSQL);

      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT a FROM t",
            "",
         ),
      ).rejects.toBeInstanceOf(PayloadTooLargeError);
      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT a FROM t",
            "",
         ),
      ).rejects.toThrow("more than 3 rows");
   });

   it("forwards non-SELECT statements verbatim with rowLimit applied", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "10";
      const rows = [{ table_name: "a" }, { table_name: "b" }];
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData("env", "conn", "SHOW TABLES", "");

      expect(runSQL.firstCall.args[0]).toBe("SHOW TABLES");
      const opts = runSQL.firstCall.args[1] as { rowLimit?: number };
      expect(opts.rowLimit).toBe(11);
   });

   it("disables rowLimit when PUBLISHER_MAX_QUERY_ROWS=0", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "0";
      const rows = Array.from({ length: 100 }, (_, i) => ({ a: i }));
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { controller } = buildController(runSQL);

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT a FROM t",
         "",
      );

      expect(runSQL.firstCall.args[0]).toBe("SELECT a FROM t");
      const opts = runSQL.firstCall.args[1] as { rowLimit?: number };
      expect(opts.rowLimit).toBeUndefined();
      const parsed = JSON.parse(result.data ?? "") as { rows: unknown[] };
      expect(parsed.rows.length).toBe(100);
   });

   it("uses the default cap when PUBLISHER_MAX_QUERY_ROWS is unset", async () => {
      delete process.env.PUBLISHER_MAX_QUERY_ROWS;
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData("env", "conn", "SELECT 1", "");

      const opts = runSQL.firstCall.args[1] as { rowLimit?: number };
      // Default cap is 100_000, so we request cap+1 = 100_001.
      expect(opts.rowLimit).toBe(100_001);
   });

   it("preserves a caller-supplied rowLimit when it is below the cap", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "100";
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT 1",
         JSON.stringify({ rowLimit: 5 }),
      );

      const opts = runSQL.firstCall.args[1] as { rowLimit?: number };
      expect(opts.rowLimit).toBe(5);
   });

   it("clamps a caller-supplied rowLimit that exceeds the cap+1 sentinel and replaces any caller-supplied abortSignal with the publisher's timeout signal", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "10";
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT 1",
         JSON.stringify({ rowLimit: 50, abortSignal: {} }),
      );

      const opts = runSQL.firstCall.args[1] as {
         rowLimit?: number;
         abortSignal?: AbortSignal;
      };
      expect(opts.rowLimit).toBe(11);
      // Step 5: the controller always installs its own AbortSignal
      // (sourced from runWithQueryTimeout) so a hung driver call can
      // be canceled. The caller-supplied placeholder is dropped at
      // the JSON-parse boundary — it could never be a real
      // AbortSignal anyway — and replaced with a live one. Prior to
      // Step 5 the assertion was `toBeUndefined`; that behavior is
      // gone on purpose.
      expect(opts.abortSignal).toBeInstanceOf(AbortSignal);
      expect(opts.abortSignal?.aborted).toBe(false);
   });

   /**
    * Express parses repeated query parameters and array-shaped JSON bodies
    * as `string[]`, not `string`. The route handlers up-cast for TypeScript,
    * so we re-validate at the controller boundary. CodeQL flagged the
    * unvalidated dataflow as
    * `js/type-confusion-through-parameter-tampering`.
    */
   it.each([
      ["array sqlStatement", ["SELECT 1", "SELECT 2"]],
      ["object sqlStatement", { evil: true }],
      ["number sqlStatement", 42],
      ["null sqlStatement", null],
      ["undefined sqlStatement", undefined],
   ])(
      "rejects non-string sqlStatement (%s) with BadRequestError",
      async (_label, sqlStatement) => {
         const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
         const { controller } = buildController(runSQL);

         await expect(
            controller.getConnectionQueryData(
               "env",
               "conn",
               sqlStatement as unknown as string,
               "",
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
         expect(runSQL.called).toBe(false);
      },
   );

   it.each([
      ["array options", ["{}", "{}"]],
      ["object options", { foo: "bar" }],
      ["number options", 42],
   ])(
      "rejects non-string options (%s) with BadRequestError",
      async (_label, options) => {
         const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
         const { controller } = buildController(runSQL);

         await expect(
            controller.getConnectionQueryData(
               "env",
               "conn",
               "SELECT 1",
               options as unknown as string,
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
         expect(runSQL.called).toBe(false);
      },
   );

   it("accepts undefined / null options as 'no options'", async () => {
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT 1",
         undefined as unknown as string,
      );
      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT 1",
         null as unknown as string,
      );

      expect(runSQL.callCount).toBe(2);
   });

   /**
    * `JSON.parse("null")` / `'"foo"'` / `'42'` / `'[1,2,3]'` all parse to
    * non-objects. Without a guard, `runSQLOptions.abortSignal` would crash
    * on `null` (→ 500) or `runSQLOptions.rowLimit = ...` would mutate the
    * caller's array / coerce a primitive and pass that to the connector.
    * Reject at the controller boundary alongside the other type guards.
    */
   it.each([
      ["JSON null", "null"],
      ["JSON string", '"hello"'],
      ["JSON number", "42"],
      ["JSON boolean", "true"],
      ["JSON array", "[1,2,3]"],
   ])(
      "rejects non-object JSON options (%s) with BadRequestError",
      async (_label, options) => {
         const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
         const { controller } = buildController(runSQL);

         await expect(
            controller.getConnectionQueryData(
               "env",
               "conn",
               "SELECT 1",
               options,
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
         expect(runSQL.called).toBe(false);
      },
   );

   it("rejects malformed JSON options with BadRequestError", async () => {
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT 1",
            "{not json",
         ),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(runSQL.called).toBe(false);
   });
});

/**
 * Tests for the streaming branch of
 * {@link ConnectionController.getConnectionQueryData}. When the
 * connection implements `canStream`, the controller routes through
 * `streamSqlWithBudget` so the byte cap can fire mid-stream. The
 * row cap is still enforced via `RunSQLOptions.rowLimit = cap+1` on
 * the way in, plus an overflow check on the way out (same sentinel
 * pattern as the non-streaming path).
 */
describe("ConnectionController.getConnectionQueryData streaming", () => {
   const originalRowsEnv = process.env.PUBLISHER_MAX_QUERY_ROWS;
   const originalBytesEnv = process.env.PUBLISHER_MAX_RESPONSE_BYTES;

   afterEach(() => {
      sinon.restore();
      if (originalRowsEnv === undefined) {
         delete process.env.PUBLISHER_MAX_QUERY_ROWS;
      } else {
         process.env.PUBLISHER_MAX_QUERY_ROWS = originalRowsEnv;
      }
      if (originalBytesEnv === undefined) {
         delete process.env.PUBLISHER_MAX_RESPONSE_BYTES;
      } else {
         process.env.PUBLISHER_MAX_RESPONSE_BYTES = originalBytesEnv;
      }
   });

   /**
    * Build a controller backed by a fake `StreamingConnection`.
    * `seenSql` and `seenOptions` let tests assert the controller
    * passed the SQL straight through and forwarded the budget-derived
    * `rowLimit` to the driver.
    */
   function buildStreamingController(opts: {
      rows: QueryRecord[];
      honorRowLimit?: boolean;
   }): {
      controller: ConnectionController;
      seenSql: { value: string | undefined };
      seenOptions: { value: RunSQLOptions | undefined };
   } {
      const { rows, honorRowLimit = true } = opts;
      const seenSql = { value: undefined as string | undefined };
      const seenOptions = { value: undefined as RunSQLOptions | undefined };
      const fakeConnection = {
         canStream(): true {
            return true;
         },
         async *runSQLStream(
            sql: string,
            options?: RunSQLOptions,
         ): AsyncIterableIterator<QueryRecord> {
            seenSql.value = sql;
            seenOptions.value = options;
            const limit =
               honorRowLimit && typeof options?.rowLimit === "number"
                  ? options.rowLimit
                  : rows.length;
            for (let i = 0; i < Math.min(rows.length, limit); i += 1) {
               yield rows[i];
            }
         },
      } as unknown as StreamingConnection;
      const fakeEnv = {
         assertCanAdmitQuery: sinon.stub().returns(undefined),
      };
      const fakeStore = {
         getEnvironment: sinon.stub().resolves(fakeEnv),
      } as unknown as EnvironmentStore;
      const controller = new ConnectionController(fakeStore);
      sinon
         .stub(
            controller as unknown as {
               getMalloyConnection: (...args: unknown[]) => Promise<Connection>;
            },
            "getMalloyConnection",
         )
         .resolves(fakeConnection as unknown as Connection);
      return { controller, seenSql, seenOptions };
   }

   it("routes through runSQLStream and asks the driver for cap+1 rows", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "5";
      const rows = [{ a: 1 }, { a: 2 }];
      const { controller, seenSql, seenOptions } = buildStreamingController({
         rows,
      });

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT a FROM t",
         "",
      );

      expect(seenSql.value).toBe("SELECT a FROM t");
      expect(seenOptions.value?.rowLimit).toBe(6);
      const parsed = JSON.parse(result.data ?? "") as { rows: QueryRecord[] };
      expect(parsed.rows).toEqual(rows);
   });

   it("preserves a caller-supplied rowLimit when it is below the cap+1 ceiling", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "100";
      const { controller, seenOptions } = buildStreamingController({
         rows: [{ a: 1 }],
      });

      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT a FROM t",
         JSON.stringify({ rowLimit: 10 }),
      );

      expect(seenOptions.value?.rowLimit).toBe(10);
   });

   it("throws PayloadTooLargeError when the stream yields more than the row cap", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "2";
      const rows = [{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }];
      const { controller } = buildStreamingController({
         rows,
         honorRowLimit: false,
      });

      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT a FROM t",
            "",
         ),
      ).rejects.toBeInstanceOf(PayloadTooLargeError);
      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT a FROM t",
            "",
         ),
      ).rejects.toThrow("more than 2 rows");
   });

   it("throws PayloadTooLargeError when the stream exceeds the byte cap", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
      process.env.PUBLISHER_MAX_RESPONSE_BYTES = "60";
      const big = "x".repeat(40);
      const rows = [{ s: big }, { s: big }, { s: big }];
      const { controller } = buildStreamingController({
         rows,
         honorRowLimit: false,
      });

      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT s FROM t",
            "",
         ),
      ).rejects.toBeInstanceOf(PayloadTooLargeError);
      await expect(
         controller.getConnectionQueryData(
            "env",
            "conn",
            "SELECT s FROM t",
            "",
         ),
      ).rejects.toThrow("exceeded 60 bytes");
   });

   it("disables byte cap when PUBLISHER_MAX_RESPONSE_BYTES=0", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "1000";
      process.env.PUBLISHER_MAX_RESPONSE_BYTES = "0";
      const big = "x".repeat(10_000);
      const rows: QueryRecord[] = Array.from({ length: 3 }, () => ({ s: big }));
      const { controller } = buildStreamingController({
         rows,
         honorRowLimit: false,
      });

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT s FROM t",
         "",
      );

      const parsed = JSON.parse(result.data ?? "") as { rows: QueryRecord[] };
      expect(parsed.rows.length).toBe(3);
   });

   it("omits rowLimit when PUBLISHER_MAX_QUERY_ROWS=0 and no caller limit is given", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "0";
      const rows = [{ a: 1 }, { a: 2 }, { a: 3 }];
      const { controller, seenOptions } = buildStreamingController({
         rows,
         honorRowLimit: false,
      });

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT a FROM t",
         "",
      );

      expect(seenOptions.value?.rowLimit).toBeUndefined();
      const parsed = JSON.parse(result.data ?? "") as { rows: QueryRecord[] };
      expect(parsed.rows.length).toBe(3);
   });

   it("replaces caller-supplied abortSignal with a real AbortSignal", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "10";
      const { controller, seenOptions } = buildStreamingController({
         rows: [{ a: 1 }],
      });

      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT 1",
         JSON.stringify({ abortSignal: {} }),
      );

      // The controller clears the caller-supplied abortSignal (which
      // arrives over the wire as a JSON-shaped placeholder, not a
      // real AbortSignal), and the streaming helper installs its own
      // so it can abort the iterator on overflow.
      const signal = seenOptions.value?.abortSignal;
      expect(signal).toBeInstanceOf(AbortSignal);
      expect(signal?.aborted).toBe(false);
   });
});

/**
 * Tests that the controller calls {@link Environment.assertCanAdmitQuery}
 * *before* doing any disk / DB work. Under back-pressure the publisher
 * must shed load immediately with HTTP 503 — not start a query and
 * crash partway through.
 */
describe("ConnectionController.getConnectionQueryData admission gate", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("re-throws the ServiceUnavailableError without ever calling the connector", async () => {
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { ServiceUnavailableError } = await import("../errors");
      const assertCanAdmitQuery = sinon
         .stub()
         .throws(
            new ServiceUnavailableError(
               'Publisher is under memory pressure and cannot accept new queries (environment "env").',
            ),
         );
      const { controller, runSQL: capturedRunSQL } = buildController(runSQL, {
         assertCanAdmitQuery,
      });

      await expect(
         controller.getConnectionQueryData("env", "conn", "SELECT 1", ""),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);

      // The gate must fire *before* the connector — no SQL should ever
      // have been issued. This is the load-shedding invariant.
      expect(assertCanAdmitQuery.called).toBe(true);
      expect(capturedRunSQL.called).toBe(false);
   });

   it("proceeds normally when the gate is a no-op", async () => {
      const runSQL = sinon.stub().resolves({ rows: [{ a: 1 }], totalRows: 1 });
      const { controller, assertCanAdmitQuery } = buildController(runSQL);

      await controller.getConnectionQueryData("env", "conn", "SELECT 1", "");

      expect(assertCanAdmitQuery.called).toBe(true);
      expect(runSQL.called).toBe(true);
   });
});

describe("ConnectionController.getConnectionQueryData query timeout", () => {
   const originalTimeout = process.env.PUBLISHER_QUERY_TIMEOUT_MS;

   afterEach(() => {
      sinon.restore();
      if (originalTimeout === undefined) {
         delete process.env.PUBLISHER_QUERY_TIMEOUT_MS;
      } else {
         process.env.PUBLISHER_QUERY_TIMEOUT_MS = originalTimeout;
      }
   });

   it("surfaces QueryTimeoutError when the driver outlasts PUBLISHER_QUERY_TIMEOUT_MS, with the publisher's AbortSignal delivered to runSQL", async () => {
      process.env.PUBLISHER_QUERY_TIMEOUT_MS = "20";
      // The driver "hangs" until the publisher's abort signal
      // fires; on abort it rejects with a driver-shaped error. The
      // controller's runWithQueryTimeout wrapper converts that into
      // QueryTimeoutError (mapped to HTTP 504 by `errors.ts`).
      let observedSignal: AbortSignal | undefined;
      const runSQL = sinon.stub().callsFake(
         (_sql: string, opts: RunSQLOptions) =>
            new Promise((_resolve, reject) => {
               observedSignal = opts.abortSignal;
               opts.abortSignal?.addEventListener("abort", () =>
                  reject(new Error("driver aborted")),
               );
            }),
      );
      const { controller } = buildController(runSQL);

      const { QueryTimeoutError } = await import("../errors");
      await expect(
         controller.getConnectionQueryData("env", "conn", "SELECT 1", ""),
      ).rejects.toBeInstanceOf(QueryTimeoutError);

      // Critical invariant: the abort signal MUST actually fire
      // — without this the driver call would leak past the 504
      // response.
      expect(observedSignal).toBeDefined();
      expect(observedSignal?.aborted).toBe(true);
   });
});

/**
 * Mirrors `buildController` but injects a `manifestTemporaryTable`
 * stub instead of `runSQL`. `getConnectionTemporaryTable` casts the
 * Malloy connection to `PersistSQLResults`, so the fake connection
 * needs that method on its surface for the test to exercise the real
 * code path through admission and the timeout wrapper.
 */
function buildTemporaryTableController(
   manifestTemporaryTable: sinon.SinonStub,
   opts: { assertCanAdmitQuery?: sinon.SinonStub } = {},
): {
   controller: ConnectionController;
   manifestTemporaryTable: sinon.SinonStub;
   assertCanAdmitQuery: sinon.SinonStub;
} {
   const fakeConnection = { manifestTemporaryTable } as unknown as Connection;
   const assertCanAdmitQuery =
      opts.assertCanAdmitQuery ?? sinon.stub().returns(undefined);
   const fakeEnv = { assertCanAdmitQuery };
   const fakeStore = {
      getEnvironment: sinon.stub().resolves(fakeEnv),
   } as unknown as EnvironmentStore;
   const controller = new ConnectionController(fakeStore);
   sinon
      .stub(
         controller as unknown as {
            getMalloyConnection: (...args: unknown[]) => Promise<Connection>;
         },
         "getMalloyConnection",
      )
      .resolves(fakeConnection);
   return { controller, manifestTemporaryTable, assertCanAdmitQuery };
}

/**
 * `getConnectionTemporaryTable` issues a real `CREATE TEMPORARY TABLE
 * AS (<sql>)` against the connector via `manifestTemporaryTable`, so
 * the same three OOM guards as `getConnectionQueryData` must apply:
 * sqlStatement shape, admission, and wall-clock timeout. The
 * per-pod concurrency cap is wired at the route layer in `server.ts`
 * and exercised by `oom_guards.integration.spec.ts`.
 */
describe("ConnectionController.getConnectionTemporaryTable guards", () => {
   const originalTimeout = process.env.PUBLISHER_QUERY_TIMEOUT_MS;

   afterEach(() => {
      sinon.restore();
      if (originalTimeout === undefined) {
         delete process.env.PUBLISHER_QUERY_TIMEOUT_MS;
      } else {
         process.env.PUBLISHER_QUERY_TIMEOUT_MS = originalTimeout;
      }
   });

   it("rejects non-string sqlStatement at the boundary (CodeQL type guard)", async () => {
      const manifestTemporaryTable = sinon.stub().resolves("temp_table_name");
      const { controller } = buildTemporaryTableController(
         manifestTemporaryTable,
      );

      await expect(
         controller.getConnectionTemporaryTable("env", "conn", [
            "SELECT 1",
            "SELECT 2",
         ] as unknown as string),
      ).rejects.toBeInstanceOf(BadRequestError);

      expect(manifestTemporaryTable.called).toBe(false);
   });

   it("re-throws ServiceUnavailableError under back-pressure without touching the connector", async () => {
      const manifestTemporaryTable = sinon.stub().resolves("temp_table_name");
      const { ServiceUnavailableError } = await import("../errors");
      const assertCanAdmitQuery = sinon
         .stub()
         .throws(
            new ServiceUnavailableError(
               'Publisher is under memory pressure and cannot accept new queries (environment "env").',
            ),
         );
      const { controller } = buildTemporaryTableController(
         manifestTemporaryTable,
         { assertCanAdmitQuery },
      );

      await expect(
         controller.getConnectionTemporaryTable("env", "conn", "SELECT 1"),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);

      // Load-shedding invariant: the gate must fire BEFORE any
      // connector work. `CREATE TEMPORARY TABLE` is a real DDL —
      // we must not start it under memory pressure.
      expect(assertCanAdmitQuery.called).toBe(true);
      expect(manifestTemporaryTable.called).toBe(false);
   });

   it("happy path: admission passes through and the connector is called", async () => {
      const manifestTemporaryTable = sinon.stub().resolves("temp_table_name");
      const { controller, assertCanAdmitQuery } = buildTemporaryTableController(
         manifestTemporaryTable,
      );

      const result = await controller.getConnectionTemporaryTable(
         "env",
         "conn",
         "SELECT 1",
      );

      expect(assertCanAdmitQuery.called).toBe(true);
      expect(manifestTemporaryTable.calledOnceWith("SELECT 1")).toBe(true);
      expect(JSON.parse(result.table as string)).toBe("temp_table_name");
   });

   it("surfaces QueryTimeoutError when manifestTemporaryTable outlasts PUBLISHER_QUERY_TIMEOUT_MS", async () => {
      process.env.PUBLISHER_QUERY_TIMEOUT_MS = "20";
      // `manifestTemporaryTable` does not accept an abortSignal, so
      // the timeout is a pure wall-clock guard — the DDL keeps
      // running inside the DB but the HTTP response unblocks as
      // QueryTimeoutError → 504, releasing the slot.
      const manifestTemporaryTable = sinon
         .stub()
         .callsFake(() => new Promise(() => undefined /* never resolves */));
      const { controller } = buildTemporaryTableController(
         manifestTemporaryTable,
      );

      const { QueryTimeoutError } = await import("../errors");
      await expect(
         controller.getConnectionTemporaryTable("env", "conn", "SELECT 1"),
      ).rejects.toBeInstanceOf(QueryTimeoutError);
   });
});

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

   /**
    * Build a controller whose `getMalloyConnection` resolves to a
    * fake `Connection` with a sinon-stubbed `runSQL`. We bypass the
    * normal EnvironmentStore lookup entirely so the test stays a
    * single-file unit test.
    */
   function buildController(runSQL: sinon.SinonStub): {
      controller: ConnectionController;
      runSQL: sinon.SinonStub;
   } {
      const fakeConnection = { runSQL } as unknown as Connection;
      const controller = new ConnectionController(
         {} as unknown as EnvironmentStore,
      );
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
      return { controller, runSQL };
   }

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

   it("clamps a caller-supplied rowLimit that exceeds the cap+1 sentinel", async () => {
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
         abortSignal?: unknown;
      };
      expect(opts.rowLimit).toBe(11);
      expect(opts.abortSignal).toBeUndefined();
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
      const controller = new ConnectionController(
         {} as unknown as EnvironmentStore,
      );
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

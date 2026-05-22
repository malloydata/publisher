import type { Connection } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";

import { BadRequestError, PayloadTooLargeError } from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import { PUBLISHER_CAP_ALIAS } from "../sql_helpers";
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

   it("wraps a SELECT and pushes LIMIT cap+1 to the connection", async () => {
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
      const wrappedSql = runSQL.firstCall.args[0] as string;
      expect(wrappedSql).toContain(`(SELECT a FROM t)`);
      expect(wrappedSql).toContain(PUBLISHER_CAP_ALIAS);
      expect(wrappedSql.endsWith("LIMIT 6")).toBe(true);
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

   it("passes non-SELECT statements through unchanged (no wrap, no cap check)", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "1";
      // Return more rows than the cap to prove we do NOT apply the
      // overflow check on the non-SELECT path.
      const rows = [{ table_name: "a" }, { table_name: "b" }];
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { controller } = buildController(runSQL);

      const result = await controller.getConnectionQueryData(
         "env",
         "conn",
         "SHOW TABLES",
         "",
      );

      expect(runSQL.firstCall.args[0]).toBe("SHOW TABLES");
      const parsed = JSON.parse(result.data ?? "") as { rows: unknown[] };
      expect(parsed.rows.length).toBe(2);
   });

   it("passes multi-statement SQL through unchanged", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "10";
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData(
         "env",
         "conn",
         "SELECT 1; SELECT 2",
         "",
      );

      expect(runSQL.firstCall.args[0]).toBe("SELECT 1; SELECT 2");
   });

   it("disables wrapping when PUBLISHER_MAX_QUERY_ROWS=0", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "0";
      // Even with cap=0, returning many rows must NOT trigger an
      // overflow — the cap is explicitly disabled.
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
      const parsed = JSON.parse(result.data ?? "") as { rows: unknown[] };
      expect(parsed.rows.length).toBe(100);
   });

   it("uses the default cap when PUBLISHER_MAX_QUERY_ROWS is unset", async () => {
      delete process.env.PUBLISHER_MAX_QUERY_ROWS;
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const { controller } = buildController(runSQL);

      await controller.getConnectionQueryData("env", "conn", "SELECT 1", "");

      const wrappedSql = runSQL.firstCall.args[0] as string;
      // Default cap is 100_000, so the wrapper should ask for cap+1 = 100_001.
      expect(wrappedSql.endsWith("LIMIT 100001")).toBe(true);
   });

   /**
    * Express parses repeated query parameters and array-shaped JSON bodies
    * as `string[]`, not `string`. The route handlers up-cast for TypeScript,
    * so we re-validate at the controller boundary. Without this guard, a
    * non-string `sqlStatement` would slip past `indexOfUnquotedSemicolon`
    * inside `wrapWithRowLimit` (array `.indexOf(";")` is always -1), and
    * a multi-statement payload could be wrapped into the LIMIT subquery —
    * the exact dataflow CodeQL flags as
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

   it("forwards parsed RunSQLOptions, scrubbing abortSignal", async () => {
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
      expect(opts.rowLimit).toBe(50);
      expect(opts.abortSignal).toBeUndefined();
   });
});

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { components } from "../api";
import { registerClickhouseServers } from "./connection";

type ClickhouseServer = components["schemas"]["ClickhouseServer"];

/**
 * Exercises the macro against a real ClickHouse server, which is the only thing
 * that can prove the URL, the Parquet format negotiation, and the Basic-auth
 * secret actually line up. Skipped unless CLICKHOUSE_TEST_HOST is set.
 *
 *   docker run -d --name malloy-ch -p 8123:8123 \
 *     -e CLICKHOUSE_USER=malloy -e CLICKHOUSE_PASSWORD=malloy \
 *     clickhouse/clickhouse-server:latest
 *   CLICKHOUSE_TEST_HOST=localhost CLICKHOUSE_TEST_USER=malloy \
 *     CLICKHOUSE_TEST_PASSWORD=malloy bun test clickhouse_servers.integration
 */
const host = process.env.CLICKHOUSE_TEST_HOST;
const port = Number(process.env.CLICKHOUSE_TEST_PORT ?? 8123);
const user = process.env.CLICKHOUSE_TEST_USER;
const password = process.env.CLICKHOUSE_TEST_PASSWORD;

const describeIfClickhouse = host ? describe : describe.skip;

describeIfClickhouse("registerClickhouseServers against a live server", () => {
   let connection: DuckDBConnection;

   const server = (
      overrides: Partial<ClickhouseServer> = {},
   ): ClickhouseServer =>
      ({
         name: "ch",
         host,
         port,
         user,
         password,
         ...overrides,
      }) as ClickhouseServer;

   beforeAll(async () => {
      connection = new DuckDBConnection("duckdb", ":memory:");
      await registerClickhouseServers(connection, [server()]);
   });

   afterAll(async () => {
      await connection?.close();
   });

   it("round-trips a scalar through the HTTP interface", async () => {
      const result = await connection.runSQL(
         `SELECT * FROM ch('SELECT 42 AS answer')`,
      );
      expect(Number(result.rows[0]!["answer"])).toBe(42);
   });

   it("URL-encodes a query containing characters that break a raw query string", async () => {
      // '+' and '&' are exactly what the chsql ch_scan macro corrupts.
      const result = await connection.runSQL(
         `SELECT * FROM ch('SELECT 1 + 1 AS sum_value, ''a&b'' AS amp')`,
      );
      expect(Number(result.rows[0]!["sum_value"])).toBe(2);
      expect(String(result.rows[0]!["amp"])).toBe("a&b");
   });

   it("preserves ClickHouse types through Parquet", async () => {
      const result = await connection.runSQL(
         `SELECT * FROM ch('SELECT toDate(''2025-01-15'') AS d,
                                   toDecimal64(12.34, 2) AS amount,
                                   CAST(NULL AS Nullable(Float64)) AS missing')`,
      );
      const row = result.rows[0]!;
      expect(String(row["d"])).toContain("2025-01-15");
      expect(Number(row["amount"])).toBeCloseTo(12.34, 2);
      expect(row["missing"]).toBeNull();
   });

   it("aggregates server-side so only the rollup crosses the wire", async () => {
      const result = await connection.runSQL(
         `SELECT * FROM ch('SELECT count() AS n FROM numbers(100000)')`,
      );
      expect(Number(result.rows[0]!["n"])).toBe(100000);
   });

   it("rejects a bad password rather than silently returning nothing", async () => {
      const bad = new DuckDBConnection("duckdb", ":memory:");
      try {
         await registerClickhouseServers(bad, [
            server({ name: "bad", password: "definitely-not-the-password" }),
         ]);
         await expect(
            bad.runSQL(`SELECT * FROM bad('SELECT 1 AS x')`),
         ).rejects.toThrow();
      } finally {
         await bad.close();
      }
   });
});

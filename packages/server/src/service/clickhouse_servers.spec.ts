import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { components } from "../api";
import { registerClickhouseServers } from "./connection";
import { assembleEnvironmentConnections } from "./connection_config";

type ClickhouseServer = components["schemas"]["ClickhouseServer"];
type ApiConnection = components["schemas"]["Connection"];

/**
 * These run against a real in-memory DuckDB rather than asserting on generated
 * SQL strings: DuckDB's own parser and secret manager are the oracle for
 * "is this valid", and a hand-written matcher would happily pass on SQL DuckDB
 * rejects. Registering a macro never contacts ClickHouse, so no server is
 * needed here -- the gated integration test below covers the wire.
 */
describe("registerClickhouseServers", () => {
   let connection: DuckDBConnection;
   let databaseDir: string;

   // Each test gets its own database file rather than ":memory:". Malloy shares
   // a single DuckDB instance between connections whose config hashes equal, so
   // every ":memory:" connection in the process -- including other spec files
   // running alongside this one -- would otherwise see each other's macros and
   // secrets, and the absence assertions below would depend on test ordering.
   beforeEach(async () => {
      databaseDir = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-clickhouse-"),
      );
      connection = new DuckDBConnection(
         "duckdb",
         path.join(databaseDir, "test.duckdb"),
      );
   });

   afterEach(async () => {
      await connection.close();
      await fs.rm(databaseDir, { recursive: true, force: true });
   });

   const server = (
      overrides: Partial<ClickhouseServer> = {},
   ): ClickhouseServer =>
      ({
         name: "ch",
         host: "localhost",
         port: 18123,
         ...overrides,
      }) as ClickhouseServer;

   const macroBody = async (name: string): Promise<string> => {
      const result = await connection.runSQL(
         `SELECT macro_definition FROM duckdb_functions() WHERE function_name = '${name}'`,
      );
      const rows = result.rows ?? [];
      expect(rows.length).toBeGreaterThan(0);
      return String(rows[0]!["macro_definition"]);
   };

   it("registers a callable table macro named after the server", async () => {
      await registerClickhouseServers(connection, [server({ name: "events" })]);

      const result = await connection.runSQL(
         `SELECT count(*) AS n FROM duckdb_functions()
          WHERE function_name = 'events' AND function_type = 'table_macro'`,
      );
      expect(Number(result.rows[0]!["n"])).toBe(1);
   });

   it("URL-encodes the query argument", async () => {
      // The chsql ch_scan macro concatenates the query raw, so a '+' or '&' in
      // the query silently corrupts the request. Pin that we encode.
      await registerClickhouseServers(connection, [server()]);
      expect(await macroBody("ch")).toContain("url_encode");
   });

   it("keeps credentials out of the macro body", async () => {
      await registerClickhouseServers(connection, [
         server({ user: "malloy", password: "sup3rsecret" }),
      ]);

      const body = await macroBody("ch");
      expect(body).not.toContain("sup3rsecret");
      expect(body).not.toContain("password");
      // ...and out of the URL the macro builds, since ClickHouse logs URLs.
      expect(body).not.toContain("malloy");
   });

   it("stores credentials in a DuckDB secret scoped to the server", async () => {
      await registerClickhouseServers(connection, [
         server({ user: "malloy", password: "sup3rsecret" }),
      ]);

      const secrets = await connection.runSQL(
         `SELECT name, type, scope FROM duckdb_secrets()
          WHERE name = 'secret_clickhouse_ch'`,
      );
      expect(secrets.rows).toHaveLength(1);
      expect(String(secrets.rows[0]!["type"])).toBe("http");
      expect(String(secrets.rows[0]!["scope"])).toContain(
         "http://localhost:18123",
      );
   });

   it("creates no secret for an anonymous server", async () => {
      await registerClickhouseServers(connection, [server()]);
      // Scoped to this server's secret name: DuckDB's secret manager is shared
      // process-wide, so a global count would pick up other specs' secrets.
      const secrets = await connection.runSQL(
         `SELECT name FROM duckdb_secrets() WHERE name = 'secret_clickhouse_ch'`,
      );
      expect(secrets.rows).toHaveLength(0);
   });

   it("uses https and the configured port when useSsl is set", async () => {
      await registerClickhouseServers(connection, [
         server({
            name: "secure",
            host: "ch.example.com",
            port: 8443,
            useSsl: true,
         }),
      ]);
      expect(await macroBody("secure")).toContain(
         "https://ch.example.com:8443/",
      );
   });

   it("defaults to port 8123", async () => {
      await registerClickhouseServers(connection, [
         { name: "defaulted", host: "ch.example.com" } as ClickhouseServer,
      ]);
      expect(await macroBody("defaulted")).toContain("ch.example.com:8123");
   });

   it("passes the default database through as a URL parameter", async () => {
      await registerClickhouseServers(connection, [
         server({ database: "demo" }),
      ]);
      expect(await macroBody("ch")).toContain("database=demo");
   });

   it("omits the database parameter when unset", async () => {
      await registerClickhouseServers(connection, [server()]);
      expect(await macroBody("ch")).not.toContain("database=");
   });

   it("registers each server independently", async () => {
      await registerClickhouseServers(connection, [
         server({ name: "warm", host: "a.example.com" }),
         server({ name: "replica", host: "b.example.com" }),
      ]);
      expect(await macroBody("warm")).toContain("a.example.com");
      expect(await macroBody("replica")).toContain("b.example.com");
   });

   it("rejects a reserved SQL word as a name", async () => {
      // `primary` matches the identifier pattern but a model calling
      // primary('SELECT ...') would not parse, so fail at config time instead.
      await expect(
         registerClickhouseServers(connection, [server({ name: "primary" })]),
      ).rejects.toThrow(/reserved SQL word in DuckDB/);
   });

   it("rejects a name that is not a SQL identifier", async () => {
      // Without this the name is interpolated straight into DDL.
      await expect(
         registerClickhouseServers(connection, [
            server({ name: "ch; DROP TABLE users; --" }),
         ]),
      ).rejects.toThrow(/expected a SQL identifier/);
   });

   it("rejects duplicate names instead of silently overwriting", async () => {
      await expect(
         registerClickhouseServers(connection, [
            server({ name: "dup", host: "a.example.com" }),
            server({ name: "dup", host: "b.example.com" }),
         ]),
      ).rejects.toThrow(/Duplicate clickhouseServers\[\]\.name 'dup'/);
   });

   it("rejects a missing host", async () => {
      await expect(
         registerClickhouseServers(connection, [
            { name: "nohost", host: "" } as ClickhouseServer,
         ]),
      ).rejects.toThrow(/Missing clickhouseServers\[\]\.host/);
   });

   it("escapes single quotes in configured values", async () => {
      // A quote in a host would otherwise terminate the SQL literal.
      await registerClickhouseServers(connection, [
         server({ name: "quoted", host: "it's.example.com" }),
      ]);
      expect(await macroBody("quoted")).toContain("it''s.example.com");
   });

   it("is idempotent across repeated registration", async () => {
      await registerClickhouseServers(connection, [server()]);
      await registerClickhouseServers(connection, [server()]);
      const result = await connection.runSQL(
         `SELECT count(*) AS n FROM duckdb_functions() WHERE function_name = 'ch'`,
      );
      expect(Number(result.rows[0]!["n"])).toBe(1);
   });

   it("does nothing when no servers are configured", async () => {
      await registerClickhouseServers(connection, []);
      const result = await connection.runSQL(
         `SELECT count(*) AS n FROM duckdb_functions()
          WHERE function_type = 'table_macro' AND function_name = 'ch'`,
      );
      expect(Number(result.rows[0]!["n"])).toBe(0);
   });
});

describe("clickhouseServers config validation", () => {
   const assemble = (connection: ApiConnection) =>
      assembleEnvironmentConnections(
         [connection],
         "/tmp/publisher-clickhouse-spec",
      );

   it("accepts a DuckDB connection carrying only clickhouseServers", () => {
      const assembled = assemble({
         name: "ch_only",
         type: "duckdb",
         duckdbConnection: {
            clickhouseServers: [{ name: "ch", host: "localhost" }],
         },
      } as ApiConnection);

      expect(assembled.metadata.get("ch_only")?.clickhouseServers).toHaveLength(
         1,
      );
   });

   it("still rejects a DuckDB connection with no data sources at all", () => {
      expect(() =>
         assemble({
            name: "empty",
            type: "duckdb",
            duckdbConnection: {},
         } as ApiConnection),
      ).toThrow(/has no data sources/);
   });

   it("rejects unsupported DuckDB fields such as setupSQL", () => {
      // clickhouseServers widened the surface deliberately; setupSQL must stay out.
      expect(() =>
         assemble({
            name: "sneaky",
            type: "duckdb",
            duckdbConnection: {
               clickhouseServers: [{ name: "ch", host: "localhost" }],
               setupSQL: "ATTACH 'x'",
            },
         } as unknown as ApiConnection),
      ).toThrow(/Unsupported DuckDB connection field\(s\): setupSQL/);
   });
});

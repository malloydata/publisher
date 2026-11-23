// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import type { components } from "../../api";
import type { ConnectionOrigin } from "../../config";
import { installSettings } from "../../test_helpers/settings_env";
import { assembleEnvironmentConnections } from "../connection_config";

/**
 * Each connection type's setup, as the registry dispatches it: the core
 * entry a configured connection assembles to, and the type's refusal of a
 * missing config, plus the type-specific checks that refuse a bad value.
 * A prototype name and an unknown one are refused as unsupported.
 */

type ApiConnection = components["schemas"]["Connection"];

/**
 * The temp directory and the digest a locked DuckDB tier names its private
 * directory with, which differ per machine, as tokens, and every path
 * separator as `/`, so the expectations hold on Windows too.
 */
function stable(value: unknown): unknown {
   return JSON.parse(
      JSON.stringify(value)
         .replaceAll(
            JSON.stringify(fs.realpathSync(os.tmpdir())).slice(1, -1),
            "<tmp>",
         )
         .replace(/(locked-[a-z]+-)[0-9a-f]{16,}/g, "$1<digest>")
         // Windows joins paths with a backslash (JSON-escaped here).
         .replaceAll("\\\\", "/"),
   );
}

function assemble(
   type: string | undefined,
   key?: string,
   config?: Record<string, unknown>,
   origin: ConnectionOrigin = "connection",
): unknown {
   const connection = {
      name: "c",
      type,
      ...(key ? { [key]: config } : {}),
   } as unknown as ApiConnection;
   return stable(
      assembleEnvironmentConnections([connection], "/env", origin).pojo
         .connections["c"],
   );
}

const refusal = (
   type: string | undefined,
   key?: string,
   config?: Record<string, unknown>,
) => {
   try {
      assemble(type, key, config);
   } catch (error) {
      return error instanceof Error ? error.message : String(error);
   }
   throw new Error(`${String(type)} was not refused`);
};

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("connection setup is dispatched by type", () => {
   it("assembles each type's configured connection to its core entry", () => {
      expect(
         assemble("postgres", "postgresConnection", {
            host: "db",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "p",
            statementTimeoutMilliseconds: 1000,
         }),
      ).toEqual({
         is: "postgres",
         host: "db",
         port: 5432,
         username: "u",
         password: "p",
         databaseName: "d",
      });
      expect(
         assemble("postgres", "postgresConnection", {
            host: "db",
            sslmode: "no-verify",
         }),
      ).toEqual({
         is: "postgres",
         host: "db",
         connectionString: "postgresql://db?sslmode=no-verify",
      });
      expect(
         assemble("mysql", "mysqlConnection", {
            host: "h",
            port: 3306,
            database: "d",
            user: "u",
            password: "p",
         }),
      ).toEqual({
         is: "mysql",
         host: "h",
         port: 3306,
         user: "u",
         password: "p",
         database: "d",
      });
      expect(
         assemble("sqlserver", "sqlserverConnection", {
            server: "s",
            port: 1433,
            instanceName: "i",
         }),
      ).toEqual({
         is: "sqlserver",
         server: "s",
         port: 1433,
         instanceName: "i",
      });
      expect(assemble("bigquery", "bigqueryConnection", {})).toEqual({
         is: "bigquery",
      });
      expect(
         assemble("snowflake", "snowflakeConnection", {
            account: "a",
            username: "u",
            password: "p",
            warehouse: "w",
            database: "d",
            schema: "s",
            role: "r",
            responseTimeoutMilliseconds: null,
         }),
      ).toEqual({
         is: "snowflake",
         account: "a",
         username: "u",
         password: "p",
         warehouse: "w",
         database: "d",
         schema: "s",
         role: "r",
         timeoutMs: null,
         poolMin: 1,
         poolMax: 20,
      });
      expect(
         assemble("trino", "trinoConnection", {
            server: "http://t",
            port: 8080,
            catalog: "c",
            schema: "s",
            user: "u",
            password: "p",
         }),
      ).toEqual({
         is: "trino",
         server: "http://t:8080",
         port: 8080,
         catalog: "c",
         schema: "s",
         user: "u",
      });
      expect(
         assemble("databricks", "databricksConnection", {
            host: "h",
            path: "/sql/1.0/x",
            token: "t",
            defaultCatalog: "c",
         }),
      ).toEqual({
         is: "databricks",
         host: "h",
         path: "/sql/1.0/x",
         token: "t",
         defaultCatalog: "c",
      });
      expect(
         assemble("duckdb", "duckdbConnection", {
            attachedDatabases: [
               {
                  name: "pg1",
                  type: "postgres",
                  postgresConnection: { host: "h", userName: "u" },
               },
            ],
         }),
      ).toEqual({
         is: "duckdb",
         databasePath: "/env/c.duckdb",
         tempDirectory: "<tmp>/publisher-duckdb/locked-attached-<digest>",
      });
      expect(
         assemble("motherduck", "motherduckConnection", {
            accessToken: "t",
            database: "d",
         }),
      ).toEqual({
         is: "duckdb",
         databasePath: "md:d?attach_mode=single",
         motherDuckToken: "t",
         tempDirectory: "<tmp>/publisher-duckdb/locked-motherduck-<digest>",
      });
      expect(
         assemble("ducklake", "ducklakeConnection", {
            catalog: { postgresConnection: { host: "h", databaseName: "d" } },
            storage: {
               s3Connection: { accessKeyId: "a", secretAccessKey: "s" },
            },
         }),
      ).toEqual({
         is: "duckdb",
         databasePath: ":memory:",
         tempDirectory: "<tmp>/publisher-duckdb/locked-ducklake-<digest>",
      });
   });

   it("assembles the same entry for a storage destination", () => {
      const config = { host: "db", port: 5432, databaseName: "d" };
      expect(
         assemble(
            "postgres",
            "postgresConnection",
            config,
            "storage-destination",
         ),
      ).toEqual(assemble("postgres", "postgresConnection", config));
   });

   it("refuses each type's missing config in its own words", () => {
      // Postgres, MySQL and BigQuery assemble from their defaults.
      for (const type of ["postgres", "mysql", "bigquery"]) {
         expect(assemble(type)).toEqual({ is: type });
      }
      const missing: Record<string, string> = {
         sqlserver: "SQL Server connection configuration is missing.",
         snowflake: "Snowflake connection configuration is missing.",
         trino: "Trino connection configuration is missing.",
         databricks: "Databricks connection configuration is missing.",
         duckdb: "DuckDB connection configuration is missing.",
         motherduck: "MotherDuck connection configuration is missing.",
         ducklake: "DuckLake connection configuration is missing.",
      };
      for (const [type, message] of Object.entries(missing)) {
         expect(refusal(type)).toBe(message);
      }
      expect(refusal("publisher")).toStartWith(
         "Publisher proxy connection 'c' is disabled in this deployment.",
      );
   });

   it("runs each type's own checks on a bad value", () => {
      expect(
         refusal("postgres", "postgresConnection", {
            host: "db",
            sslmode: "bogus",
         }),
      ).toBe(
         "Connection 'c' has unsupported sslmode 'bogus' (expected disable | no-verify | verify-ca | verify-full).",
      );
      expect(
         refusal("bigquery", "bigqueryConnection", {
            defaultProjectId: "proj",
            impersonateServiceAccount: "a@b.c",
         }),
      ).toStartWith(
         "Connection 'c' sets impersonateServiceAccount but no billingProjectId.",
      );
      expect(refusal("snowflake", "snowflakeConnection", {})).toBe(
         "Snowflake account is required.",
      );
      expect(refusal("trino", "trinoConnection", { server: "t" })).toBe(
         'Invalid Trino connection: expected "http://server:port" or "https://server:port".',
      );
      expect(
         refusal("databricks", "databricksConnection", {
            host: "h",
            path: "/sql",
            oauthClientId: "id",
         }),
      ).toBe(
         "Databricks requires either a personal access token or OAuth M2M client ID and secret.",
      );
      expect(
         refusal("duckdb", "duckdbConnection", { setupSQL: "SET threads=1" }),
      ).toStartWith(
         'setupSQL on DuckDB connection "c" is disabled in this deployment.',
      );
      expect(
         refusal("motherduck", "motherduckConnection", { database: "d" }),
      ).toBe("MotherDuck access token is required.");
      expect(
         refusal("ducklake", "ducklakeConnection", {
            catalog: { postgresConnection: { host: "h", sslmode: "bogus" } },
         }),
      ).toBe(
         "DuckLake catalog of connection 'c' has unsupported sslmode 'bogus' (expected disable | no-verify | verify-ca | verify-full).",
      );
   });

   it("refuses a prototype name and an unknown type", () => {
      for (const type of ["constructor", "redshift", undefined]) {
         expect(refusal(type)).toBe(
            `Unsupported connection type: ${String(type)}`,
         );
      }
   });
});

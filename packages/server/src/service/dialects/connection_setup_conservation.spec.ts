// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { components } from "../../api";
import type { ConnectionOrigin } from "../../config";
import { installSettings } from "../../test_helpers/settings_env";
import { assembleEnvironmentConnections } from "../connection_config";

/**
 * The connection setup the registry took over from connection_config.ts and
 * connection.ts, run for every connection type (and for types no entry
 * names) and compared with what the code produced before the move,
 * recorded in connection_setup.golden.json: each fixture's assembled core
 * entry, or the message it is refused with, under both origins.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_CONNECTION_SETUP=1 bun test <this file>`.
 */

type ApiConnection = components["schemas"]["Connection"];

const GOLDEN = path.join(__dirname, "connection_setup.golden.json");
const ORIGINS: ConnectionOrigin[] = ["connection", "storage-destination"];
const KEY_JSON = JSON.stringify({
   project_id: "proj",
   client_email: "svc@proj.iam.gserviceaccount.com",
   private_key: "k",
});

/** Per type: its sub-config key and the sub-configs to assemble it with. */
const FIXTURES: Record<string, [string, Record<string, unknown>[]]> = {
   postgres: [
      "postgresConnection",
      [
         {
            host: "db",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "p",
            statementTimeoutMilliseconds: 1000,
         },
         { connectionString: "postgres://u:p@h:5432/d?sslmode=no-verify" },
         { host: "db", sslmode: "no-verify" },
         { host: "db", sslmode: "bogus" },
         { host: "db", statementTimeoutMilliseconds: -1 },
      ],
   ],
   mysql: [
      "mysqlConnection",
      [{ host: "h", port: 3306, database: "d", user: "u", password: "p" }],
   ],
   sqlserver: [
      "sqlserverConnection",
      [
         {
            server: "s",
            port: 1433,
            database: "d",
            user: "u",
            password: "p",
            encrypt: true,
            trustServerCertificate: false,
            instanceName: "",
         },
         { server: "s", port: 1433, instanceName: "i" },
      ],
   ],
   bigquery: [
      "bigqueryConnection",
      [
         {
            defaultProjectId: "proj",
            serviceAccountKeyJson: KEY_JSON,
            location: "US",
            dbmeta: "true",
         },
         { defaultProjectId: "proj", impersonateServiceAccount: "a@b.c" },
         { defaultProjectId: "proj", dbmeta: "maybe" },
         { serviceAccountKeyJson: "{not json" },
      ],
   ],
   snowflake: [
      "snowflakeConnection",
      [
         {
            account: "a",
            username: "u",
            password: "p",
            warehouse: "w",
            database: "d",
            schema: "s",
            role: "r",
            responseTimeoutMilliseconds: null,
         },
         { account: "a", username: "u", privateKey: "not a key" },
      ],
   ],
   trino: [
      "trinoConnection",
      [
         {
            server: "http://t",
            port: 8080,
            catalog: "c",
            schema: "s",
            user: "u",
            password: "p",
         },
         { server: "t" },
      ],
   ],
   databricks: [
      "databricksConnection",
      [
         { host: "h", path: "/sql/1.0/x", token: "t", defaultCatalog: "c" },
         { host: "h", path: "/sql", oauthClientId: "id" },
      ],
   ],
   duckdb: [
      "duckdbConnection",
      [
         {
            attachedDatabases: [
               {
                  name: "pg1",
                  type: "postgres",
                  postgresConnection: { host: "h", userName: "u" },
               },
            ],
         },
         { attachedDatabases: [{ name: "bad name", type: "postgres" }] },
         { setupSQL: "SET threads=1" },
      ],
   ],
   motherduck: [
      "motherduckConnection",
      [{ accessToken: "t", database: "d" }, { database: "d" }],
   ],
   ducklake: [
      "ducklakeConnection",
      [
         {
            catalog: {
               postgresConnection: { host: "h", databaseName: "d" },
            },
            storage: {
               s3Connection: { accessKeyId: "a", secretAccessKey: "s" },
            },
         },
         { catalog: { postgresConnection: { host: "h", sslmode: "bogus" } } },
         { storage: {} },
      ],
   ],
   publisher: [
      "publisherConnection",
      [
         {
            connectionUri: "http://p:4000/api/v0/environments/e/connections/c",
            accessToken: "t",
         },
         { connectionUri: "not a uri" },
      ],
   ],
};

/** Every case: a bare and an empty one per type, then the type's fixtures. */
function cases(): [string, ApiConnection][] {
   const out: [string, ApiConnection][] = [];
   for (const [type, [key, configs]] of Object.entries(FIXTURES)) {
      const base = { name: "c", type } as ApiConnection;
      out.push([`${type} bare`, base]);
      out.push([`${type} empty`, { ...base, [key]: {} } as ApiConnection]);
      configs.forEach((config, i) =>
         out.push([
            `${type} #${i}`,
            { ...base, [key]: config } as ApiConnection,
         ]),
      );
   }
   for (const type of ["redshift", "constructor", "toString", undefined]) {
      out.push([
         `unregistered ${String(type)}`,
         { name: "c", type } as unknown as ApiConnection,
      ]);
   }
   return out;
}

function run(connection: ApiConnection, origin: ConnectionOrigin): unknown {
   try {
      const assembled = assembleEnvironmentConnections(
         [structuredClone(connection)],
         "/env",
         origin,
      );
      return { entry: assembled.pojo.connections["c"] ?? null };
   } catch (error) {
      return { error: error instanceof Error ? error.message : String(error) };
   }
}

/**
 * The temp directory and the digest a locked DuckDB tier names its private
 * directory with, which differ per machine, as tokens.
 */
function stable(value: unknown): unknown {
   return JSON.parse(
      JSON.stringify(value)
         .replaceAll(
            JSON.stringify(fs.realpathSync(os.tmpdir())).slice(1, -1),
            "<tmp>",
         )
         .replace(/(locked-[a-z]+-)[0-9a-f]{16,}/g, "$1<digest>"),
   );
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("connection setup is unchanged by the registry", () => {
   it("assembles every type's fixtures as before the move", () => {
      const actual: Record<string, unknown> = {};
      for (const [label, connection] of cases()) {
         for (const origin of ORIGINS) {
            actual[`${label} (${origin})`] = stable(run(connection, origin));
         }
      }
      if (process.env.RECORD_CONNECTION_SETUP === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   });
});

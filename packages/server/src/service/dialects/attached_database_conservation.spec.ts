// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { components } from "../../api";
import { installSettings } from "../../test_helpers/settings_env";
import { testConnectionConfig } from "../connection";

/**
 * The attached-database setup the registry took over from connection.ts
 * (each type's attach and connection-test probe), run for every type an
 * attached database may name (every connection type, the object-storage
 * types, and types nothing names) and compared with what the code produced
 * before the move, recorded in attached_database.golden.json: the status a
 * DuckDB connection test reports with that one database attached.
 *
 * No case reaches a network: each either fails before its attach loads an
 * extension or dials a closed local port.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_ATTACHED_DATABASE=1 bun test <this file>`.
 */

type AttachedDatabase = components["schemas"]["AttachedDatabase"];

const GOLDEN = path.join(__dirname, "attached_database.golden.json");

const TYPES = [
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
   "gcs",
   "s3",
   "azure",
   "constructor",
   "toString",
   "nope",
];

/** Per type, the configured attachments beyond a bare one. */
const CONFIGURED: Record<string, Partial<AttachedDatabase>[]> = {
   postgres: [
      {
         postgresConnection: {
            host: "127.0.0.1",
            port: 1,
            databaseName: "d",
            userName: "u",
            password: "p",
         },
      },
   ],
   bigquery: [
      { bigqueryConnection: { defaultProjectId: "proj" } },
      { bigqueryConnection: { serviceAccountKeyJson: "{not json" } },
   ],
   snowflake: [
      { snowflakeConnection: { account: "a", username: "u" } },
      { snowflakeConnection: { username: "u", password: "p" } },
   ],
};

function cases(): [string, AttachedDatabase][] {
   const out: [string, AttachedDatabase][] = [];
   for (const type of TYPES) {
      const base = { name: "att", type } as AttachedDatabase;
      out.push([`${type} bare`, base]);
      (Object.hasOwn(CONFIGURED, type) ? CONFIGURED[type] : []).forEach(
         (extra, i) =>
            out.push([
               `${type} #${i}`,
               { ...base, ...extra } as AttachedDatabase,
            ]),
      );
   }
   return out;
}

/** The temp directory, which differs per machine, as a token. */
function stable(value: unknown): unknown {
   return JSON.parse(
      JSON.stringify(value).replaceAll(
         JSON.stringify(fs.realpathSync(os.tmpdir())).slice(1, -1),
         "<tmp>",
      ),
   );
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("attached databases are unchanged by the registry", () => {
   it("tests every type's attachment as before the move", async () => {
      const actual: Record<string, unknown> = {};
      for (const [label, attachedDb] of cases()) {
         const status = await testConnectionConfig({
            name: "probe",
            type: "duckdb",
            duckdbConnection: { attachedDatabases: [attachedDb] },
         });
         actual[label] = stable(status);
      }
      if (process.env.RECORD_ATTACHED_DATABASE === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   }, 120_000);
});

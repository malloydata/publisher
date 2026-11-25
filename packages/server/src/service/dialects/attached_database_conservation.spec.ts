// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../api";
import { installSettings } from "../../test_helpers/settings_env";
import { testConnectionConfig } from "../connection";

/**
 * Each attached-database type's attach, as the registry dispatches it: the
 * status a DuckDB connection test reports with that one database attached.
 * One case per type that attaches (its own refusal of a missing config, and
 * for Postgres a real attach that dials a closed local port), the types that
 * do not attach, a prototype name and an unknown one.
 *
 * No case reaches a network: each either fails before its attach loads an
 * extension or dials a closed local port.
 */

type AttachedDatabase = components["schemas"]["AttachedDatabase"];

async function attachError(
   type: string,
   extra: Partial<AttachedDatabase> = {},
): Promise<string | undefined> {
   const status = await testConnectionConfig({
      name: "probe",
      type: "duckdb",
      duckdbConnection: {
         attachedDatabases: [
            { name: "att", type, ...extra } as AttachedDatabase,
         ],
      },
   });
   expect(status.status).toBe("failed");
   return status.errorMessage;
}

const FAILED = "Failed to attach database att: ";

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("an attached database is attached by its type", () => {
   it("refuses each attaching type's missing config in its own words", async () => {
      const missing: Record<string, string> = {
         postgres: "PostgreSQL connection configuration missing for: att",
         bigquery: "BigQuery connection configuration missing for: att",
         snowflake: "Snowflake connection configuration missing for: att",
         gcs: "GCS connection configuration missing for: att",
         s3: "S3 connection configuration missing for: att",
         azure: "Azure connection configuration missing for: att",
      };
      for (const [type, message] of Object.entries(missing)) {
         expect(await attachError(type)).toBe(FAILED + message);
      }
   });

   it("runs each attaching type's own checks on a configured attachment", async () => {
      expect(
         await attachError("postgres", {
            postgresConnection: {
               host: "127.0.0.1",
               port: 1,
               databaseName: "d",
               userName: "u",
               password: "p",
            },
         }),
      ).toStartWith(
         `${FAILED}IO Error: Unable to connect to Postgres at "host=127.0.0.1 port=1 dbname=d"`,
      );
      expect(
         await attachError("bigquery", {
            bigqueryConnection: { defaultProjectId: "proj" },
         }),
      ).toBe(
         `${FAILED}BigQuery project_id and service account key required for: att`,
      );
      expect(
         await attachError("snowflake", {
            snowflakeConnection: { account: "a", username: "u" },
         }),
      ).toBe(`${FAILED}Snowflake password is required for: att`);
   });

   it("does not attach the connection types with no attach", async () => {
      for (const type of [
         "trino",
         "databricks",
         "mysql",
         "sqlserver",
         "duckdb",
         "motherduck",
         "ducklake",
         "publisher",
         "nope",
      ]) {
         expect(await attachError(type)).toBe(
            `${FAILED}Unsupported database type: ${type}`,
         );
      }
   });

   it("refuses a prototype name before any attach", async () => {
      expect(await attachError("constructor")).toBe(
         'attachedDatabases[0].type on connection "probe" is invalid: it is not an attached database type.',
      );
   });
});

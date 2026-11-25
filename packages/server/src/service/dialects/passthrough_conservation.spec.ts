// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../test_helpers/settings_env";
import { BadRequestError } from "../../errors";
import { federateSourceForPassthrough } from ".";
import { wrapPassthrough } from "../materialization/run/materialization_build_session";
import type { FederatedSourceType } from ".";

/**
 * Each connection type's passthrough, as the registry dispatches it: the
 * wrapped SELECT (each passthrough type's own function and argument order)
 * or the refusal, and the federation's refusal of a config missing its
 * credentials.
 *
 * No case reaches a network: each federation fails before it loads an
 * extension.
 */

const NO_PASSTHROUGH = [
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
   "constructor",
   "nope",
];

const wrap = (type: string) =>
   wrapPassthrough(type as FederatedSourceType, "h'andle", "SELECT 'x' AS y");

async function federationError(
   type: string,
   config: Record<string, unknown> = {},
): Promise<string> {
   const connection = new DuckDBConnection("passthrough-conservation");
   try {
      await federateSourceForPassthrough(
         connection,
         type as FederatedSourceType,
         { name: "src", ...config },
      );
      throw new Error("federation was not refused");
   } catch (error) {
      return error instanceof Error ? error.message : String(error);
   } finally {
      await connection.close();
   }
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("the passthrough is dispatched by type", () => {
   it("wraps each passthrough type in its own function", () => {
      expect(wrap("postgres")).toBe(
         "SELECT * FROM postgres_query('h''andle', 'SELECT ''x'' AS y')",
      );
      expect(wrap("bigquery")).toBe(
         "SELECT * FROM bigquery_query('h''andle', 'SELECT ''x'' AS y')",
      );
      expect(wrap("snowflake")).toBe(
         "SELECT * FROM snowflake_query('SELECT ''x'' AS y', 'h''andle')",
      );
   });

   it("refuses to wrap or federate every other type", async () => {
      for (const type of NO_PASSTHROUGH) {
         expect(() => wrap(type)).toThrow(BadRequestError);
         expect(() => wrap(type)).toThrow(
            `Unsupported passthrough source type: ${type}`,
         );
         expect(await federationError(type)).toBe(
            `No native query-passthrough for source type '${type}'`,
         );
      }
   });

   it("refuses each passthrough type's federation without its credentials", async () => {
      expect(await federationError("postgres")).toBe(
         "PostgreSQL connection configuration missing for: src",
      );
      expect(
         await federationError("bigquery", {
            bigqueryConnection: { defaultProjectId: "p" },
         }),
      ).toBe("BigQuery project_id and service account key required for: src");
      expect(
         await federationError("bigquery", {
            bigqueryConnection: { impersonateServiceAccount: "a@b.c" },
         }),
      ).toStartWith(
         "BigQuery connection 'src' uses impersonateServiceAccount, which cannot federate through DuckDB",
      );
      expect(
         await federationError("snowflake", {
            snowflakeConnection: { account: "a", username: "u" },
         }),
      ).toBe("Snowflake privateKey or password is required for: src");
   });
});

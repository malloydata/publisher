// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { installSettings } from "../../test_helpers/settings_env";
import { federateSourceForPassthrough } from ".";
import { wrapPassthrough } from "../materialization/run/materialization_build_session";
import type { FederatedSourceType } from ".";

/**
 * The passthrough the registry took over from connection.ts and the build
 * session (each type's federation and wrap), run for every connection type
 * and for types nothing names, and compared with what the code produced
 * before the move, recorded in passthrough.golden.json: the wrapped SELECT
 * or the refusal, and the refusal of a federation missing its credentials.
 *
 * No case reaches a network: each federation fails before it loads an
 * extension.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_PASSTHROUGH=1 bun test <this file>`.
 */

const GOLDEN = path.join(__dirname, "passthrough.golden.json");

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
   "constructor",
   "nope",
];

/** Per type, federation configs that are refused before any extension. */
const FEDERATIONS: Record<string, Record<string, unknown>[]> = {
   bigquery: [
      { bigqueryConnection: { defaultProjectId: "p" } },
      { bigqueryConnection: { impersonateServiceAccount: "a@b.c" } },
   ],
   snowflake: [
      { snowflakeConnection: { account: "a" } },
      { snowflakeConnection: { account: "a", username: "u" } },
   ],
};

function outcome(run: () => unknown): unknown {
   try {
      return { value: run() };
   } catch (error) {
      return {
         error: error instanceof Error ? error.message : String(error),
         name: error instanceof Error ? error.constructor.name : typeof error,
      };
   }
}

async function federation(
   type: string,
   config: Record<string, unknown>,
): Promise<unknown> {
   const connection = new DuckDBConnection("passthrough-conservation");
   try {
      const handle = await federateSourceForPassthrough(
         connection,
         type as FederatedSourceType,
         { name: "src", ...config },
      );
      return { value: handle };
   } catch (error) {
      return { error: error instanceof Error ? error.message : String(error) };
   } finally {
      await connection.close();
   }
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("the passthrough is unchanged by the registry", () => {
   it("wraps and federates every type as before the move", async () => {
      const actual: Record<string, unknown> = {};
      for (const type of TYPES) {
         actual[`wrap ${type}`] = outcome(() =>
            wrapPassthrough(
               type as FederatedSourceType,
               "h'andle",
               "SELECT 'x' AS y",
            ),
         );
         actual[`federate ${type} bare`] = await federation(type, {});
         const configs = Object.hasOwn(FEDERATIONS, type)
            ? FEDERATIONS[type]
            : [];
         for (const [i, config] of configs.entries()) {
            actual[`federate ${type} #${i}`] = await federation(type, config);
         }
      }
      if (process.env.RECORD_PASSTHROUGH === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   });
});

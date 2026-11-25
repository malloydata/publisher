// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A Snowflake connection that answers from snowflake-sdk's own value
 * conversions instead of a warehouse, for specs that have no credentials.
 *
 * {@link driverValue} is the value the driver hands back for one raw wire
 * value: its `Column` converts it exactly as a statement's result does,
 * under the statement parameters a session carries (the connector pins the
 * NTZ and LTZ formats; TIMESTAMP_TZ falls back to the account's
 * `TIMESTAMP_OUTPUT_FORMAT`).
 */

import { createRequire } from "node:module";
import { PublisherSnowflakeConnection } from "../service/snowflake_connection";

// snowflake-sdk is the connector's dependency, not ours: resolve it from there.
const sdkRequire = createRequire(
   createRequire(require.resolve("@malloydata/db-snowflake")).resolve(
      "snowflake-sdk",
   ),
);
const SdkColumn = sdkRequire("./lib/connection/result/column") as new (
   options: Record<string, unknown>,
   index: number,
   statementParameters: Record<string, unknown>,
   resultVersion: string,
) => { getRowValue(row: unknown): unknown };

/** A session as the connector leaves it: UTC, NTZ and LTZ pinned, TZ not. */
const SESSION = {
   JS_TREAT_INTEGER_AS_BIGINT: true,
   TIMEZONE: "UTC",
   TIMESTAMP_OUTPUT_FORMAT: "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
   TIMESTAMP_NTZ_OUTPUT_FORMAT: "YYYY-MM-DDTHH24:MI:SS.FF3TZH:TZM",
   TIMESTAMP_LTZ_OUTPUT_FORMAT: "YYYY-MM-DDTHH24:MI:SS.FF3TZH:TZM",
   DATE_OUTPUT_FORMAT: "YYYY-MM-DD",
};

/** The value snowflake-sdk hands back for one raw wire value of a column. */
export function driverValue(type: string, raw: string, scale = 0): unknown {
   const column = new SdkColumn(
      { name: "c", type, scale, precision: 38, nullable: true },
      0,
      SESSION,
      "1",
   );
   return column.getRowValue({ values: [raw], _arrayProcessedColumns: [] });
}

/**
 * A Publisher Snowflake connection whose executor answers every statement
 * from `rows` (built fresh per call, as the driver builds them), and a
 * DESCRIBE from `describe`. No pool connection is made.
 */
export function fakeSnowflake(
   rows: () => Record<string, unknown>[],
   describe: { name: string; type: string }[] = [],
): PublisherSnowflakeConnection {
   const connection = new PublisherSnowflakeConnection("snowflake", {
      connOptions: {
         account: "test-account",
         username: "test-user",
         warehouse: "test-warehouse",
         password: "pw",
         jsTreatIntegerAsBigInt: true,
      },
      poolOptions: { min: 0, max: 1 },
   });
   const batch = async (sql: string) => {
      if (/^\s*DESCRIBE/i.test(sql)) return describe;
      if (/^\s*CREATE/i.test(sql)) return [];
      return rows();
   };
   (connection as unknown as { executor: unknown }).executor = {
      batch,
      tryBatch: batch,
      stream: async () =>
         (async function* () {
            yield* rows();
         })(),
      done: async () => {},
   };
   return connection;
}

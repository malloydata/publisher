// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   buildPoolOptions,
   SnowflakeConnection,
} from "@malloydata/db-snowflake";
import type {
   MalloyQueryData,
   QueryRecord,
   QueryValue,
   RunSQLOptions,
} from "@malloydata/malloy";
import type { components } from "../api";
import {
   normalizeSnowflakePrivateKey,
   removeUndefined,
} from "./connection_config";

type ApiSnowflakeConnection = components["schemas"]["SnowflakeConnection"];
type SnowflakeConnOptions = NonNullable<
   NonNullable<
      ConstructorParameters<typeof SnowflakeConnection>[1]
   >["connOptions"]
>;

/**
 * The connection options of every Snowflake connection Publisher builds,
 * password or key pair.
 *
 * Integers are read as the connector's own default reads them
 * (`jsTreatIntegerAsBigInt`, `SnowflakeExecutor.defaultConnectionOptions`),
 * which the connector applies only to options it reads from the environment
 * or a TOML file, never to options it is handed. Without it snowflake-sdk
 * reads a `NUMBER(p,0)` through a JavaScript number, so a value past 2^53
 * comes back rounded while Malloy's dialect says it is exact.
 *
 * Key-pair auth is chosen by presence: a private key selects `SNOWFLAKE_JWT`
 * (a password, if also set, is passed along and unused). Unset and `null`
 * optional fields are dropped: a surviving `null` reaches `makeDigest`, which
 * throws on it.
 */
export function buildSnowflakeConnOptions(
   name: string,
   snowflake: ApiSnowflakeConnection | undefined,
): SnowflakeConnOptions {
   if (!snowflake?.account) {
      throw new Error(`Snowflake account is required for connection ${name}`);
   }
   if (!snowflake.username) {
      throw new Error(`Snowflake username is required for connection ${name}`);
   }
   if (!snowflake.warehouse) {
      throw new Error(`Snowflake warehouse is required for connection ${name}`);
   }
   if (!snowflake.privateKey && !snowflake.password) {
      throw new Error(
         `Snowflake privateKey or password is required for connection ${name}`,
      );
   }
   return {
      account: snowflake.account,
      username: snowflake.username,
      warehouse: snowflake.warehouse,
      ...(snowflake.privateKey
         ? {
              privateKey: normalizeSnowflakePrivateKey(snowflake.privateKey),
              authenticator: "SNOWFLAKE_JWT",
           }
         : {}),
      ...removeUndefined({
         password: snowflake.password,
         privateKeyPass: snowflake.privateKeyPass,
         database: snowflake.database,
         schema: snowflake.schema,
         role: snowflake.role,
      }),
      jsTreatIntegerAsBigInt: true,
   };
}

/** Builds the Snowflake connection a connection config names. */
export function buildSnowflakeConnection(
   name: string,
   snowflake: ApiSnowflakeConnection | undefined,
): SnowflakeConnection {
   return new PublisherSnowflakeConnection(name, {
      connOptions: buildSnowflakeConnOptions(name, snowflake),
      timeoutMs: snowflake?.responseTimeoutMilliseconds ?? undefined,
      // Server-owned pool sizing, not in the API.
      poolOptions: buildPoolOptions({ poolMin: 1, poolMax: 20 }),
   });
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * One value snowflake-sdk returned, in the form DuckDB's connection returns
 * it, which is what a stored copy answers from.
 *
 * - An integer arrives as the driver's `big-integer` wrapper (it carries the
 *   exact value as a native BigInt in `.value`). A safe integer becomes a
 *   number, as without `jsTreatIntegerAsBigInt`; a larger one a BigInt, as
 *   DuckDB returns a BIGINT, so Malloy's result keeps every digit and a
 *   compact answer gives it as a string (`json_utils.bigIntReplacer`).
 * - A DATE or TIMESTAMP_* value arrives as a Date whose own `toJSON` renders
 *   it in the session's output format (`2026-01-15 10:00:00.000 -0800` for a
 *   TIMESTAMP_TZ, `2026-01-15` for a DATE). It becomes a plain Date of the
 *   same instant, so it serializes as ISO 8601 in UTC, as a stored copy's
 *   value does.
 *
 * Plain arrays and objects (a structured column) are walked; anything else is
 * returned as it is.
 */
export function normalizeSnowflakeValue(value: unknown): unknown {
   if (value === null || typeof value !== "object") return value;
   if (value instanceof Date) {
      return Object.hasOwn(value, "toJSON") ? new Date(value.getTime()) : value;
   }
   if (Array.isArray(value)) return value.map(normalizeSnowflakeValue);
   const exact = (value as { value?: unknown }).value;
   if (typeof exact === "bigint" && !isPlainObject(value)) {
      return exact > MAX_SAFE || exact < -MAX_SAFE ? exact : Number(exact);
   }
   if (isPlainObject(value)) {
      return Object.fromEntries(
         Object.entries(value).map(([k, v]) => [k, normalizeSnowflakeValue(v)]),
      );
   }
   return value;
}

function isPlainObject(value: object): boolean {
   const proto = Object.getPrototypeOf(value);
   return proto === Object.prototype || proto === null;
}

function normalizeRecord(row: QueryRecord): QueryRecord {
   const out: QueryRecord = {};
   for (const [key, value] of Object.entries(row)) {
      out[key] = normalizeSnowflakeValue(value) as QueryValue;
   }
   return out;
}

/**
 * A Snowflake connection whose rows hold the values a stored copy's do
 * ({@link normalizeSnowflakeValue}), for query answers and Publisher's own
 * warehouse reads alike.
 */
export class PublisherSnowflakeConnection extends SnowflakeConnection {
   async runSQL(
      sql: string,
      options?: RunSQLOptions,
   ): Promise<MalloyQueryData> {
      const result = await super.runSQL(sql, options);
      return { ...result, rows: result.rows.map(normalizeRecord) };
   }

   async *runSQLStream(
      sqlCommand: string,
      options?: RunSQLOptions,
   ): AsyncIterableIterator<QueryRecord> {
      for await (const row of super.runSQLStream(sqlCommand, options)) {
         yield normalizeRecord(row);
      }
   }
}

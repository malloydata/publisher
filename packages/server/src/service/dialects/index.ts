// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The connection-type registry: one entry per type a connection can be
 * configured as, each in a module of its own. The record is total over
 * {@link ConnectionType}, so a new type does not compile until it has an
 * entry.
 *
 * Keyed by the configured type, never by Malloy's dialect name: see
 * `types.ts`. Looked up with `Object.hasOwn`, so a type string that happens
 * to name an `Object.prototype` member (`constructor`, `toString`) is no
 * entry.
 */

import { BIGQUERY } from "./bigquery";
import { DATABRICKS } from "./databricks";
import { DUCKDB } from "./duckdb";
import { DUCKLAKE } from "./ducklake";
import { MOTHERDUCK } from "./motherduck";
import { MYSQL } from "./mysql";
import { POSTGRES } from "./postgres";
import { PUBLISHER } from "./publisher";
import { SNOWFLAKE } from "./snowflake";
import { SQLSERVER } from "./sqlserver";
import { TRINO } from "./trino";
import type { ConnectionDialect, ConnectionType } from "./types";

export type {
   ConnectionAttributes,
   ConnectionDialect,
   ConnectionType,
} from "./types";

/** Every connection type's entry. */
export const CONNECTION_DIALECTS: Readonly<
   Record<ConnectionType, ConnectionDialect>
> = Object.freeze({
   postgres: POSTGRES,
   bigquery: BIGQUERY,
   snowflake: SNOWFLAKE,
   trino: TRINO,
   databricks: DATABRICKS,
   mysql: MYSQL,
   sqlserver: SQLSERVER,
   duckdb: DUCKDB,
   motherduck: MOTHERDUCK,
   ducklake: DUCKLAKE,
   publisher: PUBLISHER,
});

/** `type` as a registered connection type, or undefined for any other value. */
export function registeredConnectionType(
   type: unknown,
): ConnectionType | undefined {
   return typeof type === "string" && Object.hasOwn(CONNECTION_DIALECTS, type)
      ? (type as ConnectionType)
      : undefined;
}

/** The registry entry for `type`, or undefined for an unregistered one. */
export function connectionDialectOf(
   type: unknown,
): ConnectionDialect | undefined {
   const registered = registeredConnectionType(type);
   return registered === undefined
      ? undefined
      : CONNECTION_DIALECTS[registered];
}

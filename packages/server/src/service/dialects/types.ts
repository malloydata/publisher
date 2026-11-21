// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shape of one connection type's entry in the registry (`index.ts`): what
 * Publisher knows about a connection by its configured type (`postgres`,
 * `ducklake`, ...), as opposed to what Malloy's dialect for it can do
 * (`materialization/domain/dialect.ts`, keyed by Malloy's `dialectName`).
 *
 * The two keys differ: `duckdb`, `motherduck` and `ducklake` share the
 * dialect `duckdb`, and Redshift connects under the `postgres` type. A branch
 * keyed on the connection type reads this registry; one keyed on the dialect
 * reads the capability table. Neither is derived from the other at a branch.
 */

import type { components } from "../../api";
import type { ConnectionOrigin } from "../../config";

/** A connection's configured type, as the API spells it. */
export type ConnectionType = NonNullable<
   components["schemas"]["Connection"]["type"]
>;

/** A connection type's static attributes, as the API reports them. */
export type ConnectionAttributes =
   components["schemas"]["ConnectionAttributes"];

/** A connection as the API carries it. */
export type ApiConnection = components["schemas"]["Connection"];

/** A connection past assembly's name check. */
export type NamedConnection = ApiConnection & { name: string };

/** The Malloy core connection entry a connection assembles into. */
export type CoreConnectionEntry = {
   is: string;
   [key: string]: unknown;
};

/** One connection type's entry in the registry. */
export interface ConnectionDialect {
   /** The type this entry is for, as its registry key. */
   readonly type: ConnectionType;
   /**
    * What the API reports for a connection of this type before it is opened:
    * its Malloy dialect, whether it pools, persists and streams. Undefined
    * for a type whose attributes are only known once it resolves
    * (`publisher`, which names another server's connection).
    */
   readonly attributes: Readonly<ConnectionAttributes> | undefined;
   /**
    * The type's own shape checks, run at config load after the checks every
    * connection gets (connection_config.ts, validateConnectionShape). Absent
    * for a type with none of its own (postgres, mysql).
    */
   readonly validateShape?: (
      connection: ApiConnection,
      origin: ConnectionOrigin,
   ) => void;
   /**
    * The Malloy core connection entry a connection of this type assembles
    * into; `environmentPath` resolves a relative file a DuckDB-family
    * connection names.
    */
   readonly coreEntry: (
      connection: NamedConnection,
      environmentPath: string,
   ) => CoreConnectionEntry;
}

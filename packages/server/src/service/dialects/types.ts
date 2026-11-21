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

/** A connection's configured type, as the API spells it. */
export type ConnectionType = NonNullable<
   components["schemas"]["Connection"]["type"]
>;

/** A connection type's static attributes, as the API reports them. */
export type ConnectionAttributes =
   components["schemas"]["ConnectionAttributes"];

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
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The table an auto-run names for a source, for specs that build real
 * packages and look for the tables they wrote: Publisher's readable name
 * (service/materialization/domain/table_name.ts), for a source declared in
 * `modelPath` and built into DuckDB unless `dialect` says otherwise.
 */

import { readableTableName } from "../service/materialization/domain/table_name";

export function autoRunTableName(
   environmentName: string,
   packageName: string,
   sourceName: string,
   options: {
      /** The declaring model, package-relative. */
      modelPath: string;
      version?: string;
      dialect?: string;
      container?: string;
      /** A `name=`'s last segment, under `scope: version`. */
      prefix?: string;
      inDestination?: boolean;
      /** What the name reads, when it is not the source's name (a rollup). */
      label?: string;
   },
): string {
   return readableTableName({
      identity: {
         environmentName,
         packageName,
         modelPath: options.modelPath,
         sourceName,
         ...(options.version !== undefined ? { version: options.version } : {}),
      },
      label: options.label ?? sourceName,
      dialect: options.dialect ?? "duckdb",
      inDestination: options.inDestination ?? false,
      ...(options.container ? { container: options.container } : {}),
      ...(options.prefix !== undefined ? { prefix: options.prefix } : {}),
   });
}

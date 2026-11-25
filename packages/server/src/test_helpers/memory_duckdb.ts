// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";

/**
 * A `MalloyConfig` whose only connection is a fresh in-memory DuckDB named
 * `duckdb`, for a spec that loads a package needing no other connection. The
 * caller closes `duckdb`.
 */
export function memoryDuckdbConfig(): {
   malloyConfig: MalloyConfig;
   duckdb: DuckDBConnection;
} {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   return { malloyConfig, duckdb };
}

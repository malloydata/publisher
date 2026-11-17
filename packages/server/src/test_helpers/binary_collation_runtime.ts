// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stand-in for a compiled query's runtime whose every connection compares
 * strings byte-exact, for router specs whose stub answers read stored tables:
 * the stored-side collation check (`serve/gate_collation.ts`) then admits
 * them, as a plain DuckDB or DuckLake destination does.
 */
export function binaryCollationRuntime(): unknown {
   const connection = {
      runSQL: async (sql: string) => ({
         rows: /default_collation/.test(sql)
            ? [{ c: "" }]
            : /duckdb_tables\(\)/.test(sql)
              ? [{ sql: "CREATE TABLE t(c VARCHAR);", n: 0 }]
              : [],
      }),
   };
   return {
      connections: { lookupConnection: async () => connection },
   };
}

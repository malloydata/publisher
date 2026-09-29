// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A connection type that is not one of the server's built-ins, for the
// preload integration test. It is DuckDB under another name: the built-in
// factory does the work, so a query proves the whole path from
// PUBLISHER_PRELOAD_MODULES through `pluginConnection` to a result, and the
// extra password-typed property proves the response withholds a credential
// the server has never heard of.
import "@malloydata/db-duckdb/native";
import { registerConnectionType } from "@malloydata/malloy";
import { getConnectionTypeDef } from "@malloydata/malloy/connection";

const duckdb = getConnectionTypeDef("duckdb");
if (!duckdb) throw new Error("probe_duckdb: the duckdb type is not registered");

registerConnectionType("probe_duckdb", {
   displayName: "Probe DuckDB",
   properties: [
      ...duckdb.properties,
      {
         name: "secretToken",
         displayName: "Secret token",
         type: "password",
         optional: true,
      },
   ],
   factory: async (config, raw) => {
      const { secretToken: _ignored, ...rest } = config;
      return duckdb.factory(rest, raw);
   },
});

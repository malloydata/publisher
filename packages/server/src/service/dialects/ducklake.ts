// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `ducklake` connection type's registry entry: a DuckDB-dialect connection over a DuckLake catalog. */

import type { ConnectionDialect } from "./types";

export const DUCKLAKE: ConnectionDialect = Object.freeze({
   type: "ducklake",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
});

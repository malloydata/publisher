// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `motherduck` connection type's registry entry: a DuckDB-dialect connection to MotherDuck. */

import type { ConnectionDialect } from "./types";

export const MOTHERDUCK: ConnectionDialect = Object.freeze({
   type: "motherduck",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
});

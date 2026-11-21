// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `duckdb` connection type's registry entry. */

import type { ConnectionDialect } from "./types";

export const DUCKDB: ConnectionDialect = Object.freeze({
   type: "duckdb",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
});

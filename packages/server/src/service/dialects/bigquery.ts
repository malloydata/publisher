// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `bigquery` connection type's registry entry: Malloy's dialect for it is `standardsql`. */

import type { ConnectionDialect } from "./types";

export const BIGQUERY: ConnectionDialect = Object.freeze({
   type: "bigquery",
   attributes: Object.freeze({
      dialectName: "standardsql",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
});

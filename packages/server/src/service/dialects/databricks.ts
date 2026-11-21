// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `databricks` connection type's registry entry. */

import type { ConnectionDialect } from "./types";

export const DATABRICKS: ConnectionDialect = Object.freeze({
   type: "databricks",
   attributes: Object.freeze({
      dialectName: "databricks",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
});

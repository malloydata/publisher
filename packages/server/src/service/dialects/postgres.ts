// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `postgres` connection type's registry entry (Redshift connects as it). */

import type { ConnectionDialect } from "./types";

export const POSTGRES: ConnectionDialect = Object.freeze({
   type: "postgres",
   attributes: Object.freeze({
      dialectName: "postgres",
      isPool: true,
      canPersist: true,
      canStream: true,
   }),
});

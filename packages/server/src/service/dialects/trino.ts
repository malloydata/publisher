// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `trino` connection type's registry entry. */

import type { ConnectionDialect } from "./types";

export const TRINO: ConnectionDialect = Object.freeze({
   type: "trino",
   attributes: Object.freeze({
      dialectName: "trino",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
});

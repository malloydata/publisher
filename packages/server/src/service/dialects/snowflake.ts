// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `snowflake` connection type's registry entry. */

import type { ConnectionDialect } from "./types";

export const SNOWFLAKE: ConnectionDialect = Object.freeze({
   type: "snowflake",
   attributes: Object.freeze({
      dialectName: "snowflake",
      isPool: true,
      canPersist: true,
      canStream: true,
   }),
});

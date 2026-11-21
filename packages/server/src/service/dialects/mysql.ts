// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `mysql` connection type's registry entry. */

import type { ConnectionDialect } from "./types";

export const MYSQL: ConnectionDialect = Object.freeze({
   type: "mysql",
   attributes: Object.freeze({
      dialectName: "mysql",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
});

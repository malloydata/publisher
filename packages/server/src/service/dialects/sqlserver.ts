// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `sqlserver` connection type's registry entry. */

import type { ConnectionDialect } from "./types";

export const SQLSERVER: ConnectionDialect = Object.freeze({
   type: "sqlserver",
   // The connector is read-only: it creates no tables and has no
   // PersistSQLResults, so a database `#@ persist` on a SQL Server source is
   // refused at load (READ_ONLY_DIALECTS in materialization_eligibility.ts)
   // rather than failing mid-build.
   attributes: Object.freeze({
      dialectName: "sqlserver",
      isPool: true,
      canPersist: false,
      canStream: true,
   }),
});

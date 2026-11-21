// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `mysql` connection type's registry entry. */

import type {
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const MYSQL: ConnectionDialect = Object.freeze({
   type: "mysql",
   attributes: Object.freeze({
      dialectName: "mysql",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
   coreEntry: buildMySQLEntry,
});

function buildMySQLEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "mysql",
      host: connection.mysqlConnection?.host,
      port: connection.mysqlConnection?.port,
      user: connection.mysqlConnection?.user,
      password: connection.mysqlConnection?.password,
      database: connection.mysqlConnection?.database,
   };
}

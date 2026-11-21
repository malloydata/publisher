// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `postgres` connection type's registry entry (Redshift connects as it). */

import { buildPostgresConnectionString } from "../postgres_conninfo";
import type {
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const POSTGRES: ConnectionDialect = Object.freeze({
   type: "postgres",
   attributes: Object.freeze({
      dialectName: "postgres",
      isPool: true,
      canPersist: true,
      canStream: true,
   }),
   coreEntry: buildPostgresEntry,
});

function buildPostgresEntry(connection: NamedConnection): CoreConnectionEntry {
   const postgresConnection = connection.postgresConnection;
   return {
      is: "postgres",
      host: postgresConnection?.host,
      port: postgresConnection?.port,
      username: postgresConnection?.userName,
      password: postgresConnection?.password,
      databaseName: postgresConnection?.databaseName,
      connectionString: postgresConnection
         ? buildPostgresConnectionString(postgresConnection, {
              applySslmode: !connection.proxy,
           })
         : undefined,
   };
}

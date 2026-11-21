// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `motherduck` connection type's registry entry: a DuckDB-dialect connection to MotherDuck. */

import { networkedDuckDBOptions } from "../duckdb_lockdown";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const MOTHERDUCK: ConnectionDialect = Object.freeze({
   type: "motherduck",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateMotherDuckShape,
   coreEntry: buildMotherDuckEntry,
});

function validateMotherDuckShape(connection: ApiConnection): void {
   if (!connection.motherduckConnection) {
      throw new Error("MotherDuck connection configuration is missing.");
   }
   if (!connection.motherduckConnection.accessToken) {
      throw new Error("MotherDuck access token is required.");
   }
}

function buildMotherDuckEntry(
   connection: NamedConnection,
): CoreConnectionEntry {
   if (!connection.motherduckConnection?.accessToken) {
      throw new Error("MotherDuck access token is required.");
   }

   return {
      is: "duckdb",
      databasePath: connection.motherduckConnection.database
         ? `md:${connection.motherduckConnection.database}?attach_mode=single`
         : "md:",
      motherDuckToken: connection.motherduckConnection.accessToken,
      // Locked after open like any networked reader (duckdb_lockdown.ts).
      ...networkedDuckDBOptions("motherduck", ""),
   };
}

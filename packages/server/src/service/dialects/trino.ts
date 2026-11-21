// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `trino` connection type's registry entry. */

import type { components } from "../../api";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const TRINO: ConnectionDialect = Object.freeze({
   type: "trino",
   attributes: Object.freeze({
      dialectName: "trino",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
   validateShape: validateTrinoShape,
   coreEntry: buildTrinoEntry,
});

function validateTrinoShape(connection: ApiConnection): void {
   if (!connection.trinoConnection) {
      throw new Error("Trino connection configuration is missing.");
   }
}

function buildTrinoEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "trino",
      ...validateAndBuildTrinoCoreConfig(connection.trinoConnection),
   };
}

function validateAndBuildTrinoCoreConfig(
   trinoConfig: components["schemas"]["TrinoConnection"] | undefined,
): Record<string, unknown> {
   if (!trinoConfig) {
      return {};
   }

   const server =
      trinoConfig.server && trinoConfig.port
         ? trinoConfig.server.includes(trinoConfig.port.toString())
            ? trinoConfig.server
            : `${trinoConfig.server}:${trinoConfig.port}`
         : trinoConfig.server;

   const baseConfig: Record<string, unknown> = {
      server,
      port: trinoConfig.port,
      catalog: trinoConfig.catalog,
      schema: trinoConfig.schema,
      user: trinoConfig.user,
   };

   if (trinoConfig.peakaKey) {
      baseConfig.extraCredential = {
         peakaKey: trinoConfig.peakaKey,
      };
      return baseConfig;
   }

   if (server?.startsWith("https://") && trinoConfig.password) {
      baseConfig.password = trinoConfig.password;
   }

   if (server?.startsWith("http://") || server?.startsWith("https://")) {
      return baseConfig;
   }

   throw new Error(
      `Invalid Trino connection: expected "http://server:port" or "https://server:port".`,
   );
}

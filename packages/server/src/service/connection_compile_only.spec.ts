// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyConfig } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { ConnectionNotFoundError } from "../errors";
import {
   compileOnlyDestinationConfig,
   STORAGE_DESTINATION_DIALECT,
} from "./connection";

function configThatThrows(error: Error): MalloyConfig {
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(() => ({
      lookupConnection: async () => {
         throw error;
      },
   }));
   return config;
}

describe("compileOnlyDestinationConfig", () => {
   it("answers a destination it cannot reach with its dialect, and refuses to run SQL on it", async () => {
      const config = compileOnlyDestinationConfig(
         configThatThrows(
            new Error(
               'Existing DuckLake at metadata catalog "postgres:…" does not exist - and creating a new DuckLake is explicitly disabled',
            ),
         ),
      );
      const connection = await config.connections.lookupConnection("lake");
      expect(connection.dialectName).toBe(STORAGE_DESTINATION_DIALECT);
      expect(() => connection.runSQL("SELECT 1")).toThrow(
         /preview compiles against its dialect only.*does not exist/,
      );
   });

   it("lets any other lookup failure through, so the preview never predicts serving from a broken destination", async () => {
      for (const message of [
         'IO Error: Unable to connect to Postgres at "host=catalog": connection refused',
         "password authentication failed for user",
         'Cannot open file "/data/lake.ducklake": Permission denied',
      ]) {
         const config = compileOnlyDestinationConfig(
            configThatThrows(new Error(message)),
         );
         await expect(
            config.connections.lookupConnection("lake"),
         ).rejects.toThrow(message);
      }
   });

   it("still refuses a name the base config does not hold", async () => {
      const config = compileOnlyDestinationConfig(
         configThatThrows(new ConnectionNotFoundError("no lake here")),
      );
      await expect(
         config.connections.lookupConnection("lake"),
      ).rejects.toBeInstanceOf(ConnectionNotFoundError);
   });

   it("leaves a destination no config declares unknown", async () => {
      const config = compileOnlyDestinationConfig(
         configThatThrows(
            new Error('No connection named "lake" found in config'),
         ),
      );
      await expect(config.connections.lookupConnection("lake")).rejects.toThrow(
         /No connection named/,
      );
   });

   it("hands back the real connection when the lookup succeeds", async () => {
      const real = { name: "lake", dialectName: "duckdb" };
      const base = new MalloyConfig({ connections: {} });
      base.wrapConnections(() => ({
         lookupConnection: async () => real as never,
      }));
      expect(
         await compileOnlyDestinationConfig(base).connections.lookupConnection(
            "lake",
         ),
      ).toBe(real as never);
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { registerConnectionType } from "@malloydata/malloy";
import { components } from "../api";
import { assembleEnvironmentConnections } from "./connection_config";
import {
   BUILT_IN_CONNECTION_TYPES,
   isBuiltInConnectionType,
   pluginConnectionEntry,
   pluginConnectionProperties,
   publicPluginConnectionFields,
} from "./plugin_connection";

type ApiConnection = components["schemas"]["Connection"];

const PROBE = "plugin_spec_probe";
registerConnectionType(PROBE, {
   displayName: "Probe",
   properties: [
      { name: "host", displayName: "Host", type: "string" },
      { name: "port", displayName: "Port", type: "number", optional: true },
      {
         name: "region",
         displayName: "Region",
         type: "string",
         default: "us",
      },
      { name: "token", displayName: "Token", type: "password" },
      {
         name: "client",
         displayName: "Client",
         type: "opaque",
         source: "overlay",
         optional: true,
      },
   ],
   factory: async () => {
      throw new Error("never constructed here");
   },
});

const connection = (bag: Record<string, unknown>): ApiConnection =>
   ({ name: "probe", type: PROBE, pluginConnection: bag }) as ApiConnection;

describe("pluginConnectionProperties", () => {
   it("answers for a registered non-built-in type only", () => {
      expect(pluginConnectionProperties(PROBE)?.map((p) => p.name)).toEqual([
         "host",
         "port",
         "region",
         "token",
         "client",
      ]);
      // Built-ins have a typed object and a branch of their own.
      for (const type of BUILT_IN_CONNECTION_TYPES) {
         expect(isBuiltInConnectionType(type)).toBe(true);
         expect(pluginConnectionProperties(type)).toBeUndefined();
      }
      expect(pluginConnectionProperties("never_registered")).toBeUndefined();
      expect(pluginConnectionProperties(undefined)).toBeUndefined();
   });

   it("publishes the non-credential properties and nothing for an unregistered type", () => {
      expect(publicPluginConnectionFields(PROBE)).toEqual([
         "host",
         "port",
         "region",
      ]);
      expect(publicPluginConnectionFields("never_registered")).toBeUndefined();
   });
});

describe("pluginConnectionEntry", () => {
   it("forwards the bag to the factory under the type's name", () => {
      expect(
         pluginConnectionEntry(connection({ host: "h", token: "t" })),
      ).toEqual({ is: PROBE, host: "h", token: "t" });
   });

   it("names an unregistered type and the ones that are registered", () => {
      expect(() =>
         pluginConnectionEntry({
            name: "x",
            type: "never_registered",
         } as ApiConnection),
      ).toThrow(
         /Unsupported connection type: never_registered.*plugin_spec_probe/,
      );
   });

   it("refuses a key the type did not declare", () => {
      expect(() =>
         pluginConnectionEntry(connection({ host: "h", token: "t", extra: 1 })),
      ).toThrow(
         /pluginConnection\.extra, which type 'plugin_spec_probe' does not declare/,
      );
   });

   it("refuses a value for an overlay-only property", () => {
      expect(() =>
         pluginConnectionEntry(
            connection({ host: "h", token: "t", client: {} }),
         ),
      ).toThrow(/only accepts from a host overlay/);
   });

   it("requires a property that is neither optional nor defaulted, and nothing else", () => {
      expect(() => pluginConnectionEntry(connection({ token: "t" }))).toThrow(
         /missing pluginConnection\.host/,
      );
      expect(() => pluginConnectionEntry(connection({ host: "h" }))).toThrow(
         /missing pluginConnection\.token/,
      );
      // port is optional, region has a default, client is overlay-only.
      expect(() =>
         pluginConnectionEntry(connection({ host: "h", token: "t" })),
      ).not.toThrow();
   });
});

describe("assembleEnvironmentConnections with a registered type", () => {
   it("emits the core entry for it beside the built-ins", () => {
      const assembled = assembleEnvironmentConnections([
         connection({ host: "h", token: "t" }),
         {
            name: "pg",
            type: "postgres",
            postgresConnection: { host: "db", port: 5432 },
         } as ApiConnection,
      ]);
      expect(assembled.pojo.connections["probe"]).toEqual({
         is: PROBE,
         host: "h",
         token: "t",
      });
      expect(assembled.pojo.connections["pg"]?.is).toBe("postgres");
   });

   it("still refuses a type nobody registered", () => {
      expect(() =>
         assembleEnvironmentConnections([
            { name: "x", type: "never_registered" } as ApiConnection,
         ]),
      ).toThrow(/Unsupported connection type: never_registered/);
   });
});

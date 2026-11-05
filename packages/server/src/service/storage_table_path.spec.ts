// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { assembleEnvironmentConnections } from "./connection_config";
import { dropStorageTableSql } from "./materialization_build_session";
import { deriveServeBindings } from "./materialization_serve_transform";
import { destinationSchemaOf, storageTablePath } from "./storage_table_path";

type ApiConnection = components["schemas"]["Connection"];

function destination(destinationSchema?: string): ApiConnection {
   return {
      name: "lake",
      type: "ducklake",
      ducklakeConnection: {
         storage: { bucketUrl: "gs://b/ds_x" },
         catalog: {
            postgresConnection: { host: "h", databaseName: "d", userName: "u" },
            metadataSchema: "ds_x",
         },
         ...(destinationSchema === undefined ? {} : { destinationSchema }),
      },
   } as ApiConnection;
}

describe("storageTablePath", () => {
   it("keeps the two-part path for a destination that declares no schema or main", () => {
      expect(storageTablePath("lake", "daily_g003")).toBe("lake.daily_g003");
      expect(storageTablePath("lake", "daily_g003", "main")).toBe(
         "lake.daily_g003",
      );
      expect(storageTablePath("lake", "daily_g003", "")).toBe(
         "lake.daily_g003",
      );
   });

   it("puts the table in the destination's schema", () => {
      expect(storageTablePath("lake", "daily_g003", "materialized")).toBe(
         "lake.materialized.daily_g003",
      );
   });

   it("keeps an author's schema-qualified name as written, never four parts", () => {
      expect(
         storageTablePath("lake", "analytics.daily_orders", "materialized"),
      ).toBe("lake.analytics.daily_orders");
   });

   it("reads the schema off a destination, defaulting to main", () => {
      expect(destinationSchemaOf(destination())).toBe("main");
      expect(destinationSchemaOf(destination("materialized"))).toBe(
         "materialized",
      );
      expect(destinationSchemaOf(undefined)).toBe("main");
   });
});

describe("the build, the drop and the binding name the same table", () => {
   it("drops the table the build wrote, in the destination's schema", () => {
      expect(dropStorageTableSql("lake", "daily_g003")).toBe(
         'DROP TABLE IF EXISTS "lake"."daily_g003"',
      );
      expect(dropStorageTableSql("lake", "daily_g003", "materialized")).toBe(
         'DROP TABLE IF EXISTS "lake"."materialized"."daily_g003"',
      );
   });

   it("binds a served source to the table in its destination's schema", () => {
      const [binding] = deriveServeBindings(
         {
            se: {
               sourceEntityId: "se",
               sourceName: "daily",
               physicalTableName: "daily_g003",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "n", type: "BIGINT" }],
               realization: "COPY",
               rowCount: null,
            },
         } as never,
         {},
         (name) => (name === "lake" ? "materialized" : undefined),
      );
      expect(binding.tablePath).toBe("lake.materialized.daily_g003");
   });

   it("binds to main when no resolver is given, as before", () => {
      const [binding] = deriveServeBindings(
         {
            se: {
               sourceEntityId: "se",
               sourceName: "daily",
               physicalTableName: "daily_g003",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "n", type: "BIGINT" }],
               realization: "COPY",
               rowCount: null,
            },
         } as never,
         {},
      );
      expect(binding.tablePath).toBe("lake.daily_g003");
   });
});

describe("destinationSchema validation", () => {
   it("accepts a plain identifier and refuses anything else", () => {
      expect(() =>
         assembleEnvironmentConnections(
            [destination("materialized")],
            "/tmp/env",
         ),
      ).not.toThrow();
      for (const bad of [
         'x"; DROP',
         "dotted.name",
         "",
         true as unknown as string,
      ]) {
         expect(() =>
            assembleEnvironmentConnections([destination(bad)], "/tmp/env"),
         ).toThrow(/destinationSchema must be a plain identifier/);
      }
   });
});

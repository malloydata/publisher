// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { destinationOverlap, destinationOverlaps } from "./destination_overlap";

type ApiConnection = components["schemas"]["Connection"];

const lake = (
   name: string,
   catalog: { host?: string; databaseName?: string; metadataSchema?: string },
   bucketUrl?: string,
): ApiConnection =>
   ({
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: catalog.host,
               databaseName: catalog.databaseName,
            },
            ...(catalog.metadataSchema
               ? { metadataSchema: catalog.metadataSchema }
               : {}),
         },
         ...(bucketUrl ? { storage: { bucketUrl } } : {}),
      },
   }) as ApiConnection;

describe("destinationOverlap", () => {
   const destination = lake(
      "dest",
      { host: "pg", databaseName: "cat", metadataSchema: "lake" },
      "s3://bucket/lake",
   );

   it("names a connection on the same catalog database and metadata schema", () => {
      expect(
         destinationOverlap(
            lake("c", {
               host: "PG",
               databaseName: "cat",
               metadataSchema: "lake",
            }),
            destination,
         ),
      ).toContain("same DuckLake catalog");
   });

   it("names a connection on the same database whatever role, credentials or form it connects with", () => {
      const named = (connectionString: string, metadataSchema = "lake") =>
         ({
            name: "c",
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: { connectionString },
                  metadataSchema,
               },
            },
         }) as ApiConnection;
      const dest = named(
         "host=pg dbname=cat user=tier password=one sslmode=require",
      );
      for (const connection of [
         named("host=PG dbname=cat user=models password=two"),
         named("postgres://admin:three@pg:5432/cat?sslmode=disable"),
         lake("c", { host: "pg", databaseName: "cat", metadataSchema: "lake" }),
      ]) {
         expect(destinationOverlap(connection, dest)).toContain(
            "same DuckLake catalog",
         );
      }
      expect(
         destinationOverlap(named("host=pg dbname=cat", "other"), dest),
      ).toBeUndefined();
      expect(
         destinationOverlap(named("host=pg dbname=other"), dest),
      ).toBeUndefined();
   });

   it("names a connection whose data path overlaps the destination's", () => {
      for (const url of [
         "s3://bucket/lake/",
         "s3://bucket",
         "s3://bucket/lake/sub",
      ]) {
         expect(
            destinationOverlap(
               lake("c", { host: "pg", databaseName: "other" }, url),
               destination,
            ),
         ).toContain("data path");
      }
   });

   it("finds no overlap for another metadata schema and data path, or another type", () => {
      expect(
         destinationOverlap(
            lake(
               "c",
               { host: "pg", databaseName: "cat", metadataSchema: "mine" },
               "s3://bucket/lakehouse",
            ),
            destination,
         ),
      ).toBeUndefined();
      expect(
         destinationOverlap(
            { name: "pg", type: "postgres" } as ApiConnection,
            destination,
         ),
      ).toBeUndefined();
   });

   it("flags a connection that lists the destination's own schema in readableSchemas", () => {
      const withReadable = (
         readableSchemas: string[],
         destinationSchema?: string,
      ) => {
         const dest = lake(
            "dest",
            { host: "pg", databaseName: "cat", metadataSchema: "lake" },
            "s3://bucket/lake",
         );
         dest.ducklakeConnection!.destinationSchema = destinationSchema;
         const view = lake("c", {
            host: "pg",
            databaseName: "cat",
            metadataSchema: "lake",
         });
         view.ducklakeConnection!.readableSchemas = readableSchemas;
         return destinationOverlaps([view], [dest])[0].listsDestinationSchema;
      };
      expect(withReadable(["stripe", "materialized"], "materialized")).toBe(
         "materialized",
      );
      expect(withReadable(["stripe"], "materialized")).toBeUndefined();
      expect(withReadable(["materialized"], "Materialized")).toBe(
         "Materialized",
      );
      expect(withReadable(["Materialized"], "materialized")).toBe(
         "materialized",
      );
      // `main` is readable already; listing it adds no exposure.
      expect(withReadable(["stripe", "main"])).toBeUndefined();
   });

   it("names the schema each overlapping destination builds into, main when it sets none", () => {
      const view = lake("c", {
         host: "pg",
         databaseName: "cat",
         metadataSchema: "lake",
      });
      const set = lake(
         "set",
         { host: "pg", databaseName: "cat", metadataSchema: "lake" },
         "s3://bucket/lake",
      );
      set.ducklakeConnection!.destinationSchema = "materialized";
      const unset = lake(
         "unset",
         { host: "pg", databaseName: "cat", metadataSchema: "lake" },
         "s3://bucket/lake",
      );
      expect(
         destinationOverlaps([view], [set, unset]).map((o) => [
            o.destination,
            o.schema,
         ]),
      ).toEqual([
         ["set", "materialized"],
         ["unset", "main"],
      ]);
   });

   it("lists every overlapping pair", () => {
      expect(
         destinationOverlaps(
            [
               lake("same", {
                  host: "pg",
                  databaseName: "cat",
                  metadataSchema: "lake",
               }),
               lake("apart", { host: "pg", databaseName: "x" }, "gs://other"),
            ],
            [destination],
         ).map((o) => [o.connection, o.destination]),
      ).toEqual([["same", "dest"]]);
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import {
   connectionApartFromDestination,
   destinationOverlap,
   destinationOverlaps,
   destinationsApart,
} from "./destination_overlap";

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

describe("destinationsApart: whether two destinations provably hold different tables of one name", () => {
   const a = lake("a", { host: "pg", databaseName: "cat_a" }, "s3://b/a");
   it("different catalogs with data paths that do not overlap", () => {
      expect(
         destinationsApart(
            a,
            lake("b", { host: "pg", databaseName: "cat_b" }, "s3://b/b"),
         ),
      ).toBe(true);
   });
   it("not two names for one DuckLake, nor catalogs sharing a data path", () => {
      expect(
         destinationsApart(
            a,
            lake("a2", { host: "pg", databaseName: "cat_a" }, "s3://b/a"),
         ),
      ).toBe(false);
      expect(
         destinationsApart(
            a,
            lake("b", { host: "pg", databaseName: "cat_b" }, "s3://b/a/sub"),
         ),
      ).toBe(false);
      // A data path left to the catalog cannot be compared.
      expect(
         destinationsApart(a, lake("b", { host: "pg", databaseName: "cat_b" })),
      ).toBe(false);
   });
   it("one catalog: apart only by the schema each builds into", () => {
      const schema = (name: string, destinationSchema?: string) => {
         const d = lake(
            name,
            { host: "pg", databaseName: "cat_a" },
            "s3://b/a",
         );
         if (destinationSchema)
            d.ducklakeConnection!.destinationSchema = destinationSchema;
         return d;
      };
      expect(destinationsApart(schema("x"), schema("y", "staging"))).toBe(true);
      expect(destinationsApart(schema("x"), schema("y", "main"))).toBe(false);
   });
});

describe("connectionApartFromDestination", () => {
   const destination = lake(
      "dest",
      { host: "pg", databaseName: "cat" },
      "s3://b/lake",
   );
   it("a warehouse that cannot attach a DuckLake, or a DuckDB with nothing attached", () => {
      expect(
         connectionApartFromDestination(
            { name: "pg", type: "postgres" } as ApiConnection,
            destination,
         ),
      ).toBe(true);
      expect(
         connectionApartFromDestination(
            {
               name: "d",
               type: "duckdb",
               duckdbConnection: {},
            } as ApiConnection,
            destination,
         ),
      ).toBe(true);
   });
   it("not a DuckDB with setup SQL or attachments, MotherDuck, or the same lake", () => {
      expect(
         connectionApartFromDestination(
            {
               name: "d",
               type: "duckdb",
               duckdbConnection: { setupSQL: "ATTACH 'ducklake:...'" },
            } as ApiConnection,
            destination,
         ),
      ).toBe(false);
      expect(
         connectionApartFromDestination(
            { name: "m", type: "motherduck" } as ApiConnection,
            destination,
         ),
      ).toBe(false);
      expect(
         connectionApartFromDestination(
            lake("c", { host: "pg", databaseName: "cat" }, "s3://b/lake"),
            destination,
         ),
      ).toBe(false);
      expect(
         connectionApartFromDestination(
            lake("c", { host: "pg", databaseName: "other" }, "s3://b/other"),
            destination,
         ),
      ).toBe(true);
   });
});

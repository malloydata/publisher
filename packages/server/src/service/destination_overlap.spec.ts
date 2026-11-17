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

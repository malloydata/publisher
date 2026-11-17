// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
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
   const T = "t__p0123abcd";
   const a = lake("a", { host: "pg", databaseName: "cat_a" }, "s3://b/a");
   const withSchema = (d: ApiConnection, destinationSchema: string) => {
      d.ducklakeConnection!.destinationSchema = destinationSchema;
      return d;
   };
   it("different catalogs, written alike, with data paths that do not overlap", () => {
      expect(
         destinationsApart(
            a,
            lake("b", { host: "pg", databaseName: "cat_b" }, "s3://b/b"),
            T,
         ),
      ).toBe(true);
   });
   it("not two names for one DuckLake, nor catalogs sharing a data path", () => {
      expect(
         destinationsApart(
            a,
            lake("a2", { host: "pg", databaseName: "cat_a" }, "s3://b/a"),
            T,
         ),
      ).toBe(false);
      expect(
         destinationsApart(
            a,
            lake("b", { host: "pg", databaseName: "cat_b" }, "s3://b/a/sub"),
            T,
         ),
      ).toBe(false);
      // A data path left to the catalog cannot be compared.
      expect(
         destinationsApart(
            a,
            lake("b", { host: "pg", databaseName: "cat_b" }),
            T,
         ),
      ).toBe(false);
   });
   it("one catalog: apart only by the schema each builds into, compared without case, for a name with no container", () => {
      const x = () =>
         lake("x", { host: "pg", databaseName: "cat_a" }, "s3://b/a");
      expect(destinationsApart(x(), withSchema(x(), "staging"), T)).toBe(true);
      expect(destinationsApart(x(), withSchema(x(), "main"), T)).toBe(false);
      expect(destinationsApart(x(), withSchema(x(), "MAIN"), T)).toBe(false);
      // A name with its own container is placed there, whatever the schema.
      expect(
         destinationsApart(x(), withSchema(x(), "staging"), `analytics.${T}`),
      ).toBe(false);
   });
   it("one catalog or data path spelled two ways is one", () => {
      const pg = (catalog: Record<string, unknown>, bucketUrl: string) =>
         ({
            name: "y",
            type: "ducklake",
            ducklakeConnection: {
               catalog: { postgresConnection: catalog },
               storage: { bucketUrl },
            },
         }) as ApiConnection;
      const local = pg(
         { host: "localhost", port: 5432, databaseName: "cat" },
         "/data/lake",
      );
      // Loopback spelled otherwise, and a data path with a `.` segment.
      expect(
         destinationsApart(
            local,
            pg(
               { host: "127.0.0.1", port: 5432, databaseName: "other" },
               "/data/./lake",
            ),
            T,
         ),
      ).toBe(false);
      // The same catalog as a URL.
      expect(
         destinationsApart(
            local,
            pg(
               { connectionString: "postgresql://u:p@127.0.0.1:5432/cat" },
               "/data/x",
            ),
            T,
         ),
      ).toBe(false);
      // Different catalogs written in different forms are never proven apart.
      expect(
         destinationsApart(
            local,
            pg(
               { connectionString: "postgresql://u:p@pg2:5432/other" },
               "/data/x",
            ),
            T,
         ),
      ).toBe(false);
      // A scheme in upper case is the same bucket.
      const s3 = (bucketUrl: string, databaseName: string) =>
         pg({ host: "pg", databaseName }, bucketUrl);
      expect(
         destinationsApart(
            s3("s3://bucket/lake", "c1"),
            s3("S3://bucket/lake/", "c2"),
            T,
         ),
      ).toBe(false);
   });
});

describe("connectionApartFromDestination", () => {
   const T = "t__p0123abcd";
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
            T,
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
            T,
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
            T,
         ),
      ).toBe(false);
      expect(
         connectionApartFromDestination(
            { name: "m", type: "motherduck" } as ApiConnection,
            destination,
            T,
         ),
      ).toBe(false);
      expect(
         connectionApartFromDestination(
            lake("c", { host: "pg", databaseName: "cat" }, "s3://b/lake"),
            destination,
            T,
         ),
      ).toBe(false);
      expect(
         connectionApartFromDestination(
            lake("c", { host: "pg", databaseName: "other" }, "s3://b/other"),
            destination,
            T,
         ),
      ).toBe(true);
   });
});

describe("a local data path reached through a symlink", () => {
   it("is the path it links to", () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "overlap-"));
      try {
         fs.mkdirSync(path.join(root, "lake"));
         fs.symlinkSync(path.join(root, "lake"), path.join(root, "link"));
         const at = (databaseName: string, dir: string) =>
            lake("x", { host: "pg", databaseName }, path.join(root, dir));
         expect(
            destinationsApart(
               at("c1", "lake"),
               at("c2", "link"),
               "t__p0123abcd",
            ),
         ).toBe(false);
         fs.mkdirSync(path.join(root, "other"));
         expect(
            destinationsApart(
               at("c1", "lake"),
               at("c2", "other"),
               "t__p0123abcd",
            ),
         ).toBe(true);
      } finally {
         fs.rmSync(root, { recursive: true, force: true });
      }
   });
});

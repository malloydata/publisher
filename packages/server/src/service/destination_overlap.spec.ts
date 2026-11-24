// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import {
   destinationOverlaps,
   refuseListedDestinationSchema,
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

/** Why `connection` reaches `destination`, or undefined. */
const destinationOverlap = (connection: ApiConnection, dest: ApiConnection) =>
   destinationOverlaps([connection], [dest])[0]?.reason;

describe("destinationOverlaps' reasons", () => {
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

   it("says whether the connection can read the destination's schema: a managed read-only view reads main and its readableSchemas only", () => {
      const destination = (destinationSchema?: string) => {
         const dest = lake("dest", {
            host: "pg",
            databaseName: "cat",
            metadataSchema: "lake",
         });
         dest.ducklakeConnection!.destinationSchema = destinationSchema;
         return dest;
      };
      const connection = (
         extra: Partial<ApiConnection>,
         readableSchemas?: string[],
      ) => {
         const c = lake("c", {
            host: "pg",
            databaseName: "cat",
            metadataSchema: "lake",
         });
         c.ducklakeConnection!.readableSchemas = readableSchemas;
         return Object.assign(c, extra);
      };
      const readable = (c: ApiConnection, d: ApiConnection) =>
         destinationOverlaps([c], [d])[0].readable;
      const view = { managed: true } as const;
      expect(readable(connection({}), destination("materialized"))).toBe(true);
      expect(readable(connection(view), destination("materialized"))).toBe(
         false,
      );
      expect(
         readable(connection(view, ["stripe"]), destination("materialized")),
      ).toBe(false);
      expect(
         readable(
            connection(view, ["stripe", "materialized"]),
            destination("Materialized"),
         ),
      ).toBe(true);
      expect(readable(connection(view), destination())).toBe(true);
      expect(readable(connection(view), destination("main"))).toBe(true);
      const writable = connection(view);
      writable.ducklakeConnection!.readOnly = false;
      expect(readable(writable, destination("materialized"))).toBe(true);
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

describe("refuseListedDestinationSchema", () => {
   const catalog = { host: "pg", databaseName: "cat", metadataSchema: "lake" };
   const builds = (schema?: string) => {
      const dest = lake("dest", catalog, "s3://bucket/lake");
      dest.ducklakeConnection!.destinationSchema = schema;
      return dest;
   };
   const listing = (...readableSchemas: string[]) => {
      const view = lake("view", catalog);
      view.ducklakeConnection!.readableSchemas = readableSchemas;
      return view;
   };

   it("refuses, naming the connection, the destination and the schema, a readableSchemas entry a destination builds into", () => {
      let error: unknown;
      try {
         refuseListedDestinationSchema(
            [listing("stripe", "Materialized")],
            [builds("materialized")],
         );
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toContain('"view"');
      expect((error as Error).message).toContain('"dest"');
      expect((error as Error).message).toContain("materialized");
   });

   it("does not refuse a connection on another catalog whose data path merely nests with the destination's, which still overlaps", () => {
      const dest = lake(
         "dest",
         { host: "pg-a", databaseName: "cat", metadataSchema: "lake" },
         "gs://lake",
      );
      dest.ducklakeConnection!.destinationSchema = "materialized";
      const view = lake(
         "view",
         {
            host: "pg-b",
            databaseName: "other_catalog",
            metadataSchema: "lake",
         },
         "gs://lake/tenant",
      );
      view.ducklakeConnection!.readableSchemas = ["materialized"];
      refuseListedDestinationSchema([view], [dest]);
      const [overlap] = destinationOverlaps([view], [dest]);
      expect(overlap.reason).toContain("data path");
      expect(overlap.listsDestinationSchema).toBeUndefined();
   });

   it("lets through a list that omits the schema, a connection on another catalog, and main", () => {
      refuseListedDestinationSchema(
         [listing("stripe")],
         [builds("materialized")],
      );
      refuseListedDestinationSchema(
         [lake("apart", { host: "pg", databaseName: "x" })],
         [builds("materialized")],
      );
      refuseListedDestinationSchema([listing("main")], [builds()]);
      refuseListedDestinationSchema([listing("materialized")], []);
   });
});

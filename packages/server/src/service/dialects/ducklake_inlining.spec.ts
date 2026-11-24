// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../../api";
import { catalogKey, ducklakeCatalogIdentity } from "./ducklake_inlining";

type PostgresConnection = components["schemas"]["PostgresConnection"];

const asString = (connectionString: string): PostgresConnection => ({
   connectionString,
});

describe("catalogKey", () => {
   const fields: PostgresConnection = {
      host: "PG.internal",
      port: 5433,
      databaseName: "lake",
      userName: "writer",
      password: "one",
   };
   const spellings = [
      fields,
      asString(
         "host=pg.internal port=5433 dbname=lake user=reader password=two",
      ),
      asString("postgres://other:three@PG.internal:5433/lake"),
      asString(
         "postgresql://pg.internal:5433/lake?sslmode=require&options=-crole%3Downer",
      ),
      asString(
         "  dbname=lake host=pg.internal port=5433 sslmode=verify-full connect_timeout=1 ",
      ),
   ];

   it("is one key for one database, whatever its form, role, credentials or TLS and other settings", () => {
      const keys = new Set(spellings.map((pg) => catalogKey(pg)));
      expect([...keys]).toEqual(["pg.internal:5433/lake"]);
   });

   it("defaults the port to 5432 in a string and in fields", () => {
      expect(catalogKey(asString("host=pg dbname=lake"))).toBe("pg:5432/lake");
      expect(catalogKey({ host: "pg", databaseName: "lake" })).toBe(
         "pg:5432/lake",
      );
      expect(catalogKey(asString("postgres://u:p@pg/lake"))).toBe(
         "pg:5432/lake",
      );
   });

   it("differs for another database, host or port", () => {
      const base = catalogKey(asString("host=pg port=5432 dbname=lake"));
      for (const other of [
         "host=pg port=5432 dbname=other",
         "host=elsewhere port=5432 dbname=lake",
         "host=pg port=5433 dbname=lake",
      ]) {
         expect(catalogKey(asString(other))).not.toBe(base);
      }
   });

   it("sorts and lowercases a list of hosts, pairing each with its port", () => {
      expect(catalogKey(asString("host=B,a port=5433,5432 dbname=lake"))).toBe(
         catalogKey(asString("postgres://u@a:5432,b:5433/lake")),
      );
      expect(catalogKey(asString("host=b,a dbname=lake"))).toBe(
         "a:5432,b:5432/lake",
      );
   });

   it("falls back to the trimmed string when it cannot say which database the string names", () => {
      for (const raw of [
         "not a connection string",
         "host=pg port=5432",
         "dbname=lake",
         "service=prod dbname=lake host=pg",
         "host=pg hostaddr=10.0.0.1 dbname=lake",
         "host=a,b,c port=1,2 dbname=lake",
      ]) {
         expect(catalogKey(asString(`  ${raw}  `))).toBe(raw);
      }
   });

   it("is undefined for no connection, or fields without a database", () => {
      expect(catalogKey(undefined)).toBeUndefined();
      expect(catalogKey({ host: "pg" })).toBeUndefined();
   });

   it("makes a catalog's identity from the key and the metadata schema", () => {
      const identity = (connectionString: string, metadataSchema: string) =>
         ducklakeCatalogIdentity({
            catalog: {
               postgresConnection: asString(connectionString),
               metadataSchema,
            },
         });
      expect(identity("host=pg dbname=lake user=a", "ds")).toBe(
         identity("postgres://b:c@pg/lake", "ds"),
      );
      expect(identity("host=pg dbname=lake", "ds")).not.toBe(
         identity("host=pg dbname=lake", "other"),
      );
   });
});

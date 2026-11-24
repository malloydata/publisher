// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { pgDatabaseTarget } from "./postgres_conninfo";

describe("pgDatabaseTarget", () => {
   it("names the database a keyword/value string or a URI reaches, apart from who connects and how", () => {
      for (const conninfo of [
         "host=Pg.Internal port=5433 dbname=lake user=a password='p w'",
         "dbname=lake sslmode=verify-full options='-c statement_timeout=5' host=pg.internal port=5433",
         "postgres://a:p%20w@PG.internal:5433/lake?sslmode=require",
         "postgresql://pg.internal:5433/lake",
      ]) {
         expect(pgDatabaseTarget(conninfo)).toBe("pg.internal:5433/lake");
      }
   });

   it("defaults the port, and reads the last of a repeated keyword as libpq does", () => {
      expect(pgDatabaseTarget("host=pg dbname=a dbname=lake")).toBe(
         "pg:5432/lake",
      );
      expect(pgDatabaseTarget("postgres://pg/lake")).toBe("pg:5432/lake");
   });

   it("expands a dbname that is itself a connection string", () => {
      expect(pgDatabaseTarget("dbname='host=pg port=1 dbname=lake'")).toBe(
         "pg:1/lake",
      );
   });

   it("gives one port to every host, or one to each, and sorts the pairs", () => {
      expect(pgDatabaseTarget("host=b,a port=7 dbname=lake")).toBe(
         "a:7,b:7/lake",
      );
      expect(pgDatabaseTarget("host=b,a port=1,2 dbname=lake")).toBe(
         "a:2,b:1/lake",
      );
      expect(pgDatabaseTarget("postgres://b:1,a:2/lake")).toBe("a:2,b:1/lake");
   });

   it("is undefined when the string does not say which database it reaches", () => {
      for (const conninfo of [
         "",
         "garbage",
         "host=pg",
         "dbname=lake",
         "host=pg dbname=",
         "service=prod host=pg dbname=lake",
         "host=pg hostaddr=10.0.0.1 dbname=lake",
         "host=a,b,c port=1,2 dbname=lake",
         "postgres://pg",
      ]) {
         expect(pgDatabaseTarget(conninfo)).toBeUndefined();
      }
   });
});

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A Postgres attach carries its user and password in a DuckDB secret, so the
// path the ATTACH names carries no credential, in either form of connection
// string.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";
import {
   buildPgConnectionString,
   createPgAttachSecret,
   pgAttachSecretName,
   splitPgAttachCredentials,
} from "./postgres_conninfo";

const split = (conninfo: string) =>
   splitPgAttachCredentials("Connection 'c'", conninfo);

describe("splitPgAttachCredentials", () => {
   it("moves user and password out of a keyword/value string", () => {
      expect(
         split(
            "host=db.internal port=5432 dbname=cat user=reader password=s3cr3t sslmode=require",
         ),
      ).toEqual({
         path: "host=db.internal port=5432 dbname=cat sslmode=require",
         user: "reader",
         password: "s3cr3t",
         sslpassword: undefined,
      });
   });

   it("reads quoted values, and keeps the last of a repeated keyword as libpq does", () => {
      expect(
         split(
            `host=h password='first' user = 'a b' password='it\\'s \\'q\\' sp ace' options='-c statement_timeout=5'`,
         ),
      ).toEqual({
         path: "host=h options='-c statement_timeout=5'",
         user: "a b",
         password: "it's 'q' sp ace",
         sslpassword: undefined,
      });
   });

   it("reads a URI's userinfo and query, percent-decoded, into keyword/value form", () => {
      expect(
         split(
            "postgresql://re%40der:p%40ss%3Aw%2Fd@db.internal:6543/my%20db?sslmode=verify-full&application_name=pub",
         ),
      ).toEqual({
         path: "host=db.internal port=6543 dbname='my db' sslmode=verify-full application_name=pub",
         user: "re@der",
         password: "p@ss:w/d",
         sslpassword: undefined,
      });
   });

   it("reads a password given as a URI query parameter", () => {
      expect(split("postgres://h/db?user=u&password=pw")).toEqual({
         path: "host=h dbname=db",
         user: "u",
         password: "pw",
         sslpassword: undefined,
      });
   });

   it("keeps every host and port of a multi-host or IPv6 URI", () => {
      expect(split("postgresql://u:p@a:1,b,[::1]:3/d").path).toBe(
         "host=a,b,::1 port=1,,3 dbname=d",
      );
   });

   it("spells a URI's ssl=true as libpq does", () => {
      expect(split("postgresql://u:p@h/d?ssl=true").path).toBe(
         "host=h dbname=d sslmode=require",
      );
   });

   it("expands a dbname that is itself a connection string", () => {
      expect(
         split("dbname='postgresql://u:p@h/real' application_name=x"),
      ).toEqual({
         path: "host=h dbname=real application_name=x",
         user: "u",
         password: "p",
         sslpassword: undefined,
      });
   });

   it("moves a client-key password out of both forms", () => {
      expect(
         split(
            "host=h sslmode=verify-full sslkey=/k.pem sslpassword='key pass' passfile=/pgpass",
         ),
      ).toEqual({
         path: "host=h sslmode=verify-full sslkey=/k.pem passfile=/pgpass",
         user: undefined,
         password: undefined,
         sslpassword: "key pass",
      });
      expect(
         split("postgresql://u@h/d?sslkey=%2Fk.pem&sslpassword=key%20pass"),
      ).toEqual({
         path: "host=h dbname=d sslkey=/k.pem",
         user: "u",
         password: undefined,
         sslpassword: "key pass",
      });
   });

   it("leaves a string with no credential as it reads", () => {
      expect(split("service=catalog")).toEqual({
         path: "service=catalog",
         user: undefined,
         password: undefined,
         sslpassword: undefined,
      });
   });

   it("refuses a string it cannot read, without repeating it", () => {
      let message = "";
      try {
         split("host=h password=a b");
      } catch (error) {
         message = (error as Error).message;
      }
      expect(message).toContain("could not be read");
      expect(message).not.toContain("password=a");
   });

   it("round-trips a fielded config whose values need quoting", () => {
      const conninfo = buildPgConnectionString(
         {
            host: "h",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "two words'q",
         },
         { name: "c", applyConnectionOptions: false },
      );
      const { path, user, password } = split(conninfo);
      // The deployment's PGSSLMODE may add an sslmode after these.
      expect(path).toStartWith("host=h port=5432 dbname=d");
      expect(path).not.toContain("two words");
      expect({ user, password }).toEqual({
         user: "u",
         password: "two words'q",
      });
   });
});

describe("pgAttachSecretName", () => {
   it("is one identifier per handle, unique for handles that sanitize alike", () => {
      const a = pgAttachSecretName("my-lake");
      const b = pgAttachSecretName("my_lake");
      expect(a).toMatch(/^[a-z0-9_]+$/);
      expect(a).not.toBe(b);
      expect(pgAttachSecretName("my-lake")).toBe(a);
   });
});

async function postgresExtensionLoads(): Promise<boolean> {
   const probe = new DuckDBConnection({
      name: "pg_ext_probe",
      databasePath: ":memory:",
   });
   try {
      await probe.runSQL("LOAD postgres");
      return true;
   } catch {
      return false;
   } finally {
      await probe.close();
   }
}

describe.skipIf(!(await postgresExtensionLoads()))(
   "createPgAttachSecret",
   () => {
      it("keeps the credential in a temporary, redacted secret and out of the path", async () => {
         const connection = new DuckDBConnection({
            name: "pg_secret_spec",
            databasePath: ":memory:",
         });
         try {
            await connection.runSQL("LOAD postgres");
            const { path, secret } = await createPgAttachSecret(
               connection,
               "lake",
               "host=h dbname=d user=reader password=never-shown sslpassword=key-never-shown",
            );
            expect(path).toBe("host=h dbname=d");
            expect(secret).toBe(pgAttachSecretName("lake"));
            const rows = (
               await connection.runSQL(
                  `SELECT name, persistent, secret_string FROM duckdb_secrets() WHERE name = '${secret}'`,
               )
            ).rows as Record<string, unknown>[];
            expect(rows).toHaveLength(1);
            expect(rows[0].persistent).toBe(false);
            expect(String(rows[0].secret_string)).not.toContain("never-shown");
            expect(String(rows[0].secret_string)).toContain(
               "sslpassword=redacted",
            );
         } finally {
            await connection.close();
         }
      });

      it("creates no secret for a string with no credential", async () => {
         const connection = new DuckDBConnection({
            name: "pg_secret_spec_none",
            databasePath: ":memory:",
         });
         try {
            expect(
               await createPgAttachSecret(connection, "lake", "service=x"),
            ).toEqual({ path: "service=x" });
         } finally {
            await connection.close();
         }
      });
   },
);

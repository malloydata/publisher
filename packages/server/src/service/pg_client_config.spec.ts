// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The node-pg config built from the libpq connection string the admin API
// attaches with, so a session advisory lock reaches the same database as the
// DuckLake attach does.
import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { pgClientConfig } from "./connection";

const config = (conninfo: string) => pgClientConfig("test", conninfo);

describe("pgClientConfig", () => {
   const dir = mkdtempSync(join(tmpdir(), "pg-client-config-"));
   const ca = join(dir, "ca.pem");
   writeFileSync(ca, "PEM-BYTES");
   afterAll(() => rmSync(dir, { recursive: true, force: true }));

   it("maps a keyword/value string, quoted values included", () => {
      expect(
         config(
            "host=db.internal port=5433 dbname=cat user='an admin' password='p w'",
         ),
      ).toMatchObject({
         host: "db.internal",
         port: 5433,
         database: "cat",
         user: "an admin",
         password: "p w",
      });
   });

   it("maps a URI, percent-decoded, and takes the first of several hosts", () => {
      expect(
         config("postgresql://u%40x:p%2Fw@a.example:6543,b.example:6544/cat"),
      ).toMatchObject({
         host: "a.example",
         port: 6543,
         database: "cat",
         user: "u@x",
         password: "p/w",
      });
   });

   it("reads the last of a repeated keyword, as libpq does", () => {
      expect(config("host=a host=b user=u")).toMatchObject({ host: "b" });
   });

   it.each([
      [undefined, { rejectUnauthorized: false }, true],
      ["disable", false, false],
      ["allow", { rejectUnauthorized: false }, true],
      ["prefer", { rejectUnauthorized: false }, true],
      ["require", { rejectUnauthorized: false }, false],
      ["no-verify", { rejectUnauthorized: false }, false],
   ] as const)(
      "maps sslmode %s to %p, plaintext fallback %p",
      (sslmode, ssl, plaintextFallback) => {
         const conninfo = `host=h${sslmode ? ` sslmode=${sslmode}` : ""}`;
         expect(config(conninfo)).toMatchObject({ ssl, plaintextFallback });
      },
   );

   it.each(["require", "verify-ca"])(
      "verifies the chain but not the host for %s with an sslrootcert",
      (sslmode) => {
         const ssl = config(`host=h sslmode=${sslmode} sslrootcert=${ca}`)
            .ssl as unknown as Record<string, unknown>;
         expect(ssl).toMatchObject({
            rejectUnauthorized: true,
            ca: "PEM-BYTES",
         });
         expect((ssl.checkServerIdentity as () => unknown)()).toBeUndefined();
      },
   );

   it("verifies the chain and the host name for verify-full", () => {
      expect(
         config(`host=db.internal sslmode=verify-full sslrootcert=${ca}`).ssl,
      ).toEqual({
         rejectUnauthorized: true,
         ca: "PEM-BYTES",
         servername: "db.internal",
      });
   });

   it("trusts the runtime's ambient anchors when verify-* names no sslrootcert", () => {
      const { ssl } = config("host=h sslmode=verify-full") as {
         ssl: { ca: string };
      };
      expect(ssl.ca).toContain("BEGIN CERTIFICATE");
   });

   it("refuses an sslmode it does not know, naming the mode and not the string", () => {
      expect(() => config("host=h password=secret sslmode=bogus")).toThrow(
         "test has unsupported sslmode 'bogus'.",
      );
   });
});

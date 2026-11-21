// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The node-pg config built from the libpq connection string the admin API
// attaches with, so a session advisory lock reaches the same database as the
// DuckLake attach does.
import { afterAll, afterEach, describe, expect, it } from "bun:test";
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

   it("maps a URI, percent-decoded", () => {
      expect(
         config("postgresql://u%40x:p%2Fw@a.example:6543/cat"),
      ).toMatchObject({
         host: "a.example",
         port: 6543,
         database: "cat",
         user: "u@x",
         password: "p/w",
      });
   });

   it("bounds a connection that goes quiet: keepalive, and a timeout on every query", () => {
      expect(config("host=h")).toMatchObject({
         keepAlive: true,
         keepAliveInitialDelayMillis: 10_000,
         query_timeout: 30_000,
      });
   });

   it.each([
      ["service=prod host=h password=secret", "'service'"],
      ["host=h hostaddr=10.0.0.1 password=secret", "'hostaddr'"],
      ["host=a,b port=1,2 password=secret", "more than one 'host'"],
      [
         "postgresql://u:secret@a.example:6543,b.example:6544/cat",
         "more than one 'host'",
      ],
      ["host=a port=1,2 password=secret", "more than one 'port'"],
   ])(
      "refuses what a single connection cannot follow: %s",
      (conninfo, named) => {
         const error = String(
            (() => {
               try {
                  config(conninfo);
               } catch (e) {
                  return e;
               }
            })(),
         );
         expect(error).toContain(
            `test: the connection string sets ${named}, which loads cannot use; spell host, port and dbname.`,
         );
         expect(error).not.toContain("secret");
      },
   );

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

   it("verifies the chain and the host name for verify-full, leaving the server name to the driver", () => {
      expect(
         config(`host=db.internal sslmode=verify-full sslrootcert=${ca}`).ssl,
      ).toEqual({ rejectUnauthorized: true, ca: "PEM-BYTES" });
   });

   it("maps no-verify to require, as the attach does: it verifies a chain it is given a root for", () => {
      const ssl = config(`host=h sslmode=no-verify sslrootcert=${ca}`)
         .ssl as unknown as Record<string, unknown>;
      expect(ssl).toMatchObject({ rejectUnauthorized: true, ca: "PEM-BYTES" });
   });

   describe("the client certificate", () => {
      const cert = join(dir, "client.crt");
      const key = join(dir, "client.key");
      writeFileSync(cert, "CERT-BYTES");
      writeFileSync(key, "KEY-BYTES");

      it.each(["require", "prefer", "verify-ca", "verify-full"])(
         "carries sslcert, sslkey and sslpassword into the %s options",
         (sslmode) => {
            expect(
               config(
                  `host=h sslmode=${sslmode} sslrootcert=${ca} sslcert=${cert} sslkey=${key} sslpassword=pw`,
               ).ssl,
            ).toMatchObject({
               cert: "CERT-BYTES",
               key: "KEY-BYTES",
               passphrase: "pw",
            });
         },
      );

      it("sends none of it when TLS is off", () => {
         expect(
            config(`host=h sslmode=disable sslcert=${cert} sslkey=${key}`).ssl,
         ).toBe(false);
      });

      it("names the file and no secret when one cannot be read", () => {
         let error = "";
         try {
            config(
               `host=h password=hunter2 sslcert=${join(dir, "missing.crt")}`,
            );
         } catch (e) {
            error = String(e);
         }
         expect(error).toContain("missing.crt");
         expect(error).not.toContain("hunter2");
      });
   });

   describe("the deployment's PGSSLMODE", () => {
      afterEach(() => {
         delete process.env.PGSSLMODE;
      });

      it("applies when the string sets no sslmode, as libpq reads it", () => {
         process.env.PGSSLMODE = "verify-full";
         expect(config(`host=h sslrootcert=${ca}`)).toMatchObject({
            ssl: { rejectUnauthorized: true, ca: "PEM-BYTES" },
            plaintextFallback: false,
         });
         process.env.PGSSLMODE = "disable";
         expect(config("host=h").ssl).toBe(false);
      });

      it("yields to the string's own sslmode", () => {
         process.env.PGSSLMODE = "verify-full";
         expect(config("host=h sslmode=disable").ssl).toBe(false);
      });

      it("maps no-verify to require, which libpq accepts", () => {
         process.env.PGSSLMODE = "no-verify";
         expect(config("host=h")).toMatchObject({
            ssl: { rejectUnauthorized: false },
            plaintextFallback: false,
         });
      });

      it("refuses a value libpq would refuse, naming the mode and not the string", () => {
         process.env.PGSSLMODE = "bogus";
         expect(() => config("host=h password=secret")).toThrow(
            "test has unsupported sslmode 'bogus'.",
         );
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

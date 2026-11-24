// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The node-pg config built from the libpq connection string the admin API
// attaches with, so a session advisory lock reaches the same database as the
// DuckLake attach does.
import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { createServer, type Server, type Socket } from "net";
import { tmpdir } from "os";
import { join } from "path";
import { connectPgClient, pgClientConfig } from "./postgres_conninfo";

const config = (conninfo: string) => pgClientConfig("test", conninfo);

const ENV_VARIABLES = [
   "PGSSLMODE",
   "PGSSLROOTCERT",
   "PGSSLCERT",
   "PGSSLKEY",
   "PGSERVICE",
   "PGHOSTADDR",
   "PGCHANNELBINDING",
   "PGREQUIREAUTH",
   "PGGSSENCMODE",
   "PGSSLCRL",
   "PGSSLCRLDIR",
   "PGSSLMINPROTOCOLVERSION",
   "PGPASSFILE",
   "NODE_EXTRA_CA_CERTS",
] as const;
const priorEnv = new Map<string, string | undefined>();
beforeEach(() => {
   for (const name of ENV_VARIABLES) {
      priorEnv.set(name, process.env[name]);
      delete process.env[name];
   }
});
afterEach(() => {
   for (const [name, value] of priorEnv) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
   }
});

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

   it("names itself to the server, so the lock's session reads as a load's in pg_stat_activity", () => {
      expect(config("host=h application_name=other")).toMatchObject({
         application_name: "publisher-load-lock",
      });
   });

   const refusal = (conninfo: string) =>
      String(
         (() => {
            try {
               config(conninfo);
            } catch (e) {
               return e;
            }
         })(),
      );

   it.each([
      ["channel_binding", "require"],
      ["gssencmode", "require"],
      ["require_auth", "scram-sha-256"],
      ["sslcrl", "/etc/crl.pem"],
      ["sslcrldir", "/etc/crl"],
      ["ssl_min_protocol_version", "TLSv1.3"],
   ])(
      "refuses %s=%s, which node-pg would drop, naming the key and not the string",
      (key, value) => {
         const error = refusal(`host=h password=secret ${key}=${value}`);
         expect(error).toContain(
            `test: the connection string sets '${key}', which the load lock's client cannot honor; remove it.`,
         );
         expect(error).not.toContain("secret");
      },
   );

   it.each([
      ["PGCHANNELBINDING", "require"],
      ["PGGSSENCMODE", "require"],
      ["PGREQUIREAUTH", "scram-sha-256"],
      ["PGSSLCRL", "/etc/crl.pem"],
      ["PGSSLCRLDIR", "/etc/crl"],
      ["PGSSLMINPROTOCOLVERSION", "TLSv1.3"],
   ] as const)(
      "refuses %s=%s in the environment, naming the variable and not the string",
      (name, value) => {
         process.env[name] = value;
         const error = refusal("host=h password=secret");
         expect(error).toContain(
            `test: ${name} is set in the environment, which the load lock's client cannot honor; unset it.`,
         );
         expect(error).not.toContain("secret");
      },
   );

   it.each([
      "gssencmode=disable",
      "gssencmode=prefer",
      "channel_binding=disable",
      "channel_binding=prefer",
      "ssl_min_protocol_version=TLSv1.2",
      "ssl_min_protocol_version=TLSv1",
      "require_auth=",
   ])("accepts %s, which asks for nothing node-pg lacks", (setting) => {
      expect(config(`host=h ${setting}`)).toMatchObject({ host: "h" });
   });

   it.each([
      "host=h password=secret passfile=/etc/pgpass",
      "postgres://u:secret@h/cat?passfile=%2Fetc%2Fpgpass",
   ])(
      "refuses a passfile key in %s, and names PGPASSFILE instead",
      (conninfo) => {
         const error = refusal(conninfo);
         expect(error).toContain(
            "test: the connection string sets 'passfile', which the load lock's client cannot honor; remove it, and set PGPASSFILE instead.",
         );
         expect(error).not.toContain("secret");
      },
   );

   it("accepts PGPASSFILE in the environment, which node-pg reads itself", () => {
      process.env.PGPASSFILE = "/etc/pgpass";
      expect(config("host=h")).toMatchObject({ host: "h" });
      delete process.env.PGPASSFILE;
   });

   it("accepts the same relaxed values from the environment", () => {
      process.env.PGCHANNELBINDING = "prefer";
      process.env.PGGSSENCMODE = "disable";
      process.env.PGSSLMINPROTOCOLVERSION = "TLSv1.2";
      expect(config("host=h")).toMatchObject({ host: "h" });
   });

   it("lets the connection string override a demanding variable, as libpq does", () => {
      process.env.PGCHANNELBINDING = "require";
      expect(config("host=h channel_binding=disable")).toMatchObject({
         host: "h",
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

   it.each([
      ["postgres://h/cat?ssl=no-verify", { rejectUnauthorized: false }, false],
      ["postgres://h/cat?ssl=1", { rejectUnauthorized: false }, false],
      ["postgres://h/cat?ssl=true", { rejectUnauthorized: false }, false],
      // `ssl` yields to an `sslmode`, as the attach has it.
      ["postgres://h/cat?sslmode=disable&ssl=1", false, false],
      ["postgres://h/cat?ssl=no-verify&sslmode=disable", false, false],
      [
         "postgres://h/cat?sslmode=verify-full&ssl=true",
         { rejectUnauthorized: true },
         false,
      ],
   ] as const)(
      "reads the URI %s as the attach does",
      (conninfo, ssl, plaintextFallback) => {
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

      it("is case-sensitive, as libpq is", () => {
         process.env.PGSSLMODE = "Disable";
         expect(() => config("host=h")).toThrow(
            "test has unsupported sslmode 'Disable'.",
         );
      });
   });

   describe("the deployment's certificate files", () => {
      const cert = join(dir, "env.crt");
      const key = join(dir, "env.key");
      const otherCa = join(dir, "other-ca.pem");
      writeFileSync(cert, "ENV-CERT");
      writeFileSync(key, "ENV-KEY");
      writeFileSync(otherCa, "OTHER-PEM");

      it("supplies the root, certificate and key the string leaves out", () => {
         process.env.PGSSLMODE = "verify-full";
         process.env.PGSSLROOTCERT = ca;
         process.env.PGSSLCERT = cert;
         process.env.PGSSLKEY = key;
         expect(config("host=h").ssl).toEqual({
            rejectUnauthorized: true,
            ca: "PEM-BYTES",
            cert: "ENV-CERT",
            key: "ENV-KEY",
         });
      });

      it("yields to the string's own files, as libpq does", () => {
         process.env.PGSSLROOTCERT = otherCa;
         process.env.PGSSLCERT = join(dir, "missing.crt");
         expect(
            config(
               `host=h sslmode=verify-full sslrootcert=${ca} sslcert=${cert}`,
            ).ssl,
         ).toMatchObject({ ca: "PEM-BYTES", cert: "ENV-CERT" });
      });
   });

   it.each(["PGSERVICE", "PGHOSTADDR"] as const)(
      "refuses %s in the environment, naming the variable and not the string",
      (name) => {
         process.env[name] = "10.0.0.1";
         const error = String(
            (() => {
               try {
                  config("host=h password=secret");
               } catch (e) {
                  return e;
               }
            })(),
         );
         expect(error).toContain(`test: ${name} is set in the environment`);
         expect(error).not.toContain("secret");
      },
   );

   describe("verify-ca with no sslrootcert, which checks no host name", () => {
      it("trusts NODE_EXTRA_CA_CERTS alone, as the DuckDB attach does", () => {
         process.env.NODE_EXTRA_CA_CERTS = ca;
         expect(config("host=h sslmode=verify-ca").ssl).toMatchObject({
            rejectUnauthorized: true,
            ca: "PEM-BYTES",
         });
      });

      it("refuses when NODE_EXTRA_CA_CERTS is unset, rather than trust the public roots", () => {
         expect(() => config("host=h sslmode=verify-ca")).toThrow(
            "test uses sslmode 'verify-ca' but no trusted CA bundle is available (NODE_EXTRA_CA_CERTS is unset).",
         );
         process.env.PGSSLMODE = "verify-ca";
         expect(() => config("host=h")).toThrow(/NODE_EXTRA_CA_CERTS is unset/);
      });

      it("prefers a root the deployment names in PGSSLROOTCERT", () => {
         process.env.PGSSLROOTCERT = ca;
         expect(config("host=h sslmode=verify-ca").ssl).toMatchObject({
            ca: "PEM-BYTES",
         });
      });
   });

   it("trusts the runtime's ambient anchors when verify-full names no sslrootcert", () => {
      const { ssl } = config("host=h sslmode=verify-full") as {
         ssl: { ca: string };
      };
      expect(ssl.ca).toContain("BEGIN CERTIFICATE");
   });

   it("reads sslrootcert=system as the ambient anchors, defaulting to verify-full as libpq 16 does", () => {
      const { ssl } = config("host=db.internal sslrootcert=system") as {
         ssl: { ca: string; rejectUnauthorized: boolean };
      };
      expect(ssl.rejectUnauthorized).toBe(true);
      expect(ssl.ca).toContain("BEGIN CERTIFICATE");
   });

   it.each(["require", "prefer", "verify-ca"])(
      "refuses sslmode %s with sslrootcert=system",
      (sslmode) => {
         expect(() =>
            config(`host=h sslmode=${sslmode} sslrootcert=system`),
         ).toThrow(
            `test: weak sslmode '${sslmode}' may not be used with sslrootcert=system (use 'verify-full').`,
         );
      },
   );

   it("names the certificate file it cannot read", () => {
      expect(() =>
         config("host=h sslmode=verify-full sslrootcert=/nonexistent/ca.pem"),
      ).toThrow(
         "test: cannot read the sslrootcert file '/nonexistent/ca.pem' (ENOENT).",
      );
   });

   it("refuses an sslmode it does not know, naming the mode and not the string", () => {
      expect(() => config("host=h password=secret sslmode=bogus")).toThrow(
         "test has unsupported sslmode 'bogus'.",
      );
   });
});

// A server that answers an SSLRequest with `N`, then a plaintext startup with a ready session.
describe("connectPgClient's plaintext fallback", () => {
   let server: Server;
   let port: number;
   let sslRequests: number;
   let plaintextStartups: number;

   beforeEach(async () => {
      sslRequests = 0;
      plaintextStartups = 0;
      server = createServer((socket: Socket) => {
         socket.once("data", (first) => {
            // SSLRequest: length 8, code 80877103.
            if (first.length === 8 && first.readInt32BE(4) === 80877103) {
               sslRequests++;
               socket.write("N");
               return;
            }
            plaintextStartups++;
            // AuthenticationOk, then ReadyForQuery.
            socket.write(
               Buffer.from([
                  0x52, 0, 0, 0, 8, 0, 0, 0, 0, 0x5a, 0, 0, 0, 5, 0x49,
               ]),
            );
         });
         socket.on("error", () => undefined);
      });
      await new Promise<void>((resolve) =>
         server.listen(0, "127.0.0.1", resolve),
      );
      port = (server.address() as { port: number }).port;
   });
   afterEach(() => {
      server.close();
   });

   it("retries in plaintext when sslmode=prefer meets a server without TLS", async () => {
      const client = await connectPgClient(
         "test",
         `host=127.0.0.1 port=${port} user=u dbname=d sslmode=prefer`,
      );
      await client.end();
      expect([sslRequests, plaintextStartups]).toEqual([1, 1]);
   });

   it("does not downgrade under sslmode=require, which fails on the refusal", async () => {
      await expect(
         connectPgClient(
            "test",
            `host=127.0.0.1 port=${port} user=u dbname=d sslmode=require`,
         ),
      ).rejects.toThrow(/does not support SSL/i);
      expect([sslRequests, plaintextStartups]).toEqual([1, 0]);
   });
});

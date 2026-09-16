// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Unit contract for the build-scoped credential primitives: the SQL a build
// session issues to (a) RW-attach a destination DuckLake and (b) federate a
// source warehouse for a native query-passthrough. `runSQL` is stubbed so no
// live warehouse / catalog is touched — this pins the SQL *shape* (handles,
// secret scoping, read-only-ness, escaping); live behavior is the measured
// spike's job.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import type { components } from "../api";
import {
   attachDuckLakeReadWrite,
   federateSourceForPassthrough,
   buildProxiedPgAttachString,
} from "./connection";

/** A real in-memory DuckDB connection with runSQL stubbed to capture SQL. */
function stubbedConnection(): { conn: DuckDBConnection; sql: string[] } {
   const conn = new DuckDBConnection("build", ":memory:");
   const sql: string[] = [];
   sinon.stub(conn, "runSQL").callsFake(async (q: string) => {
      sql.push(q);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return { rows: [], totalRows: 0, runStats: {} } as any;
   });
   return { conn, sql };
}

const SERVICE_ACCOUNT = JSON.stringify({
   type: "service_account",
   project_id: "sa-project",
   private_key: "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
   client_email: "svc@sa-project.iam.gserviceaccount.com",
});

describe("federateSourceForPassthrough", () => {
   it("bigquery: creates a project-scoped SECRET and returns the project id as handle", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "bigquery", {
         name: "src_bq",
         bigqueryConnection: {
            defaultProjectId: "cfg-project",
            serviceAccountKeyJson: SERVICE_ACCOUNT,
         } as components["schemas"]["BigqueryConnection"],
      });
      expect(result).toEqual({ handle: "sa-project", sourceType: "bigquery" });
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"));
      expect(secret).toBeDefined();
      expect(secret).toContain("TYPE BIGQUERY");
      expect(secret).toContain("SCOPE 'bq://sa-project'");
      expect(secret).toContain("SERVICE_ACCOUNT_JSON");
      // Never ATTACH for a bigquery passthrough.
      expect(sql.some((s) => s.includes("ATTACH"))).toBe(false);
   });

   it("snowflake: creates a SECRET and returns the secret name as handle (no ATTACH)", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "snowflake", {
         name: "src_sf",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            password: "p'wd",
            database: "DB",
            warehouse: "WH",
         } as components["schemas"]["SnowflakeConnection"],
      });
      expect(result.sourceType).toBe("snowflake");
      expect(result.handle).toBe("secret_snowflake_src_sf");
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"));
      expect(secret).toContain("TYPE snowflake");
      expect(secret).toContain("ACCOUNT 'acct'");
      // Single quotes in a value are doubled (escapeSQL).
      expect(secret).toContain("PASSWORD 'p''wd'");
      expect(sql.some((s) => s.includes("ATTACH"))).toBe(false);
   });

   it("snowflake: federates a KEY PAIR connection", async () => {
      // Requiring a password made every key-pair connection unbuildable into a
      // storage destination though it queries fine live — and key-pair is where
      // Snowflake is steering programmatic access.
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "snowflake", {
         name: "src_sf_kp",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            privateKey:
               "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
            privateKeyPass: "pass'phrase",
            warehouse: "WH",
         } as components["schemas"]["SnowflakeConnection"],
      });
      expect(result.sourceType).toBe("snowflake");
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"));
      expect(secret).toContain("AUTH_TYPE 'key_pair'");
      expect(secret).toContain("PRIVATE KEY-----");
      expect(secret).toContain("PRIVATE_KEY_PASSWORD 'pass''phrase'");
      // No BARE password field — not even an empty one, which is what the
      // extension rejects alongside a key pair. Anchored to the start of the
      // emitted line: an unanchored "PASSWORD '" also matches inside
      // PRIVATE_KEY_PASSWORD and would fail on a correct secret.
      expect(secret).not.toContain("\n   PASSWORD '");
   });

   it("snowflake: omits DATABASE from the secret when the connection has none", async () => {
      // A database-less connection is now loadable and queryable, so it can also
      // reach a `storage=` build. The secret simply carries no DATABASE, which
      // means the passthrough session has no current database and any SQL run
      // through it must name tables in full. Pinned so the emitted secret is a
      // deliberate shape rather than an accident of the conditional spread: an
      // empty `DATABASE ''` line would be worse than its absence, since the
      // extension would take it as a real (empty) database name.
      const { conn, sql } = stubbedConnection();
      await federateSourceForPassthrough(conn, "snowflake", {
         name: "src_sf_nodb",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            password: "pwd",
            warehouse: "WH",
         } as components["schemas"]["SnowflakeConnection"],
      });
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"))!;
      expect(secret).not.toContain("DATABASE");
      expect(secret).toContain("WAREHOUSE 'WH'");
   });

   it("snowflake: normalizes a single-line private key, as the live path does", async () => {
      // The shape that matters. A multi-line PEM is already valid, so a test
      // using one passes whether or not the key is normalized. A SINGLE-LINE key
      // is accepted by the live path (which normalizes at its own call site) and
      // is what a user pasting from a secret store actually supplies — and left
      // unreflowed, its header has no trailing newline, which makes Go's
      // pem.Decode return nil and the build fail on a key that queries fine.
      const { conn, sql } = stubbedConnection();
      const body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQ".repeat(
         3,
      );
      await federateSourceForPassthrough(conn, "snowflake", {
         name: "src_sf_oneline",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            privateKey: `-----BEGIN PRIVATE KEY-----${body}-----END PRIVATE KEY-----`,
         } as components["schemas"]["SnowflakeConnection"],
      });
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"))!;
      // Reflowed: a newline directly after the header rather than base64.
      expect(secret).toContain("-----BEGIN PRIVATE KEY-----\n");
      expect(secret).not.toContain(
         `-----BEGIN PRIVATE KEY-----${body.slice(0, 8)}`,
      );
   });

   it("snowflake: carries ROLE and SCHEMA so a build matches the live connection", async () => {
      // Both are folded into the Malloy connection's digest, i.e. they are part
      // of what identifies the connection. Dropping them runs the build under the
      // user's DEFAULT role while live queries use the configured one.
      const { conn, sql } = stubbedConnection();
      await federateSourceForPassthrough(conn, "snowflake", {
         name: "src_sf_role",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            password: "pw",
            role: "REPORTING",
            schema: "ANALYTICS",
         } as components["schemas"]["SnowflakeConnection"],
      });
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"));
      expect(secret).toContain("ROLE 'REPORTING'");
      expect(secret).toContain("SCHEMA 'ANALYTICS'");
   });

   it("snowflake: refuses a connection carrying neither credential", async () => {
      const { conn } = stubbedConnection();
      await expect(
         federateSourceForPassthrough(conn, "snowflake", {
            name: "src_sf_none",
            snowflakeConnection: {
               account: "acct",
               username: "user",
            } as components["schemas"]["SnowflakeConnection"],
         }),
      ).rejects.toThrow(/privateKey or password is required/);
   });

   it("postgres: ATTACHes READ_ONLY and returns the alias as handle", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "postgres", {
         name: "src_pg",
         postgresConnection: {
            host: "h",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "pw",
         } as components["schemas"]["PostgresConnection"],
      });
      expect(result).toEqual({ handle: "src_pg", sourceType: "postgres" });
      const attach = sql.find((s) => s.startsWith("ATTACH"));
      expect(attach).toBeDefined();
      // OR REPLACE for within-session idempotency (a re-attach of the identical
      // source on one session is a no-op rebind). Cross-build/cross-tenant alias
      // collisions are prevented upstream by each build running on its OWN
      // private DuckDB instance (createIsolatedBuildSession); this is just
      // belt-and-suspenders.
      expect(attach).toStartWith("ATTACH OR REPLACE ");
      // The ATTACH alias is dialect-quoted; the handle stays the raw name (it
      // becomes the postgres_query string literal, which matches the catalog).
      expect(attach).toContain('AS "src_pg" (TYPE postgres, READ_ONLY)');
   });

   it("postgres: re-federating the same source on one session is idempotent (no 'already exists')", async () => {
      // Within one session, federating the same source twice must not throw:
      // both attaches use OR REPLACE (a no-op rebind to the identical source).
      // (A real DuckDB would raise "database ... already exists" on a plain
      // re-ATTACH; here runSQL is stubbed, so we pin the SQL contract.)
      const { conn, sql } = stubbedConnection();
      const cfg = {
         name: "src_pg",
         postgresConnection: {
            host: "h",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "pw",
         } as components["schemas"]["PostgresConnection"],
      };
      await federateSourceForPassthrough(conn, "postgres", cfg);
      await federateSourceForPassthrough(conn, "postgres", cfg);
      const attaches = sql.filter((s) => s.startsWith("ATTACH"));
      expect(attaches).toHaveLength(2);
      for (const a of attaches) expect(a).toStartWith("ATTACH OR REPLACE ");
   });

   it("postgres: dialect-quotes an alias that needs quoting (e.g. a hyphen)", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "postgres", {
         name: "my-pg",
         postgresConnection: {
            host: "h",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "pw",
         } as components["schemas"]["PostgresConnection"],
      });
      expect(result.handle).toBe("my-pg");
      const attach = sql.find((s) => s.startsWith("ATTACH"));
      expect(attach).toContain('AS "my-pg" (TYPE postgres, READ_ONLY)');
   });

   it("postgres: a proxied source is federated through its tunnel, not at its own host", async () => {
      const { conn, sql } = stubbedConnection();
      let closed = 0;
      let dialled: { host: string; port: number } | undefined;
      const openProxy = async (
         _proxy: components["schemas"]["ConnectionProxy"],
         target: { host: string; port: number },
      ) => {
         dialled = target;
         return {
            host: "127.0.0.1",
            port: 54321,
            close: async () => {
               closed += 1;
            },
         };
      };
      const result = await federateSourceForPassthrough(
         conn,
         "postgres",
         {
            name: "src_pg",
            postgresConnection: {
               host: "db.internal.example",
               port: 5432,
               databaseName: "d",
               userName: "u",
               password: "pw",
            } as components["schemas"]["PostgresConnection"],
            proxy: {
               type: "ssh",
               ssh: { host: "bastion.example", port: 22, username: "tunnel" },
            } as components["schemas"]["ConnectionProxy"],
         },
         { openProxy },
      );
      // The tunnel is opened to the database's own host:port …
      expect(dialled).toEqual({ host: "db.internal.example", port: 5432 });
      // … and the ATTACH goes to the tunnel's local endpoint, never the real host.
      const attach = sql.find((s) => s.startsWith("ATTACH"));
      expect(attach).toContain("host=127.0.0.1 port=54321");
      expect(attach).not.toContain("db.internal.example");
      expect(attach).toContain("dbname=d user=u password=pw");
      // Encrypt without verifying by default (a force-SSL target must not be
      // refused for plaintext; the query path defaults the same way).
      expect(attach).toContain("sslmode=require");
      expect(attach).toContain('AS "src_pg" (TYPE postgres, READ_ONLY)');
      expect(result.handle).toBe("src_pg");
      // The handle carries the tunnel's close for the build session's finally.
      expect(result.close).toBeDefined();
      await result.close!();
      expect(closed).toBe(1);
   });

   it("postgres: a proxied attach that fails closes the tunnel it opened", async () => {
      const conn = new DuckDBConnection("build", ":memory:");
      sinon.stub(conn, "runSQL").callsFake(async (q: string) => {
         if (q.startsWith("ATTACH")) throw new Error("boom");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         return { rows: [], totalRows: 0, runStats: {} } as any;
      });
      let closed = 0;
      const openProxy = async () => ({
         host: "127.0.0.1",
         port: 54321,
         close: async () => {
            closed += 1;
         },
      });
      await expect(
         federateSourceForPassthrough(
            conn,
            "postgres",
            {
               name: "src_pg",
               postgresConnection: {
                  host: "h",
                  port: 5432,
               } as components["schemas"]["PostgresConnection"],
               proxy: {
                  type: "ssh",
                  ssh: { host: "b", port: 22, username: "t" },
               } as components["schemas"]["ConnectionProxy"],
            },
            { openProxy },
         ),
      ).rejects.toThrow("boom");
      expect(closed).toBe(1);
   });

   it("postgres: an unproxied source still attaches at its own host, with no close", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "postgres", {
         name: "src_pg",
         postgresConnection: {
            host: "h",
            port: 5432,
         } as components["schemas"]["PostgresConnection"],
      });
      expect(sql.find((s) => s.startsWith("ATTACH"))).toContain(
         "host=h port=5432",
      );
      expect(result.close).toBeUndefined();
   });

   it("rejects a source type with no native passthrough", async () => {
      const { conn } = stubbedConnection();
      await expect(
         federateSourceForPassthrough(conn, "mysql" as unknown as "postgres", {
            name: "x",
         }),
      ).rejects.toThrow(/no native query-passthrough/i);
   });

   it("fails loud when the configured credentials are missing", async () => {
      const { conn } = stubbedConnection();
      await expect(
         federateSourceForPassthrough(conn, "bigquery", { name: "x" }),
      ).rejects.toThrow(/BigQuery connection configuration missing/i);
   });
});

describe("attachDuckLakeReadWrite", () => {
   const ducklakeConfig = {
      catalog: {
         postgresConnection: {
            host: "cat-host",
            port: 5432,
            databaseName: "ducklake",
            userName: "ducklake",
            password: "secret",
         },
      },
      storage: { bucketUrl: "gs://org-env-ducklake/prefix" },
   } as unknown as components["schemas"]["DucklakeConnection"];

   it("attaches read-write: no READ_ONLY, no AUTOMATIC_MIGRATION, keeps OVERRIDE_DATA_PATH", async () => {
      const { conn, sql } = stubbedConnection();
      await attachDuckLakeReadWrite(conn, "lake", ducklakeConfig);
      const attach = sql.find((s) => s.includes("ATTACH OR REPLACE"));
      expect(attach).toBeDefined();
      expect(attach).toContain("OVERRIDE_DATA_PATH true");
      expect(attach).not.toContain("READ_ONLY");
      expect(attach).not.toContain("AUTOMATIC_MIGRATION");
      expect(attach).toContain("AS lake");
   });

   // Both write bounds are covered in isolation by their own specs, which pass
   // whether or not anything calls them -- deleting a call site is invisible
   // there. This asserts the WIRING: that a read-write attach actually reaches
   // both. The read-only counterpart is not asserted here because that attach is
   // module-private and reachable only through the connection factory; the gate
   // itself is one `if (!options.readOnly)` that both calls sit inside.
   describe("write bounds reach the attach", () => {
      afterEach(() => {
         delete process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES;
         delete process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES;
      });

      it("read-write issues both configured catalog options", async () => {
         process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES = "32MB";
         process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = "256MB";
         const { conn, sql } = stubbedConnection();
         await attachDuckLakeReadWrite(conn, "lake", ducklakeConfig);
         expect(
            sql.some((s) =>
               s.includes(
                  "CALL lake.set_option('parquet_row_group_size_bytes', '32MB')",
               ),
            ),
         ).toBe(true);
         expect(
            sql.some((s) =>
               s.includes("CALL lake.set_option('target_file_size', '256MB')"),
            ),
         ).toBe(true);
      });
   });
});

describe("buildProxiedPgAttachString", () => {
   const endpoint = { host: "127.0.0.1", port: 6000, close: async () => {} };
   const pg = (sslmode?: string) =>
      ({
         host: "real.example",
         port: 5432,
         databaseName: "d",
         userName: "u",
         password: "p",
         sslmode,
      }) as components["schemas"]["PostgresConnection"];
   const withCaBundle = async (value: string | undefined, fn: () => void) => {
      const prev = process.env.NODE_EXTRA_CA_CERTS;
      if (value === undefined) delete process.env.NODE_EXTRA_CA_CERTS;
      else process.env.NODE_EXTRA_CA_CERTS = value;
      try {
         fn();
      } finally {
         if (prev === undefined) delete process.env.NODE_EXTRA_CA_CERTS;
         else process.env.NODE_EXTRA_CA_CERTS = prev;
      }
   };

   it("targets the tunnel endpoint and defaults to sslmode=require", () => {
      const s = buildProxiedPgAttachString("c", pg(undefined), endpoint);
      expect(s).toBe(
         "host=127.0.0.1 port=6000 dbname=d user=u password=p sslmode=require",
      );
   });

   it("maps disable and no-verify", () => {
      expect(
         buildProxiedPgAttachString("c", pg("disable"), endpoint),
      ).toContain("sslmode=disable");
      expect(
         buildProxiedPgAttachString("c", pg("no-verify"), endpoint),
      ).toContain("sslmode=require");
   });

   it("verify-ca needs the trusted CA bundle and passes it as sslrootcert", async () => {
      await withCaBundle(undefined, () => {
         expect(() =>
            buildProxiedPgAttachString("c", pg("verify-ca"), endpoint),
         ).toThrow(/NODE_EXTRA_CA_CERTS/);
      });
      await withCaBundle("/etc/ssl/rds.pem", () => {
         expect(
            buildProxiedPgAttachString("c", pg("verify-ca"), endpoint),
         ).toContain("sslmode=verify-ca sslrootcert=/etc/ssl/rds.pem");
      });
   });

   it("verify-full verifies the chain through the tunnel (hostname cannot be checked at 127.0.0.1)", async () => {
      await withCaBundle("/etc/ssl/rds.pem", () => {
         expect(
            buildProxiedPgAttachString("c", pg("verify-full"), endpoint),
         ).toContain("sslmode=verify-ca sslrootcert=/etc/ssl/rds.pem");
      });
   });

   it("refuses an unknown sslmode", () => {
      expect(() =>
         buildProxiedPgAttachString("c", pg("prefer"), endpoint),
      ).toThrow(/unsupported sslmode/);
   });
});

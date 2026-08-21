// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { generateKeyPairSync } from "crypto";
import { components } from "../api";
import {
   assembleEnvironmentConnections,
   normalizeSnowflakePrivateKey,
   validateStorageDestinations,
} from "./connection_config";

type ApiConnection = components["schemas"]["Connection"];

describe("assembleEnvironmentConnections — databricks", () => {
   const validBase: ApiConnection = {
      name: "dbx",
      type: "databricks",
      databricksConnection: {
         host: "dbc.cloud.databricks.com",
         path: "/sql/1.0/warehouses/abc",
         token: "dapiXXXX",
         defaultCatalog: "main",
         defaultSchema: "default",
      },
   };

   it("emits a databricks core entry with all known fields preserved", () => {
      const { pojo, apiConnections } = assembleEnvironmentConnections([
         validBase,
      ]);

      const entry = pojo.connections["dbx"];
      expect(entry.is).toBe("databricks");
      expect(entry.host).toBe("dbc.cloud.databricks.com");
      expect(entry.path).toBe("/sql/1.0/warehouses/abc");
      expect(entry.token).toBe("dapiXXXX");
      expect(entry.defaultCatalog).toBe("main");
      expect(entry.defaultSchema).toBe("default");

      expect(apiConnections).toHaveLength(1);
      expect(apiConnections[0].attributes?.dialectName).toBe("databricks");
   });

   it("accepts OAuth M2M auth (clientId + secret) without a token", () => {
      const conn: ApiConnection = {
         name: "dbx-oauth",
         type: "databricks",
         databricksConnection: {
            host: "dbc.cloud.databricks.com",
            path: "/sql/1.0/warehouses/abc",
            oauthClientId: "client-id",
            oauthClientSecret: "client-secret",
            defaultCatalog: "main",
         },
      };
      const { pojo } = assembleEnvironmentConnections([conn]);
      const entry = pojo.connections["dbx-oauth"];
      expect(entry.is).toBe("databricks");
      expect(entry.oauthClientId).toBe("client-id");
      expect(entry.oauthClientSecret).toBe("client-secret");
      expect(entry.token).toBeUndefined();
   });

   it("rejects connections missing the databricksConnection block", () => {
      const conn: ApiConnection = {
         name: "dbx",
         type: "databricks",
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks connection configuration is missing.",
      );
   });

   it("rejects connections with a missing host", () => {
      const conn: ApiConnection = {
         ...validBase,
         databricksConnection: {
            ...validBase.databricksConnection!,
            host: undefined,
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks host is required",
      );
   });

   it("rejects connections with a missing path", () => {
      const conn: ApiConnection = {
         ...validBase,
         databricksConnection: {
            ...validBase.databricksConnection!,
            path: undefined,
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks SQL warehouse HTTP path is required",
      );
   });

   it("rejects when defaultCatalog is missing", () => {
      const conn: ApiConnection = {
         name: "dbx",
         type: "databricks",
         databricksConnection: {
            host: "dbc.cloud.databricks.com",
            path: "/sql/1.0/warehouses/abc",
            token: "dapiXXXX",
            // defaultCatalog deliberately omitted
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks default catalog is required",
      );
   });

   it("rejects when neither token nor full OAuth credentials are provided", () => {
      const conn: ApiConnection = {
         name: "dbx",
         type: "databricks",
         databricksConnection: {
            host: "dbc.cloud.databricks.com",
            path: "/sql/1.0/warehouses/abc",
            // Only oauthClientId, missing secret → rejected.
            oauthClientId: "client-id",
            defaultCatalog: "main",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks requires",
      );
   });
});

describe("assembleEnvironmentConnections — snowflake", () => {
   // An EXPLICIT null in config (or a client serializing an unset optional as
   // null) survives to the connector; an omitted field arrives as `undefined`
   // and was always fine. Malloy's makeDigest reads `.length` off every part and
   // special-cases `undefined` alone, so a surviving `null` throws when a digest
   // is taken -- which happens on the package-load worker's connection-metadata
   // RPC, surfacing as the whole package failing to load with "import reference
   // failure" rather than as a connection error.
   //
   // These assertions cover the pojo path, which Malloy's own lookup also
   // guards. The key-pair path that actually reproduced the bug is pinned in
   // connection.spec.ts.
   const nullableFields = ["database", "schema", "role"] as const;

   function snowflakeConnection(
      overrides: Record<string, unknown>,
   ): ApiConnection {
      return {
         name: "sf",
         type: "snowflake",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            password: "pw",
            warehouse: "wh",
            ...overrides,
         },
      } as ApiConnection;
   }

   for (const field of nullableFields) {
      it(`omits a null ${field} from the core entry`, () => {
         const { pojo } = assembleEnvironmentConnections([
            snowflakeConnection({ [field]: null }),
         ]);

         const entry = pojo.connections["sf"] as Record<string, unknown>;
         // Explicitly not null: `undefined` is what the digest tolerates.
         expect(entry[field]).toBeUndefined();
         expect(entry[field]).not.toBeNull();
      });
   }

   it("preserves a configured database", () => {
      const { pojo } = assembleEnvironmentConnections([
         snowflakeConnection({ database: "MYDB", schema: "MYSCHEMA" }),
      ]);

      const entry = pojo.connections["sf"] as Record<string, unknown>;
      expect(entry.database).toBe("MYDB");
      expect(entry.schema).toBe("MYSCHEMA");
   });

   it("carries no null through to any core-entry value", () => {
      const { pojo } = assembleEnvironmentConnections([
         snowflakeConnection({ database: null, schema: null, role: null }),
      ]);

      const entry = pojo.connections["sf"] as Record<string, unknown>;
      const nulls = Object.entries(entry)
         .filter(([, value]) => value === null)
         .map(([key]) => key);
      expect(nulls).toEqual([]);
   });
});

describe("normalizeSnowflakePrivateKey", () => {
   const { privateKey: pkcs8Pem } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
      publicKeyEncoding: { type: "spki", format: "pem" },
   });
   const { privateKey: pkcs1Pem } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      privateKeyEncoding: { type: "pkcs1", format: "pem" },
      publicKeyEncoding: { type: "pkcs1", format: "pem" },
   });

   it("passes a multi-line PKCS#8 key through and adds a trailing newline", () => {
      const trimmed = (pkcs8Pem as string).trimEnd();
      const result = normalizeSnowflakePrivateKey(trimmed);
      expect(result).toContain("-----BEGIN PRIVATE KEY-----");
      expect(result.endsWith("\n")).toBe(true);
   });

   it("converts a multi-line PKCS#1 RSA key to PKCS#8", () => {
      const result = normalizeSnowflakePrivateKey(pkcs1Pem as string);
      expect(result).toContain("-----BEGIN PRIVATE KEY-----");
      expect(result).not.toContain("BEGIN RSA PRIVATE KEY");
      expect(result.endsWith("\n")).toBe(true);
   });

   it("converts a single-line PKCS#1 RSA key to PKCS#8", () => {
      const singleLine = (pkcs1Pem as string).replace(/\n/g, "");
      const result = normalizeSnowflakePrivateKey(singleLine);
      expect(result).toContain("-----BEGIN PRIVATE KEY-----");
      expect(result).not.toContain("BEGIN RSA PRIVATE KEY");
   });

   it("reconstructs a single-line PKCS#8 key without conversion", () => {
      const singleLine = (pkcs8Pem as string).replace(/\n/g, "");
      const result = normalizeSnowflakePrivateKey(singleLine);
      expect(result.startsWith("-----BEGIN PRIVATE KEY-----\n")).toBe(true);
      expect(result.endsWith("-----END PRIVATE KEY-----\n")).toBe(true);
   });
});

describe("assembleEnvironmentConnections — publisher", () => {
   const validBase: ApiConnection = {
      name: "analytics",
      type: "publisher",
      publisherConnection: {
         connectionUri:
            "https://org.data.example.com/api/v0/environments/proj/connections/analytics",
         accessToken: "jwt-token",
      },
   };

   // publisher connections are default-deny (SSRF gate); these assembly tests
   // exercise the enabled path, so opt in for the block and restore after.
   const priorFlag = process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
   beforeEach(() => {
      process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = "true";
   });
   afterEach(() => {
      if (priorFlag === undefined) {
         delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
      } else {
         process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = priorFlag;
      }
   });

   it("emits a publisher core entry proxying to the remote dataplane", () => {
      const { pojo } = assembleEnvironmentConnections([validBase]);

      const entry = pojo.connections["analytics"];
      expect(entry.is).toBe("publisher");
      expect(entry.connectionUri).toBe(
         "https://org.data.example.com/api/v0/environments/proj/connections/analytics",
      );
      expect(entry.accessToken).toBe("jwt-token");
   });

   it("does not populate static connection attributes (dialect is resolved at runtime)", () => {
      const { apiConnections } = assembleEnvironmentConnections([validBase]);
      expect(apiConnections).toHaveLength(1);
      expect(apiConnections[0].attributes).toBeUndefined();
   });

   it("assembles without an accessToken (optional)", () => {
      const conn: ApiConnection = {
         name: "analytics",
         type: "publisher",
         publisherConnection: {
            connectionUri:
               "https://org.data.example.com/api/v0/environments/proj/connections/analytics",
         },
      };
      const { pojo } = assembleEnvironmentConnections([conn]);
      const entry = pojo.connections["analytics"];
      expect(entry.is).toBe("publisher");
      expect(entry.accessToken).toBeUndefined();
   });

   it("rejects a publisher connection missing connectionUri with an actionable error", () => {
      const conn: ApiConnection = {
         name: "analytics",
         type: "publisher",
         publisherConnection:
            {} as components["schemas"]["PublisherConnection"],
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Invalid publisher connection 'analytics': missing connectionUri.",
      );
   });

   it("rejects a publisher connection missing the publisherConnection block", () => {
      const conn: ApiConnection = {
         name: "analytics",
         type: "publisher",
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Invalid publisher connection 'analytics': missing connectionUri.",
      );
   });

   it("rejects a publisher connection whose connectionUri is not a valid URL", () => {
      const conn: ApiConnection = {
         name: "analytics",
         type: "publisher",
         publisherConnection: { connectionUri: "not a url" },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Invalid publisher connection 'analytics': connectionUri is not a valid URL.",
      );
   });

   it("rejects a publisher connection whose connectionUri uses a non-http(s) scheme", () => {
      const conn: ApiConnection = {
         name: "analytics",
         type: "publisher",
         publisherConnection: { connectionUri: "file:///etc/passwd" },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "connectionUri must use http or https (got 'file:')",
      );
   });

   it("does not echo a credential-bearing connectionUri in the validation error", () => {
      const conn: ApiConnection = {
         name: "analytics",
         type: "publisher",
         // Userinfo present but the URI is otherwise malformed (space in host)
         // so it fails to parse and must not be reflected back verbatim.
         publisherConnection: {
            connectionUri: "https://user:s3cret@bad host/connections/x",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "connectionUri is not a valid URL",
      );
      expect(() => assembleEnvironmentConnections([conn])).not.toThrow(
         /s3cret/,
      );
   });

   it("still rejects the reserved 'duckdb' name for a publisher connection", () => {
      const conn: ApiConnection = {
         name: "duckdb",
         type: "publisher",
         publisherConnection: { connectionUri: "https://x/connections/duckdb" },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Connection name 'duckdb' is reserved",
      );
   });

   it("still rejects a publisher connection with no name", () => {
      const conn = {
         type: "publisher",
         publisherConnection: { connectionUri: "https://x/connections/y" },
      } as ApiConnection;
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Invalid connection configuration. No name.",
      );
   });

   describe("SSRF gate (PUBLISHER_ALLOW_PROXY_CONNECTIONS)", () => {
      it("denies a valid publisher connection when the flag is unset (default-deny)", () => {
         delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
         expect(() => assembleEnvironmentConnections([validBase])).toThrow(
            "Publisher proxy connection 'analytics' is disabled in this deployment",
         );
      });

      it("error names the env var to flip", () => {
         delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
         expect(() => assembleEnvironmentConnections([validBase])).toThrow(
            "Fix: set the environment variable PUBLISHER_ALLOW_PROXY_CONNECTIONS=true",
         );
      });

      it("denies for any non-'true' value (fail-closed)", () => {
         process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = "1";
         expect(() => assembleEnvironmentConnections([validBase])).toThrow(
            "is disabled in this deployment",
         );
      });

      it("gate fires before the connectionUri shape check", () => {
         // A publisher connection missing connectionUri still surfaces the
         // disabled error first when the gate is closed — the type is refused
         // outright, not validated.
         delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
         const conn: ApiConnection = { name: "analytics", type: "publisher" };
         expect(() => assembleEnvironmentConnections([conn])).toThrow(
            "is disabled in this deployment",
         );
      });

      it("allows a valid publisher connection when the flag is 'true'", () => {
         process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = "true";
         const { pojo } = assembleEnvironmentConnections([validBase]);
         expect(pojo.connections["analytics"].is).toBe("publisher");
      });
   });
});

describe("SSH proxy validation", () => {
   const validSshProxy: ApiConnection = {
      name: "pg-via-bastion",
      type: "postgres",
      postgresConnection: {
         host: "127.0.0.1",
         port: 5432,
         databaseName: "mydb",
         userName: "user",
         password: "pass",
      },
      proxy: {
         type: "ssh",
         ssh: {
            host: "bastion.example.com",
            username: "ec2-user",
            privateKey:
               "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----",
         },
      },
   };

   it("allows a postgres+ssh-proxy connection with no env flag required", () => {
      // The SSH proxy is a normal connection capability — deliberately NOT gated
      // by PUBLISHER_ALLOW_PROXY_CONNECTIONS (that flag is for the `publisher`
      // HTTP multi-hop type). Prove it assembles with the flag unset.
      const priorFlag = process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
      delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
      try {
         const { pojo } = assembleEnvironmentConnections([validSshProxy]);
         expect(pojo.connections["pg-via-bastion"].is).toBe("postgres");
      } finally {
         if (priorFlag === undefined) {
            delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
         } else {
            process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = priorFlag;
         }
      }
   });

   it("rejects a non-postgres (bigquery) connection with a proxy", () => {
      const conn: ApiConnection = {
         name: "bq-via-bastion",
         type: "bigquery",
         bigqueryConnection: {
            serviceAccountKeyJson: JSON.stringify({
               type: "service_account",
               project_id: "proj",
               private_key: "key",
               client_email: "sa@proj.iam.gserviceaccount.com",
            }),
         },
         proxy: {
            type: "ssh",
            ssh: {
               host: "bastion.example.com",
               username: "ec2-user",
               privateKey: "key",
            },
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Connection proxy is not supported for type 'bigquery' (only 'postgres' today)",
      );
   });

   it("rejects an ssh proxy with no ssh config object", () => {
      const conn: ApiConnection = {
         name: "pg-no-ssh-config",
         type: "postgres",
         postgresConnection: {
            host: "127.0.0.1",
            databaseName: "mydb",
         },
         proxy: {
            type: "ssh",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Connection proxy on 'pg-no-ssh-config' has type 'ssh' but no 'ssh' config object",
      );
   });

   it("rejects a proxied postgres connection that also carries a connectionString", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         name: "pg-with-connstring",
         postgresConnection: {
            connectionString: "postgresql://real-db.example.com:5432/mydb",
            host: "127.0.0.1",
            port: 5432,
            databaseName: "mydb",
         },
      };
      // The tunnel forwards to host/port; a connectionString would be silently
      // ignored and could point at a different database, so it's rejected.
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "does not support the connectionString form",
      );
   });

   it("rejects a proxied postgres connection missing explicit host/port", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         name: "pg-no-host-port",
         postgresConnection: {
            databaseName: "mydb",
            userName: "user",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "requires explicit host and port",
      );
   });

   it("accepts a multi-line hostKey and sslmode=no-verify", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         postgresConnection: {
            ...validSshProxy.postgresConnection!,
            sslmode: "no-verify",
         },
         proxy: {
            type: "ssh",
            ssh: {
               ...validSshProxy.proxy!.ssh!,
               hostKey:
                  "bastion ssh-ed25519 AAAAdecoy\nbastion ssh-rsa AAAAreal",
            },
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).not.toThrow();
   });

   it("rejects a hostKey that parses to zero keys (comment/blank only) — fail-closed at config load", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         proxy: {
            type: "ssh",
            ssh: {
               ...validSshProxy.proxy!.ssh!,
               hostKey: "# ssh-keyscan bastion timed out\n   \n",
            },
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "no usable host-key line",
      );
   });

   it("rejects a whitespace-only hostKey (must not silently connect unpinned)", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         proxy: {
            type: "ssh",
            ssh: {
               ...validSshProxy.proxy!.ssh!,
               hostKey: "   ",
            },
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "no usable host-key line",
      );
   });

   it("treats an empty-string hostKey as unpinned (omission-equivalent)", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         proxy: {
            type: "ssh",
            ssh: { ...validSshProxy.proxy!.ssh!, hostKey: "" },
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).not.toThrow();
   });

   it("rejects an unsupported sslmode", () => {
      const conn = {
         ...validSshProxy,
         postgresConnection: {
            ...validSshProxy.postgresConnection!,
            sslmode: "require",
         },
      } as unknown as ApiConnection;
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "unsupported sslmode 'require'",
      );
   });

   it("rejects an empty-string sslmode at config load (not deferred to connect)", () => {
      const conn = {
         ...validSshProxy,
         postgresConnection: {
            ...validSshProxy.postgresConnection!,
            sslmode: "",
         },
      } as unknown as ApiConnection;
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "unsupported sslmode",
      );
   });

   it("rejects sslmode=verify-ca when no CA bundle is available", () => {
      const prior = process.env.NODE_EXTRA_CA_CERTS;
      delete process.env.NODE_EXTRA_CA_CERTS;
      try {
         const conn: ApiConnection = {
            ...validSshProxy,
            postgresConnection: {
               ...validSshProxy.postgresConnection!,
               sslmode: "verify-ca",
            },
         };
         expect(() => assembleEnvironmentConnections([conn])).toThrow(
            "verify-ca",
         );
      } finally {
         if (prior !== undefined) process.env.NODE_EXTRA_CA_CERTS = prior;
      }
   });

   it("accepts sslmode=verify-full on a proxied connection with no CA bundle (verifies against Node's bundled CA roots + NODE_EXTRA_CA_CERTS)", () => {
      // Unlike verify-ca (which passes an explicit sslrootcert path and so
      // fails fast without NODE_EXTRA_CA_CERTS), verify-full trusts Node's
      // bundled Mozilla CA roots + NODE_EXTRA_CA_CERTS — so it must NOT require
      // a bundle at load.
      const prior = process.env.NODE_EXTRA_CA_CERTS;
      delete process.env.NODE_EXTRA_CA_CERTS;
      try {
         const conn: ApiConnection = {
            ...validSshProxy,
            postgresConnection: {
               ...validSshProxy.postgresConnection!,
               sslmode: "verify-full",
            },
         };
         expect(() => assembleEnvironmentConnections([conn])).not.toThrow();
      } finally {
         if (prior !== undefined) process.env.NODE_EXTRA_CA_CERTS = prior;
      }
   });

   it("rejects sslmode set on a non-proxied connection (silent-ignore footgun)", () => {
      const conn: ApiConnection = {
         name: "pg-direct",
         type: "postgres",
         postgresConnection: {
            host: "db.example.com",
            port: 5432,
            databaseName: "mydb",
            userName: "user",
            password: "pass",
            sslmode: "verify-ca",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "only supported for proxied connections",
      );
   });

   it("rejects a proxied database name with URI-reserved characters", () => {
      const conn: ApiConnection = {
         ...validSshProxy,
         postgresConnection: {
            ...validSshProxy.postgresConnection!,
            databaseName: "sales@prod",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "URI-reserved characters",
      );
   });
});

describe("ducklake shape validation", () => {
   // A DuckLake destination is only attached at its first BUILD, so without an
   // eager check a missing catalog or bucketUrl surfaced hours after the config
   // change that caused it. Validated at load like its duckdb-family siblings.
   const valid: ApiConnection = {
      name: "lake",
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "127.0.0.1",
               port: 5432,
               databaseName: "catalog",
               userName: "u",
               password: "p",
            },
         },
         storage: { bucketUrl: "/tmp/lake" },
      },
   };

   it("accepts a fully configured destination", () => {
      expect(() =>
         assembleEnvironmentConnections([valid], "/tmp/env"),
      ).not.toThrow();
   });

   it("rejects a missing catalog connection at load", () => {
      const c = {
         ...valid,
         ducklakeConnection: { storage: { bucketUrl: "/tmp/lake" } },
      } as ApiConnection;
      expect(() => assembleEnvironmentConnections([c], "/tmp/env")).toThrow(
         /PostgreSQL connection configuration is required for DuckLake catalog/i,
      );
   });

   it("rejects a missing bucketUrl at load", () => {
      const c = {
         ...valid,
         ducklakeConnection: { catalog: valid.ducklakeConnection!.catalog },
      } as ApiConnection;
      expect(() => assembleEnvironmentConnections([c], "/tmp/env")).toThrow(
         /Storage bucketUrl is required for DuckLake/i,
      );
   });

   // metadataSchema reaches TWO grammars: a quoted string literal in the ATTACH,
   // and a quoted identifier in the catalog-format preflight's table reference.
   // Restricting it to a plain identifier at load is what keeps one value valid in
   // both, so these pin the accept/reject boundary rather than trusting escaping.
   const withSchema = (metadataSchema: unknown): ApiConnection =>
      ({
         ...valid,
         ducklakeConnection: {
            ...valid.ducklakeConnection,
            catalog: {
               ...valid.ducklakeConnection!.catalog,
               metadataSchema,
            },
         },
      }) as ApiConnection;

   it("accepts an absent metadataSchema", () => {
      // Optional: absence keeps DuckLake's default schema, the prior behavior.
      expect(() =>
         assembleEnvironmentConnections([valid], "/tmp/env"),
      ).not.toThrow();
   });

   it("accepts a plain identifier metadataSchema", () => {
      for (const ok of ["org_a", "_private", "Lake1", "a"]) {
         expect(() =>
            assembleEnvironmentConnections([withSchema(ok)], "/tmp/env"),
         ).not.toThrow();
      }
   });

   it("rejects a metadataSchema that is not a plain identifier", () => {
      for (const bad of [
         "foo'; DROP TABLE x; --",
         "has space",
         "dotted.name",
         "1leading_digit",
         "",
         '"quoted"',
      ]) {
         expect(() =>
            assembleEnvironmentConnections([withSchema(bad)], "/tmp/env"),
         ).toThrow(/metadataSchema must be a plain identifier/i);
      }
   });

   it("rejects a non-string metadataSchema rather than coercing it", () => {
      // The value comes from untyped JSON and RegExp.test() coerces, so `true` and
      // `null` match the identifier pattern as "true"/"null" and would pass a
      // pattern-only check — then reach escapeSQL's String.replace as a non-string
      // and throw TypeError at the connection's first attach. That runtime failure is
      // the thing this load-time check exists to prevent, so the type is part of the
      // contract, not a formality.
      for (const bad of [true, false, 0, 1, null, {}, [], ["org_a"]]) {
         expect(() =>
            assembleEnvironmentConnections([withSchema(bad)], "/tmp/env"),
         ).toThrow(/metadataSchema must be a plain identifier/i);
      }
   });
});

// A credential problem used to surface only at the connection's first ATTACH — for a
// storage destination, the first BUILD, hours after the config change that caused it.
// These pin where each check now fires, including the one deliberately NOT moved.
describe("assembleEnvironmentConnections — S3 credential shape at config load", () => {
   const ducklake = (s3Connection: Record<string, unknown>): ApiConnection =>
      ({
         name: "tier",
         type: "ducklake",
         ducklakeConnection: {
            catalog: {
               postgresConnection: {
                  host: "h",
                  port: 5432,
                  userName: "u",
                  password: "p",
                  databaseName: "d",
               },
            },
            storage: { bucketUrl: "s3://bucket/prefix", s3Connection },
         },
      }) as unknown as ApiConnection;

   const duckdbWithS3 = (
      s3Connection: Record<string, unknown>,
   ): ApiConnection =>
      ({
         name: "generic",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [{ name: "root", type: "s3", s3Connection }],
         },
      }) as unknown as ApiConnection;

   describe("ducklake connection — shape only, to bound the blast radius", () => {
      // The counterpart of the duckdb case below, and the reason both are shape-only:
      // this validator's throw fails the WHOLE environment. A DuckLake carrying a
      // present-but-incomplete s3Connection has always loaded and failed when used,
      // and taking every other connection down with it is the worse trade. A storage
      // DESTINATION gets the full check instead — see the describe below.
      it("still LOADS a keyless key-based storage root", () => {
         const { pojo } = assembleEnvironmentConnections([
            ducklake({ region: "us-east-1" }),
         ]);
         expect(pojo.connections["tier"]).toBeDefined();
      });

      it("accepts a chain-auth storage root with no key pair", () => {
         const { pojo } = assembleEnvironmentConnections([
            ducklake({ provider: "credential_chain" }),
         ]);
         expect(pojo.connections["tier"]).toBeDefined();
      });

      it("rejects a key supplied alongside chain auth at load", () => {
         expect(() =>
            assembleEnvironmentConnections([
               ducklake({ provider: "credential_chain", accessKeyId: "AKIA" }),
            ]),
         ).toThrow(/must not be set when provider is 'credential_chain'/);
      });

      // Without the enum check this reads as `config`, and the error names a missing
      // access key — pointing at the wrong field for what is a typo in `provider`.
      it("names the provider field for a misspelled provider", () => {
         expect(() =>
            assembleEnvironmentConnections([
               ducklake({ provider: "credentialchain" }),
            ]),
         ).toThrow(
            /provider must be one of config, credential_chain for: tier/,
         );
      });

      it("rejects a non-string provider rather than treating it as config", () => {
         expect(() =>
            assembleEnvironmentConnections([ducklake({ provider: true })]),
         ).toThrow(/provider must be one of/);
      });

      it("rejects a non-string chain before it reaches trim()", () => {
         expect(() =>
            assembleEnvironmentConnections([
               ducklake({ provider: "credential_chain", chain: 7 }),
            ]),
         ).toThrow(/chain must be a string for: tier/);
      });
   });

   describe("duckdb attached database — shape only, on purpose", () => {
      it("rejects a misspelled provider at load", () => {
         expect(() =>
            assembleEnvironmentConnections([
               duckdbWithS3({ provider: "credentialchain" }),
            ]),
         ).toThrow(
            /provider must be one of config, credential_chain for: root/,
         );
      });

      it("rejects a key supplied alongside chain auth at load", () => {
         expect(() =>
            assembleEnvironmentConnections([
               duckdbWithS3({
                  provider: "credential_chain",
                  secretAccessKey: "shhh",
               }),
            ]),
         ).toThrow(/must not be set when provider is 'credential_chain'/);
      });

      // The deliberate asymmetry with the ducklake branch above. This validator's
      // throw fails the WHOLE environment, and nothing here checked an attached
      // database's credentials before, so moving the key-pair requirement to load
      // would stop environments loading that load today. It still fails at attach.
      it("still LOADS a keyless key-based attachment", () => {
         const { pojo } = assembleEnvironmentConnections([
            duckdbWithS3({ region: "us-east-1" }),
         ]);
         expect(pojo.connections["generic"]).toBeDefined();
      });
   });
});

// A storage destination is rejected on its own, so it can afford the check the
// environment-wide validator cannot: a destination is only attached at its first
// BUILD, which is the late failure that motivated moving these guards at all.
describe("validateStorageDestinations — S3 credential, checked in full", () => {
   const destination = (s3Connection: Record<string, unknown>): ApiConnection =>
      ({
         name: "credible",
         type: "ducklake",
         ducklakeConnection: {
            catalog: {
               postgresConnection: {
                  host: "h",
                  port: 5432,
                  userName: "u",
                  password: "p",
                  databaseName: "d",
               },
            },
            storage: { bucketUrl: "s3://bucket/prefix", s3Connection },
         },
      }) as unknown as ApiConnection;

   it("rejects a keyless key-based destination, naming it", () => {
      const { accepted, rejected } = validateStorageDestinations([
         destination({ region: "us-east-1" }),
      ]);
      expect(accepted).toHaveLength(0);
      expect(rejected).toHaveLength(1);
      expect(rejected[0].name).toBe("credible");
      expect(rejected[0].reason).toMatch(
         /accessKeyId and secretAccessKey are required/,
      );
   });

   it("accepts a chain-auth destination with no key pair", () => {
      const { accepted, rejected } = validateStorageDestinations([
         destination({ provider: "credential_chain" }),
      ]);
      expect(rejected).toHaveLength(0);
      expect(accepted).toHaveLength(1);
   });

   // The GCS arm is the same late failure, so it gets the same treatment.
   it("rejects a destination with a partial gcsConnection", () => {
      const gcs = {
         name: "credible",
         type: "ducklake",
         ducklakeConnection: {
            catalog: {
               postgresConnection: {
                  host: "h",
                  port: 5432,
                  userName: "u",
                  password: "p",
                  databaseName: "d",
               },
            },
            storage: {
               bucketUrl: "gs://bucket/prefix",
               gcsConnection: { keyId: "GOOG" },
            },
         },
      } as unknown as ApiConnection;
      const { accepted, rejected } = validateStorageDestinations([gcs]);
      expect(accepted).toHaveLength(0);
      expect(rejected[0].reason).toMatch(/GCS keyId and secret are required/);
   });

   // The whole point of doing it here rather than in validateConnectionShape.
   it("rejects only the bad destination, leaving its neighbour accepted", () => {
      const good = destination({ provider: "credential_chain" });
      good.name = "good";
      const bad = destination({ region: "us-east-1" });
      bad.name = "bad";
      const { accepted, rejected } = validateStorageDestinations([good, bad]);
      expect(accepted.map((d) => d.name)).toEqual(["good"]);
      expect(rejected.map((r) => r.name)).toEqual(["bad"]);
   });
});

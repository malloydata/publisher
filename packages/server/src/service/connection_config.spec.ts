import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { generateKeyPairSync } from "crypto";
import { components } from "../api";
import {
   assembleEnvironmentConnections,
   normalizeSnowflakePrivateKey,
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
});

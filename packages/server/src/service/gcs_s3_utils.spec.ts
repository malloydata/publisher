import { afterEach, describe, expect, it } from "bun:test";
import { components } from "../api";
import { testConnectionConfig } from "./connection";
import { assembleEnvironmentConnections } from "./connection_config";
import {
   createCloudStorageClient,
   DEFAULT_S3_CREDENTIAL_CHAIN,
   parseS3Endpoint,
   s3ConnectionToCredentials,
   validateS3ProviderShape,
} from "./gcs_s3_utils";

type ApiConnection = components["schemas"]["Connection"];

// The SDK's default chain reads the environment first, so seeding it keeps these
// tests off the network — no IMDS probe, no shared credentials file.
const CHAIN_ENV = {
   AWS_ACCESS_KEY_ID: "AKIAFROMENV",
   AWS_SECRET_ACCESS_KEY: "secretfromenv",
};

const saved: Record<string, string | undefined> = {};
const seedEnv = () => {
   for (const [key, value] of Object.entries(CHAIN_ENV)) {
      saved[key] = process.env[key];
      process.env[key] = value;
   }
};

afterEach(() => {
   for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
   }
});

describe("createCloudStorageClient", () => {
   it("signs with the configured key pair under provider 'config'", async () => {
      const client = createCloudStorageClient(
         s3ConnectionToCredentials({
            accessKeyId: "AKIACONFIGURED",
            secretAccessKey: "configured",
            region: "us-west-2",
         }),
      );
      const resolved = await client.config.credentials();
      expect(resolved.accessKeyId).toBe("AKIACONFIGURED");
      expect(resolved.secretAccessKey).toBe("configured");
   });

   // The bug this pins: a keyless S3Connection used to be passed through as
   // accessKeyId: "", so every request was signed with an empty key and failed
   // on signature rather than on configuration.
   it("defers to the host under provider 'credential_chain'", async () => {
      seedEnv();
      const client = createCloudStorageClient(
         s3ConnectionToCredentials({
            provider: "credential_chain",
            region: "us-west-2",
         }),
      );
      const resolved = await client.config.credentials();
      expect(resolved.accessKeyId).toBe(CHAIN_ENV.AWS_ACCESS_KEY_ID);
      expect(resolved.accessKeyId).not.toBe("");
   });

   it("leaves GCS on its HMAC key pair", async () => {
      seedEnv();
      const client = createCloudStorageClient({
         type: "gcs",
         accessKeyId: "GOOGHMAC",
         secretAccessKey: "hmacsecret",
         // GCS has no chain equivalent through this path; a provider set here
         // must not divert it to the AWS environment.
         provider: "credential_chain",
      });
      const resolved = await client.config.credentials();
      expect(resolved.accessKeyId).toBe("GOOGHMAC");
   });
});

describe("s3ConnectionToCredentials", () => {
   it("carries the provider and chain through", () => {
      const credentials = s3ConnectionToCredentials({
         provider: "credential_chain",
         chain: "env;instance",
      });
      expect(credentials.provider).toBe("credential_chain");
      expect(credentials.chain).toBe("env;instance");
   });

   it("leaves both unset for a key-based connection", () => {
      const credentials = s3ConnectionToCredentials({
         accessKeyId: "AKIA",
         secretAccessKey: "shhh",
      });
      expect(credentials.provider).toBeUndefined();
      expect(credentials.chain).toBeUndefined();
   });
});

describe("DEFAULT_S3_CREDENTIAL_CHAIN", () => {
   // `config` is a shared credentials file, which is the one source a container
   // image does not have, and it is what DuckDB falls back to when CHAIN is
   // omitted. Naming it here would defeat the point of the default.
   it("omits config and covers a projected token", () => {
      const providers = DEFAULT_S3_CREDENTIAL_CHAIN.split(";");
      expect(providers).toContain("web_identity");
      expect(providers).not.toContain("config");
   });
});

describe("S3_CREDENTIAL_CHAIN_POLICY", () => {
   const ORIGINAL = process.env.S3_CREDENTIAL_CHAIN_POLICY;
   afterEach(() => {
      if (ORIGINAL === undefined) delete process.env.S3_CREDENTIAL_CHAIN_POLICY;
      else process.env.S3_CREDENTIAL_CHAIN_POLICY = ORIGINAL;
   });

   const chainConn = () => ({ provider: "credential_chain" }) as never;

   it("permits chain auth anywhere when unset, so no deployment changes", () => {
      delete process.env.S3_CREDENTIAL_CHAIN_POLICY;
      expect(() => validateS3ProviderShape(chainConn(), "c")).not.toThrow();
      expect(() =>
         validateS3ProviderShape(chainConn(), "c", "storage-destination"),
      ).not.toThrow();
   });

   it("under destinations-only, refuses a connection and permits a destination", () => {
      // The whole point of the setting: a storage destination is the OPERATOR's
      // configuration, while a connection may be authored by whoever can reach the
      // connection endpoints — and Publisher's take an arbitrary body.
      process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations-only";
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         /not permitted.*destinations-only/s,
      );
      expect(() =>
         validateS3ProviderShape(chainConn(), "c", "storage-destination"),
      ).not.toThrow();
   });

   it("under off, refuses both", () => {
      process.env.S3_CREDENTIAL_CHAIN_POLICY = "off";
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         /not permitted/,
      );
      expect(() =>
         validateS3ProviderShape(chainConn(), "c", "storage-destination"),
      ).toThrow(/not permitted/);
   });

   it("defaults an unstated origin to the refused case", () => {
      // A caller that does not say which path it is on is treated as the
      // author-supplied one. Failing closed is the only safe default for a
      // parameter whose whole job is to decide whether to lend the host identity.
      process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations-only";
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         /not permitted/,
      );
   });

   it("leaves a key-pair connection alone under every policy", () => {
      const keyed = { accessKeyId: "AKIA", secretAccessKey: "s" } as never;
      for (const p of ["any", "destinations-only", "off"]) {
         process.env.S3_CREDENTIAL_CHAIN_POLICY = p;
         expect(() => validateS3ProviderShape(keyed, "c")).not.toThrow();
      }
   });

   it("refuses an unrecognized policy value rather than choosing the permissive one", () => {
      process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations_only";
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         /Invalid value for S3_CREDENTIAL_CHAIN_POLICY/,
      );
   });

   // The origin has to survive every layer between a destination and the gate;
   // these pin the other side: an author-supplied config that reaches the same
   // layers still carries the default and is still refused.
   describe("author-supplied connections stay refused under destinations-only", () => {
      const chainStorage = { bucketUrl: "s3://b/p", s3Connection: chainConn() };
      const ducklake = (): ApiConnection => ({
         name: "tenant_lake",
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
            storage: chainStorage,
         },
      });
      const duckdbAttachment = (): ApiConnection => ({
         name: "tenant_duck",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [
               { name: "att", type: "s3", s3Connection: chainConn() },
            ],
         },
      });

      it("refuses a tenant ducklake connection", () => {
         process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations-only";
         expect(() => assembleEnvironmentConnections([ducklake()])).toThrow(
            /not permitted/,
         );
      });

      it("refuses a tenant duckdb attachment, even given the destination origin", () => {
         process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations-only";
         expect(() =>
            assembleEnvironmentConnections([duckdbAttachment()]),
         ).toThrow(/not permitted/);
         expect(() =>
            assembleEnvironmentConnections(
               [duckdbAttachment()],
               "",
               "storage-destination",
            ),
         ).toThrow(/not permitted/);
      });

      it("refuses a ducklake connection through testConnectionConfig", async () => {
         process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations-only";
         const result = await testConnectionConfig(ducklake());
         expect(result.status).toBe("failed");
         expect(result.errorMessage).toMatch(/not permitted/);
      });
   });
});

describe("parseS3Endpoint", () => {
   it("returns a bare host untouched, with SSL on", () => {
      expect(parseS3Endpoint("minio:9000")).toEqual({
         hostAndPath: "minio:9000",
         useSsl: true,
      });
   });

   it("strips the scheme in any case and trailing slashes, keeping a path", () => {
      expect(parseS3Endpoint("HTTPS://h:1/prefix//")).toEqual({
         hostAndPath: "h:1/prefix",
         useSsl: true,
      });
   });

   it("turns SSL off only for http://", () => {
      expect(parseS3Endpoint("http://h:1").useSsl).toBe(false);
      expect(parseS3Endpoint("HTTP://h:1").useSsl).toBe(false);
   });

   it("rejects a query, a fragment, a foreign scheme and an empty host", () => {
      expect(() => parseS3Endpoint("h?x=1")).toThrow(/query string/);
      expect(() => parseS3Endpoint("h#f")).toThrow(/fragment/);
      expect(() => parseS3Endpoint("ftp://h")).toThrow(/scheme/);
      expect(() => parseS3Endpoint("http://")).toThrow(/host/);
   });

   it("rejects whitespace anywhere", () => {
      for (const endpoint of [" https://h", "https://h ", "h :9000", "h\t"]) {
         expect(() => parseS3Endpoint(endpoint)).toThrow(/whitespace/);
      }
   });

   it("rejects userinfo in the authority but not an @ in the path", () => {
      expect(() => parseS3Endpoint("u:p@h:9000")).toThrow(/userinfo/);
      expect(() => parseS3Endpoint("https://u@h")).toThrow(/userinfo/);
      expect(parseS3Endpoint("h/a@b").hostAndPath).toBe("h/a@b");
   });

   it("rejects an authority that starts with a slash", () => {
      expect(() => parseS3Endpoint("//h")).toThrow(/host/);
      expect(() => parseS3Endpoint("http:///p")).toThrow(/host/);
   });
});

describe("validateS3ProviderShape — endpoint", () => {
   it("names a non-string endpoint instead of throwing a TypeError", () => {
      expect(() =>
         validateS3ProviderShape({ endpoint: 123 } as never, "c"),
      ).toThrow("S3 endpoint must be a string for: c");
   });

   it("names the connection when the endpoint is malformed", () => {
      expect(() =>
         validateS3ProviderShape({ endpoint: " https://h" } as never, "c"),
      ).toThrow("S3 endpoint must not contain whitespace for: c");
   });
});

describe("createCloudStorageClient — endpoint", () => {
   const endpointOf = async (endpoint: string) => {
      const client = createCloudStorageClient(
         s3ConnectionToCredentials({
            accessKeyId: "AKIA",
            secretAccessKey: "s",
            endpoint,
         }),
      );
      const resolved = await client.config.endpoint!();
      return `${resolved.protocol}//${resolved.hostname}${resolved.port ? `:${resolved.port}` : ""}${resolved.path}`;
   };

   it("gives a bare host:port a scheme, which the SDK otherwise rejects", async () => {
      expect(await endpointOf("minio.internal:9000")).toBe(
         "https://minio.internal:9000/",
      );
   });

   it("honours http:// and keeps a path", async () => {
      expect(await endpointOf("http://minio.internal:9000/prefix/")).toBe(
         "http://minio.internal:9000/prefix",
      );
   });

   it("does not double a scheme already present", async () => {
      expect(await endpointOf("https://minio.internal")).toBe(
         "https://minio.internal/",
      );
   });
});

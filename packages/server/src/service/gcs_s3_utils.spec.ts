import { afterEach, describe, expect, it } from "bun:test";
import {
   checkS3Endpoint,
   createCloudStorageClient,
   parseS3Endpoint,
   resolveCloudStorageCredentials,
   DEFAULT_S3_CREDENTIAL_CHAIN,
   s3ConnectionToCredentials,
   validateS3ProviderShape,
} from "./gcs_s3_utils";

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

describe("parseS3Endpoint", () => {
   it("reads a bare host as TLS", () => {
      expect(parseS3Endpoint("minio:9000")).toEqual({
         hostAndPath: "minio:9000",
         useSsl: true,
         url: "https://minio:9000",
      });
   });

   it("turns TLS off only for http://, in any case, and drops trailing slashes", () => {
      expect(parseS3Endpoint("HTTP://minio:9000/")).toEqual({
         hostAndPath: "minio:9000",
         useSsl: false,
         url: "http://minio:9000",
      });
      expect(parseS3Endpoint("https://s3.example.com/base").useSsl).toBe(true);
   });

   // What DuckDB and the SDK read differently, each refused. `https:///foo`
   // would otherwise write ENDPOINT '/foo'.
   it.each([
      ["ftp://minio", "scheme must be http:// or https://, got ftp://"],
      ["http://", "must include a host"],
      ["https:///foo", "must include a host before any path"],
      ["https://user:pw@minio", "must not contain userinfo"],
      ["minio?x=1", "query string or fragment"],
      ["minio#frag", "query string or fragment"],
      ["minio name", "must not contain whitespace"],
   ])("refuses %p", (endpoint, message) => {
      expect(() => parseS3Endpoint(endpoint)).toThrow(message);
   });

   it("never repeats an endpoint that carries a credential", () => {
      for (const endpoint of [
         "ftp://key:secret@minio",
         "https://key:secret@minio",
      ]) {
         try {
            parseS3Endpoint(endpoint);
            throw new Error("expected a refusal");
         } catch (error) {
            expect((error as Error).message).not.toContain("secret");
         }
      }
   });
});

describe("checkS3Endpoint", () => {
   it("refuses plain http for credential_chain, whose requests carry the host's credentials", () => {
      expect(() =>
         checkS3Endpoint("http://minio:9000", "credential_chain"),
      ).toThrow("must use https:// with provider credential_chain");
      expect(checkS3Endpoint("http://minio:9000", "config").useSsl).toBe(false);
      expect(
         checkS3Endpoint("https://minio:9000", "credential_chain").useSsl,
      ).toBe(true);
   });
});

describe("resolveCloudStorageCredentials endpoint", () => {
   // A storage destination resolves its credentials when it is configured, so
   // an unusable endpoint is refused then, not at its first build.
   it("refuses an unusable endpoint when the credentials are resolved, naming the connection", () => {
      expect(() =>
         resolveCloudStorageCredentials({
            name: "lake",
            type: "s3",
            s3Connection: {
               accessKeyId: "AKIA",
               secretAccessKey: "s",
               endpoint: "ftp://minio",
            },
         }),
      ).toThrow("got ftp:// (lake)");
   });
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

   // The SDK rejects a bare host as `Invalid URL`; DuckDB's form must reach it as a URL.
   it("gives the SDK a URL for an endpoint written as a bare host", async () => {
      const client = createCloudStorageClient(
         s3ConnectionToCredentials({
            accessKeyId: "AKIA",
            secretAccessKey: "s",
            region: "us-east-1",
            endpoint: "minio:9000",
         }),
      );
      const endpoint = await client.config.endpoint!();
      expect(
         `${endpoint.protocol}//${endpoint.hostname}:${endpoint.port}`,
      ).toBe("https://minio:9000");
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
});

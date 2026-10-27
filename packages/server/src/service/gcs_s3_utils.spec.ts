import { afterEach, describe, expect, it } from "bun:test";
import {
   createCloudStorageClient,
   DEFAULT_S3_CREDENTIAL_CHAIN,
   s3ConnectionToCredentials,
   validateS3ProviderShape,
} from "./gcs_s3_utils";
import { installSettings } from "../test_helpers/settings_env";

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

let restoreSettings: (() => void) | undefined;

/** Make `env` the only variables the settings were loaded from. */
function installEnv(env: Record<string, string>): void {
   restoreSettings?.();
   restoreSettings = installSettings({ env }).restore;
}

afterEach(() => {
   for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
   }
   restoreSettings?.();
   restoreSettings = undefined;
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
   const chainConn = () => ({ provider: "credential_chain" }) as never;

   it("permits chain auth anywhere when unset, so no deployment changes", () => {
      installEnv({});
      expect(() => validateS3ProviderShape(chainConn(), "c")).not.toThrow();
      expect(() =>
         validateS3ProviderShape(chainConn(), "c", "storage-destination"),
      ).not.toThrow();
   });

   it("under destinations-only, refuses a connection and permits a destination", () => {
      // The whole point of the setting: a storage destination is the OPERATOR's
      // configuration, while a connection may be authored by whoever can reach the
      // connection endpoints — and Publisher's take an arbitrary body.
      installEnv({ S3_CREDENTIAL_CHAIN_POLICY: "destinations-only" });
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         /not permitted.*destinations-only/s,
      );
      expect(() =>
         validateS3ProviderShape(chainConn(), "c", "storage-destination"),
      ).not.toThrow();
   });

   it("under off, refuses both", () => {
      installEnv({ S3_CREDENTIAL_CHAIN_POLICY: "off" });
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
      installEnv({ S3_CREDENTIAL_CHAIN_POLICY: "destinations-only" });
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         /not permitted/,
      );
   });

   it("leaves a key-pair connection alone under every policy", () => {
      const keyed = { accessKeyId: "AKIA", secretAccessKey: "s" } as never;
      for (const p of ["any", "destinations-only", "off"]) {
         installEnv({ S3_CREDENTIAL_CHAIN_POLICY: p });
         expect(() => validateS3ProviderShape(keyed, "c")).not.toThrow();
      }
   });

   it("refuses an unrecognized policy value rather than choosing the permissive one", () => {
      installEnv({ S3_CREDENTIAL_CHAIN_POLICY: "destinations_only" });
      expect(() => validateS3ProviderShape(chainConn(), "c")).toThrow(
         'allow.s3CredentialChain from S3_CREDENTIAL_CHAIN_POLICY: expected one of "any", "destinations-only" or "off", got "destinations_only"',
      );
   });
});

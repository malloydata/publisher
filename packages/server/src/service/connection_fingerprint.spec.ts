import { describe, expect, it } from "bun:test";
import { components } from "../api";
import {
   buildEnvironmentMalloyConfig,
   EnvironmentMalloyConfig,
} from "./connection";

type ApiConnection = components["schemas"]["Connection"];

/**
 * The connection `fingerprint` seam: when a connection carries an API
 * fingerprint, the resolved Malloy connection's getDigest() must return it
 * verbatim. Every consumer of a connection's identity — build planning, the
 * package-load worker's digest RPC, and Malloy's serve-time manifest
 * resolution — reads getDigest() off a connection resolved through the same
 * environment lookup wrapper, so this one seam is what keeps build-time and
 * serve-time source ids in agreement.
 *
 * Uses a postgres connection because it is constructed lazily (no network I/O
 * until a query runs), so digests can be read offline.
 */

function pgConnection(overrides: Partial<ApiConnection> = {}): ApiConnection {
   return {
      name: "pg",
      type: "postgres",
      postgresConnection: {
         host: "db.example.com",
         port: 5432,
         databaseName: "analytics",
         userName: "svc_user",
         password: "hunter2",
      },
      ...overrides,
   };
}

async function digestOf(connection: ApiConnection): Promise<string> {
   let config: EnvironmentMalloyConfig | undefined;
   try {
      config = buildEnvironmentMalloyConfig([connection]);
      const resolved = await config.malloyConfig.connections.lookupConnection(
         connection.name,
      );
      return resolved.getDigest();
   } finally {
      await config?.releaseConnections();
   }
}

describe("connection fingerprint", () => {
   it("uses the fingerprint verbatim as the connection digest", async () => {
      const digest = await digestOf(
         pgConnection({ fingerprint: "fp-opaque-token-1" }),
      );
      expect(digest).toBe("fp-opaque-token-1");
   });

   it("keeps the digest stable across a credential rotation", async () => {
      const before = await digestOf(pgConnection({ fingerprint: "fp-stable" }));
      const after = await digestOf(
         pgConnection({
            fingerprint: "fp-stable",
            postgresConnection: {
               host: "db.example.com",
               port: 5432,
               databaseName: "analytics",
               userName: "rotated_user",
               password: "rotated-password",
            },
         }),
      );
      expect(after).toBe(before);
   });

   it("changes the digest when the fingerprint changes", async () => {
      const one = await digestOf(pgConnection({ fingerprint: "fp-1" }));
      const two = await digestOf(pgConnection({ fingerprint: "fp-2" }));
      expect(one).not.toBe(two);
   });

   it("falls back to the locally derived digest when omitted", async () => {
      const digest = await digestOf(pgConnection());
      expect(typeof digest).toBe("string");
      expect(digest.length).toBeGreaterThan(0);
      // Behavior is unchanged from before the fingerprint existed: the digest
      // is the connection's own derivation, deterministic for one config.
      expect(await digestOf(pgConnection())).toBe(digest);
   });

   it("round-trips the fingerprint on the API connection", async () => {
      const config = buildEnvironmentMalloyConfig([
         pgConnection({ fingerprint: "fp-round-trip" }),
      ]);
      try {
         const api = config.apiConnections.find((c) => c.name === "pg");
         expect(api?.fingerprint).toBe("fp-round-trip");
      } finally {
         await config.releaseConnections();
      }
   });
});

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import sinon from "sinon";
import { getExtensionFetchPolicy } from "../config";
import { buildEnvironmentMalloyConfig } from "./connection";

describe("getExtensionFetchPolicy", () => {
   const original = process.env.EXTENSION_FETCH_POLICY;
   afterEach(() => {
      if (original === undefined) delete process.env.EXTENSION_FETCH_POLICY;
      else process.env.EXTENSION_FETCH_POLICY = original;
   });

   it("defaults to on-demand when unset or empty", () => {
      delete process.env.EXTENSION_FETCH_POLICY;
      expect(getExtensionFetchPolicy()).toBe("on-demand");
      process.env.EXTENSION_FETCH_POLICY = "   ";
      expect(getExtensionFetchPolicy()).toBe("on-demand");
   });

   it("accepts the two policies case-insensitively", () => {
      process.env.EXTENSION_FETCH_POLICY = "local-only";
      expect(getExtensionFetchPolicy()).toBe("local-only");
      process.env.EXTENSION_FETCH_POLICY = "On-Demand";
      expect(getExtensionFetchPolicy()).toBe("on-demand");
   });

   it("throws loudly on an unrecognised value", () => {
      process.env.EXTENSION_FETCH_POLICY = "offline";
      expect(() => getExtensionFetchPolicy()).toThrow(
         /Invalid value for EXTENSION_FETCH_POLICY/,
      );
   });
});

describe("EXTENSION_FETCH_POLICY=local-only extension loading", () => {
   const envPath = path.join(process.cwd(), "test-extension-policy");
   const original = process.env.EXTENSION_FETCH_POLICY;

   beforeEach(async () => {
      await fs.mkdir(envPath, { recursive: true });
   });
   afterEach(async () => {
      sinon.restore();
      if (original === undefined) delete process.env.EXTENSION_FETCH_POLICY;
      else process.env.EXTENSION_FETCH_POLICY = original;
      await fs.rm(envPath, { recursive: true, force: true }).catch(() => {});
   });

   it("never runs INSTALL and raises a loud, actionable error when a required extension is missing", async () => {
      process.env.EXTENSION_FETCH_POLICY = "local-only";

      // Session PRAGMAs and metadata probes succeed; the first extension LOAD
      // fails, simulating an extension that was never baked into the image.
      const runSQL = sinon
         .stub(DuckDBConnection.prototype, "runSQL")
         .resolves({ rows: [] } as never);
      runSQL
         .withArgs(sinon.match(/^LOAD ducklake/))
         .rejects(new Error('Extension "ducklake" not found'));

      const config = buildEnvironmentMalloyConfig(
         [
            {
               name: "ducklake_localonly",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        host: "192.0.2.1",
                        port: 5432,
                        userName: "nobody",
                        password: "nobody",
                        databaseName: "catalog",
                     },
                  },
                  storage: {
                     bucketUrl: "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: "test",
                        secretAccessKey: "test",
                     },
                  },
               },
            },
         ] as never,
         envPath,
      );

      await expect(
         config.malloyConfig.connections.lookupConnection("ducklake_localonly"),
      ).rejects.toThrow(/local-only.*ducklake|ducklake.*local-only/s);

      // local-only must never issue an INSTALL — not even for a missing extension.
      const ranInstall = runSQL
         .getCalls()
         .some((c) => /^\s*INSTALL\s/i.test(String(c.args[0])));
      expect(ranInstall).toBe(false);

      // And it did pin implicit auto-install off on the session (item 3).
      const setAutoinstallOff = runSQL
         .getCalls()
         .some((c) =>
            /autoinstall_known_extensions\s*=\s*false/i.test(String(c.args[0])),
         );
      expect(setAutoinstallOff).toBe(true);

      await config.releaseConnections().catch(() => {});
   });
});

describe("EXTENSION_FETCH_POLICY=on-demand (default) — no behaviour change", () => {
   const envPath = path.join(process.cwd(), "test-extension-policy-ondemand");
   const original = process.env.EXTENSION_FETCH_POLICY;

   beforeEach(async () => {
      await fs.mkdir(envPath, { recursive: true });
   });
   afterEach(async () => {
      sinon.restore();
      if (original === undefined) delete process.env.EXTENSION_FETCH_POLICY;
      else process.env.EXTENSION_FETCH_POLICY = original;
      await fs.rm(envPath, { recursive: true, force: true }).catch(() => {});
   });

   it("leaves autoinstall at DuckDB's default on a generic attach session", async () => {
      // Default policy: an ordinary DuckDB connection with an attachment must
      // behave exactly as before — Publisher installs the attachment's extension
      // explicitly, but it must NOT disable DuckDB's implicit auto-install (that
      // would regress a user whose query references an un-provisioned extension).
      delete process.env.EXTENSION_FETCH_POLICY;

      const runSQL = sinon
         .stub(DuckDBConnection.prototype, "runSQL")
         .resolves({ rows: [] } as never);

      const config = buildEnvironmentMalloyConfig(
         [
            {
               name: "gen_duck",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "pg_attached",
                        type: "postgres",
                        postgresConnection: {
                           connectionString: "postgresql://localhost/test",
                        },
                     },
                  ],
               },
            },
         ] as never,
         envPath,
      );

      await config.malloyConfig.connections.lookupConnection("gen_duck");

      const issued = runSQL.getCalls().map((c) => String(c.args[0]));
      // The attachment path ran (postgres explicitly installed)...
      expect(issued.some((sql) => /^\s*INSTALL\s+postgres/i.test(sql))).toBe(
         true,
      );
      // ...but implicit auto-install was left untouched — no behaviour change.
      expect(
         issued.some((sql) =>
            /autoinstall_known_extensions\s*=\s*false/i.test(sql),
         ),
      ).toBe(false);

      await config.releaseConnections().catch(() => {});
   });
});

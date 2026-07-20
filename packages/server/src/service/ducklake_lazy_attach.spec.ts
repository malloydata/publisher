import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import sinon from "sinon";
import { components } from "../api";
import { buildEnvironmentMalloyConfig } from "./connection";

type ApiConnection = components["schemas"]["Connection"];

// A DuckLake connection whose Postgres catalog points at an unroutable host.
// If anything on the boot path tried to reach the catalog, these tests would
// hang or fail — which is precisely the regression they guard against.
const UNREACHABLE_DUCKLAKE: ApiConnection = {
   name: "ducklake_lazy",
   type: "ducklake",
   ducklakeConnection: {
      catalog: {
         postgresConnection: {
            host: "192.0.2.1", // TEST-NET-1: guaranteed non-routable
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
} as ApiConnection;

describe("DuckLake lazy attach", () => {
   const envPath = path.join(process.cwd(), "test-ducklake-lazy-attach");

   beforeEach(async () => {
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      sinon.restore();
      await fs.rm(envPath, { recursive: true, force: true }).catch(() => {});
   });

   it("does not attach the catalog while building the environment config (boot path)", async () => {
      // Stub the DuckDB session so no real database or network work happens; the
      // stub also lets us observe whether ANY SQL ran during config build.
      const runSQL = sinon
         .stub(DuckDBConnection.prototype, "runSQL")
         .resolves({ rows: [] } as never);

      // Building the config is the worker boot path. It must be synchronous and
      // must not open, construct, or touch the DuckLake session at all.
      const config = buildEnvironmentMalloyConfig(
         [UNREACHABLE_DUCKLAKE],
         envPath,
      );

      expect(config.apiConnections.map((c) => c.name)).toContain(
         "ducklake_lazy",
      );
      // The definitive assertion: zero SQL was issued building the config, so
      // the (unreachable) catalog could not have been contacted on boot.
      expect(runSQL.callCount).toBe(0);
   });

   it("cannot let an unavailable catalog block startup", async () => {
      sinon
         .stub(DuckDBConnection.prototype, "runSQL")
         .resolves({ rows: [] } as never);

      // Even with an unreachable catalog, constructing the environment config
      // resolves promptly and never awaits the catalog. If attach were on the
      // boot path, this would hang against 192.0.2.1 until a connect timeout.
      const config = buildEnvironmentMalloyConfig(
         [UNREACHABLE_DUCKLAKE],
         envPath,
      );
      expect(config).toBeDefined();
      expect(config.malloyConfig).toBeDefined();
   });

   it("attaches the catalog only on first connection lookup (serve path)", async () => {
      const runSQL = sinon
         .stub(DuckDBConnection.prototype, "runSQL")
         .resolves({ rows: [] } as never);

      const config = buildEnvironmentMalloyConfig(
         [UNREACHABLE_DUCKLAKE],
         envPath,
      );
      expect(runSQL.callCount).toBe(0);

      // The lazy attach fires here, on the first lookup — not before.
      await config.malloyConfig.connections.lookupConnection("ducklake_lazy");

      const issued = runSQL.getCalls().map((c) => String(c.args[0]));
      const attachedDuckLake = issued.some((sql) =>
         sql.includes("ducklake:postgres:"),
      );
      expect(runSQL.callCount).toBeGreaterThan(0);
      expect(attachedDuckLake).toBe(true);

      await config.releaseConnections().catch(() => {});
   });
});

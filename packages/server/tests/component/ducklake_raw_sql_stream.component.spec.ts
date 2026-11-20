// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The raw SQL route (`sqlQuery`) on a DuckLake destination streams its rows
 * (stream_helpers.ts), so it bypasses `runSQL`. It must still renew an
 * expired object-storage credential, and confirm a lost catalog so the next
 * request is answered from a reopened instance, as a modeled query does.
 * The controller runs here as the route calls it, over a destination looked
 * up for real on each request, with the DuckLake statements stubbed
 * (test_helpers/ducklake_fake.ts).
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import sinon from "sinon";

import { ConnectionController } from "../../src/controller/connection.controller";
import { StorageAccessDeniedError } from "../../src/errors";
import { logger } from "../../src/logger";
import { buildEnvironmentMalloyConfig } from "../../src/service/connection";
import type { ConnectionAdmin } from "../../src/service/environment_ports";
import {
   DESTINATION,
   loseCatalog,
   stubDuckLake,
} from "../../src/test_helpers/ducklake_fake";
import { fakeOf } from "../../src/test_helpers/store_fakes";

type Env = ReturnType<typeof buildEnvironmentMalloyConfig>;

/** A controller whose every request looks the destination up in `env`. */
function controllerOver(env: Env): ConnectionController {
   const fakeEnv = {
      assertCanAdmitQuery: () => undefined,
      getApiConnection: () => ({ name: "credible", type: "duckdb" }),
      isConfiguredStorageUrl: () => true,
   };
   const controller = new ConnectionController(
      fakeOf<ConnectionAdmin>({
         getEnvironment: sinon.stub().resolves(fakeEnv),
      }),
   );
   sinon
      .stub(
         controller as unknown as {
            getMalloyConnection: (...args: unknown[]) => Promise<Connection>;
         },
         "getMalloyConnection",
      )
      .callsFake(() =>
         env.malloyConfig.connections.lookupConnection("credible"),
      );
   return controller;
}

/** One raw SQL request, as the route makes it: its rows, or its error. */
async function sqlQuery(
   controller: ConnectionController,
   sql: string,
): Promise<{ rows?: unknown[]; error?: unknown }> {
   try {
      const result = await controller.getConnectionQueryData(
         "env",
         "credible",
         sql,
         "",
         undefined,
         undefined,
         undefined,
         {},
      );
      return {
         rows: (JSON.parse(result.data ?? "") as { rows: unknown[] }).rows,
      };
   } catch (error) {
      return { error };
   }
}

describe("raw SQL on a DuckLake destination", () => {
   let root: string;
   let env: Env;

   beforeEach(() => {
      root = mkdtempSync(join(tmpdir(), "ducklake-raw-sql-"));
   });

   afterEach(async () => {
      await env?.releaseConnections().catch(() => {});
      sinon.restore();
      rmSync(root, { recursive: true, force: true });
   });

   it("reports a lost catalog as such, and the next request is answered from a reopened instance", async () => {
      const { stateOf } = stubDuckLake({});
      const reopened: string[] = [];
      env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
         onDuckLakeReopened: (name) => reopened.push(name),
      });
      const controller = controllerOver(env);
      const sql = "SELECT x FROM credible.main.t UNION ALL SELECT 7 AS x";

      expect(await sqlQuery(controller, sql)).toEqual({ rows: [{ x: 7 }] });
      // Checked once, then cached: without the stream confirming the loss,
      // every later request would fail on the binder error.
      await sqlQuery(controller, sql);
      const connection = (await env.malloyConfig.connections.lookupConnection(
         "credible",
      )) as DuckDBConnection;
      await loseCatalog(connection, stateOf(connection));

      const lost = await sqlQuery(controller, sql);
      expect(String(lost.error)).toContain(
         'DuckLake catalog "credible" is not attached on its DuckDB instance',
      );

      expect(await sqlQuery(controller, sql)).toEqual({ rows: [{ x: 7 }] });
      expect(reopened).toEqual(["credible"]);
   });

   it("renews an expired credential once for two requests that meet it together, and answers both", async () => {
      stubDuckLake({});
      env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      const controller = controllerOver(env);
      const connection = (await env.malloyConfig.connections.lookupConnection(
         "credible",
      )) as DuckDBConnection & {
         setStorageSecretRenewer(renew: () => Promise<void>): void;
      };
      let renewed = false;
      const renew = sinon.fake(async () => {
         await new Promise((r) => setTimeout(r, 20));
         renewed = true;
      });
      connection.setStorageSecretRenewer(renew);
      // The storage refuses reads until the renewal, as it refuses an aged
      // chain secret. Injected into the native DuckDB stream, so the statement
      // gate, the lockdown and the hard close run around it as in production.
      const native = (
         connection as unknown as {
            connection: { stream(sql: string): Promise<unknown> };
         }
      ).connection;
      const stream = native.stream.bind(native);
      let refused = 0;
      sinon.stub(native, "stream").callsFake(async (sql: string) => {
         if (!renewed) {
            refused++;
            throw new Error("HTTP Error: ExpiredToken: token expired");
         }
         return stream(sql);
      });

      const answers = await Promise.all([
         sqlQuery(controller, "SELECT 1 AS x"),
         sqlQuery(controller, "SELECT 1 AS x"),
      ]);
      expect(answers).toEqual([{ rows: [{ x: 1 }] }, { rows: [{ x: 1 }] }]);
      expect(renew.callCount).toBe(1);
      expect(refused).toBe(2);
   });

   it("answers both requests the storage refusal when the renewal they share fails", async () => {
      stubDuckLake({});
      env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      const controller = controllerOver(env);
      const connection = (await env.malloyConfig.connections.lookupConnection(
         "credible",
      )) as DuckDBConnection & {
         setStorageSecretRenewer(renew: () => Promise<void>): void;
      };
      // The renewal fails (STS unreachable), after both requests have met the
      // expired credential, so the second waits on the first's renewal.
      let refused = 0;
      let bothRefused!: () => void;
      const refusedTwice = new Promise<void>((r) => (bothRefused = r));
      const renew = sinon.fake(async () => {
         await refusedTwice;
         // Past the second request's refusal reaching the renewal it joins.
         await new Promise((r) => setTimeout(r, 20));
         throw new Error("STS: Could not connect to sts.amazonaws.com");
      });
      connection.setStorageSecretRenewer(renew);
      const warn = sinon.spy(logger, "warn");
      const native = (
         connection as unknown as {
            connection: { stream(sql: string): Promise<unknown> };
         }
      ).connection;
      sinon.stub(native, "stream").callsFake(async () => {
         if (++refused === 2) bothRefused();
         throw new Error(
            "HTTP Error: HTTP GET error on 's3://lake/data/t.parquet' (HTTP 403) ExpiredToken: The provided token has expired.",
         );
      });

      const answers = await Promise.all([
         sqlQuery(controller, "SELECT 1 AS x"),
         sqlQuery(controller, "SELECT 1 AS x"),
      ]);
      for (const { error } of answers) {
         expect(error).toBeInstanceOf(StorageAccessDeniedError);
         expect(String(error)).toContain("ExpiredToken");
      }
      expect(renew.callCount).toBe(1);
      expect(refused).toBe(2);
      expect(
         warn
            .getCalls()
            .filter((c) =>
               String(c.args[0]).includes("credential renewal failed"),
            ),
      ).toHaveLength(1);
   });
});

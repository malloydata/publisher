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

   it("renews an expired credential and answers the request", async () => {
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
      const renew = sinon.fake.resolves(undefined);
      connection.setStorageSecretRenewer(renew);
      // The storage refuses the first read, as an aged chain secret is.
      let reads = 0;
      sinon
         .stub(DuckDBConnection.prototype, "runSQLStream")
         .callsFake(async function* () {
            if (reads++ === 0) {
               throw new Error("HTTP Error: ExpiredToken: token expired");
            }
            yield { x: 1 };
         });

      expect(
         await sqlQuery(controller, "SELECT x FROM credible.main.t"),
      ).toEqual({ rows: [{ x: 1 }] });
      expect(renew.callCount).toBe(1);
      expect(reads).toBe(2);
   });
});

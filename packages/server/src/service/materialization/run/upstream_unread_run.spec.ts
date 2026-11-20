// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection as MalloyDuckDB } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as path from "path";
import { __setPackageLoadPoolForTests } from "../../../package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { unrecordedAudit } from "../../access_record";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { QueryPrivileges } from "../../model_query_request";
import type { Package } from "../../package";
import { storageBindingResolverFor } from "../../versions/materialization_scope";
import { MaterializationService } from "./materialization_service";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";

/**
 * R-13 when the catalog cannot be read: a source whose upstream table has no
 * reading in this process (its read failed, after retrying) is refused alone
 * as `upstream_unread` rather than addressed by an unknown schema. It answers
 * live and no run builds it; once a reload reads the catalog it is planned at
 * the address a successful read gives it, and serves its table equal to live.
 * A failure after a success keeps the last reading, and the address.
 *
 * Each Environment is a fresh process here: its connections are new objects,
 * so it holds no reading of its own. The warehouse's `ev` comes from the
 * connection's setup SQL, so every environment reads the same table.
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: {
      setupSQL:
         "CREATE TABLE IF NOT EXISTS ev AS SELECT * FROM (VALUES (1, 'east', 10.5), (2, 'east', 20.0), (3, 'west', 5.5)) AS t(id, region, amt)",
   },
} as const;

const MODEL = `source: ev is warehouse.table('ev')

#(materialize)
source: ev_all is ev extend {
  measure: total is amt.sum(), n is count()
}
`;

const QUERY = "run: ev_all -> { group_by: region; aggregate: total, n }";

/** The catalog read of `ev` fails while `failing.on`; nothing else does. */
const failing = { on: false };

describe("an upstream table whose catalog read fails", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;
   let describeSpy: { mockRestore(): void };

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: {
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      const runSQL = MalloyDuckDB.prototype.runSQL;
      describeSpy = spyOn(MalloyDuckDB.prototype, "runSQL").mockImplementation(
         function (this: MalloyDuckDB, ...args: Parameters<typeof runSQL>) {
            if (failing.on && args[0] === "DESCRIBE SELECT * FROM ev") {
               return Promise.reject(new Error("catalog unavailable"));
            }
            return runSQL.apply(this, args);
         },
      );
      ({ root, envPath, db, repo } = await openTempStorage("upstream-unread-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
   });

   afterEach(async () => {
      failing.on = false;
      describeSpy.mockRestore();
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restoreSettings();
      await __setPackageLoadPoolForTests(null);
   });

   /** A new environment over the same files: a fresh process. */
   async function freshProcess(): Promise<void> {
      await env?.closeAllConnections().catch(() => undefined);
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      // As at startup: a load binds the tables the local store records.
      env.setStorageBindingResolver(storageBindingResolverFor(repo, ENV_ID));
      env.restoreUnversionedBindingsFromLocalStore();
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   }

   const pkg = (reload = false): Promise<Package> =>
      env.getPackage(parsePackageName(PKG), reload);

   async function addressOf(): Promise<string | undefined> {
      return Object.values((await pkg()).getBuildPlan()?.sources ?? {}).find(
         (s) => s.name === "ev_all",
      )?.sourceEntityId;
   }

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
      const deadline = Date.now() + 60_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   async function builtTables(): Promise<string[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (
         await connection.runSQL(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' AND table_name <> 'ev' ORDER BY 1",
         )
      ).rows.map((r) => String((r as Record<string, unknown>).table_name));
   }

   /** The query served (bindings in force) and live (none), as row JSON. */
   async function servedAndLive(): Promise<{
      served: string;
      live: string;
      kind: string;
      tableId?: string;
   }> {
      const model = (await pkg()).getModel("model.malloy")!;
      const run = () =>
         model.runQuery(
            unrecordedAudit(),
            { query: QUERY } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      const served = await run();
      model.setServeBindings([]);
      let live: Awaited<ReturnType<typeof run>>;
      try {
         live = await run();
      } finally {
         (
            model as unknown as { bindingsOverride: undefined }
         ).bindingsOverride = undefined;
      }
      const rows = (r: typeof served) =>
         JSON.stringify(
            (r.compactResult as unknown as Record<string, unknown>[])
               .map((row) => JSON.stringify(row))
               .sort(),
         );
      const servedBy = served.path?.servedBy as
         | { kind: string; tableId?: string }
         | undefined;
      return {
         served: rows(served),
         live: rows(live),
         kind: servedBy?.kind ?? "live",
         ...(servedBy?.tableId ? { tableId: servedBy.tableId } : {}),
      };
   }

   it("refuses the source in a fresh process, answers live, builds nothing, and plans it at its address once a reload reads the catalog", async () => {
      // A process whose read succeeds: the address a reading gives.
      await freshProcess();
      const address = await addressOf();
      expect(address).toBeDefined();

      // A fresh process whose read fails.
      failing.on = true;
      await freshProcess();
      const loaded = await pkg();
      expect(await addressOf()).toBeUndefined();
      const refused = Object.values(
         loaded.getBuildPlan()?.refusedSources ?? {},
      ).filter((r) => r.name === "ev_all");
      expect(refused.map((r) => r.reason)).toEqual(["upstream_unread"]);
      const warning = (loaded.getPackageMetadata().warnings ?? []).find(
         (w) => w.code === "materialize-not-built" && w.subject === "ev_all",
      );
      expect(warning?.message).toContain('"ev" on connection "warehouse"');

      const nothing = await build();
      expect(nothing.status).not.toBe("FAILED");
      expect(await builtTables()).toEqual([]);
      const live = await servedAndLive();
      expect(live.kind).toBe("live");
      expect(live.served).toBe(live.live);

      // The catalog answers again: a reload plans it at the same address,
      // and a run builds the table it serves, equal to live.
      failing.on = false;
      await pkg(true);
      expect(await addressOf()).toBe(address);
      expect((await build()).status).toBe("MANIFEST_FILE_READY");
      const served = await servedAndLive();
      expect(served.kind).toBe("materialized");
      expect(served.tableId).toBe(address);
      expect(served.served).toBe(served.live);

      // A failure after that success keeps the last reading: the address
      // does not move, and the table still serves.
      failing.on = true;
      await pkg(true);
      expect(await addressOf()).toBe(address);
      const still = await servedAndLive();
      expect(still.kind).toBe("materialized");
      expect(still.served).toBe(still.live);
   }, 120_000);
});

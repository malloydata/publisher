// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../access_record";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import {
   installSettings,
   MATERIALIZATION_ON,
} from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { MaterializationService } from "./materialization_service";
import { KEPT_TABLES_KEY } from "./reclaimer";

/**
 * A persisted named query's table reads as the query (`<pkg>__<file>__<query>
 * __p<8 hex>`), as a persisted view's reads with no `_view`. A table an
 * earlier release built under `…__<query>_query__p<8 hex>` keeps serving
 * until the next auto-run, which builds the new name once and gives the old
 * table back by the one rename rule: dropped only after the new table is
 * served, never before.
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const MODEL = [
   "##! experimental.persistence",
   `source: base is warehouse.sql("SELECT 'fresh' AS who")`,
   "#@ persist",
   "query: top is base -> { group_by: who }",
   "",
].join("\n");

describe("a persisted named query's table name", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: {
            ...MATERIALIZATION_ON,
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      ({ root, envPath, db, repo } = await openTempStorage(
         "persisted-query-rename-",
         { id: ENV_ID, name: ENV },
      ));
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restoreSettings();
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
      const deadline = Date.now() + 60_000;
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status) &&
            !running.has(created.id)
         ) {
            return m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   /** Bind the package to what the store records, as a loading server does. */
   async function rebind(): Promise<void> {
      const committer = (
         service as unknown as {
            committer: {
               rebindLoadedVersions: (e: string, p: string) => Promise<boolean>;
            };
         }
      ).committer;
      expect(await committer.rebindLoadedVersions(ENV, PKG)).toBe(true);
   }

   async function sql(text: string): Promise<Record<string, unknown>[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (await connection.runSQL(text)).rows as Record<string, unknown>[];
   }

   async function tables(): Promise<string[]> {
      return (
         await sql(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY 1",
         )
      ).map((r) => String(r.table_name));
   }

   /** What `run: top` answers, and whether a stored table answered it. */
   async function top(): Promise<{ who: string; stored: boolean }> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const result = (await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), { query: "run: top" })) as unknown as {
         compactResult: { who: string }[];
         path?: { servedBy?: { kind?: string } };
      };
      return {
         who: result.compactResult[0].who,
         stored: result.path?.servedBy?.kind === "query",
      };
   }

   it("reads as the query, and the table an earlier release named `_query` is dropped only once the new one serves", async () => {
      const first = await build();
      expect(first.status).toBe("MANIFEST_FILE_READY");
      const [address, entry] = Object.entries(first.manifest!.entries!)[0];
      const fresh = entry.physicalTableName;
      expect(fresh).toMatch(/^pa__model__top__p[0-9a-f]{8}$/);

      // The table as an earlier release named and recorded it, holding rows
      // only it holds, so an answer says which table gave it.
      const legacy = fresh.replace(/__p([0-9a-f]{8})$/, "_query__p$1");
      await sql(`ALTER TABLE ${fresh} RENAME TO ${legacy}`);
      await sql(`UPDATE ${legacy} SET who = 'legacy'`);
      await repo.updateMaterialization(first.id, {
         manifest: {
            ...first.manifest!,
            entries: {
               [address]: { ...entry, physicalTableName: legacy },
            },
         } as never,
      });
      await rebind();
      expect(await top()).toEqual({ who: "legacy", stored: true });

      // At the drop, the new table is built and is what the package serves.
      const reclaimer = service.reclaimer as unknown as {
         dropManifestEntries: (...a: unknown[]) => Promise<unknown>;
      };
      const drop = reclaimer.dropManifestEntries.bind(reclaimer);
      const atDrop: {
         tables: string[];
         answer: { who: string; stored: boolean };
      }[] = [];
      reclaimer.dropManifestEntries = async (...a: unknown[]) => {
         atDrop.push({ tables: await tables(), answer: await top() });
         return drop(...a);
      };

      const second = await build();
      expect(second.status).toBe("MANIFEST_FILE_READY");
      expect(
         Object.values(second.manifest!.entries!)[0].physicalTableName,
      ).toBe(fresh);
      expect(atDrop).toHaveLength(1);
      expect(atDrop[0].tables).toEqual(expect.arrayContaining([legacy, fresh]));
      expect(atDrop[0].answer).toEqual({ who: "fresh", stored: true });
      expect(await tables()).toContain(fresh);
      expect(await tables()).not.toContain(legacy);
      expect(second.metadata?.[KEPT_TABLES_KEY]).toBeUndefined();
      expect(await top()).toEqual({ who: "fresh", stored: true });

      // Once: the next run carries the table and offers nothing.
      atDrop.length = 0;
      const third = await build();
      expect(Object.values(third.manifest!.entries!)[0].physicalTableName).toBe(
         fresh,
      );
      expect(atDrop).toEqual([]);
   });

   it("keeps the `_query` table while the run that would replace it has not committed", async () => {
      const first = await build();
      const [address, entry] = Object.entries(first.manifest!.entries!)[0];
      const legacy = entry.physicalTableName.replace(
         /__p([0-9a-f]{8})$/,
         "_query__p$1",
      );
      await sql(`ALTER TABLE ${entry.physicalTableName} RENAME TO ${legacy}`);
      await repo.updateMaterialization(first.id, {
         manifest: {
            ...first.manifest!,
            entries: { [address]: { ...entry, physicalTableName: legacy } },
         } as never,
      });
      await rebind();
      // The new table's build fails: the run commits nothing.
      const collaborators = service.collaborators as unknown as {
         buildOneSource: (...a: unknown[]) => Promise<unknown>;
      };
      collaborators.buildOneSource = async () => {
         throw new Error("the warehouse is down");
      };
      const second = await build();
      expect(second.status).toBe("FAILED");
      expect(await tables()).toContain(legacy);
      expect(await tables()).not.toContain(entry.physicalTableName);
   });

   it("builds a query, a view and a source whose names read alike into tables of their own", async () => {
      fs.writeFileSync(
         path.join(envPath, PKG, "model.malloy"),
         [
            "##! experimental.persistence",
            `source: orders is warehouse.sql("SELECT 'view' AS who") extend {`,
            "  #@ persist",
            "  view: top is { group_by: who }",
            "}",
            "#@ persist",
            `source: orders_top is warehouse.sql("SELECT 'source' AS who") -> { group_by: who }`,
            "#@ persist",
            `query: orders__top is warehouse.sql("SELECT 'query' AS who") -> { group_by: who }`,
            "",
         ].join("\n"),
      );
      await env.getPackage(parsePackageName(PKG), true);
      const run = await build();
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const built = Object.values(run.manifest!.entries!).map(
         (e) => e.physicalTableName,
      );
      expect(built).toHaveLength(3);
      expect(new Set(built).size).toBe(3);
      // All three read `orders_top`; the hash keeps them apart.
      for (const name of built) {
         expect(name).toMatch(/^pa__model__orders_top__p[0-9a-f]{8}$/);
      }
      expect(
         (await Promise.all(built.map((n) => sql(`SELECT who FROM ${n}`))))
            .map((r) => String(r[0].who))
            .sort(),
      ).toEqual(["query", "source", "view"]);
   });
});

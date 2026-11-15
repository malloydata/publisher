// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../access_record";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../storage/duckdb/schema";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import type { EnvironmentStore } from "../../environment_store";
import { parsePackageName } from "../../identity";
import { autoRunTableName } from "../../../test_helpers/readable_table_name";
import { MaterializationService } from "./materialization_service";

/**
 * One source's VALID annotation Publisher cannot build refuses that source
 * alone: the package loads, a run builds every other source, and the refused
 * one answers live with live's rows. (One source's INVALID annotation fails
 * the package's load instead, so no run ever starts over it: a run is never
 * refused whole for one source.)
 */

const ENV = "e1";
const ENV_ID = "env-1";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const MODEL = [
   "##! experimental.persistence",
   `source: base is warehouse.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'a')) AS t(k, who)")`,
   // Valid, but DuckDB cannot refresh incrementally: refused alone.
   '#@ persist refresh="incremental" watermark="k"',
   "source: inc is base -> { group_by: k, who }",
   "#@ persist",
   "source: summary is base -> { group_by: who; aggregate: n is count() }",
   "",
].join("\n");

describe("a run beside a source refused alone", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "refused-alone-run-")),
      );
      envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, "pa"),
         { "model.malloy": MODEL },
         { manifest: { name: "pa", description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage("pa");
      const store = {
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore;
      service = new MaterializationService(store);
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
      restoreSettings();
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, "pa", {});
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

   async function rows(query: string): Promise<unknown[]> {
      const pkg = await env.getPackage(parsePackageName("pa"), false);
      const result = await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), { query });
      return (result as unknown as { compactResult: unknown[] }).compactResult;
   }

   async function tables(): Promise<string[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (
         await connection.runSQL(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY 1",
         )
      ).rows.map((r) => String((r as Record<string, unknown>).table_name));
   }

   it("loads, builds the others, and answers the refused source live", async () => {
      const pkg = await env.getPackage(parsePackageName("pa"), false);
      const refused = (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "persist-not-built",
      );
      expect(refused.map((w) => w.subject)).toEqual(["inc"]);

      const run = await build();
      expect(run.status).toBe("MANIFEST_FILE_READY");
      expect(await tables()).toEqual([
         autoRunTableName(ENV, "pa", "summary", { modelPath: "model.malloy" }),
      ]);
      expect(await rows("run: inc -> { select: k, who; order_by: k }")).toEqual(
         await rows("run: base -> { select: k, who; order_by: k }"),
      );
   });
});

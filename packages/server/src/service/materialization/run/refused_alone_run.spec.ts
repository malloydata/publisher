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
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { autoRunTableName } from "../../../test_helpers/readable_table_name";
import { MaterializationService } from "./materialization_service";
import { BuildPlanUnavailableError } from "../../../errors";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";

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
      ({ root, envPath, db, repo } = await openTempStorage(
         "refused-alone-run-",
         {
            id: ENV_ID,
            name: ENV,
         },
      ));
      writePackageFiles(
         path.join(envPath, "pa"),
         { "model.malloy": MODEL },
         { manifest: { name: "pa", description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage("pa");
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

   it("refuses, at creation (409), a run of a package whose last reload refused its files", async () => {
      // The package loaded; then a save gave a source an invalid annotation,
      // which its reload refuses, so it serves the model from before.
      fs.writeFileSync(
         path.join(envPath, "pa", "model.malloy"),
         MODEL.replace("#@ persist\n", '#@ persist nmae="t"\n'),
      );
      await expect(
         env.getPackage(parsePackageName("pa"), true),
      ).rejects.toThrow("[persist-unknown-key]");
      const err = await service
         .createMaterialization(ENV, "pa", {})
         .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BuildPlanUnavailableError);
      expect(await tables()).toEqual([]);
   });

   it("refuses a run over files on disk the load would refuse, with the load's own text", async () => {
      // Saved, never reloaded: the run compiles the files, and its guard
      // reads the load gate's text off them (an invalid incremental
      // declaration here, which the plan's findings alone do not carry).
      fs.writeFileSync(
         path.join(envPath, "pa", "model.malloy"),
         MODEL.replace("#@ persist\n", '#@ persist refresh="incremental"\n'),
      );
      const run = await build();
      expect(run.status).toBe("FAILED");
      expect(run.error).toContain("no watermark=");
      expect(run.error).toContain("[persist-invalid]");
      expect(await tables()).toEqual([]);
   });

   it("under PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED=false, builds neither source an author's name= puts on one table, and the rest", async () => {
      const { restore } = installSettings({
         env: {
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED: "false",
         },
      });
      try {
         fs.writeFileSync(
            path.join(envPath, "pa", "model.malloy"),
            [
               "##! experimental.persistence",
               `source: base is warehouse.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(k, who)")`,
               '#@ persist name="dup"',
               "source: one is base -> { group_by: who }",
               '#@ persist name="dup"',
               "source: two is base -> { group_by: k }",
               "#@ persist",
               "source: summary is base -> { group_by: who; aggregate: n is count() }",
               "",
            ].join("\n"),
         );
         await env.getPackage(parsePackageName("pa"), true);
         const run = await build();
         expect(run.status).toBe("MANIFEST_FILE_READY");
         // Nothing at dup: neither overwrote the other.
         expect(await tables()).toEqual([
            autoRunTableName(ENV, "pa", "summary", {
               modelPath: "model.malloy",
            }),
         ]);
         const refused = Object.values(
            (
               run.metadata as {
                  refusedSources?: Record<
                     string,
                     { name: string; reason: string }
                  >;
               }
            )?.refusedSources ?? {},
         )
            .map((r) => [r.name, r.reason])
            .sort();
         expect(refused).toEqual([
            ["one", "table_name_collision"],
            ["two", "table_name_collision"],
         ]);
      } finally {
         restore();
      }
   });
});

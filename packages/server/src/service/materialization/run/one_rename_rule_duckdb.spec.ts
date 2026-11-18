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
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { TABLE_KEPT_CODE } from "../package_warnings";
import { MaterializationService } from "./materialization_service";
import { KEPT_TABLES_KEY, RENAMED_TABLES_KEY } from "./reclaimer";
import type { KeptTable } from "../domain/types";
import { ownedTableComment } from "./table_ownership";

/**
 * The one rename rule end to end on a DuckDB warehouse: a source's table is
 * renamed between two auto-runs by an edit to its `name=`, and the table it
 * replaced is dropped once the second run commits and is served when
 * Publisher can prove it is its own, and is kept, listed on the run
 * (`metadata.keptTables`) and raised as `table-kept`, when it is
 * at an author's exact `name=`. A table another package has marked, or one
 * this package's records never built, is never dropped.
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

/** The model, with `summary`'s `#@ persist` line as given. */
const model = (persist: string, who = "fresh") =>
   [
      "##! experimental.persistence",
      `source: base is warehouse.sql("SELECT '${who}' AS who")`,
      persist,
      "source: summary is base -> { group_by: who }",
      "",
   ].join("\n");

describe("the one rename rule on DuckDB", () => {
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
      ({ root, envPath, db, repo } = await openTempStorage("one-rename-rule-", {
         id: ENV_ID,
         name: ENV,
      }));
      for (const pkg of [PKG, "pb"]) {
         writePackageFiles(
            path.join(envPath, pkg),
            { "model.malloy": model("#@ persist", pkg) },
            { manifest: { name: pkg, description: "fixture" } },
         );
      }
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage(PKG);
      await env.addPackage("pb");
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

   async function build(pkg = PKG): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, pkg, {});
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

   /** Edit `summary`'s persist line, and reload the package. */
   async function edit(persist: string): Promise<void> {
      fs.writeFileSync(
         path.join(envPath, PKG, "model.malloy"),
         model(persist, PKG),
      );
      await env.getPackage(parsePackageName(PKG), true);
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

   const tableOf = (m: Materialization) =>
      Object.values(m.manifest?.entries ?? {})[0].physicalTableName;

   async function keptWarnings() {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      return (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === TABLE_KEPT_CODE,
      );
   }

   it("drops the readable-name table once the author adds name=", async () => {
      const first = await build();
      const readable = tableOf(first);
      expect(readable).toMatch(/^pa__model__summary__p[0-9a-f]{8}$/);
      await edit('#@ persist name="summary_t"');
      const second = await build();
      expect(second.status).toBe("MANIFEST_FILE_READY");
      expect(tableOf(second)).toBe("summary_t");
      expect(await tables()).not.toContain(readable);
      expect(await tables()).toContain("summary_t");
      expect(second.metadata?.[RENAMED_TABLES_KEY]).toBeUndefined();
      expect(second.metadata?.[KEPT_TABLES_KEY]).toBeUndefined();
      expect(await keptWarnings()).toEqual([]);
   });

   it("keeps and lists an author's exact name= when it changes, until the name is used again", async () => {
      await edit('#@ persist name="summary_t"');
      await build();
      await edit('#@ persist name="summary_v2"');
      const renamed = (await build()).id;
      const m = (await repo.getMaterializationById(renamed))!;
      expect(tableOf(m)).toBe("summary_v2");
      // Never dropped: the author chose it.
      expect(await tables()).toEqual(
         expect.arrayContaining(["summary_t", "summary_v2"]),
      );
      expect(m.metadata?.[KEPT_TABLES_KEY]).toEqual([
         {
            sourceEntityId: expect.any(String),
            sourceName: "summary",
            physicalTableName: "summary_t",
            connectionName: "warehouse",
            reason: "author_name",
         } satisfies Partial<KeptTable>,
      ]);
      const [warning] = await keptWarnings();
      expect(warning).toMatchObject({
         subject: "summary",
         model: "model.malloy",
         severity: "warn",
      });
      expect(warning.message).toContain("`summary_t`");

      // A later run carries it forward.
      const carried = await build();
      expect(carried.metadata?.[KEPT_TABLES_KEY]).toHaveLength(1);

      // Removed: kept too. Then back at its first name: no longer listed.
      await edit("#@ persist");
      const removed = await build();
      expect(
         (removed.metadata?.[KEPT_TABLES_KEY] as KeptTable[]).map(
            (t) => t.physicalTableName,
         ),
      ).toEqual(["summary_t", "summary_v2"]);
      await edit('#@ persist name="summary_t"');
      const back = await build();
      expect(
         (back.metadata?.[KEPT_TABLES_KEY] as KeptTable[]).map(
            (t) => t.physicalTableName,
         ),
      ).toEqual(["summary_v2"]);
      expect(await tables()).toContain("summary_t");
   });

   it("never drops a table another package has marked as its own", async () => {
      const readable = tableOf(await build());
      await sql(
         `COMMENT ON TABLE ${readable} IS '${ownedTableComment(`${ENV}/pb`)}'`,
      );
      await edit('#@ persist name="summary_t"');
      const second = await build();
      expect(tableOf(second)).toBe("summary_t");
      expect(await tables()).toContain(readable);
      // Left in place: still recorded, for a later run.
      expect(second.metadata?.[RENAMED_TABLES_KEY]).toHaveLength(1);
   });

   it("never drops another package's table, whatever a record names", async () => {
      const theirs = tableOf(await build("pb"));
      await build();
      await edit('#@ persist name="summary_t"');
      // A record of this package's that names the other package's table.
      const plan = (
         await env.getPackage(parsePackageName(PKG), false)
      ).getBuildPlan()!;
      const [source] = Object.values(plan.sources);
      const seeded = await repo.createMaterialization(ENV_ID, PKG, "PENDING", {
         mode: "auto",
         [RENAMED_TABLES_KEY]: [
            {
               sourceEntityId: source.sourceEntityId,
               previousName: theirs,
               physicalTableName: "summary_t",
               connectionName: "warehouse",
            },
         ],
      });
      await repo.updateMaterialization(seeded.id, {
         status: "MANIFEST_FILE_READY",
         manifest: { builtAt: new Date().toISOString(), entries: {} } as never,
         completedAt: new Date(),
      } as never);
      const second = await build();
      expect(tableOf(second)).toBe("summary_t");
      expect(await tables()).toContain(theirs);
      const pb = await env.getPackage(parsePackageName("pb"), false);
      expect(pb.getBuildPlan()).not.toBeNull();
   });

   /** The run's background work, its reclaim included, finished. */
   async function finished(id: string): Promise<Materialization> {
      const deadline = Date.now() + 60_000;
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      for (;;) {
         const m = await repo.getMaterializationById(id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status) &&
            !running.has(id)
         ) {
            return m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   async function who(): Promise<string> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const result = await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), {
            query: "run: summary -> { select: who }",
         });
      return (result as unknown as { compactResult: { who: string }[] })
         .compactResult[0].who;
   }

   it("never drops the table a run committed while an earlier run's reclaim was deciding", async () => {
      const readable = tableOf(await build());
      await edit('#@ persist name="summary_t"');
      // Hold the renaming run's reclaim at its bind, after it has decided
      // the readable table is reclaimable, until a later run has committed.
      const committer = (
         service as unknown as {
            committer: {
               rebindLoadedVersions: (...a: unknown[]) => Promise<boolean>;
            };
         }
      ).committer;
      const rebind = committer.rebindLoadedVersions.bind(committer);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      let entered!: () => void;
      const held = new Promise<void>((resolve) => (entered = resolve));
      let calls = 0;
      committer.rebindLoadedVersions = async (...a: unknown[]) => {
         calls += 1;
         if (calls === 1) {
            entered();
            await gate;
         }
         return rebind(...a);
      };
      const renaming = await service.createMaterialization(ENV, PKG, {});
      await held;
      // The author reverts: the next run rebuilds the readable name.
      await edit("#@ persist");
      const reverted = await build();
      expect(tableOf(reverted)).toBe(readable);
      release();
      await finished(renaming.id);
      expect(await tables()).toContain(readable);
      expect(await who()).toBe(PKG);
   });

   it("drops a table whose record outlived its replacement (A to B to C) once it is provably this package's", async () => {
      const a = tableOf(await build());
      await sql(`COMMENT ON TABLE ${a} IS '${ownedTableComment(`${ENV}/pb`)}'`);
      await edit('#@ persist name="b_t"');
      const r2 = await build();
      // Another package's mark: left in place, still recorded.
      expect(r2.metadata?.[RENAMED_TABLES_KEY]).toHaveLength(1);
      await edit('#@ persist name="c_t"');
      await build();
      expect(await tables()).toContain(a);
      // The mark goes: A is now provably this package's.
      await sql(`COMMENT ON TABLE ${a} IS NULL`);
      const r4 = await build();
      expect(r4.status).toBe("MANIFEST_FILE_READY");
      expect(await tables()).not.toContain(a);
      const records = (await repo.listMaterializations(ENV_ID, PKG)).filter(
         (m) => Array.isArray(m.metadata?.[RENAMED_TABLES_KEY]),
      );
      expect(records).toEqual([]);
      // B, an author's name, is kept and listed; C serves.
      expect(
         (r4.metadata?.[KEPT_TABLES_KEY] as KeptTable[]).map(
            (t) => t.physicalTableName,
         ),
      ).toEqual(["b_t"]);
      expect(await who()).toBe(PKG);
   });

   it("stops listing, and warning about, a kept table dropped by hand", async () => {
      await edit('#@ persist name="summary_t"');
      await build();
      await edit('#@ persist name="summary_v2"');
      await build();
      expect(await keptWarnings()).toHaveLength(1);
      await sql("DROP TABLE summary_t");
      const next = await build();
      expect(next.metadata?.[KEPT_TABLES_KEY]).toEqual([]);
      expect(await keptWarnings()).toEqual([]);
   });

   it("never drops, by reclaim, a table a container-only change still names", async () => {
      const [{ db: catalog }] = await sql("SELECT current_database() AS db");
      await edit('#@ persist name="main.summary_t"');
      const first = await build();
      expect(tableOf(first)).toBe("main.summary_t");
      // The same table, through the database's catalog name.
      await edit(`#@ persist name="${String(catalog)}.summary_t"`);
      const second = await build();
      expect(tableOf(second)).toBe(`${String(catalog)}.summary_t`);
      expect(await tables()).toContain("summary_t");
      expect(
         (second.metadata?.[KEPT_TABLES_KEY] as KeptTable[]).map((t) => [
            t.physicalTableName,
            t.reason,
         ]),
      ).toEqual([["main.summary_t", "may_be_served"]]);
      expect(await who()).toBe(PKG);
   });
});

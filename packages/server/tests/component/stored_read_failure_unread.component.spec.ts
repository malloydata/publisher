// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const PKG = "pkg";

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1), ('a', 2), ('b', 3)) AS t(cat, n)
""")

#@ persist name="rf_a"
source: a is raw -> { group_by: cat; aggregate: total is n.sum() }
`;
const QUERY = "run: a -> { select: *; order_by: cat }";

/**
 * A query's failed read of a stored table (a rebuild's swap renaming it
 * aside, a permission the worker lacks, a live table the query joined) is
 * never a verdict on the table: the table is failed closed, unread and
 * answered live, until the package's rebind reads its catalog, and only that
 * read, held to the table's contract, could find it incompatible. A standalone
 * run never rebuilds it for that alone. DuckDB.
 */
describe("a failed stored read leaves its table unread, never incompatible", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   let table: string;
   let address: string;

   const ask = async (): Promise<string | undefined> => {
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query: QUERY } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return (result.path as { servedBy?: { kind?: string } } | undefined)
         ?.servedBy?.kind;
   };
   const verdictOf = () =>
      pkg
         .materialization()
         .serveVerdicts()
         .find((v) => v.sourceName === "a") as
         | { verdict?: string; reason?: string }
         | undefined;
   const until = async (ok: () => Promise<boolean>) => {
      const deadline = Date.now() + 60_000;
      while (!(await ok())) {
         if (Date.now() > deadline) throw new Error("never settled");
         await new Promise((r) => setTimeout(r, 100));
      }
   };
   /** The read trigger, as a query's failed read reports its tables. */
   const readFailed = () =>
      (
         pkg.materialization() as unknown as {
            onIncompatible(handles: readonly string[]): void;
         }
      ).onIncompatible([address]);

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "rf-")));
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         ["env-1", ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [],
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      const created = await service.createMaterialization(ENV, PKG, {});
      let m: Materialization | null = null;
      await until(async () => {
         m = await repo.getMaterializationById(created.id);
         return !!m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status);
      });
      const entry = Object.values(
         (
            m!.manifest as {
               entries: Record<
                  string,
                  { physicalTableName: string; sourceEntityId: string }
               >;
            }
         ).entries,
      )[0];
      table = entry.physicalTableName;
      address = entry.sourceEntityId;
      await until(async () => (await ask()) === "materialized");
   }, 300_000);
   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("fails a read in a swap's rename window closed as unread, then serves once the rebind reads the table back", async () => {
      const warehouse = await env.getMalloyConnection("warehouse");
      await warehouse.runSQL(`ALTER TABLE ${table} RENAME TO ${table}_aside`);
      readFailed();
      expect(verdictOf()?.reason).not.toBe("incompatible");
      expect(pkg.incompatibleTables().has(address)).toBe(false);
      expect(await ask()).toBe("live");
      await warehouse.runSQL(`ALTER TABLE ${table}_aside RENAME TO ${table}`);
      await until(async () => (await ask()) === "materialized");
      expect(verdictOf()?.reason).not.toBe("incompatible");
      expect(pkg.incompatibleTables().has(address)).toBe(false);
   }, 120_000);

   it("keeps a table failed closed when a bind replaced the entries its rebind read, and reads it again", async () => {
      const mz = pkg.materialization() as unknown as {
         readDatabaseColumns: (...a: unknown[]) => Promise<unknown>;
         databaseEntries: Record<string, unknown> | undefined;
         readFailed: ReadonlySet<string>;
      };
      const read = mz.readDatabaseColumns.bind(mz);
      let reads = 0;
      let failedAfterDiscard: boolean | undefined;
      mz.readDatabaseColumns = async (...a: unknown[]) => {
         reads += 1;
         const out = await read(...a);
         if (reads === 1) {
            // A bind lands while the first rebind reads: its read is stale.
            mz.databaseEntries = { ...mz.databaseEntries };
            setTimeout(() => {
               failedAfterDiscard ??= mz.readFailed.has(address);
            }, 0);
         }
         return out;
      };
      try {
         readFailed();
         await until(async () => (await ask()) === "materialized");
         expect(reads).toBeGreaterThanOrEqual(2);
         expect(failedAfterDiscard).toBe(true);
      } finally {
         mz.readDatabaseColumns = read;
      }
   }, 120_000);

   it("runs overlapping commits one at a time, so none clears the other's pending compile", async () => {
      const mz = pkg.materialization() as unknown as {
         commit(change: () => void, models?: "installed"): Promise<void>;
         committing: unknown;
      };
      const model = pkg.getModel("m.malloy")! as unknown as {
         prepareServedCopyOver: (...a: unknown[]) => Promise<unknown>;
      };
      const prepare = model.prepareServedCopyOver.bind(model);
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      let compiles = 0;
      model.prepareServedCopyOver = async (...a: unknown[]) => {
         compiles += 1;
         if (compiles === 1) await gate;
         return prepare(...a);
      };
      try {
         const order: string[] = [];
         const first = mz.commit(() => order.push("first"), "installed");
         const second = mz.commit(() => order.push("second"), "installed");
         await new Promise((r) => setTimeout(r, 50));
         // The first is compiling: the second has not begun, and the
         // models still read the snapshot from before the first.
         expect(order).toEqual(["first"]);
         expect(mz.committing).toBeDefined();
         release();
         await Promise.all([first, second]);
         expect(order).toEqual(["first", "second"]);
         expect(compiles).toBe(2);
         expect(mz.committing).toBeUndefined();
      } finally {
         model.prepareServedCopyOver = prepare;
      }
   }, 60_000);

   it("never finds a table incompatible for a read that failed on something else (a permission)", async () => {
      readFailed();
      expect(verdictOf()?.reason).not.toBe("incompatible");
      expect(pkg.incompatibleTables().has(address)).toBe(false);
      await until(async () => (await ask()) === "materialized");
      expect(verdictOf()?.reason).not.toBe("incompatible");
   }, 120_000);
});

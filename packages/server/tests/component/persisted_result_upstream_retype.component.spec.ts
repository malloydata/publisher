// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every materialization mechanism over a warehouse column retyped after the
 * build: the table is as built, and its content address does not move (the
 * build's SQL is the same), so the bind finds it, but the source now declares
 * another type than the one the table holds. A query that reads the column is
 * answered live, equal to live, with the reason on its path report, and the
 * package warns `storage-column-type-mismatch` for each table; a query that
 * does not read it is still served. Never served from the table, whose values
 * Malloy would read as the type it no longer holds (before this, the query
 * failed: `Invalid string 1`).
 *
 * Mechanisms: a persisted view and a persisted named query (read by Malloy's
 * manifest substitution), a `#(materialize)` source and a `#@ persist` source
 * (each through the served copy), and a `#(preaggregate)` rollup.
 *
 * The retype is read as a server that has not seen the table before reads it:
 * the package-load schema cache is off and the connection's own schema cache
 * is emptied, standing in for a fresh process (those caches never go stale on
 * their own).
 */

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
source: raw is warehouse.table('t') extend {
  measure: total is n.max()
}
source: viewed is raw extend {
  #@ persist
  view: v is { group_by: cat; aggregate: total; order_by: cat }
}
#@ persist
query: q is raw -> { group_by: cat; aggregate: top is n.max(); order_by: cat }
#(materialize)
source: mat is warehouse.table('t')
#@ persist
source: per is warehouse.table('t') -> { select: cat, n, k is 1 }
source: rolled is raw extend {
  #(preaggregate) grain=["cat"]
  measure: rtop is n.max()
  #(preaggregate) grain=["cat"]
  measure: rc is count()
}
`;

const QUERIES = {
   view: "run: viewed -> v",
   query: "run: q",
   mat_n: "run: mat -> { group_by: n; order_by: n }",
   mat_cat: "run: mat -> { group_by: cat; order_by: cat }",
   per_n: "run: per -> { group_by: n; order_by: n }",
   per_cat: "run: per -> { group_by: cat; order_by: cat }",
   roll_n: "run: rolled -> { group_by: cat; aggregate: rtop; order_by: cat }",
   roll_c: "run: rolled -> { group_by: cat; aggregate: rc; order_by: cat }",
} as const;

interface Answer {
   rows: unknown;
   kind?: string;
   considered?: { outcome?: string; reason?: string; detail?: string }[];
}

describe("every materialization over a column retyped upstream after its build", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   const before: Record<string, Answer> = {};
   const after: Record<string, Answer> = {};
   const live: Record<string, unknown> = {};

   const ask = async (query: string): Promise<Answer> => {
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      const report = result.path as
         | {
              servedBy?: { kind?: string };
              considered?: Answer["considered"];
           }
         | undefined;
      return {
         rows: JSON.parse(JSON.stringify(result.compactResult)),
         kind: report?.servedBy?.kind,
         considered: report?.considered,
      };
   };

   const build = async (): Promise<Materialization> => {
      const created = await service.createMaterialization(ENV, PKG, {});
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   };

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0",
         },
      }).restore;
      await freshLoadPool();
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "upstream-retype-")),
      );
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
      const warehouse = await env.getMalloyConnection("warehouse");
      await warehouse.runSQL(
         "CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('b', 2)) AS x(cat, n)",
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      expect((await build()).status).toBe("MANIFEST_FILE_READY");
      for (const [name, query] of Object.entries(QUERIES)) {
         before[name] = await ask(query);
      }
      const bound = pkg.materialization().boundColocatedEntries() ?? {};

      // Upstream, out of band: the source's column changes type.
      await warehouse.runSQL("ALTER TABLE t ALTER n TYPE VARCHAR");
      (warehouse as unknown as { schemaCache: object }).schemaCache = {};
      pkg = await env.getPackage(parsePackageName(PKG), true);
      // Live's answers, before the tables are bound again.
      for (const [name, query] of Object.entries(QUERIES)) {
         live[name] = (await ask(query)).rows;
      }
      // The bind a run's commit, a manifest bind and a restart take.
      await pkg.reloadAllModels(bound);
      for (const [name, query] of Object.entries(QUERIES)) {
         try {
            after[name] = await ask(query);
         } catch (e) {
            after[name] = { rows: `ERR ${(e as Error).message}` };
         }
      }
   }, 300_000);

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   /** What reads the retyped column, by query, and the kind each served before. */
   const READS: Record<keyof typeof QUERIES, { kind: string; reads?: string }> =
      {
         view: { kind: "view", reads: "total" },
         query: { kind: "query", reads: "top" },
         mat_n: { kind: "materialized", reads: "n" },
         mat_cat: { kind: "materialized" },
         per_n: { kind: "materialized", reads: "n" },
         per_cat: { kind: "materialized" },
         roll_n: { kind: "rollup", reads: "rtop" },
         roll_c: { kind: "rollup" },
      };

   it("served each from its table before the retype", () => {
      for (const [name, { kind }] of Object.entries(READS)) {
         expect({ name, kind: before[name].kind }).toEqual({ name, kind });
      }
   });

   for (const [name, { kind, reads }] of Object.entries(READS)) {
      if (reads) {
         it(`${name}: reads \`${reads}\`, so answered live, equal to live, with the reason on its path report`, () => {
            expect(after[name].kind).toBe("live");
            expect(after[name].rows).toEqual(live[name]);
            expect(after[name].considered).toEqual([
               expect.objectContaining({
                  outcome: "skipped",
                  reason: "MISSING_FIELD",
                  detail: expect.stringContaining(`\`${reads}\``),
               }),
            ]);
         });
      } else {
         it(`${name}: reads no retyped column, so still served from its table, equal to live`, () => {
            expect(after[name].kind).toBe(kind);
            expect(after[name].rows).toEqual(live[name]);
         });
      }
   }

   it("warns for each table, naming the column and the forced rebuild that clears it", () => {
      const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "storage-column-type-mismatch",
      );
      expect(warnings.map((w) => w.subject).sort()).toEqual([
         "mat",
         "per",
         "q__query",
         "rolled",
         "viewed__v__view",
      ]);
      for (const w of warnings) {
         expect(w.message).toContain("not its declared Malloy type `string`");
         expect(w.message).toContain(
            "what its source reads changed type since the build",
         );
         expect(w.message).toContain("forceRefresh");
      }
   });
});

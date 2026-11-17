// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Sources declared in a file that imports their base's file, served from the
 * tables the base's file declares, end to end through a real Environment, a
 * MaterializationService build on DuckDB, and the REST query controller, so
 * each answer's access record is read too.
 *
 * P2-22 (F-09): a `where:` declared in the importing file over a stored
 * big-integer column (DuckDB's BIGINT, read by the served copy through its
 * native type) was denied 403 by the filter-binding guard: it resolved the
 * condition in the author's compile of the importing file, where the column
 * is the warehouse's, not in the served copy's, where it is the re-root's
 * read. Every answer below equals live and its access record reads `ok`. A
 * cross-file `#(materialize)` extension is now re-rooted on its own table.
 *
 * P2-41 (F-12): an extension declared in the importing file over a
 * table-rooted base with rollups was answered from the rollup, but the path
 * report said live with nothing considered; and a sibling declaration in the
 * importing file that reads a column outside the grain broke every rollup
 * variant compiled in that file. Both now serve from the rollup, and a query
 * the rollup cannot answer gives a coded reason.
 */

import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
   spyOn,
} from "bun:test";
import * as path from "path";
import { QueryController } from "../../src/controller/query.controller";
import { logger } from "../../src/logger";
import { QuerySlots } from "../../src/query_concurrency";
import type { AccessRecord } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import type {
   McpExposureReader,
   PackageLookup,
} from "../../src/service/environment_ports";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
import { fakeOf } from "../../src/test_helpers/store_fakes";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { QueryUseCase } from "../../src/usecases/query";

setDefaultTimeout(120_000);

const ENV = "e1";
const PKG = "pkg";

// Read from CSV, so DuckDB types `dur` BIGINT: the served copy reads it
// through its native type, a redefinition of the column.
const TRIPS =
   "d,cat,amt,dur\n2017-01-02,a,1.5,10\n2017-03-02,b,2.5,4000\n2018-01-02,a,3.5,5000\n2017-05-05,c,-1,7200\n2017-06-01,b,4.0,3700\n";

const FILES: Record<string, string> = {
   "trips.csv": TRIPS,
   "base.malloy": `##! experimental.persistence
source: trips is duckdb.table('trips.csv')
#(materialize)
source: y is trips extend { where: amt > 0 }
#(materialize)
source: long_same is y extend { where: dur > 3600 }
`,
   "other.malloy": `##! experimental.persistence
import "base.malloy"
#(materialize)
source: long_other is y extend { where: dur > 3600 }
source: plain_other is y extend { where: dur > 3600 }
source: by_cat is y extend { where: cat != 'c' }
`,
   "rollup.malloy": `##! experimental.persistence
source: tr is duckdb.table('trips.csv') extend {
  dimension: start_day is d.day
  #(preaggregate) grain=["start_day"]
  measure: trip_count is count()
}
`,
   "report.malloy": `##! experimental.persistence
import "rollup.malloy"
source: tr_report is tr extend { dimension: early is start_day < @2017-06-01 }
`,
   "report_sibling.malloy": `##! experimental.persistence
import "rollup.malloy"
source: tr_sibling is tr extend { dimension: is_a is cat = 'a' }
`,
};

type Answer = {
   rows: unknown[];
   servedBy?: { kind: string; tableId?: string; via?: string };
   considered: { kind: string; outcome: string; reason?: string }[];
   record: AccessRecord;
};

describe("sources in an importing file, served from their bases' file's tables", () => {
   let root: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let restore: (() => void) | undefined;
   let built: Materialization;
   const live = new Map<string, unknown[]>();
   const slots = new QuerySlots();

   const QUERIES: [string, string][] = [
      ["other.malloy", "run: long_other -> { aggregate: n is count() }"],
      ["other.malloy", "run: plain_other -> { aggregate: n is count() }"],
      ["other.malloy", "run: by_cat -> { aggregate: n is count() }"],
      ["base.malloy", "run: long_same -> { aggregate: n is count() }"],
      [
         "report.malloy",
         "run: tr_report -> { group_by: start_day; aggregate: trip_count; order_by: start_day }",
      ],
      [
         "report_sibling.malloy",
         "run: tr -> { group_by: start_day; aggregate: trip_count; order_by: start_day }",
      ],
      [
         "report_sibling.malloy",
         "run: tr_sibling -> { group_by: start_day; aggregate: trip_count; order_by: start_day }",
      ],
      [
         "report_sibling.malloy",
         "run: tr_sibling -> { group_by: is_a; aggregate: trip_count; order_by: is_a }",
      ],
   ];

   async function ask(modelPath: string, query: string): Promise<Answer> {
      const info = spyOn(logger, "info");
      try {
         const store = fakeOf<PackageLookup & McpExposureReader>({
            getEnvironment: async () => env as never,
            mcpIncludeHiddenFilesAndSources: false,
         });
         const result = (await new QueryController(
            new QueryUseCase(store, slots),
         ).getQuery({
            environmentName: ENV,
            packageName: PKG,
            modelPath,
            sourceName: "",
            queryName: "",
            query,
            compactJson: true,
            filterParams: {},
            givens: undefined,
            bypassAuthorize: false,
            includeHiddenFilesAndSources: false,
            identity: { caller: "test@example.com" },
         })) as {
            result: string;
            path?: {
               servedBy?: Answer["servedBy"];
               considered?: Answer["considered"];
            };
         };
         const records = (info.mock.calls as unknown[][])
            .filter(([m]) => m === "Access record")
            .map(([, r]) => r as AccessRecord);
         return {
            rows: JSON.parse(result.result) as unknown[],
            servedBy: result.path?.servedBy,
            considered: result.path?.considered ?? [],
            record: records[records.length - 1],
         };
      } finally {
         info.mockRestore();
      }
   }

   function entryOf(name: string) {
      const entries = Object.values(
         (
            built.manifest as {
               entries: Record<
                  string,
                  { sourceName: string; sourceEntityId: string }
               >;
            }
         ).entries,
      );
      return entries.find((e) => e.sourceName === name);
   }

   beforeAll(async () => {
      let envPath: string;
      ({ root, envPath, db, repo } = await openTempStorage("cross-file-", {
         id: "env-1",
         name: ENV,
      }));
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      writePackageFiles(path.join(envPath, PKG), FILES, {
         manifest: { name: PKG, description: "fixture" },
      });
      env = await Environment.create(ENV, envPath, []);
      await env.addPackage(PKG);
      for (const [model, query] of QUERIES) {
         live.set(`${model} ${query}`, (await ask(model, query)).rows);
      }
      const service: MaterializationService = serviceOverEnvironment(
         () => repo,
         () => env,
      );
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
               await new Promise((r) => setTimeout(r, 10));
            }
            built = (await repo.getMaterializationById(created.id)) ?? m;
            break;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
      expect([built.status, built.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      await env.getPackage(parsePackageName(PKG), false);
   });

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restore?.();
      await __setPackageLoadPoolForTests(null);
   });

   async function served(model: string, query: string): Promise<Answer> {
      const answer = await ask(model, query);
      expect(answer.rows).toEqual(live.get(`${model} ${query}`)!);
      expect([answer.record.outcome, answer.record.status]).toEqual([
         "ok",
         200,
      ]);
      return answer;
   }

   describe("P2-22", () => {
      it("a cross-file #(materialize) extension over a #(materialize) source, filtered on a big integer, is served from its own table", async () => {
         const [model, query] = QUERIES[0];
         const answer = await served(model, query);
         expect(answer.servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
            tableId: entryOf("long_other")!.sourceEntityId,
         });
      });

      it("a cross-file plain extension filtered on a big integer is served from its base's table", async () => {
         const [model, query] = QUERIES[1];
         const answer = await served(model, query);
         expect(answer.servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
            tableId: entryOf("y")!.sourceEntityId,
         });
      });

      it("a cross-file extension filtered on a string column is served (control)", async () => {
         const [model, query] = QUERIES[2];
         expect((await served(model, query)).servedBy?.kind).toBe(
            "materialized",
         );
      });

      it("the same extension in the base's own file is served (control)", async () => {
         const [model, query] = QUERIES[3];
         expect((await served(model, query)).servedBy).toMatchObject({
            kind: "materialized",
            tableId: entryOf("long_same")!.sourceEntityId,
         });
      });

      it("still denies a caller's inline extend that rebinds the column a source's where: reads, as live does", async () => {
         const query =
            "run: plain_other extend { rename: dur_was is dur; dimension: dur is amt * 10000 } -> { aggregate: n is count() }";
         await expect(ask("other.malloy", query)).rejects.toThrow(
            /Access denied/,
         );
      });
   });

   describe("P2-41", () => {
      it("a cross-file extension of a table-rooted base is served from the base's rollup, and reported so", async () => {
         const [model, query] = QUERIES[4];
         const answer = await served(model, query);
         expect(answer.servedBy?.kind).toBe("rollup");
         expect(answer.considered).toEqual([
            expect.objectContaining({ kind: "rollup", outcome: "used" }),
         ]);
      });

      it("the base queried from a file whose sibling extension reads a column outside the grain is served from the rollup", async () => {
         const [model, query] = QUERIES[5];
         expect((await served(model, query)).servedBy?.kind).toBe("rollup");
      });

      it("that sibling extension itself is served from the rollup", async () => {
         const [model, query] = QUERIES[6];
         expect((await served(model, query)).servedBy?.kind).toBe("rollup");
      });

      it("a cross-file extension grouped by its own dimension over a column outside the grain answers live with a coded reason", async () => {
         const [model, query] = QUERIES[7];
         const answer = await served(model, query);
         expect(answer.servedBy?.kind).toBe("live");
         expect(answer.considered).toEqual([
            expect.objectContaining({
               kind: "rollup",
               outcome: "skipped",
               reason: "GRAIN_TOO_COARSE",
            }),
         ]);
      });
   });
});

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
   "d,cat,amt,dur,org\n2017-01-02,a,1.5,10,1\n2017-03-02,b,2.5,4000,2\n2018-01-02,a,3.5,5000,1\n2017-05-05,c,-1,7200,2\n2017-06-01,b,4.0,3700,1\n";

const FILES: Record<string, string> = {
   "trips.csv": TRIPS,
   "cats.csv": "cat,label\na,Alpha\nb,Beta\nc,Gamma\n",
   "cats2.csv": "cat,label\na,EVIL_A\nb,EVIL_B\nc,EVIL_C\n",
   "base.malloy": `##! experimental.persistence
source: trips is duckdb.table('trips.csv')
#(materialize)
source: y is trips extend { where: amt > 0 }
#(materialize)
source: long_same is y extend { where: dur > 3600 }
source: lookup is duckdb.table('cats.csv') extend { primary_key: cat }
source: lk is trips extend { join_one: L is lookup on cat = L.cat }
#(materialize)
source: lkm is trips extend { join_one: L is lookup on cat = L.cat }
`,
   // A selective import, and a \`lookup\` of the file's own: blocks carried
   // from base.malloy would join this file's \`lookup\`, not base's.
   "shadow.malloy": `##! experimental.persistence
import { lk, lkm } from "base.malloy"
source: lookup is duckdb.table('cats2.csv') extend { primary_key: cat }
#(materialize)
source: lk_other is lk extend { where: amt > 0 }
#(materialize)
source: lkm_other is lkm extend { where: amt > 0 }
`,
   // The same through a whole import, where the names are base's.
   "whole.malloy": `##! experimental.persistence
import "base.malloy"
#(materialize)
source: lk_whole is lk extend { where: amt > 0 }
`,
   // An importing file with no #(materialize) of its own: only the guard
   // decides whether its where: binds on the served copy.
   "plain.malloy": `##! experimental { persistence givens }
import "base.malloy"
given: ORG :: number
source: plain2 is y extend { where: dur > 3600 }
#(access_filter) org = $ORG
source: plain2_gated is y extend { where: dur > 3600 }
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
  #(rollup) grain=["start_day"]
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
   let liveGated: unknown[] = [];
   const slots = new QuerySlots();

   const QUERIES: [string, string][] = [
      // [0..] the P2-22 and P2-41 cases below index into the tail.

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
      [
         "shadow.malloy",
         "run: lk_other -> { group_by: L.label; aggregate: n is count(); order_by: label }",
      ],
      [
         "shadow.malloy",
         "run: lkm_other -> { group_by: L.label; aggregate: n is count(); order_by: label }",
      ],
      [
         "whole.malloy",
         "run: lk_whole -> { group_by: L.label; aggregate: n is count(); order_by: label }",
      ],
      ["plain.malloy", "run: plain2 -> { aggregate: n is count() }"],
   ];
   /** The gated query, asked with a given (not in QUERIES, which run without). */
   const GATED: [string, string] = [
      "plain.malloy",
      "run: plain2_gated -> { aggregate: n is count() }",
   ];

   async function ask(
      modelPath: string,
      query: string,
      givens?: Record<string, unknown>,
   ): Promise<Answer> {
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
            givens: givens as never,
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
      liveGated = (await ask(GATED[0], GATED[1], { ORG: 1 })).rows;
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
         // Built over `y`'s table, as `long_same` in base.malloy is, with the
         // same SQL: the two share one address, so one table, written under
         // `long_same`'s name, serves both.
         expect(entryOf("long_other")).toBeUndefined();
         expect(answer.servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
            tableId: entryOf("long_same")!.sourceEntityId,
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

   describe("P2-22: a join in carried blocks", () => {
      it("never reads a source the importing file declares under the same name (selective import)", async () => {
         for (const [model, query] of [QUERIES[8], QUERIES[9]]) {
            const answer = await served(model, query);
            expect(JSON.stringify(answer.rows)).not.toContain("EVIL");
            expect(JSON.stringify(answer.rows)).toContain("Alpha");
         }
      });

      it("is carried through a whole import, and served from the extension's table", async () => {
         // lk_whole builds y's SQL (its join is not read by the build), so the
         // two share one table: the served copy reads it with base's join.
         const [model, query] = QUERIES[10];
         const answer = await served(model, query);
         expect(answer.servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
         });
         expect(JSON.stringify(answer.rows)).toContain("Alpha");
      });
   });

   describe("P2-22: an importing file with no #(materialize) of its own", () => {
      it("serves its where: on a big integer, equal to live", async () => {
         const [model, query] = QUERIES[11];
         const answer = await served(model, query);
         expect(answer.servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
            tableId: entryOf("y")!.sourceEntityId,
         });
      });

      it("serves the same past an #(access_filter), equal to live", async () => {
         const answer = await ask(GATED[0], GATED[1], { ORG: 1 });
         expect(answer.rows).toEqual(liveGated);
         expect([answer.record.outcome, answer.record.status]).toEqual([
            "ok",
            200,
         ]);
         expect(answer.servedBy?.kind).toBe("materialized");
      });

      it("denies a caller's rebinding of the column its where: reads", async () => {
         await expect(
            ask(
               "plain.malloy",
               "run: plain2 extend { rename: dur_was is dur; dimension: dur is amt * 10000 } -> { aggregate: n is count() }",
            ),
         ).rejects.toThrow(/Access denied/);
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

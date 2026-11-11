// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The workload log end to end (materialization v2, T1): real queries through
 * the one record-emitting wrapper around `Model.runQuery`, into the local
 * store in Publisher's own metadata database, read back by shape.
 */

import type { GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../src/logger";
import { withAccessRecord } from "../../src/service/access_record";
import type { Model } from "../../src/service/model";
import { QueryPrivileges } from "../../src/service/model_query_request";
import {
   setWorkloadLogSink,
   WorkloadLogStore,
} from "../../src/service/workload_log_store";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { withAuthorizeModel } from "../../src/test_helpers/authorize_fixture";

const MODEL = `##! experimental.givens
#(secure)
given: ROLES :: string[]

source: kids is duckdb.table('child') extend {
   dimension: label is name
   measure: n is count()
   view: by_label is { group_by: label; aggregate: n }
}

#(authorize) 'clinician' in $ROLES
source: chart is duckdb.table('child') extend {
   measure: n is count()
}
`;

const BASE = {
   environment: "env",
   package: "pkg",
   modelPath: "m.malloy",
   caller: "alice@example.com",
} as const;

describe("the workload log, from query to store", () => {
   let db: DuckDBConnection;
   let store: WorkloadLogStore;
   let info: ReturnType<typeof spyOn>;

   beforeAll(async () => {
      info = spyOn(logger, "info").mockImplementation(() => logger);
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db, false);
      store = new WorkloadLogStore(
         { isInitialized: () => true, getDuckDbConnection: () => db },
         { retentionDays: 30, maxRows: 1000 },
      );
      setWorkloadLogSink(store);

      await withAuthorizeModel(MODEL, async ({ model, error }) => {
         expect(error).toBeUndefined();
         const run = (
            request: {
               query?: string;
               queryName?: string;
               sourceName?: string;
            },
            givens: Record<string, GivenValue> = {},
         ) =>
            withAccessRecord(
               { ...BASE, adHoc: request.query !== undefined },
               undefined,
               (audit) =>
                  (model as Model).runQuery(
                     audit,
                     { ...request, filterParams: {}, givens },
                     QueryPrivileges.of({
                        bypassFilters: true,
                        bypassAuthorize: false,
                     }),
                  ),
            ).catch(() => undefined);
         // One question asked twice with different values: one shape.
         await run({
            query: "run: kids -> { where: label = 'a'; group_by: label; aggregate: n }",
         });
         await run({
            query: "run: kids -> { where: label = 'b'; group_by: label; aggregate: n }",
         });
         // Another question.
         await run({ query: "run: kids -> { aggregate: n }" });
         // A named view.
         await run({ queryName: "by_label", sourceName: "kids" });
         // A caller the lock refuses, and one it admits.
         await run({ query: "run: chart -> { aggregate: n }" }, {
            ROLES: ["viewer"],
         } as never);
         await run({ query: "run: chart -> { aggregate: n }" }, {
            ROLES: ["clinician"],
         } as never);
      });
      await store.flush();
   });

   afterAll(async () => {
      setWorkloadLogSink(undefined);
      info.mockRestore();
      await db.close();
   });

   it("stores one row per query, refusals included", async () => {
      const rows = await store.rows();
      expect(rows).toHaveLength(6);
      expect(rows.map((r) => r.outcome).sort()).toEqual([
         "denied_lock",
         "ok",
         "ok",
         "ok",
         "ok",
         "ok",
      ]);
   });

   it("groups runs that differ only in their values under one shape", async () => {
      const counts = await db.all<{ shape_key: string; n: number | bigint }>(
         `SELECT shape_key, count(*) AS n FROM workload_log
          WHERE source = 'kids' GROUP BY shape_key ORDER BY n DESC, shape_key`,
      );
      expect(counts.map((c) => Number(c.n))).toEqual([2, 1, 1]);
      const [filtered] = (await store.rows()).filter(
         (r) => r.shapeKey === counts[0].shape_key,
      );
      expect(filtered.record.shape).toMatchObject({
         source: "kids",
         dimensions: ["label"],
         measures: ["n"],
         filteredDimensions: ["label"],
         filters: ["=(label,?)"],
      });
   });

   it("names the view a request named", async () => {
      const rows = await store.rows();
      expect(
         rows.find((r) => r.record.shape?.view)?.record.shape,
      ).toMatchObject({
         view: "by_label",
         dimensions: ["label"],
         measures: ["n"],
      });
   });

   it("keeps what the workload is read by beside the shape", async () => {
      const [admitted] = (await store.rows()).filter(
         (r) => r.source === "chart" && r.outcome === "ok",
      );
      expect(admitted).toMatchObject({
         environment: "env",
         package: "pkg",
         modelPath: "m.malloy",
         rowCount: 1,
         servedFrom: null,
      });
      expect(admitted.durationMs).toBeGreaterThanOrEqual(0);
      expect(admitted.record.caller).toBe("alice@example.com");
      // The record as logged, less the #(secure) given values.
      expect(admitted.record.givenNames).toEqual(["ROLES"]);
      expect(admitted.record.secureGivens).toBeUndefined();
   });

   it("stores a refusal without a shape, since nothing compiled for the caller", async () => {
      const [denied] = (await store.rows()).filter(
         (r) => r.outcome === "denied_lock",
      );
      expect(denied.shapeKey).toBeNull();
      expect(denied.record.status).toBe(403);
   });
});

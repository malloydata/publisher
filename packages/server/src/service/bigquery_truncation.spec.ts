// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { BigQueryConnection } from "@malloydata/db-bigquery";
import type {
   Connection,
   MalloyQueryData,
   RunSQLOptions,
} from "@malloydata/malloy";
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { internalErrorToHttpError, ResultTruncatedError } from "../errors";
import {
   assertCompleteBigQueryPage,
   guardBigQueryTruncation,
} from "./bigquery_truncation";
import { buildEnvironmentMalloyConfig } from "./connection";

function page(returned: number, totalRows: number): MalloyQueryData {
   return {
      rows: Array.from({ length: returned }, (_, i) => ({ i })),
      totalRows,
   };
}

/** A connection whose runSQL answers `data` and records the options it got. */
function fakeConnection(data: MalloyQueryData): {
   connection: Connection;
   calls: Array<RunSQLOptions | undefined>;
} {
   const calls: Array<RunSQLOptions | undefined> = [];
   const connection = {
      name: "bq",
      runSQL: async (_sql: string, options?: RunSQLOptions) => {
         calls.push(options);
         return data;
      },
   } as unknown as Connection;
   return { connection, calls };
}

describe("assertCompleteBigQueryPage", () => {
   it("refuses a page cut short of both the row limit and the reported rows", () => {
      // The campaign's shape: limit 99000, one ~10 MB page of 50,000 rows.
      expect(() => assertCompleteBigQueryPage(page(50, 99), 99, 0)).toThrow(
         ResultTruncatedError,
      );
   });

   it("passes a page the row limit cut", () => {
      assertCompleteBigQueryPage(page(10, 99), 10, 0);
   });

   it("passes a page holding every reported row", () => {
      assertCompleteBigQueryPage(page(99, 99), 1000, 0);
   });

   it("counts the reported rows from rowIndex", () => {
      assertCompleteBigQueryPage(page(9, 99), 1000, 90);
      expect(() => assertCompleteBigQueryPage(page(5, 99), 1000, 90)).toThrow(
         ResultTruncatedError,
      );
   });

   it("does not check a run with no row limit, which reads the connector's default page", () => {
      assertCompleteBigQueryPage(page(10, 99), undefined, 0);
   });

   it("passes a response that reports no total", () => {
      assertCompleteBigQueryPage(page(10, 0), 1000, 0);
   });
});

describe("guardBigQueryTruncation", () => {
   it("refuses a short page through runSQL and passes the options on", async () => {
      const { connection, calls } = fakeConnection(page(50, 99));
      guardBigQueryTruncation(connection);
      await expect(
         connection.runSQL("SELECT 1", { rowLimit: 99 }),
      ).rejects.toBeInstanceOf(ResultTruncatedError);
      expect(calls).toEqual([{ rowLimit: 99 }]);
   });

   it("wraps a connection once, however often it is looked up", async () => {
      const { connection, calls } = fakeConnection(page(10, 10));
      guardBigQueryTruncation(connection);
      guardBigQueryTruncation(connection);
      await connection.runSQL("SELECT 1", { rowLimit: 100 });
      expect(calls.length).toBe(1);
   });

   it("answers 502 RESULT_TRUNCATED, naming both counts", () => {
      const http = internalErrorToHttpError(new ResultTruncatedError(50, 99), {
         log: false,
      });
      expect(http.status).toBe(502);
      expect(http.json.reason).toBe("RESULT_TRUNCATED");
      expect(http.json.message).toContain("50 of the 99 rows");
   });
});

describe("an environment's BigQuery connection", () => {
   const stubs: Array<{ mockRestore(): void }> = [];
   afterEach(() => {
      for (const stub of stubs.splice(0)) stub.mockRestore();
   });

   it("refuses a short page from a model query's runSQL", async () => {
      // _runSQL is the connector's one read of the page; stubbed here so no
      // query reaches BigQuery.
      stubs.push(
         spyOn(
            BigQueryConnection.prototype as unknown as {
               _runSQL: () => Promise<{ data: MalloyQueryData }>;
            },
            "_runSQL",
         ).mockResolvedValue({ data: page(50, 99) }),
      );
      const { malloyConfig, releaseConnections } = buildEnvironmentMalloyConfig(
         [
            {
               name: "bq",
               type: "bigquery",
               bigqueryConnection: { defaultProjectId: "test-project" },
            },
         ],
      );
      try {
         const connection =
            await malloyConfig.connections.lookupConnection("bq");
         await expect(
            connection.runSQL("SELECT 1", { rowLimit: 99 }),
         ).rejects.toBeInstanceOf(ResultTruncatedError);
      } finally {
         await releaseConnections();
      }
   });
});

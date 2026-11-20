// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A classified database failure keeps its status on both run paths: a model
 * query and a report cell. A truncated BigQuery page (502 RESULT_TRUNCATED)
 * and a row that does not fit the result's schema (502) must not become the
 * cell path's generic 400.
 */
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import {
   internalErrorToHttpError,
   ResultShapeError,
   ResultTruncatedError,
} from "../errors";
import { queryRows } from "../test_helpers/authorize_fixture";
import { seededDuckdb, writeTempPackage } from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Model, ModelPolicy } from "./model";

const MODEL = `source: t is duckdb.sql("""select {'n': 1, 's': 'a'} as obj""")
`;

const REPORT = `## artifact { layout=linear title="Cells" tiles=["t -> { select: obj }"] }
import "m.malloy"
`;

describe("a classified database failure", () => {
   let model: Model;
   let duckdb: DuckDBConnection;
   let removeDir: () => void;
   let cell = -1;

   beforeAll(async () => {
      const written = writeTempPackage({
         "m.malloy": MODEL,
         "report.malloy": REPORT,
      });
      removeDir = written.cleanup;
      duckdb = await seededDuckdb();
      model = await Model.create(
         "cells-pkg",
         written.dir,
         "report.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
         {
            policy: ModelPolicy.fixed({
               queryBoundary: {
                  mode: "all",
                  exploresDeclared: false,
                  isQueryEntryPoint: true,
                  layout: "linear",
               },
            }),
         },
      );
      expect(
         model.attachServedNotebookCells(model.getCompiledSourceText()!),
      ).toBe("ok");
      const listed = (await model.getNotebook()).notebookCells ?? [];
      cell = listed.findIndex((c) => c.kind === "query");
      expect(cell).toBeGreaterThanOrEqual(0);
   });

   afterAll(async () => {
      await duckdb?.close();
      removeDir?.();
   });

   /** Run `body` with the database's answer replaced by `answer`. */
   async function withDatabase<T>(
      answer: (
         sql: string,
         runSQL: DuckDBConnection["runSQL"],
      ) => Promise<unknown>,
      body: () => Promise<T>,
   ): Promise<unknown> {
      const runSQL = duckdb.runSQL.bind(duckdb);
      const spy = spyOn(duckdb, "runSQL").mockImplementation((async (
         sql: string,
         ...rest: unknown[]
      ) => {
         if (!/obj/.test(sql)) return runSQL(sql, ...(rest as []));
         return answer(sql, runSQL);
      }) as never);
      try {
         await body();
         return undefined;
      } catch (err) {
         return err;
      } finally {
         spy.mockRestore();
      }
   }

   const truncated = () => Promise.reject(new ResultTruncatedError(50, 99));
   const misshapen = async (
      sql: string,
      runSQL: DuckDBConnection["runSQL"],
   ) => {
      const data = await runSQL(sql);
      for (const row of data.rows) {
         (row as { obj: Record<string, unknown> }).obj.s = 5;
      }
      return data;
   };
   const runCell = () => model.executeNotebookCell(unrecordedAudit(), cell);
   const runQuery = () => queryRows(model, "run: t -> { select: obj }");

   function expect502(thrown: unknown, kind: new (...a: never[]) => Error) {
      expect(thrown).toBeInstanceOf(kind);
      expect(
         internalErrorToHttpError(thrown as Error, { log: false }).status,
      ).toBe(502);
   }

   it("keeps a truncated page's 502 RESULT_TRUNCATED on a report cell", async () => {
      const thrown = await withDatabase(truncated, runCell);
      expect502(thrown, ResultTruncatedError);
      expect(
         internalErrorToHttpError(thrown as Error, { log: false }).json.reason,
      ).toBe("RESULT_TRUNCATED");
   });

   it("keeps a truncated page's 502 RESULT_TRUNCATED on a model query", async () => {
      expect502(await withDatabase(truncated, runQuery), ResultTruncatedError);
   });

   it("keeps a misshapen row's 502 on a report cell", async () => {
      expect502(await withDatabase(misshapen, runCell), ResultShapeError);
   });

   it("keeps a misshapen row's 502 on a model query", async () => {
      expect502(await withDatabase(misshapen, runQuery), ResultShapeError);
   });
});

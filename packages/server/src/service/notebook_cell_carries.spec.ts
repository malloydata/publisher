// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A linear report's query cell under a `#(carries)` ceiling: a cell that
 * reads its source past the ceiling is refused, as the same query is, and the
 * refusal writes an access record that says what the cell read. Pins the
 * ceiling on the cell's `govern()` request, which a query and a cell build
 * alike.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import { withAccessRecord, type AccessRecord } from "./access_record";
import { Model, ModelPolicy } from "./model";
import { seededDuckdb, writeTempPackage } from "../test_helpers/temp_package";

const SEED_SQL = `
CREATE OR REPLACE TABLE people (id INTEGER, ssn VARCHAR);
INSERT INTO people VALUES (1, '111'), (2, '222');
`;

const MODEL = `##! experimental { access_modifiers }
source: base is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
  #(classification) "none"
  dimension: pid is id
}
#(carries) ["none"]
source: car is base include { internal: social; public: pid }
`;

const PAST = "car extend { dimension: z is social } -> { select: z }";
const WITHIN = "car -> { select: pid }";

const REPORT = `## artifact { layout=linear title="Cells" tiles=["${PAST}", "${WITHIN}"] }
import "m.malloy"
`;

describe("a linear report's query cell under a #(carries) ceiling", () => {
   let model: Model;
   let duckdb: DuckDBConnection;
   let removeDir: () => void;
   let past = -1;
   let within = -1;
   let info: ReturnType<typeof spyOn>;

   beforeEach(() => {
      info = spyOn(logger, "info").mockImplementation(() => logger);
   });
   afterEach(() => {
      info.mockRestore();
   });

   beforeAll(async () => {
      const written = writeTempPackage({
         "m.malloy": MODEL,
         "report.malloy": REPORT,
      });
      removeDir = written.cleanup;
      duckdb = await seededDuckdb(SEED_SQL);
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
      past = listed.findIndex((c) => c.text === `run: ${PAST}`);
      within = listed.findIndex((c) => c.text === `run: ${WITHIN}`);
      expect(past).toBeGreaterThanOrEqual(0);
      expect(within).toBeGreaterThanOrEqual(0);
   });

   afterAll(async () => {
      await duckdb?.close();
      removeDir?.();
   });

   const run = (index: number) =>
      withAccessRecord(
         {
            environment: "env",
            package: "cells-pkg",
            modelPath: "report.malloy",
            adHoc: false,
            caller: "c",
         },
         undefined,
         (audit) => model.executeNotebookCell(audit, index),
      );

   /** The one access record the last cell run wrote. */
   function lastRecord(): AccessRecord {
      const records = (info.mock.calls as unknown[][])
         .filter(([message]) => message === "Access record")
         .map(([, record]) => record as AccessRecord);
      expect(records.length).toBeGreaterThan(0);
      return records[records.length - 1];
   }

   it("refuses a cell that reads its source past the ceiling, and records the refusal", async () => {
      const refused = await run(past).then(
         () => undefined,
         (err: unknown) => err,
      );
      expect(refused).toBeInstanceOf(BadRequestError);
      expect((refused as Error).message).toContain("not on the list");
      const record = lastRecord();
      expect(record.outcome).not.toBe("ok");
      expect(record.status).toBe(400);
   });

   it("answers a cell that stays within the ceiling", async () => {
      await run(within);
      expect(lastRecord().outcome).toBe("ok");
   });
});

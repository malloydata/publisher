// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../logger";
import {
   INTROSPECTION_ROW_LIMIT,
   runIntrospectionSQL,
} from "./introspection_sql";

/** A connection whose runSQL answers `result` (or rejects with it). */
function connectionAnswering(result: unknown, rejects = false) {
   const runSQL = rejects
      ? sinon.stub().rejects(result)
      : sinon.stub().resolves(result);
   return { connection: { runSQL } as unknown as Connection, runSQL };
}

const rows = (n: number) => Array.from({ length: n }, (_, i) => ({ i }));

describe("runIntrospectionSQL", () => {
   let warn: sinon.SinonStub;
   beforeEach(() => {
      warn = sinon.stub(logger, "warn");
   });
   afterEach(() => warn.restore());
   /**
    * This module's own warnings: the stub is the shared logger's, so a
    * timer another spec file left running may log into it too.
    */
   const rowCapWarnings = () =>
      warn.getCalls().filter((c) => /row cap/.test(String(c.args[0])));

   it("runs the query with the introspection row cap rather than the driver's default, and returns its result", async () => {
      // The driver default is 10 rows on DuckDB, BigQuery and Trino, which
      // silently truncated schemas; the cap must be passed explicitly.
      const result = { rows: rows(3), totalRows: 3 };
      const { connection, runSQL } = connectionAnswering(result);

      expect(await runIntrospectionSQL(connection, "SELECT * FROM cols")).toBe(
         result,
      );
      expect(runSQL.firstCall.args).toEqual([
         "SELECT * FROM cols",
         { rowLimit: INTROSPECTION_ROW_LIMIT },
      ]);
      expect(rowCapWarnings()).toEqual([]);
   });

   it.each([
      ["{rows}", { rows: rows(INTROSPECTION_ROW_LIMIT) }],
      ["bare array", rows(INTROSPECTION_ROW_LIMIT)],
   ])(
      "warns that the result may be truncated when a %s result lands on the cap",
      async (_shape, result) => {
         const { connection } = connectionAnswering(result);

         await runIntrospectionSQL(connection, "SELECT 1");

         expect(rowCapWarnings()).toHaveLength(1);
      },
   );

   it("propagates a failed query unchanged", async () => {
      const failure = new Error("Catalog Error: Table does not exist");
      const { connection } = connectionAnswering(failure, true);

      await expect(runIntrospectionSQL(connection, "SELECT 1")).rejects.toBe(
         failure,
      );
      expect(rowCapWarnings()).toEqual([]);
   });
});

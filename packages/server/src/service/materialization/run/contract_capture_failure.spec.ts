// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database build whose column contract does not read back: the read runs
 * over the staging table before the swap, so a failure drops the staging
 * table and leaves the served table as it was, and the source fails with
 * `contract_capture_failed` (retryable). The `storage=` transaction and the
 * delta placements are pinned in
 * tests/component/materialization_ducklake_lifecycle.component.spec.ts.
 */

import { Manifest } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { ContractCaptureFailedError } from "../../../errors";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import { classifyBuildError } from "./build_failure_class";

describe("a database build whose column contract does not read back", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function build(runSQL: sinon.SinonStub): Promise<unknown> {
      return (
         ctx.service.collaborators as unknown as {
            buildOneSource: (options: unknown) => Promise<unknown>;
         }
      ).buildOneSource({
         persistSource: fakeSource({
            name: "orders",
            sourceEntityId: "abcdef1234567890",
            sql: "SELECT * FROM t",
            dialectName: "duckdb",
         }),
         instruction: {
            sourceEntityId: "abcdef1234567890",
            materializedTableId: "mt-1",
            physicalTableName: "orders_v1",
            realization: "COPY",
         },
         connection: { runSQL, dialectName: "duckdb" },
         manifest: new Manifest(),
         environment: {
            getApiConnection: () => {
               throw new Error("no connection config in this database test");
            },
            getEnvironmentName: () => "env",
            getEnvironmentPath: () => "/tmp/env",
         },
         builtEntries: {},
      });
   }

   it("drops the staging table and never swaps, failing the source contract_capture_failed", async () => {
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (sql.startsWith("DESCRIBE")) {
            throw new Error("injected: catalog read failed");
         }
         return { rows: [] };
      });
      let err: unknown;
      try {
         await build(runSQL);
      } catch (e) {
         err = e;
      }
      expect(err).toBeInstanceOf(ContractCaptureFailedError);
      expect((err as ContractCaptureFailedError).code).toBe(
         "contract_capture_failed",
      );
      expect(String((err as Error).message)).toContain(
         "The column contract of the table orders_v1 could not be read back (its columns did not read",
      );
      expect(classifyBuildError(err)).toBe(true);
      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql[0]).toBe('DROP TABLE IF EXISTS "orders_v1_abcdef123456"');
      expect(sql[1]).toBe(
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
      );
      // The capture read the staging table, not the served one.
      expect(sql).toContain('DESCRIBE "orders_v1_abcdef123456"');
      // The staging table dropped; the served table never touched.
      expect(sql[sql.length - 1]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
      expect(sql.some((s) => /\bRENAME\b|"orders_v1";/.test(s))).toBe(false);
   });
});

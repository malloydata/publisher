// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The Trino row of the capability table (`domain/dialect.ts`), observed on a
 * real server's `memory` catalog: each test runs what the capability
 * promises against the engine, through the statements Publisher sends where
 * they exist, and asserts the observation matches `CAPABILITIES.trino`. A
 * test name here is a probe reference in `domain/dialect_evidence.ts`;
 * renaming one fails `dialect_evidence.spec.ts`.
 *
 * Opt-in: set PUBLISHER_TEST_TRINO_URL (`http://host:port`) to a server whose
 * `memory` catalog the test may create and drop a schema in (CI's
 * connection-integration job).
 */

import { TrinoConnection } from "@malloydata/db-trino";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import { CAPABILITIES } from "../../src/service/materialization/domain/dialect";
import {
   ownedTableProbeSql,
   relationProbeSql,
   tableCommentSql,
} from "../../src/service/materialization/run/warehouse_sql";

const url = process.env.PUBLISHER_TEST_TRINO_URL;
setDefaultTimeout(120_000);

const CAN = CAPABILITIES.trino;

describe.skipIf(!url)("Trino conformance", () => {
   const schema = `conf_${Math.random().toString(36).slice(2, 8)}`;
   let trino: TrinoConnection;
   const t = (name: string) => `memory.${schema}.${name}`;

   async function rows(sql: string): Promise<Record<string, unknown>[]> {
      return (await trino.runSQL(sql)).rows as Record<string, unknown>[];
   }
   async function attempt(sql: string): Promise<string | undefined> {
      try {
         await trino.runSQL(sql);
         return undefined;
      } catch (err) {
         return err instanceof Error ? err.message : String(err);
      }
   }

   beforeAll(async () => {
      trino = new TrinoConnection("trino", undefined, {
         server: url,
         catalog: "memory",
         schema: "default",
         user: "conformance",
      });
      await trino.runSQL(`CREATE SCHEMA memory.${schema}`);
   });

   afterAll(async () => {
      for (const name of ["c", "o", "folded"]) {
         await attempt(`DROP TABLE IF EXISTS ${t(name)}`);
      }
      await attempt(`DROP SCHEMA IF EXISTS memory.${schema}`);
      await trino?.close();
   });

   it("relationProbe: a table's comment reads back through the relation probe", async () => {
      await trino.runSQL(`CREATE TABLE ${t("c")} AS SELECT 1 AS v`);
      await trino.runSQL(
         tableCommentSql(["memory", schema, "c"], "it's marked", "trino"),
      );
      const found = await rows(relationProbeSql([schema, "c"], "trino"));
      expect(found).toEqual([
         expect.objectContaining({
            kind: "BASE TABLE",
            comment: "it's marked",
         }),
      ]);
      const none = await rows(relationProbeSql([schema, "nope"], "trino"));
      expect(none).toEqual([]);
      expect(found.length === 1 && none.length === 0).toBe(CAN.relationProbe);
   });

   it("tableOwnership: the ownership mark's comment reads back with its location", async () => {
      await trino.runSQL(`CREATE TABLE ${t("o")} AS SELECT 1 AS v`);
      await trino.runSQL(
         tableCommentSql(["memory", schema, "o"], "owner=e/p", "trino"),
      );
      const found = await rows(
         ownedTableProbeSql(["memory", schema, "o"], "trino"),
      );
      expect(found).toEqual([
         expect.objectContaining({
            comment: "owner=e/p",
            location_database: "memory",
            location_schema: schema,
            location_name: "o",
         }),
      ]);
      expect(found.length === 1).toBe(CAN.tableOwnership);
   });

   it("castsToIntegerType: CAST AS BIGINT", async () => {
      const [row] = await rows(`SELECT CAST(41.9 AS bigint) AS i`);
      expect(Number(row?.["i"])).toBe(42);
      expect(row !== undefined).toBe(CAN.castsToIntegerType);
   });

   it("foldsTableNameCase: a quoted name resolves case-insensitively", async () => {
      await trino.runSQL(
         `CREATE TABLE memory.${schema}."Folded" AS SELECT 1 AS v`,
      );
      const found = await attempt(`SELECT v FROM memory.${schema}."FOLDED"`);
      expect(found === undefined).toBe(CAN.foldsTableNameCase);
   });
});

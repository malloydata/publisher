// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The MySQL row of the capability table (`domain/dialect.ts`), observed on a
 * real server: each test runs what the capability promises against the
 * engine, through the statements Publisher sends where they exist, and
 * asserts the observation matches `CAPABILITIES.mysql`. A test name here is
 * a probe reference in `domain/dialect_evidence.ts`; renaming one fails
 * `dialect_evidence.spec.ts`.
 *
 * Opt-in: set PUBLISHER_TEST_MYSQL_URL (`mysql://user:password@host:port`)
 * to a server the test may create and drop a database and a user on (CI's
 * connection-integration job).
 */

import { MySQLConnection } from "@malloydata/db-mysql";
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
   tableCommentSql,
} from "../../src/service/materialization/run/warehouse_sql";

const url = process.env.PUBLISHER_TEST_MYSQL_URL;
setDefaultTimeout(120_000);

const CAN = CAPABILITIES.mysql;

describe.skipIf(!url)("MySQL conformance", () => {
   const suffix = Math.random().toString(36).slice(2, 8);
   const db = `conf_${suffix}`;
   const user = `conf_u_${suffix}`;
   const password = `pw_${suffix}`;
   let admin: MySQLConnection;
   const config = () => {
      const u = new URL(url!);
      return {
         host: u.hostname,
         port: Number(u.port || 3306),
         user: decodeURIComponent(u.username),
         password: decodeURIComponent(u.password),
      };
   };

   async function rows(
      conn: MySQLConnection,
      sql: string,
   ): Promise<Record<string, unknown>[]> {
      return (await conn.runSQL(sql)).rows as Record<string, unknown>[];
   }
   async function attempt(sql: string): Promise<string | undefined> {
      try {
         await admin.runSQL(sql);
         return undefined;
      } catch (err) {
         return err instanceof Error ? err.message : String(err);
      }
   }

   beforeAll(async () => {
      admin = new MySQLConnection("admin", config());
      await admin.runSQL(`CREATE DATABASE ${db}`);
   });

   afterAll(async () => {
      await attempt(`DROP USER IF EXISTS '${user}'@'%'`);
      await attempt(`DROP DATABASE IF EXISTS ${db}`);
      await admin?.close();
   });

   it("tableOwnership: the ownership mark's comment reads back with its location", async () => {
      await admin.runSQL(`CREATE TABLE ${db}.o AS SELECT 1 AS v`);
      await admin.runSQL(tableCommentSql([db, "o"], "owner=e/p", "mysql"));
      const found = await rows(admin, ownedTableProbeSql([db, "o"], "mysql"));
      expect(found).toEqual([
         expect.objectContaining({
            comment: "owner=e/p",
            location_schema: db,
            location_name: "o",
         }),
      ]);
      const none = await rows(admin, ownedTableProbeSql([db, "nope"], "mysql"));
      expect(none).toEqual([]);
      expect(found.length === 1 && none.length === 0).toBe(CAN.tableOwnership);
   });

   it("catalogHidesTables: a table the session's grants do not cover is missing from its catalog", async () => {
      await admin.runSQL(`CREATE TABLE ${db}.shown AS SELECT 1 AS v`);
      await admin.runSQL(`CREATE TABLE ${db}.hidden AS SELECT 1 AS v`);
      await admin.runSQL(
         `CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'`,
      );
      await admin.runSQL(`GRANT SELECT ON ${db}.shown TO '${user}'@'%'`);
      const limited = new MySQLConnection("limited", {
         ...config(),
         user,
         password,
      });
      try {
         const probe = async (name: string) =>
            (await rows(limited, ownedTableProbeSql([db, name], "mysql")))
               .length;
         const shown = await probe("shown");
         const hidden = await probe("hidden");
         // The table exists: the administrator's catalog lists it.
         expect(
            (await rows(admin, ownedTableProbeSql([db, "hidden"], "mysql")))
               .length,
         ).toBe(1);
         expect(shown).toBe(1);
         expect(hidden === 0).toBe(CAN.catalogHidesTables);
      } finally {
         await limited.close();
      }
   });

   it("castsToIntegerType: CAST names no integer type", async () => {
      const refused = await attempt(`SELECT CAST(41.9 AS bigint) AS i`);
      // What it takes instead.
      const [row] = await rows(admin, `SELECT CAST(41.9 AS SIGNED) AS i`);
      expect(Number(row?.["i"])).toBe(42);
      expect(refused === undefined).toBe(CAN.castsToIntegerType);
   });
});

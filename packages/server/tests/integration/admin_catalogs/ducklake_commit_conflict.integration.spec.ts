// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the pinned DuckLake does when two instances (two replicas) commit
 * against one catalog database: the reason a table load takes an advisory lock
 * instead of trusting the engine. Same environment as the admin integration
 * spec; skipped without it.
 */
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { CatalogAdmin } from "../../../src/service/catalog_admin";
import { attachDuckLakeReadWrite } from "../../../src/service/connection";
import { createIsolatedBuildSession } from "../../../src/service/materialization/run/materialization_build_session";
import { SUPERUSER, superuser } from "./superuser";

const DB = `ducklake_conflict_${process.pid}`;
const CATALOG = "conflict_lake";
/** DuckLake's own default for `ducklake_max_retry_count`. */
const DEFAULT_RETRIES = 10;

async function count(session: DuckDBConnection, sql: string): Promise<number> {
   const r = (await session.runSQL(sql)) as unknown as {
      rows: { n: unknown }[];
   };
   return Number(r.rows[0].n);
}

async function commitOutcome(session: DuckDBConnection): Promise<string> {
   return session.runSQL("COMMIT").then(
      () => "committed",
      (error: Error) => error.message,
   );
}

describe.skipIf(!SUPERUSER)(
   "DuckLake commit conflicts across instances",
   () => {
      const root = mkdtempSync(join(tmpdir(), "ducklake-conflict-"));
      const disposers: (() => Promise<void>)[] = [];
      const conninfo = `${SUPERUSER} dbname=${DB}`;

      /** A fresh instance on the catalog, as a separate replica would be. */
      async function instance(
         name: string,
         retries: number,
      ): Promise<DuckDBConnection> {
         const { session, dispose } = createIsolatedBuildSession(name);
         disposers.push(dispose);
         await attachDuckLakeReadWrite(session, CATALOG, {
            catalog: {
               postgresConnection: { connectionString: conninfo },
               metadataSchema: CATALOG,
            },
         } as Parameters<typeof attachDuckLakeReadWrite>[2]);
         await session.runSQL(`SET ducklake_max_retry_count = ${retries}`);
         return session;
      }

      /** Both transactions delete-then-insert the same new key; A commits first. */
      async function sameNewKey(
         id: number,
         retries: number,
      ): Promise<{ second: string; rows: number }> {
         const a = await instance(`conflict_a_${id}`, retries);
         const b = await instance(`conflict_b_${id}`, retries);
         for (const [session, label] of [
            [a, "from_a"],
            [b, "from_b"],
         ] as const) {
            await session.runSQL("BEGIN");
            await session.runSQL(
               `DELETE FROM ${CATALOG}.s1.t WHERE id IN (${id})`,
            );
            await session.runSQL(
               `INSERT INTO ${CATALOG}.s1.t VALUES (${id}, '${label}')`,
            );
         }
         expect(await commitOutcome(a)).toBe("committed");
         const second = await commitOutcome(b);
         return {
            second,
            rows: await count(
               a,
               `SELECT count(*) AS n FROM ${CATALOG}.s1.t WHERE id = ${id}`,
            ),
         };
      }

      beforeAll(async () => {
         await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
         await superuser(`CREATE DATABASE ${DB}`);
         await new CatalogAdmin({ catalogDatabase: conninfo }).createCatalog({
            name: CATALOG,
            dataPath: join(root, "data"),
            encrypted: false,
            dataInliningRowLimit: 0,
            schemas: ["s1", "s2"],
         });
         const setup = await instance("conflict_setup", DEFAULT_RETRIES);
         await setup.runSQL(
            `CREATE TABLE ${CATALOG}.s1.t (id INTEGER, v VARCHAR)`,
         );
         await setup.runSQL(`CREATE TABLE ${CATALOG}.s2.t2 (id INTEGER)`);
      });

      afterAll(async () => {
         for (const dispose of disposers) await dispose();
         rmSync(root, { recursive: true, force: true });
         await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
      });

      it("with the engine's retry on (its default), both commits of the same new key succeed and the key is duplicated", async () => {
         const { second, rows } = await sameNewKey(10, DEFAULT_RETRIES);
         // Observed: the engine's conflict check is per table and operation, not per key.
         expect(second).toBe("committed");
         expect(rows).toBe(2);
      });

      it("with retry off, the later commit fails only on the snapshot id the earlier one took", async () => {
         const { second, rows } = await sameNewKey(20, 0);
         // Observed: not a key check; any two overlapping commits collide, so retry off would refuse every concurrent load.
         expect(second).toContain("ducklake_snapshot_pkey");
         expect(rows).toBe(1);
      });

      it("fails an insert into a table whose schema another instance dropped", async () => {
         const inserter = await instance("conflict_drop_b", DEFAULT_RETRIES);
         const dropper = await instance("conflict_drop_a", DEFAULT_RETRIES);
         await inserter.runSQL("BEGIN");
         await inserter.runSQL(`INSERT INTO ${CATALOG}.s2.t2 VALUES (1)`);
         await dropper.runSQL(`DROP SCHEMA ${CATALOG}.s2 CASCADE`);
         // Observed: DuckLake detects a dropped table, so a schema drop beats an in-flight insert.
         expect(await commitOutcome(inserter)).toContain(
            "but another transaction has dropped it",
         );
      });
   },
);

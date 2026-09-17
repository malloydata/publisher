// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The OPERATOR's own DuckLake client — deliberately EXTERNAL to the publisher.
// A real orchestrator provisions the catalog (CREATE SCHEMA, grants, …) through
// its own read-write DuckLake connection, NOT through anything the publisher
// exposes: the publisher's serve/sqlQuery attach is read-only by design, and no
// publisher endpoint offers read-write DDL on a storage destination. This client
// stands in for that orchestrator: it opens its own DuckDB session and attaches
// the same physical catalog + storage read-write, independently of the connection
// the publisher is configured with. Nothing here goes through the publisher.

import { mkdtempSync, rmSync } from "fs";
import os from "os";
import path from "path";
import { DuckDBConnection } from "@malloydata/db-duckdb";

export interface LakeAttach {
   host: string;
   port: number;
   user: string;
   password: string;
   catalogDb: string;
   storageDir: string;
}

/**
 * Attach the DuckLake read-write and run one or more `;`-separated statements,
 * returning the LAST statement's rows.
 *
 * Rows come back because provisioning and asserting are the same act from an
 * operator's side: the thing that can `CREATE SCHEMA` on a destination is the
 * thing that can look at what the tier wrote there. Returning nothing would make
 * an `Expect:` on such a step compare against an empty set, which passes or
 * fails for reasons unrelated to the rule under test.
 *
 * The LAST statement's, so a multi-statement body may set up and then read —
 * every earlier statement runs for its effect, as before.
 */
export async function runLakeSql(
   attach: LakeAttach,
   sql: string,
): Promise<Record<string, string>[]> {
   const wd = mkdtempSync(path.join(os.tmpdir(), "hammer-lake-op-"));
   const conn = new DuckDBConnection("operator", ":memory:", wd);
   try {
      await conn.runSQL(
         "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
      );
      const cs = `host=${attach.host} port=${attach.port} dbname=${attach.catalogDb} user=${attach.user} password=${attach.password}`;
      await conn.runSQL(
         `ATTACH 'ducklake:postgres:${cs}' AS lake (DATA_PATH '${attach.storageDir}', OVERRIDE_DATA_PATH true)`,
      );
      // Make `lake` the default catalog so unqualified DDL (e.g. CREATE SCHEMA
      // analytics) targets the lake, not the session's :memory: primary.
      await conn.runSQL("USE lake");
      let rows: Record<string, string>[] = [];
      for (const stmt of sql
         .split(";")
         .map((s) => s.trim())
         .filter(Boolean)) {
         const result = await conn.runSQL(stmt);
         rows = (Array.isArray(result) ? result : (result?.rows ?? [])) as Record<
            string,
            string
         >[];
      }
      return rows;
   } finally {
      await conn.close();
      rmSync(wd, { recursive: true, force: true });
   }
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The storage delta path executed against a real DuckDB destination, in DuckDB's
// OWN dialect — the engine a `storage=` destination's DML is actually issued in.
//
// Everything else that covers this path stops short of the engine: the unit specs
// drive a fake session that pattern-matches SQL and returns fabricated rows, and
// incremental_dml_semantics.spec.ts does execute but renders the Postgres
// spelling. Both bugs this file pins were invisible to that arrangement, because
// each is a property of the driver and the engine rather than of the SQL text:
// a probe truncated by a row limit, and a session left unusable by a failed
// script. A table is ATTACHed rather than created in the default catalog so the
// probe has to resolve a catalog the way it does against a real destination.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   applyDeltaScript,
   deltaStatements,
   probeTargetColumns,
} from "./incremental_apply";
import { createIsolatedBuildSession } from "./materialization_build_session";

// The PRODUCTION session factory, not a local stand-in: its query options are
// half of what these tests are about, so building a session here by hand would
// pass while the real one truncated.
let session: DuckDBConnection;
let dispose: () => Promise<void>;
const DEST = "dest";
let n = 0;

beforeAll(async () => {
   ({ session, dispose } = createIsolatedBuildSession("storage-engine-spec"));
   await session.runSQL(`ATTACH ':memory:' AS ${DEST}`);
});

afterAll(async () => {
   await dispose();
});

/** A fresh table in the attached destination; `:memory:` is process-shared. */
async function wideTable(columns: number): Promise<string> {
   const name = `t${n++}`;
   const cols = Array.from({ length: columns }, (_, i) => `c${i} INTEGER`);
   await session.runSQL(
      `CREATE TABLE ${DEST}.${name} (wm INTEGER, ${cols.join(", ")})`,
   );
   return name;
}

const runner = (sql: string) => session.runSQL(sql);

describe("probing a stored table's shape", () => {
   it("reports every column of a table wider than the driver's row limit", async () => {
      // db-duckdb's DEFAULT_QUERY_OPTIONS caps runSQL at 10 rows, and
      // duckdb_columns() returns one row per COLUMN. Unlifted, an 11-column
      // table's shape comes back short, the planner calls that a shape mismatch,
      // and the source rebuilds in full on every refresh — the exact cost the
      // delta path exists to avoid.
      const name = await wideTable(14); // 15 columns with the watermark
      const probe = await probeTargetColumns(
         runner,
         "duckdb",
         `${DEST}.${name}`,
      );
      expect(probe.error).toBeUndefined();
      expect(probe.columns.length).toBe(15);
      expect(probe.columns).toContain("c13");
   });

   it("answers emptily for a table the destination does not hold", async () => {
      // The distinction the planner's table_unreadable vs no-boundary rests on.
      const probe = await probeTargetColumns(
         runner,
         "duckdb",
         `${DEST}.never_created`,
      );
      expect(probe.columns).toEqual([]);
   });
});

describe("applying a delta in DuckDB's own dialect", () => {
   it("replaces exactly the half-open range and leaves the rest", async () => {
      const name = await wideTable(1);
      const t = `${DEST}.${name}`;
      await session.runSQL(
         `INSERT INTO ${t} VALUES (1,10),(2,20),(3,30),(4,40)`,
      );
      const statements = deltaStatements({
         dialect: "duckdb",
         quotedTablePath: t,
         columns: ["wm", "c0"],
         watermarkName: "wm",
         start: { malloyType: "number", value: "2" },
         end: { malloyType: "number", value: "4" },
         mergeKeys: [],
         deltaSQL: `SELECT 2 AS wm, 222 AS c0 UNION ALL SELECT 3 AS wm, 333 AS c0`,
      });
      await applyDeltaScript(runner, "duckdb", statements);
      const rows = await session.runSQL(`SELECT wm, c0 FROM ${t} ORDER BY wm`);
      // 1 and 4 untouched (4 is the excluded end), 2 and 3 replaced.
      expect((rows.rows ?? []).map((r) => `${r.wm}:${r.c0}`)).toEqual([
         "1:10",
         "2:222",
         "3:333",
         "4:40",
      ]);
   });

   it("leaves the session usable after a failed script, having applied nothing", async () => {
      // DuckDB does NOT abandon a failed script's transaction by itself: once
      // BEGIN has opened one, every later statement is refused with "Current
      // transaction is aborted" until something rolls back. So the rollback
      // applyDeltaScript issues is what keeps the session — and, if this session
      // ever becomes reusable, the build — alive.
      const name = await wideTable(1);
      const t = `${DEST}.${name}`;
      await session.runSQL(`INSERT INTO ${t} VALUES (1,10),(2,20)`);
      const statements = deltaStatements({
         dialect: "duckdb",
         quotedTablePath: t,
         columns: ["wm", "c0"],
         watermarkName: "wm",
         start: { malloyType: "number", value: "1" },
         end: { malloyType: "number", value: "3" },
         mergeKeys: [],
         // Fails at RUN time, after BEGIN has opened the transaction — the case
         // a bind-time failure does not reach.
         deltaSQL: `SELECT 1 AS wm, (1/0)::INTEGER AS c0`,
      });
      await expect(
         applyDeltaScript(runner, "duckdb", statements),
      ).rejects.toThrow();

      // The session still answers, which is the regression this pins.
      const after = await session.runSQL(`SELECT count(*) AS n FROM ${t}`);
      expect(String((after.rows ?? [])[0]?.n)).toBe("2");
   });
});

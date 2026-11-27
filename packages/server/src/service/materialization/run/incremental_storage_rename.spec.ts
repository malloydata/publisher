// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental `storage=` source that renames a column (`rename: od is
 * order_date`) stores it under the name its build SQL emits (`order_date`).
 * Every piece of SQL a refresh writes names it that way too: the delta's range
 * predicate and frontier probe over the build SQL, the DELETE, the MERGE's key
 * match and the INSERT's column list on the stored table, and the seed's
 * boundary read. Driven end to end through `storageRefreshFor` against a real
 * DuckDB destination, the source warehouse stood in for by the same DuckDB
 * (the passthrough is unwrapped), so the refreshed table can be compared with
 * a recompute.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { IsolatedSessionConnection } from "./materialization_build_session";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type PersistSource,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { logger } from "../../../logger";
import {
   buildSQLOf,
   plannedSources,
   projectToStoredColumns,
} from "../plan/build_plan";
import type { BuildInstruction, IncrementalStrategy } from "../domain/types";
import type { IncrementalLineage } from "./incremental_apply";
import {
   indexCallerLedger,
   storageRefreshFor,
   type IncrementalRunContext,
} from "./incremental_build";

const dir = mkdtempSync(join(tmpdir(), "storage-rename-"));
const duckdb = new DuckDBConnection("duckdb", join(dir, "w.duckdb"));
let n = 0;

beforeAll(async () => {
   await duckdb.runSQL(`ATTACH '${join(dir, "lake.duckdb")}' AS lake`);
});
afterAll(async () => {
   await duckdb.close();
   rmSync(dir, { recursive: true, force: true });
});

async function compile(model: string): Promise<PersistSource> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([["file:///m.malloy", model]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const compiled = await runtime
      .loadModel(new URL("file:///m.malloy"))
      .getModel();
   const [only] = plannedSources(await runtime.getBuildTargets(compiled));
   return only;
}

/**
 * The build session: every statement runs on the DuckDB that holds both the
 * source table and the destination, the passthrough unwrapped to its SQL.
 */
const session = {
   runSQL: (sql: string) =>
      duckdb.runSQL(
         sql.replace(
            /postgres_query\('h', '((?:[^']|'')*)'\)/g,
            (_m, inner: string) => `(${inner.replace(/''/g, "'")})`,
         ),
         { rowLimit: 100_000 },
      ),
} as unknown as IsolatedSessionConnection;

interface Case {
   /** The source's columns, its query and its `extend` (renames). */
   columns: string;
   extend: string;
   watermark: { name: string; type: "date" | "number" };
   mergeKeys?: string[];
   /** The rows the seed held, the rows that arrive after, as VALUES. */
   seeded: string;
   arrived: string;
   coveredThrough: string;
   orderBy: string;
   /**
    * The recompute's rows a refresh reaches, when not all: a number range is
    * half-open, so rows at the frontier wait for the next refresh.
    */
   reached?: string;
}

/**
 * Seed the stored table from the build's own SQL over `seeded`, add
 * `arrived` to the source, refresh, and return what the refresh did beside
 * the table's rows and a recompute's.
 */
async function refresh(c: Case) {
   const table = `ev_${n++}`;
   await duckdb.runSQL(
      `CREATE TABLE ${table} AS SELECT * FROM (VALUES ${c.seeded}) AS t(${c.columns})`,
   );
   const source = await compile(`##! experimental.persistence
source: base is duckdb.table('${table}')
#@ persist name="t" storage="lake"
source: t is base -> { select: * } extend { ${c.extend} }
`);
   const buildSQL = buildSQLOf(source, { entries: {} });
   const stored = projectToStoredColumns(source, buildSQL);
   const physical = `t_${table}`;
   const quoted = `"lake"."${physical}"`;
   await duckdb.runSQL(`CREATE TABLE ${quoted} AS ${stored}`);
   await duckdb.runSQL(
      `INSERT INTO ${table} SELECT * FROM (VALUES ${c.arrived}) AS t(${c.columns})`,
   );
   const strategy: IncrementalStrategy = c.mergeKeys?.length
      ? "merge"
      : "range_replace";
   const lineage: IncrementalLineage = {
      physicalTableName: physical,
      connectionName: "duckdb",
      storageDestinationName: "lake",
      sourceEntityId: "addr",
      watermarkName: c.watermark.name,
      watermarkType: c.watermark.type,
      mergeKeys: c.mergeKeys ?? [],
      scopeColumns: [],
      strategy,
   };
   const context: IncrementalRunContext = {
      environmentId: "env",
      packageName: "pkg",
      materializationId: "m1",
      forceRefresh: false,
      now: new Date("2024-06-15T00:00:00Z"),
      declarations: {},
      ledger: {} as IncrementalRunContext["ledger"],
      callerLedger: indexCallerLedger([
         {
            sourceEntityId: "addr",
            coveredThrough: c.coveredThrough,
            coveredThroughType: c.watermark.type,
            watermark: c.watermark.name,
            mergeKeys: c.mergeKeys ?? [],
            strategy,
            physicalTableName: physical,
            connectionName: "duckdb",
            storageDestinationName: "lake",
         } as unknown as Parameters<typeof indexCallerLedger>[0][number],
      ]),
   };
   const warn = spyOn(logger, "warn");
   try {
      const result = await storageRefreshFor({
         context,
         lineage,
         persistSource: source,
         instruction: { sourceEntityId: "addr" } as BuildInstruction,
         destinationName: "lake",
         physicalTableName: physical,
         buildSQL,
      }).plan({
         session,
         sourceType: "postgres",
         handle: "h",
         quotedTablePath: quoted,
      });
      const seeded = (warn.mock.calls as unknown[][]).find(
         ([message]) => message === "Rebuilding an incremental source in full",
      );
      const rows = async (sql: string) =>
         (await duckdb.runSQL(`${sql} ORDER BY ${c.orderBy}`)).rows;
      return {
         result,
         seed: seeded?.[1] as { reasonCode?: string } | undefined,
         table: await rows(`SELECT * FROM ${quoted}`),
         recompute: await rows(
            `SELECT * FROM (${stored}) AS r${c.reached ? ` WHERE ${c.reached}` : ""}`,
         ),
      };
   } finally {
      warn.mockRestore();
   }
}

describe("an incremental storage= refresh of a source with a rename:", () => {
   it("applies a delta when the date watermark is renamed, equal to a recompute", async () => {
      const out = await refresh({
         columns: "id, order_date, amt",
         extend: "rename: od is order_date",
         watermark: { name: "od", type: "date" },
         seeded: "(1, DATE '2024-05-20', 1.0), (2, DATE '2024-05-25', 2.0)",
         arrived: "(3, DATE '2024-06-05', 3.0), (4, DATE '2024-06-10', 4.0)",
         coveredThrough: "2024-06-01",
         orderBy: "id",
      });
      expect(out.seed).toBeUndefined();
      expect(out.result?.refresh).toBe("delta");
      expect(out.table).toHaveLength(4);
      expect(out.table).toEqual(out.recompute);
   });

   it("applies a delta when the numeric watermark is renamed, equal to a recompute", async () => {
      const out = await refresh({
         columns: "seq, amt",
         extend: "rename: s is seq",
         watermark: { name: "s", type: "number" },
         seeded: "(1, 1.0), (2, 2.0), (3, 3.0)",
         arrived: "(4, 4.0), (5, 5.0)",
         coveredThrough: "3",
         orderBy: "seq",
         // The frontier, 5, is left for the next refresh (half-open range).
         reached: "seq < 5",
      });
      expect(out.seed).toBeUndefined();
      expect(out.result?.refresh).toBe("delta");
      expect(out.table).toHaveLength(4);
      expect(out.table).toEqual(out.recompute);
   });

   it("merges by a renamed key, equal to a recompute", async () => {
      // The MERGE matches on the stored key column (`id`), not `k`.
      const out = await refresh({
         columns: "id, updated_at, amt",
         extend: "rename: k is id",
         watermark: { name: "updated_at", type: "date" },
         mergeKeys: ["k"],
         seeded: "(1, DATE '2024-05-20', 1.0), (2, DATE '2024-05-25', 2.0)",
         arrived: "(3, DATE '2024-06-05', 3.0)",
         coveredThrough: "2024-06-01",
         orderBy: "id, updated_at",
      });
      expect(out.seed).toBeUndefined();
      expect(out.result?.refresh).toBe("delta");
      expect(out.table).toEqual(out.recompute);
   });

   it("is unchanged for a source with no rename", async () => {
      const out = await refresh({
         columns: "id, order_date, amt",
         extend: "measure: total is amt.sum()",
         watermark: { name: "order_date", type: "date" },
         seeded: "(1, DATE '2024-05-20', 1.0)",
         arrived: "(2, DATE '2024-06-05', 2.0)",
         coveredThrough: "2024-06-01",
         orderBy: "id",
      });
      expect(out.seed).toBeUndefined();
      expect(out.result?.refresh).toBe("delta");
      expect(out.table).toEqual(out.recompute);
   });
});

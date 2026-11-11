// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4, "the same result": the differential harness for materialization.
 *
 * A scenario is model files, seed SQL and queries. The runner compiles the
 * model, runs each persist source's real build SQL into a DuckDB schema named
 * after the destination, derives serve bindings through the real
 * `deriveServeBindings` / `planSourceIds` path, and runs every query twice:
 * live (no bindings) and served. A served run must report that a stored table
 * answered, so two runs are never equal only because the second fell back to
 * live. A query may also carry `truthSql`, an answer computed independently of
 * Malloy, because live itself can be wrong (malloydata/malloy#1762).
 *
 * Today the served side is the v1 storage serve shape; materialization v2's
 * rewriter is checked against the same scenarios when it lands (phase 3).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { unrecordedAudit } from "../../../src/service/access_record";
import { plannedSources } from "../../../src/service/materialization/plan/build_plan";
import {
   deriveServeBindings,
   groupAliasesByName,
   planSourceIds,
   type ServeBinding,
} from "../../../src/service/materialization/serve/materialization_serve_transform";
import {
   castToDeclaredTypes,
   persistSourceDeclaredColumns,
   persistSourceQueryTimezone,
} from "../../../src/service/materialization/stored_column_types";
import type { ManifestEntry } from "../../../src/service/materialization/types";
import { Model } from "../../../src/service/model";
import { QueryPrivileges } from "../../../src/service/model_query_request";

/**
 * The storage destination every scenario builds into: the scenario's own
 * database, named `duckdb` like the connection, so `duckdb.<table>` resolves
 * as a catalog-qualified table, as it would against an attached destination.
 */
export const DESTINATION = "duckdb";

export interface R4Query {
   /** A short label, for failure messages. */
   name: string;
   /** A `run:` statement. */
   malloy: string;
   /**
    * SQL over the seed tables that computes the right answer without Malloy,
    * as rows with the same column names. Checked against both runs.
    */
   truthSql?: string;
   /**
    * When the served run must NOT read storage (default false: it must). Such a
    * run reports no tier, or `live_fallback`.
    */
   expectLive?: boolean;
}

export interface R4Scenario {
   /** Model files by name; the first is the model queries run against. */
   files: Record<string, string>;
   /** SQL that creates and fills the source tables, run once. */
   seedSql: string[];
   /**
    * Build as a storage build did before it cast columns to their declared
    * types (ms2data/service#6642), to check what the serve side does with a
    * table an older build typed. Default false: the build casts, as it does now.
    */
   uncast?: boolean;
}

export type Row = Record<string, unknown>;

export interface R4Run {
   live: Row[];
   served: Row[];
   servedFrom: string | null;
   truth?: Row[];
}

/** A built scenario: run queries against it. */
export interface R4Fixture {
   bindings: ServeBinding[];
   run(query: R4Query): Promise<R4Run>;
   /**
    * Point the model at `bindings` (`[]` for live), clearing the memoized
    * serve shape, so the next served query compiles it again. Kept apart from
    * {@link runAsIs} so a benchmark can do it outside its stopwatch.
    */
   setBindings(bindings: ServeBinding[]): void;
   /** One query under the bindings already set, with nothing around it. */
   runAsIs(malloy: string): Promise<{
      rows: Row[];
      servedFrom: string | null;
      path: Awaited<ReturnType<Model["runQuery"]>>["path"];
   }>;
   /** The bound columns the serve side leaves off for their stored type. */
   typeMismatches(): ReturnType<Model["storedColumnTypeMismatches"]>;
}

/**
 * A fresh DuckDB for one scenario, in its own directory. A file, not
 * `:memory:`: DuckDB shares an in-memory database by connection name, and
 * Malloy looks a connection up by that name, so every scenario's `duckdb`
 * would otherwise be one database.
 */
export function freshDuckDB(
   name: string,
   dir: string = scratchDir(),
): DuckDBConnection {
   const connection = new DuckDBConnection(name, path.join(dir, `${name}.db`));
   opened.push(connection);
   return connection;
}

// Every connection and directory a scenario made, so disposeScenarios can
// close and remove them: an open DuckDB file is locked on Windows.
const opened: DuckDBConnection[] = [];
const scratchDirs: string[] = [];

function scratchDir(): string {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-r4-"));
   scratchDirs.push(dir);
   return dir;
}

/** Close every scenario's DuckDB and remove its directory. Call in afterAll. */
export async function disposeScenarios(): Promise<void> {
   for (const connection of opened.splice(0)) {
      await connection.close().catch(() => {});
   }
   for (const dir of scratchDirs.splice(0)) {
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

/**
 * Build every persist source of `scenario` and return a fixture that runs
 * queries live and served.
 */
export async function buildScenario(scenario: R4Scenario): Promise<R4Fixture> {
   const dir = scratchDir();
   const duckdb = freshDuckDB("duckdb", dir);
   for (const sql of scenario.seedSql) await duckdb.runSQL(sql);
   // On disk, as a package's files are: the serve shape lifts a source's joins
   // and views verbatim from its file, and drops them when it cannot read it.
   for (const [file, text] of Object.entries(scenario.files)) {
      fs.writeFileSync(path.join(dir, file), text);
   }
   // pathToFileURL, not `file://${dir}`: on Windows `dir` is `D:\…`, and the
   // in-memory reader's keys must match the URL Malloy normalizes to. Each
   // file's own URL too, so a name needing escapes (`my model.malloy`) agrees.
   const root = `${pathToFileURL(dir).href}/`;
   const urlOf = (file: string) => pathToFileURL(path.join(dir, file)).href;

   const authorConnections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const [entry] = Object.keys(scenario.files);
   const urlReader = new InMemoryURLReader(
      new Map(
         Object.entries(scenario.files).map(([file, text]) => [
            urlOf(file),
            text,
         ]),
      ),
   );
   const runtime = new Runtime({ urlReader, connections: authorConnections });
   const materializer = runtime.loadModel(new URL(urlOf(entry)), {
      importBaseURL: new URL(root),
   });
   const compiled = await materializer.getModel();

   // Build each persist source with its real SQL.
   const entries: Record<string, ManifestEntry> = {};
   const plan: Record<string, { name: string; sourceEntityId: string }> = {};
   const sources = plannedSources(await runtime.getBuildTargets(compiled));
   sources.forEach((source, i) => {
      plan[source.sourceID] = { name: source.name, sourceEntityId: `h${i}` };
   });
   for (const [i, source] of sources.entries()) {
      const table = `r4_${source.name}_${i}`;
      // Cast to the declared types as the storage build does.
      const select = scenario.uncast
         ? source.getSQL()
         : castToDeclaredTypes(
              source.getSQL(),
              persistSourceDeclaredColumns(source),
              { queryTimezone: persistSourceQueryTimezone(source) },
           );
      await duckdb.runSQL(`CREATE TABLE ${DESTINATION}.${table} AS ${select}`);
      const described = await duckdb.runSQL(`DESCRIBE ${DESTINATION}.${table}`);
      entries[`h${i}`] = {
         sourceEntityId: `h${i}`,
         sourceName: source.name,
         physicalTableName: table,
         connectionName: "duckdb",
         storageDestinationName: DESTINATION,
         schema: (
            described.rows as { column_name: string; column_type: string }[]
         ).map((c) => ({ name: c.column_name, type: c.column_type })),
         realization: "COPY",
         rowCount: null,
      } as ManifestEntry;
   }
   const bindings = deriveServeBindings(
      entries,
      groupAliasesByName(Object.values(plan)),
      undefined,
      planSourceIds(plan),
   );

   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef;
   const model = new Model({
      packageName: "r4",
      modelPath: entry,
      dataStyles: {},
      modelType: "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      modelMaterializer: materializer as any,
      modelDef,
      modelInfo: modelDefToModelInfo(modelDef),
   });
   const serveConfig = new MalloyConfig({ connections: {} });
   serveConfig.wrapConnections(
      () =>
         new FixedConnectionMap(new Map([[DESTINATION, duckdb]]), DESTINATION),
   );
   model.setServeDestinationConfig(() => serveConfig);

   const runOnce = async (malloy: string) => {
      const result = await model.runQuery(
         unrecordedAudit(),
         { query: malloy, filterParams: {} },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      return {
         rows: result.compactResult as unknown as Row[],
         servedFrom: result.servedFrom,
         path: result.path,
      };
   };

   return {
      bindings,
      setBindings(next) {
         model.setServeBindings(next);
      },
      runAsIs: runOnce,
      typeMismatches() {
         model.setServeBindings(bindings);
         return model.storedColumnTypeMismatches();
      },
      async run(query: R4Query): Promise<R4Run> {
         model.setServeBindings([]);
         const live = await runOnce(query.malloy);
         model.setServeBindings(bindings);
         const served = await runOnce(query.malloy);
         const truth = query.truthSql
            ? ((await duckdb.runSQL(query.truthSql)).rows as Row[])
            : undefined;
         return {
            live: live.rows,
            served: served.rows,
            servedFrom: served.servedFrom,
            ...(truth ? { truth } : {}),
         };
      },
   };
}

/** Relative tolerance for floats, as R4 states it. */
const FLOAT_TOLERANCE = 1e-9;

function normalizeValue(value: unknown): unknown {
   if (typeof value === "bigint") return Number(value);
   if (value instanceof Date) return value.toISOString();
   if (
      typeof value === "string" &&
      /^-?\d+$/.test(value) &&
      value.length < 16
   ) {
      // DuckDB returns some integer aggregates as strings over raw SQL.
      return Number(value);
   }
   return value;
}

function valuesEqual(a: unknown, b: unknown): boolean {
   const x = normalizeValue(a);
   const y = normalizeValue(b);
   if (typeof x === "number" && typeof y === "number") {
      if (x === y) return true;
      const scale = Math.max(Math.abs(x), Math.abs(y), 1);
      return Math.abs(x - y) <= FLOAT_TOLERANCE * scale;
   }
   return JSON.stringify(x) === JSON.stringify(y);
}

function sortKey(row: Row, columns: string[]): string {
   return JSON.stringify(columns.map((c) => normalizeValue(row[c])));
}

/**
 * The difference between two row sets, ignoring row order, or undefined when
 * they match: same columns, same row count, and each value equal within the
 * float tolerance.
 */
export function rowDifference(
   expected: Row[],
   actual: Row[],
): string | undefined {
   const columns = Object.keys(expected[0] ?? actual[0] ?? {}).sort();
   const actualColumns = Object.keys(actual[0] ?? expected[0] ?? {}).sort();
   if (JSON.stringify(columns) !== JSON.stringify(actualColumns)) {
      return `columns differ: ${columns.join(",")} against ${actualColumns.join(",")}`;
   }
   if (expected.length !== actual.length) {
      return `row counts differ: ${expected.length} against ${actual.length}`;
   }
   const a = [...expected].sort((p, q) =>
      sortKey(p, columns).localeCompare(sortKey(q, columns)),
   );
   const b = [...actual].sort((p, q) =>
      sortKey(p, columns).localeCompare(sortKey(q, columns)),
   );
   for (let i = 0; i < a.length; i++) {
      for (const c of columns) {
         if (!valuesEqual(a[i][c], b[i][c])) {
            return `row ${i} column ${c}: ${JSON.stringify(normalizeValue(a[i][c]))} against ${JSON.stringify(normalizeValue(b[i][c]))}`;
         }
      }
   }
   return undefined;
}

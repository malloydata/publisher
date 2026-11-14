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

import { expect } from "bun:test";
import { loadLeavingOutCrossConnectionJoins } from "../../../src/service/materialization/plan/cross_connection_joins";
import { tryCompileMaterializeCompanion } from "../../../src/service/materialization/plan/materialize_compile";
import { assertInlinedJoinsUnique } from "../../../src/service/materialization/run/inline_uniqueness";
import {
   compileParentKeyChecks,
   parentKeyCheckCompiler,
   tryCompileRollupCompanion,
} from "../../../src/service/materialization/plan/rollup_compile";
import {
   joinOneKey,
   joinOneKeysOf,
   joinedKeyNeedsOf,
   parentKeyNeedsOf,
   storedKeyChecksOf,
} from "../../../src/service/materialization/plan/rollup_plan";
import { SourceMap } from "../../../src/service/materialization/text/source_map";
import {
   runRollupKeyCheck,
   storedKeyCheckSql,
} from "../../../src/service/materialization/run/inline_uniqueness";
import { createModelRuntime } from "../../../src/service/model_runtime";
import { tryCompilePersistedViewCompanion } from "../../../src/service/materialization/plan/persisted_view_compile";
import {
   extensionOverStoredModel,
   type MaterializePlan,
} from "../../../src/service/materialization/plan/build_file_generator";
import { planChainedDownstream } from "../../../src/service/materialization/run/materialization_build_session";
import { storageTablePath } from "../../../src/service/materialization/domain/storage_table_path";
import type { ServedCopyServe } from "../../../src/service/materialization/serve/served_copy_serve";
import { deriveColocatedServeBindings } from "../../../src/service/materialization/serve/colocated_bindings";
import { capturedNativeSchema } from "../../../src/service/materialization/run/colocated_native_types";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   type Connection,
   FixedConnectionMap,
   type GivenValue,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   type PersistSource,
   type QueryRecord,
   Runtime,
} from "@malloydata/malloy";
import { unrecordedAudit } from "../../../src/service/access_record";
import {
   buildSQLOf,
   plannedSources,
} from "../../../src/service/materialization/plan/build_plan";
import {
   compilerBuildId,
   presentAs,
} from "../../../src/service/materialization/plan/addressing";
import {
   buildVirtualMap,
   deriveServeBindings,
   declarableColumns,
   groupAliasesByName,
   planSourceIds,
   type ServeBinding,
} from "../../../src/service/materialization/serve/materialization_serve_transform";
import {
   castToDeclaredTypes,
   persistSourceDeclaredColumns,
   persistSourceQueryTimezone,
} from "../../../src/service/materialization/stored_column_types";
import type {
   KeyUniqueness,
   ManifestEntry,
} from "../../../src/service/materialization/domain/types";
import { Model } from "../../../src/service/model";
import type { HydrationRuntime } from "../../../src/service/model_runtime";
import { malloyGivenToApi, type MalloyGiven } from "../../../src/service/given";
import { ModelPolicy } from "../../../src/service/model_policy";
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
   /** The caller's given values, for both runs. */
   givens?: Record<string, GivenValue>;
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
   /**
    * Build the generated `#(materialize)` sources (`<src>__mat`, in the
    * companion model) instead of the author's `#@ persist` sources, and hand
    * the model their plans, as a package with PUBLISHER_MATERIALIZE on does.
    */
   materialize?: boolean;
   /**
    * Pin the scenario's clock: Malloy's `now` (DuckDB's `LOCALTIMESTAMP`)
    * reads this instant, in builds and reads alike, until
    * {@link R4Fixture.setClock} moves it. Unpinned by default.
    */
   clock?: string;
   /**
    * Origin connections besides `duckdb`, by name, each a DuckDB of its own
    * with its seed SQL. A source on one is built there and its rows copied
    * into the destination, as a storage build does across connections.
    */
   origins?: Record<string, string[]>;
   /** Planned sources, by name, to leave unbuilt (no table, no binding). */
   unbuilt?: string[];
   /**
    * Run each `#(materialize)` plan's pre-build checks (an inlined join_one's
    * uniqueness, a pair table's keys) as a build does, and leave a source
    * whose check fails unbuilt, with the error on
    * {@link R4Fixture.checkFailures}. Default false.
    */
   checkKeys?: boolean;
   /**
    * Also build the generated `#(preaggregate)` rollups (the rollup companion
    * beside the entry model): a `storage=` rollup into the destination, a
    * colocated one into the scenario's own database under the build
    * manifest, as a package with PUBLISHER_MATERIALIZE on does.
    */
   preaggregate?: boolean;
   /**
    * With {@link preaggregate}: run each colocated rollup's key check (known
    * limitation 29) once its table is built, as an auto-run does, and bind
    * what it found. `false` binds no check, as for a table a host's manifest
    * binds or a build that could not check; `"unique"` binds a pass without
    * running the check, as a check over earlier rows would have left it.
    */
   rollupKeyChecks?: boolean | "unique";
   /**
    * Also build the generated `#@ persist` view sources (`<src>__<view>__view`,
    * in the persisted-view companion), as a package with PUBLISHER_MATERIALIZE
    * on does. Their refusals are on {@link R4Fixture.viewRefusals}.
    */
   persistedViews?: boolean;
   /**
    * Planned sources, by name, bound colocated: their tables are read through
    * the build manifest (Malloy substitutes them), not a storage binding.
    */
   colocated?: string[];
   /**
    * A warehouse other than DuckDB (a real Postgres) under `name`, the
    * scenario's default connection: the seed SQL runs on it, and each
    * colocated `#(materialize)` table is built in it, in `schema`, and read
    * by the served copy there. The scenario's DuckDB stays as the storage
    * destination. Colocated `#(materialize)` scenarios only.
    */
   warehouse?: {
      name: string;
      connection: Connection;
      schema: string;
   };
}

export type Row = Record<string, unknown>;

export interface R4Run {
   live: Row[];
   served: Row[];
   servedFrom: string | null;
   /** Each result's fields with their wire types (a timeframe included). */
   liveFields: { name: string; type: unknown }[];
   servedFields: { name: string; type: unknown }[];
   /** `served_copy` when the served copy on `virtual` answered. */
   servedVia?: string;
   /** The path report's `servedBy`: which table answered. */
   servedBy?: { kind: string; tableId?: string };
   truth?: Row[];
   /** The query that ran. */
   malloy?: string;
}

/** A built scenario: run queries against it. */
export interface R4Fixture {
   bindings: ServeBinding[];
   /** Each built source's table id (its content address in the plan), by name. */
   tableIds: Record<string, string>;
   /**
    * For a `#(materialize)` scenario: the author sources each generated build
    * reads the tables of (Malloy's build order), by the author's source name.
    */
   buildReads: Record<string, string[]>;
   /**
    * For each `storage=` extension table (P10): the SQL its build ran in the
    * destination over its parent's stored table, by the author's source name.
    */
   storedBuildSQL: Record<string, string>;
   /** Each built source's stored table, by the author's source name. */
   physicalTables: Record<string, string>;
   /**
    * The SELECT each stored table was built from (cast to the declared types,
    * as a storage build casts), by the author's source name: what a refresh
    * reads again, for a test that refreshes a table in place.
    */
   builtSQL: Record<string, string>;
   /** With {@link R4Scenario.checkKeys}: each source a check left unbuilt, and why. */
   checkFailures: Record<string, string>;
   /** The scenario's database: its source tables and its stored ones. */
   duckdb: DuckDBConnection;
   /**
    * With {@link R4Scenario.preaggregate}: each colocated `#(preaggregate)`
    * rollup built, by its generated source name, to the table it was built
    * into in the scenario's database.
    */
   rollupTables: Record<string, string>;
   /** The model queries run against, for a test that reaches its internals. */
   model: Model;
   /** The scenario's package directory, where its model files are written. */
   packagePath: string;
   run(query: R4Query): Promise<R4Run>;
   /**
    * Point the model at `bindings` (`[]` for live), clearing the memoized
    * serve shape, so the next served query compiles it again. Kept apart from
    * {@link runAsIs} so a benchmark can do it outside its stopwatch.
    */
   setBindings(bindings: ServeBinding[]): void;
   /** One query under the bindings already set, with nothing around it. */
   runAsIs(
      malloy: string,
      givens?: Record<string, GivenValue>,
   ): Promise<{
      rows: Row[];
      servedFrom: string | null;
      path: Awaited<ReturnType<Model["runQuery"]>>["path"];
   }>;
   /** The bound columns the serve side leaves off for their stored type. */
   typeMismatches(): ReturnType<Model["storedColumnTypeMismatches"]>;
   /** The served copy's problems after compiling it over {@link bindings}. */
   servedCopyProblems(): Promise<ReturnType<Model["servedCopyProblems"]>>;
   /** The annotation lines on a field of a source in the compiled served copy. */
   servedFieldNotes(source: string, field: string): Promise<string[]>;
   /** The model's served copy, bound to {@link bindings}. */
   servedCopy(): ServedCopyServe;
   /** Move a pinned clock (see {@link R4Scenario.clock}) for later runs. */
   setClock(at: string): void;
   /** Why the persisted-view planner refused a view, one sentence each. */
   viewRefusals: string[];
   /** Rows of a stored table, by its physical name, with SQL over it. */
   storedSQL(
      sql: (table: (sourceName: string) => string) => string,
   ): Promise<Row[]>;
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

/**
 * A DuckDB whose current time is pinned: every statement reads `clock` where
 * Malloy's `now` compiles to `LOCALTIMESTAMP`. DuckDB has no clock to set, so
 * the pin rewrites the statement; the scenario's own SQL never spells it.
 */
export class PinnedClockDuckDB extends DuckDBConnection {
   clock: string | undefined;
   private pin(sql: string): string {
      return this.clock === undefined
         ? sql
         : sql.replace(
              /\b(LOCALTIMESTAMP|CURRENT_TIMESTAMP)\b/g,
              `TIMESTAMP '${this.clock}'`,
           );
   }
   protected override runDuckDBQuery(
      sql: string,
   ): Promise<{ rows: QueryRecord[]; totalRows: number }> {
      return super.runDuckDBQuery(this.pin(sql));
   }
   override runSQLStream(
      sql: string,
      options?: Parameters<DuckDBConnection["runSQLStream"]>[1],
   ): ReturnType<DuckDBConnection["runSQLStream"]> {
      return super.runSQLStream(this.pin(sql), options);
   }
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
 * The scenario's files with every `#(materialize)` made colocated: its
 * `storage=` removed, and a `name=` added where it has none (a colocated one
 * needs one), so the same scenario runs against tables in the source's own
 * warehouse (phase 3 pull request 6).
 */
export function colocatedFiles(
   files: Record<string, string>,
): Record<string, string> {
   let n = 0;
   return Object.fromEntries(
      Object.entries(files).map(([file, text]) => [
         file,
         text.replace(/^(\s*#\(materialize\))([^\n]*)$/gm, (_m, head, rest) => {
            let keys = (rest as string)
               .replace(/\s*\bstorage\s*=\s*("[^"]*"|\S+)/g, "")
               .trimEnd();
            if (!/\bname\s*=/.test(keys)) keys = ` name="r4c_${n++}"${keys}`;
            return `${head}${keys}`;
         }),
      ]),
   );
}

/**
 * Build every persist source of `scenario` and return a fixture that runs
 * queries live and served. With `R4_MATERIALIZE_TIER=colocated` in the
 * environment, a `#(materialize)` scenario runs colocated instead
 * ({@link colocatedFiles}): an exploratory switch for running the whole R4
 * suite against colocated tables; the checked-in colocated parity lives in
 * served_copy_colocated.component.spec.ts.
 */
export async function buildScenario(scenario: R4Scenario): Promise<R4Fixture> {
   const exploringColocated =
      process.env.R4_MATERIALIZE_TIER === "colocated" && !!scenario.materialize;
   if (exploringColocated) {
      scenario = { ...scenario, files: colocatedFiles(scenario.files) };
   }
   const dir = scratchDir();
   let duckdb: DuckDBConnection;
   let pinned: PinnedClockDuckDB | undefined;
   if (scenario.clock !== undefined) {
      pinned = new PinnedClockDuckDB("duckdb", path.join(dir, "duckdb.db"));
      pinned.clock = scenario.clock;
      opened.push(pinned);
      duckdb = pinned;
   } else {
      duckdb = freshDuckDB("duckdb", dir);
   }
   const warehouse = scenario.warehouse;
   if (warehouse) {
      for (const sql of scenario.seedSql)
         await warehouse.connection.runSQL(sql);
   } else {
      for (const sql of scenario.seedSql) await duckdb.runSQL(sql);
   }
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

   const connections = new Map<string, Connection>([["duckdb", duckdb]]);
   if (warehouse) connections.set(warehouse.name, warehouse.connection);
   for (const [name, sql] of Object.entries(scenario.origins ?? {})) {
      const origin = freshDuckDB(name, dir);
      for (const statement of sql) await origin.runSQL(statement);
      connections.set(name, origin);
   }
   const authorConnections = new FixedConnectionMap(
      connections,
      warehouse?.name ?? "duckdb",
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
   let runtime = new Runtime({ urlReader, connections: authorConnections });
   let materializer = runtime.loadModel(new URL(urlOf(entry)), {
      importBaseURL: new URL(root),
   });
   // A model joining sources across origin connections does not compile;
   // with the materialize switch it loads from its build view (#6982), as a
   // package load does.
   let buildView: ReadonlyMap<string, string> = new Map();
   let compiled: Awaited<ReturnType<typeof materializer.getModel>>;
   try {
      compiled = await materializer.getModel();
   } catch (first) {
      const view = await loadLeavingOutCrossConnectionJoins({
         firstError: first,
         readText: (url) =>
            Object.entries(scenario.files).find(([f]) => urlOf(f) === url)?.[1],
         load: async (overlay) => {
            const rt = new Runtime({
               urlReader: new InMemoryURLReader(
                  new Map([
                     ...Object.entries(scenario.files).map(
                        ([file, text]) => [urlOf(file), text] as const,
                     ),
                     ...overlay,
                  ]),
               ),
               connections: authorConnections,
            });
            const mm = rt.loadModel(new URL(urlOf(entry)), {
               importBaseURL: new URL(root),
            });
            return { rt, mm, model: await mm.getModel() };
         },
      });
      if (!view) throw first;
      runtime = view.result.rt;
      materializer = view.result.mm;
      compiled = view.result.model;
      buildView = view.overlay;
   }

   // Build each persist source with its real SQL.
   const entries: Record<string, ManifestEntry> = {};
   const plan: Record<string, { name: string; sourceEntityId: string }> = {};
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const authorDef = (compiled as any)._modelDef;
   const companion = scenario.materialize
      ? await tryCompileMaterializeCompanion({
           packagePath: dir,
           modelPath: entry,
           contents: authorDef.contents,
           givens: authorDef.givens ?? {},
           getRuntime: async (overlay) => ({
              runtime: new Runtime({
                 urlReader: new InMemoryURLReader(
                    new Map([
                       ...Object.entries(scenario.files).map(
                          ([file, text]) => [urlOf(file), text] as const,
                       ),
                       ...buildView,
                       ...overlay,
                    ]),
                 ),
                 connections: authorConnections,
              }),
              importBaseURL: new URL(root),
           }),
           // P9's probes, against the author's model, as the build plan runs them.
           probe: materializer,
        }).then((c) => c.generated)
      : undefined;
   if (scenario.materialize && !companion) {
      throw new Error("the #(materialize) companion did not compile");
   }
   const materializeByName = new Map(
      (companion?.plans ?? []).map((p) => [p.buildSourceName, p]),
   );
   // A generated build source is presented as the author's source, as the
   // package's build plan presents it (collectGeneratedMaterialize).
   const sources = companion
      ? plannedSources(
           await companion.runtime.getBuildTargets(companion.model),
        ).flatMap((s) => {
           const p = materializeByName.get(s.name);
           return p
              ? [presentAs(s, { name: p.sourceName, sourceID: p.sourceID })]
              : [];
        })
      : !compiled
             .tagParse({ prefix: /^##!\s*/ })
             .tag.has("experimental", "persistence")
        ? []
        : plannedSources(await runtime.getBuildTargets(compiled));
   // `#(preaggregate)`: the rollup companion's targets, built beside the rest.
   // Compiled as a package compiles it, with the package's connections, so
   // its build ids are the ones a served query looks up.
   const rollups = scenario.preaggregate
      ? await tryCompileRollupCompanion({
           packagePath: dir,
           modelPath: entry,
           modelDef: authorDef,
           getRuntime: async (overlay) =>
              createModelRuntime(dir, entry, new Map([["duckdb", duckdb]]), {
                 overlay,
              }),
        })
      : undefined;
   if (scenario.preaggregate && !rollups?.generated) {
      throw new Error(
         `the #(preaggregate) companion did not compile: ${JSON.stringify(rollups?.refused)}`,
      );
   }
   // A rollup whose build reads a given is left out, as the package's build
   // plan leaves it out (`preaggregate-reads-given`).
   const allRollupTargets = rollups?.generated
      ? await rollups.generated.runtime.getBuildTargets(rollups.generated.model)
      : undefined;
   const rollupTargets = allRollupTargets
      ? {
           ...allRollupTargets,
           connections: allRollupTargets.connections.map((c) => ({
              ...c,
              targets: c.targets.filter((t) => !t.readsGivens),
           })),
        }
      : undefined;
   const rollupPlanByName = new Map(
      (rollups?.generated?.plans ?? []).map((p) => [p.rollupSourceName, p]),
   );
   // A colocated rollup: built into the scenario's database, read through the
   // build manifest keyed by its build id.
   const colocatedEntries: Record<
      string,
      { tableName: string; keyUniqueness?: KeyUniqueness }
   > = {};
   // The join_ones a package's build reads the rollups' key checks for.
   const joinOneKeys = joinOneKeysOf(SourceMap.of(authorDef));
   // The query sources' primary key checks, compiled as a package's build
   // plan compiles them: against the author's model.
   if (rollups?.generated) {
      await compileParentKeyChecks(
         rollups.generated.plans,
         parentKeyNeedsOf(SourceMap.of(authorDef)).map((need) => ({
            need,
            compile: parentKeyCheckCompiler(materializer),
         })),
         joinedKeyNeedsOf(SourceMap.of(authorDef)).map((need) => ({
            need,
            compile: parentKeyCheckCompiler(materializer),
         })),
      );
   }
   const rollupPlanned: [
      string,
      {
         sourceID: string;
         name: string;
         sourceEntityId: string;
         origin: "preaggregate";
      },
   ][] = [];
   for (const connection of rollupTargets?.connections ?? []) {
      for (const target of connection.targets) {
         const source = target.sources[0];
         const plan = source && rollupPlanByName.get(source.name);
         if (!source || !plan || plan.storage !== undefined) continue;
         const table = `r4_rollup_${Object.keys(colocatedEntries).length}`;
         await duckdb.runSQL(
            `CREATE TABLE ${table} AS ${target.buildSQL({ entries: {} })}`,
         );
         colocatedEntries[target.buildId] = { tableName: table };
         const check = plan.keyCheck;
         const checks = scenario.rollupKeyChecks ?? true;
         if (
            check &&
            checks !== false &&
            joinOneKeys.has(joinOneKey(plan.baseSourceID, check.keyColumn))
         ) {
            const record =
               checks === "unique"
                  ? { keyColumn: check.keyColumn, unique: true }
                  : await runRollupKeyCheck(check, duckdb, table);
            if (record) {
               colocatedEntries[target.buildId].keyUniqueness = {
                  keyColumn: record.keyColumn,
                  unique: record.unique,
               };
            }
         }
         if (checks !== false && (plan.parentKeyChecks ?? []).length > 0) {
            const parentKeys: Record<string, boolean> = {};
            for (const parent of plan.parentKeyChecks ?? []) {
               const record =
                  checks === "unique"
                     ? { unique: true }
                     : await runRollupKeyCheck(parent, duckdb, table);
               if (record) parentKeys[parent.sourceID] = record.unique;
            }
            colocatedEntries[target.buildId].keyUniqueness = {
               ...colocatedEntries[target.buildId].keyUniqueness,
               parentKeys,
            };
         }
         rollupPlanned.push([
            source.sourceID,
            {
               sourceID: source.sourceID,
               name: source.name,
               sourceEntityId: target.buildId,
               origin: "preaggregate",
            },
         ]);
      }
   }
   const storageRollups = rollupTargets
      ? plannedSources(rollupTargets).filter(
           (s) => rollupPlanByName.get(s.name)?.storage !== undefined,
        )
      : [];
   const rollupOrigin = new Set(storageRollups.map((s) => s.sourceID));
   sources.push(...storageRollups);
   // `#@ persist` views (phase 4, P14): their generated build sources keep
   // their own sourceIDs, as the package's build plan keeps them.
   const viewRefusals: string[] = [];
   if (scenario.persistedViews) {
      const views = await tryCompilePersistedViewCompanion({
         packagePath: dir,
         modelPath: entry,
         modelDef: authorDef,
         materializer,
         getRuntime: async (overlay) => ({
            runtime: new Runtime({
               urlReader: new InMemoryURLReader(
                  new Map([
                     ...Object.entries(scenario.files).map(
                        ([file, text]) => [urlOf(file), text] as const,
                     ),
                     ...buildView,
                     ...overlay,
                  ]),
               ),
               connections: authorConnections,
            }),
            importBaseURL: new URL(root),
         }),
      });
      viewRefusals.push(...views.refused.map((r) => r.reason));
      if (views.generated) {
         const targets = await views.generated.runtime.getBuildTargets(
            views.generated.model,
         );
         const names = new Set(
            views.generated.plans.map((p) => p.buildSourceName),
         );
         for (const target of targets.connections.flatMap((c) => c.targets)) {
            if (target.readsGivens) {
               for (const s of target.sources) {
                  if (names.has(s.name)) viewRefusals.push("reads a given");
               }
            }
         }
         sources.push(
            ...plannedSources({
               ...targets,
               connections: targets.connections.map((c) => ({
                  ...c,
                  targets: c.targets.filter((t) => !t.readsGivens),
               })),
            }).filter((s) => names.has(s.name)),
         );
      }
   }
   const buildReads: Record<string, string[]> = {};
   if (companion) {
      const targets = await companion.runtime.getBuildTargets(companion.model);
      for (const connection of targets.connections) {
         for (const target of connection.targets) {
            const reads = target.dependsOn
               .flatMap((d) => d.sources.map((s) => s.name))
               .flatMap((n) => {
                  const p = materializeByName.get(n);
                  return p ? [p.sourceName] : [];
               });
            for (const s of target.sources) {
               const p = materializeByName.get(s.name);
               if (p) buildReads[p.sourceName] = reads;
            }
         }
      }
   }
   const materializePlans: Record<string, MaterializePlan> = {};
   for (const plan of companion?.plans ?? []) {
      materializePlans[plan.sourceID] = plan;
   }
   // A colocated `#(materialize)` source (no `storage=`): built into the
   // scenario's own database, the source's connection, and read by the
   // served copy there (phase 3 pull request 6).
   const colocatedMaterialize = (source: (typeof sources)[number]) => {
      const p = materializePlans[source.sourceID];
      return p !== undefined && p.persist.storage === undefined;
   };
   // A colocated table is looked up by the compiler under its own build id.
   const entityIdOf = (source: (typeof sources)[number], i: number) =>
      scenario.colocated?.includes(source.name) || colocatedMaterialize(source)
         ? compilerBuildId(source, {})
         : `h${i}`;
   sources.forEach((source, i) => {
      plan[source.sourceID] = {
         name: source.name,
         sourceEntityId: entityIdOf(source, i),
      };
   });
   // Each built source's entry by sourceID, for an extension table built over
   // its parent's stored table.
   const entryBySourceID = new Map<string, ManifestEntry>();
   const storedBuildSQL: Record<string, string> = {};
   const colocatedMaterializeEntries: Record<
      string,
      { tableName: string; nativeSchema?: { name: string; type: string }[] }
   > = {};
   const builtSQL: Record<string, string> = {};
   const checkFailures: Record<string, string> = {};
   // The stored-table key checks a package's build plan asks for.
   const authorMap = SourceMap.of(authorDef);
   const storedKeyChecks = storedKeyChecksOf(
      rollups?.generated?.plans ?? [],
      parentKeyNeedsOf(authorMap),
      (sourceID) => {
         const key = (authorMap.node(sourceID)?.def as { primaryKey?: unknown })
            ?.primaryKey;
         return typeof key === "string" ? key : undefined;
      },
      sources
         .map((source) => source.sourceID)
         .filter((sourceID) => !rollupOrigin.has(sourceID)),
   );
   for (const [i, source] of sources.entries()) {
      if (scenario.unbuilt?.includes(source.name)) continue;
      if (scenario.checkKeys) {
         try {
            await assertInlinedJoinsUnique(
               materializePlans[source.sourceID],
               duckdb,
            );
         } catch (err) {
            checkFailures[source.name] =
               err instanceof Error ? err.message : String(err);
            continue;
         }
      }
      if (colocatedMaterialize(source)) {
         // As a colocated build builds it: its own SQL, in its warehouse,
         // reading a colocated parent's table through the build manifest.
         const table =
            warehouse && source.connectionName === warehouse.name
               ? `${warehouse.schema}.r4_${source.name}_${i}`
               : `r4_${source.name}_${i}`;
         const builder = connections.get(source.connectionName) ?? duckdb;
         await builder.runSQL(
            `CREATE TABLE ${table} AS ${buildSQLOf(source, {
               entries: { ...colocatedEntries },
            })}`,
         );
         colocatedEntries[entityIdOf(source, i)] = { tableName: table };
         // The warehouse's own column types, captured as a colocated build
         // captures them (colocated_native_types.ts).
         const nativeSchema = await capturedNativeSchema({
            run: (sql) => builder.runSQL(sql, { rowLimit: 100_000 }),
            physicalTableName: table,
            dialect: builder.dialectName,
            sourceName: source.name,
         });
         colocatedMaterializeEntries[entityIdOf(source, i)] = {
            tableName: table,
            ...(nativeSchema ? { nativeSchema } : {}),
         };
         continue;
      }
      if (scenario.colocated?.includes(source.name)) {
         const table = `r4_${source.name}_${i}`;
         await duckdb.runSQL(
            `CREATE TABLE ${table} AS ${buildSQLOf(source, { entries: {} })}`,
         );
         colocatedEntries[entityIdOf(source, i)] = { tableName: table };
         continue;
      }
      const table = `r4_${source.name}_${i}`;
      // A `storage=` extension table (P10) is built as its storage build
      // builds it: from its plan's chain, over its parent's stored table, in
      // the destination alone. Any other source from its own SQL.
      const over = materializePlans[source.sourceID]?.buildsOver;
      const parentEntry = over?.overStored
         ? entryBySourceID.get(over.sourceID)
         : undefined;
      let built: { sql: string; source: PersistSource } = {
         sql: buildSQLOf(source, { entries: {} }),
         source,
      };
      if (over?.overStored && parentEntry) {
         const schema = (parentEntry.schema ?? []).map((c) => ({
            name: c.name as string,
            type: c.type as string,
         }));
         const planned = await planChainedDownstream({
            transientModel: extensionOverStoredModel({
               plan: materializePlans[source.sourceID],
               chain: over.overStored,
               connectionName: DESTINATION,
               virtualHandle: parentEntry.sourceEntityId,
               columns: declarableColumns(schema).declared.map((c) => ({
                  name: c.name,
                  type: c.malloyType,
               })),
            }),
            downstreamName: materializePlans[source.sourceID].buildSourceName,
            virtualMap: buildVirtualMap([
               {
                  sourceName: over.overStored.parentBuildSourceName,
                  destinationName: DESTINATION,
                  virtualHandle: parentEntry.sourceEntityId,
                  tablePath: storageTablePath(
                     DESTINATION,
                     parentEntry.physicalTableName,
                  ),
                  schema,
               },
            ]),
            connections: new FixedConnectionMap(
               new Map([[DESTINATION, duckdb]]),
               DESTINATION,
            ),
         });
         built = { sql: planned.sql, source: planned.downstream };
         storedBuildSQL[source.name] = planned.sql;
      }
      // Cast to the declared types as the storage build does.
      const select = scenario.uncast
         ? built.sql
         : castToDeclaredTypes(
              built.sql,
              persistSourceDeclaredColumns(built.source),
              { queryTimezone: persistSourceQueryTimezone(built.source) },
           );
      builtSQL[source.name] = select;
      const origin = connections.get(source.connectionName);
      if (origin && origin !== duckdb) {
         // Built on its origin, its rows copied into the destination.
         const file = path.join(dir, `${table}.parquet`);
         await origin.runSQL(`COPY (${select}) TO '${file}' (FORMAT parquet)`);
         await duckdb.runSQL(
            `CREATE TABLE ${DESTINATION}.${table} AS SELECT * FROM read_parquet('${file}')`,
         );
      } else {
         await duckdb.runSQL(
            `CREATE TABLE ${DESTINATION}.${table} AS ${select}`,
         );
      }
      // runSQL returns 10 rows unless told otherwise: one per column here.
      const described = await duckdb.runSQL(
         `DESCRIBE ${DESTINATION}.${table}`,
         { rowLimit: 10_000 },
      );
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
         ...(rollupOrigin.has(source.sourceID)
            ? { origin: "preaggregate" }
            : {}),
      } as ManifestEntry;
      // A stored source's primary key, checked over its table as its build
      // checks it, when a storage rollup is read through a join on it.
      const storedKey = storedKeyChecks[source.sourceID];
      const checks = scenario.rollupKeyChecks ?? true;
      if (
         storedKey !== undefined &&
         checks !== false &&
         entries[`h${i}`].schema?.some((c) => c.name === storedKey)
      ) {
         const record =
            checks === "unique"
               ? { unique: true }
               : await runRollupKeyCheck(
                    {
                       keyColumn: storedKey,
                       sql: storedKeyCheckSql(
                          `${DESTINATION}.${table}`,
                          storedKey,
                       ),
                    },
                    duckdb,
                    table,
                 );
         if (record) {
            (
               entries[`h${i}`] as ManifestEntry & {
                  keyUniqueness?: KeyUniqueness;
               }
            ).keyUniqueness = {
               parentKeys: { [source.sourceID]: record.unique },
            };
         }
      }
      // A storage rollup's base key check, run on the base's connection
      // over its rows, as its build runs it (`compileParentKeyChecks`).
      const rollupChecks = rollupOrigin.has(source.sourceID)
         ? (rollupPlanByName.get(source.name)?.parentKeyChecks ?? [])
         : [];
      if (rollupChecks.length > 0 && checks !== false) {
         const parentKeys: Record<string, boolean> = {};
         for (const parent of rollupChecks) {
            const record =
               checks === "unique"
                  ? { unique: true }
                  : await runRollupKeyCheck(
                       parent,
                       connections.get(parent.connectionName ?? "duckdb") ??
                          duckdb,
                       table,
                    );
            if (record) parentKeys[parent.sourceID] = record.unique;
         }
         const entry = entries[`h${i}`] as ManifestEntry & {
            keyUniqueness?: KeyUniqueness;
         };
         entry.keyUniqueness = {
            ...entry.keyUniqueness,
            parentKeys: { ...entry.keyUniqueness?.parentKeys, ...parentKeys },
         };
      }
      entryBySourceID.set(source.sourceID, entries[`h${i}`]);
   }
   const bindings = [
      ...deriveServeBindings(
         entries,
         groupAliasesByName(Object.values(plan)),
         undefined,
         planSourceIds(plan),
      ),
      // The colocated `#(materialize)` tables, through the package's own
      // derivation.
      ...deriveColocatedServeBindings({
         entries: colocatedMaterializeEntries,
         planSources: Object.fromEntries(
            sources.map((source) => [
               source.sourceID,
               {
                  name: source.name,
                  sourceEntityId: plan[source.sourceID].sourceEntityId,
                  connectionName: source.connectionName,
                  dialect: source.dialectName,
               },
            ]),
         ),
         materializePlans,
         eligible: () => true,
      }),
   ];

   const modelDef = authorDef;
   // The plan's catalog, as a package hands it to its models, so the path
   // report names each stored table by its content address.
   // The manifest is passed only on a served run, so a live run reads live.
   let manifestOn = false;
   const policy = ModelPolicy.fixed({
      freshBuildManifestEntries: () =>
         manifestOn && Object.keys(colocatedEntries).length > 0
            ? colocatedEntries
            : undefined,
      materializedTables: {
         planned: new Map([
            ...sources.map(
               (source) =>
                  [
                     source.sourceID,
                     {
                        sourceID: source.sourceID,
                        name: source.name,
                        sourceEntityId: plan[source.sourceID].sourceEntityId,
                        origin: rollupOrigin.has(source.sourceID)
                           ? ("preaggregate" as const)
                           : ("persist" as const),
                     },
                  ] as const,
            ),
            ...rollupPlanned,
         ]),
         bound: colocatedEntries,
      },
      preaggregateEntityIds: new Set(
         rollupPlanned.map(([, p]) => p.sourceEntityId),
      ),
   });
   const model = new Model({
      packageName: "r4",
      modelPath: entry,
      dataStyles: {},
      modelType: "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      modelMaterializer: materializer as any,
      modelDef,
      modelInfo: modelDefToModelInfo(modelDef),
      // The model's given surface, as a package derives it, so a row-level
      // gate reading a given can be decided.
      givens: (
         Array.from(compiled.givens.values()) as unknown as MalloyGiven[]
      ).map(malloyGivenToApi),
      // A row-level gate grafts through the model's runtime, as a package's does.
      gateRuntime: runtime as unknown as HydrationRuntime,
      policy,
   });
   const serveConfig = new MalloyConfig({ connections: {} });
   serveConfig.wrapConnections(
      () =>
         new FixedConnectionMap(new Map([[DESTINATION, duckdb]]), DESTINATION),
   );
   model.setServeDestinationConfig(() => serveConfig);
   // The served copy (materialization v2) compiles the author's own files with
   // the package's connections; only routed when its setting is on.
   model.setServedCopyContext(dir, connections, materializePlans);

   // The ordering the author's query asks for, from compiling it against the
   // author's model: the served answer is held to it, not to its own.
   const orderingOf = async (
      malloy: string,
      givens?: Record<string, GivenValue>,
   ): Promise<RowOrdering | undefined> => {
      try {
         const prepared = await materializer
            .loadQuery(malloy, givens ? { givens } : {})
            .getPreparedResult();
         const structs = prepared._rawQuery.structs;
         const result = structs[structs.length - 1];
         return result
            ? orderingOfStruct(result as unknown as ResultStructShape)
            : undefined;
      } catch {
         // A query the author's model alone cannot compile (one that only
         // the served side answers): its rows carry no ordering, and
         // rowDifference reads them strictly.
         return undefined;
      }
   };

   const runOnce = async (
      malloy: string,
      givens?: Record<string, GivenValue>,
   ) => {
      const ordering = await orderingOf(malloy, givens);
      const result = await model.runQuery(
         unrecordedAudit(),
         { query: malloy, filterParams: {}, ...(givens ? { givens } : {}) },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      return {
         rows: withOrdering(result.compactResult as unknown as Row[], ordering),
         // The result's field types as the wire carries them (a timeframe
         // included), so a test can hold served metadata to live's.
         fields: (result.result?.schema?.fields ?? []).map((f) => ({
            name: f.name,
            type: (f as { type?: unknown }).type,
         })),
         servedFrom: result.servedFrom,
         path: result.path,
      };
   };

   const physicalNames = new Map([
      ...Object.values(entries).map(
         (e) => [e.sourceName, e.physicalTableName] as const,
      ),
      ...bindings
         .filter((b) => b.colocated)
         .map((b) => [b.sourceName, b.tablePath] as const),
   ]);
   return {
      bindings,
      packagePath: dir,
      tableIds: Object.fromEntries(
         Object.values(plan).map((p) => [p.name, p.sourceEntityId]),
      ),
      buildReads,
      storedBuildSQL,
      physicalTables: Object.fromEntries(physicalNames),
      builtSQL,
      checkFailures,
      duckdb,
      model,
      viewRefusals,
      setClock(at) {
         if (!pinned) throw new Error("the scenario's clock is not pinned");
         pinned.clock = at;
      },
      async storedSQL(sql) {
         const text = sql((sourceName) => {
            const table = physicalNames.get(sourceName);
            if (!table) throw new Error(`no stored table for ${sourceName}`);
            return `${DESTINATION}.${table}`;
         });
         return (await duckdb.runSQL(text)).rows as Row[];
      },
      setBindings(next) {
         model.setServeBindings(next);
         manifestOn = next.length > 0 || next === bindings;
      },
      rollupTables: Object.fromEntries(
         rollupPlanned.map(([, p]) => [
            p.name,
            colocatedEntries[p.sourceEntityId]?.tableName ?? "",
         ]),
      ),
      runAsIs: runOnce,
      async servedFieldNotes(source, field) {
         model.setServeBindings(bindings);
         const servedCopy = (
            model as unknown as {
               servedCopy: { compiledModelDef(): Promise<unknown> };
            }
         ).servedCopy;
         const servedDef = (await servedCopy.compiledModelDef()) as {
            contents?: Record<
               string,
               {
                  fields?: {
                     name?: string;
                     as?: string;
                     annotations?: { notes?: { text: string }[] };
                     annotation?: { notes?: { text: string }[] };
                  }[];
               }
            >;
         };
         const f = servedDef?.contents?.[source]?.fields?.find(
            // `as` first: a renamed field keeps its old `name`.
            (x) => (x.as ?? x.name) === field,
         );
         return ((f?.annotations ?? f?.annotation)?.notes ?? []).map((n) =>
            n.text.trim(),
         );
      },
      servedCopy() {
         model.setServeBindings(bindings);
         return (model as unknown as { servedCopy: ServedCopyServe })
            .servedCopy;
      },
      async servedCopyProblems() {
         model.setServeBindings(bindings);
         await model.prepareServedCopy();
         // By name and reason: a problem's sourceID (what a caller keys on)
         // carries the scenario's temporary path.
         return model
            .servedCopyProblems()
            .map(({ sourceID: _sourceID, ...problem }) => problem);
      },
      typeMismatches() {
         model.setServeBindings(bindings);
         return model.storedColumnTypeMismatches();
      },
      async run(query: R4Query): Promise<R4Run> {
         model.setServeBindings([]);
         manifestOn = false;
         const live = await runOnce(query.malloy, query.givens);
         model.setServeBindings(bindings);
         manifestOn = true;
         const served = await runOnce(query.malloy, query.givens);
         const truth = query.truthSql
            ? ((await duckdb.runSQL(query.truthSql, { rowLimit: 10_000 }))
                 .rows as Row[])
            : undefined;
         return {
            malloy: query.malloy,
            live: live.rows,
            served: served.rows,
            liveFields: live.fields,
            servedFields: served.fields,
            // Under the exploratory colocated switch a served-copy answer
            // from a colocated table reports no storage hit; read it as the
            // `storage=` scenario's would, so the run shows what else differs.
            servedFrom:
               exploringColocated &&
               served.servedFrom === null &&
               served.path?.servedBy?.via === "served_copy"
                  ? "storage"
                  : served.servedFrom,
            servedBy: served.path?.servedBy,
            ...(served.path?.servedBy?.via
               ? { servedVia: served.path.servedBy.via }
               : {}),
            ...(truth ? { truth } : {}),
         };
      },
   };
}

/**
 * Whether a run over a pair table (`inline=` on a join_many) was served by the
 * served copy, as it should be: since Malloy 0.0.438 (malloydata/malloy#3208)
 * live counts a parent with no joined row as none, which the pair table's
 * null-extended row for that parent cannot be told apart from, so a query
 * that counts through the inlined path (`segments.count()`, or one of the
 * fixture's measures that does, named in `counts`) is answered live. Rows are
 * compared by the caller.
 */
export function expectPairServedVia(
   run: R4Run,
   counts: readonly string[],
   servedHere = true,
): void {
   const malloy = run.malloy ?? "";
   const countsThroughPath =
      /\.count\(\)/.test(malloy) ||
      counts.some((n) => new RegExp(`\\b${n}\\b`).test(malloy));
   expect(run.servedVia).toBe(
      servedHere && !countsThroughPath ? "served_copy" : undefined,
   );
}

/** Relative tolerance for floats, as R4 states it. */
const FLOAT_TOLERANCE = 1e-9;

/**
 * How a query orders its rows, and each of its nests' rows, as its compiled
 * result shape states it. Malloy orders every nest: by its `order_by`, else by
 * its default (the first aggregate descending, and so on), so the order of a
 * nest is part of its answer, the same as its values.
 */
export interface RowOrdering {
   /** The output fields the rows are sorted by, in order; empty when unordered. */
   orderBy: { field: string; dir: "asc" | "desc" }[];
   /** Each nested result's own ordering, by its output name. */
   nests: Record<string, RowOrdering>;
}

/** The ordering each run's rows were answered under, by the rows array. */
const orderings = new WeakMap<Row[], RowOrdering>();

/** Record the ordering `rows` were answered under, for {@link rowDifference}. */
export function withOrdering(rows: Row[], ordering?: RowOrdering): Row[] {
   if (ordering) orderings.set(rows, ordering);
   return rows;
}

interface ResultStructShape {
   fields?: ResultFieldShape[];
   resultMetadata?: {
      orderBy?: { field: string | number; dir?: "asc" | "desc" }[];
   };
}
interface ResultFieldShape extends ResultStructShape {
   name: string;
   as?: string;
}

/** A compiled result struct's ordering, and its nests', from Malloy's result metadata. */
export function orderingOfStruct(struct: ResultStructShape): RowOrdering {
   const fields = struct.fields ?? [];
   const orderBy = (struct.resultMetadata?.orderBy ?? []).flatMap((o) => {
      const field =
         typeof o.field === "number"
            ? (fields[o.field - 1]?.as ?? fields[o.field - 1]?.name)
            : o.field;
      return field ? [{ field, dir: o.dir ?? "asc" }] : [];
   });
   const nests: Record<string, RowOrdering> = {};
   for (const f of fields) {
      if (Array.isArray(f.fields)) nests[f.as ?? f.name] = orderingOfStruct(f);
   }
   return { orderBy, nests };
}

/** A value in the form two runs are compared in. */
function normalizeValue(value: unknown): unknown {
   if (typeof value === "bigint") {
      // Past 2^53 a Number would round two different integers together.
      return value >= Number.MIN_SAFE_INTEGER &&
         value <= Number.MAX_SAFE_INTEGER
         ? Number(value)
         : value.toString();
   }
   if (value instanceof Date) return value.toISOString();
   if (
      typeof value === "string" &&
      /^-?\d+$/.test(value) &&
      value.length < 16
   ) {
      // DuckDB returns some integer aggregates as strings over raw SQL.
      return Number(value);
   }
   if (Array.isArray(value)) return value.map(normalizeValue);
   if (isRecord(value)) {
      return Object.fromEntries(
         Object.entries(value).map(([k, v]) => [k, normalizeValue(v)]),
      );
   }
   return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
   return (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      !(value instanceof Date)
   );
}

/** JSON with object keys sorted, so key order never decides a row's place. */
function canonicalJson(value: unknown): string {
   return JSON.stringify(value, (_key, v: unknown) =>
      isRecord(v)
         ? Object.fromEntries(
              Object.keys(v)
                 .sort()
                 .map((k) => [k, v[k]]),
           )
         : v,
   );
}

/**
 * A row's sort key for pairing it with its counterpart: its normalized value
 * with nested rows in a canonical order, so two rows that differ only in how
 * tied nested rows came back still pair. Only the pairing uses it; the nested
 * rows' order is judged separately, against the nest's own ordering.
 */
function pairingKey(value: unknown): string {
   const canonical = (v: unknown): unknown => {
      if (Array.isArray(v)) {
         const items = v.map(canonical);
         return items.every(isRecord)
            ? items
                 .map((item) => ({ item, key: canonicalJson(item) }))
                 .sort((p, q) => p.key.localeCompare(q.key))
                 .map(({ item }) => item)
            : items;
      }
      if (isRecord(v)) {
         return Object.fromEntries(
            Object.entries(v).map(([k, x]) => [k, canonical(x)]),
         );
      }
      return v;
   };
   return canonicalJson(canonical(normalizeValue(value)));
}

/**
 * Integers (counts, sums of integers) must match exactly; other numbers
 * within the relative float tolerance, floored at 1 so two values near zero
 * are compared absolutely. NaN matches NaN: DuckDB divides 0.0 by 0 as NaN,
 * which a ratio over a childless parent's sum and count is.
 */
function numbersEqual(x: number, y: number): boolean {
   if (x === y || (Number.isNaN(x) && Number.isNaN(y))) return true;
   if (Number.isSafeInteger(x) && Number.isSafeInteger(y)) return false;
   const scale = Math.max(Math.abs(x), Math.abs(y), 1);
   return Math.abs(x - y) <= FLOAT_TOLERANCE * scale;
}

/** Two normalized scalars (or nests with no known ordering, in order) equal. */
function normalizedEqual(x: unknown, y: unknown): boolean {
   if (typeof x === "number" && typeof y === "number")
      return numbersEqual(x, y);
   if (Array.isArray(x) && Array.isArray(y)) {
      return (
         x.length === y.length && x.every((v, i) => normalizedEqual(v, y[i]))
      );
   }
   if (isRecord(x) && isRecord(y)) {
      const keys = Object.keys(x).sort();
      return (
         JSON.stringify(keys) === JSON.stringify(Object.keys(y).sort()) &&
         keys.every((k) => normalizedEqual(x[k], y[k]))
      );
   }
   return canonicalJson(x) === canonicalJson(y);
}

/**
 * The difference between two sets of rows, or undefined when they match.
 *
 * With an ordering that sorts by some fields, the rows must come back in that
 * order: row i's sort-key values must equal row i's on the other side, so the
 * only freedom left is among rows that tie on every sort key, whose order the
 * engine decides. Unordered (no ordering, at the top level, or a nest whose
 * ordering is empty), the rows are compared as a set. A nest whose ordering is
 * not known at all is compared in order, the strict reading.
 */
function rowsDifference(
   expected: unknown[],
   actual: unknown[],
   ordering: RowOrdering | undefined,
   where: string,
): string | undefined {
   if (expected.length !== actual.length) {
      return `${where}row counts differ: ${expected.length} against ${actual.length}`;
   }
   const records = expected.every(isRecord) && actual.every(isRecord);
   if (!records) {
      return normalizedEqual(normalizeValue(expected), normalizeValue(actual))
         ? undefined
         : `${where}${JSON.stringify(normalizeValue(expected))} against ${JSON.stringify(normalizeValue(actual))}`;
   }
   const sortedBy = (ordering?.orderBy ?? []).filter(
      (o) =>
         expected.every((r) => o.field in r) &&
         actual.every((r) => o.field in r),
   );
   for (let i = 0; i < expected.length && sortedBy.length > 0; i++) {
      for (const { field } of sortedBy) {
         const x = normalizeValue(expected[i][field]);
         const y = normalizeValue(actual[i][field]);
         if (!normalizedEqual(x, y)) {
            return `${where}row ${i} is out of order on ${field}: ${JSON.stringify(x)} against ${JSON.stringify(y)}`;
         }
      }
   }
   const pair = (rows: Record<string, unknown>[]) =>
      ordering === undefined && where !== ""
         ? rows
         : rows
              .map((row) => ({ row, key: pairingKey(row) }))
              .sort((p, q) => p.key.localeCompare(q.key))
              .map(({ row }) => row);
   const a = pair(expected);
   const b = pair(actual);
   for (let i = 0; i < a.length; i++) {
      const columns = Object.keys(a[i]).sort();
      const otherColumns = Object.keys(b[i]).sort();
      if (JSON.stringify(columns) !== JSON.stringify(otherColumns)) {
         return `${where}columns differ: ${columns.join(",")} against ${otherColumns.join(",")}`;
      }
      for (const c of columns) {
         const x = a[i][c];
         const y = b[i][c];
         if (Array.isArray(x) && Array.isArray(y)) {
            const inner = rowsDifference(
               x,
               y,
               ordering?.nests[c],
               `${where}row ${i} column ${c}: `,
            );
            if (inner) return inner;
         } else if (!normalizedEqual(normalizeValue(x), normalizeValue(y))) {
            return `${where}row ${i} column ${c}: ${JSON.stringify(normalizeValue(x))} against ${JSON.stringify(normalizeValue(y))}`;
         }
      }
   }
   return undefined;
}

/**
 * The difference between two row sets, or undefined when they match: same
 * columns, same row count, each value equal (integers exactly, other numbers
 * within the float tolerance), and rows in the order the query asked for, up
 * to ties on its sort keys.
 *
 * The ordering is the one recorded for the rows ({@link withOrdering}; every
 * run of a fixture records its query's). The top level is held to it only
 * when both sides are such answers: rows from raw SQL (a test's truth, a
 * stored table) come back in no order the query decides, so against them the
 * top level is compared as a set, and each nest is still held to its own
 * ordering. With no ordering on either side, the top level is a set and each
 * nest is compared in order.
 */
export function rowDifference(
   expected: Row[],
   actual: Row[],
   ordering?: RowOrdering,
): string | undefined {
   const columns = Object.keys(expected[0] ?? actual[0] ?? {}).sort();
   const actualColumns = Object.keys(actual[0] ?? expected[0] ?? {}).sort();
   if (JSON.stringify(columns) !== JSON.stringify(actualColumns)) {
      return `columns differ: ${columns.join(",")} against ${actualColumns.join(",")}`;
   }
   const recorded = orderings.get(expected) ?? orderings.get(actual);
   const known = ordering ?? recorded;
   const bothAnswers =
      ordering !== undefined ||
      (orderings.has(expected) && orderings.has(actual));
   return rowsDifference(
      expected,
      actual,
      known && !bothAnswers ? { orderBy: [], nests: known.nests } : known,
      "",
   );
}

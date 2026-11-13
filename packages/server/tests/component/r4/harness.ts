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

import { loadLeavingOutCrossConnectionJoins } from "../../../src/service/materialization/plan/cross_connection_joins";
import { tryCompileMaterializeCompanion } from "../../../src/service/materialization/plan/materialize_compile";
import { tryCompileRollupCompanion } from "../../../src/service/materialization/plan/rollup_compile";
import { createModelRuntime } from "../../../src/service/model_runtime";
import { tryCompilePersistedViewCompanion } from "../../../src/service/materialization/plan/persisted_view_compile";
import type { MaterializePlan } from "../../../src/service/materialization/plan/build_file_generator";
import type { ServedCopyServe } from "../../../src/service/materialization/serve/served_copy_serve";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   FixedConnectionMap,
   type GivenValue,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
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
    * Also build the generated `#(preaggregate)` rollups (the rollup companion
    * beside the entry model): a `storage=` rollup into the destination, a
    * colocated one into the scenario's own database under the build
    * manifest, as a package with PUBLISHER_MATERIALIZE on does.
    */
   preaggregate?: boolean;
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
}

/** A built scenario: run queries against it. */
export interface R4Fixture {
   bindings: ServeBinding[];
   /**
    * With {@link R4Scenario.preaggregate}: each colocated `#(preaggregate)`
    * rollup built, by its generated source name, to the table it was built
    * into in the scenario's database.
    */
   rollupTables: Record<string, string>;
   /** The model queries run against, for a test that reaches its internals. */
   model: Model;
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
 * Build every persist source of `scenario` and return a fixture that runs
 * queries live and served.
 */
export async function buildScenario(scenario: R4Scenario): Promise<R4Fixture> {
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

   const connections = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);
   for (const [name, sql] of Object.entries(scenario.origins ?? {})) {
      const origin = freshDuckDB(name, dir);
      for (const statement of sql) await origin.runSQL(statement);
      connections.set(name, origin);
   }
   const authorConnections = new FixedConnectionMap(connections, "duckdb");
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
   const rollupTargets = rollups?.generated
      ? await rollups.generated.runtime.getBuildTargets(rollups.generated.model)
      : undefined;
   const rollupPlanByName = new Map(
      (rollups?.generated?.plans ?? []).map((p) => [p.rollupSourceName, p]),
   );
   // A colocated rollup: built into the scenario's database, read through the
   // build manifest keyed by its build id.
   const colocatedEntries: Record<string, { tableName: string }> = {};
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
   const materializePlans: Record<string, MaterializePlan> = {};
   for (const plan of companion?.plans ?? []) {
      materializePlans[plan.sourceID] = plan;
   }
   // A colocated table is looked up by the compiler under its own build id.
   const entityIdOf = (source: (typeof sources)[number], i: number) =>
      scenario.colocated?.includes(source.name)
         ? compilerBuildId(source, {})
         : `h${i}`;
   sources.forEach((source, i) => {
      plan[source.sourceID] = {
         name: source.name,
         sourceEntityId: entityIdOf(source, i),
      };
   });
   for (const [i, source] of sources.entries()) {
      if (scenario.unbuilt?.includes(source.name)) continue;
      if (scenario.colocated?.includes(source.name)) {
         const table = `r4_${source.name}_${i}`;
         await duckdb.runSQL(
            `CREATE TABLE ${table} AS ${buildSQLOf(source, { entries: {} })}`,
         );
         colocatedEntries[entityIdOf(source, i)] = { tableName: table };
         continue;
      }
      const table = `r4_${source.name}_${i}`;
      // Cast to the declared types as the storage build does.
      const select = scenario.uncast
         ? buildSQLOf(source, { entries: {} })
         : castToDeclaredTypes(
              buildSQLOf(source, { entries: {} }),
              persistSourceDeclaredColumns(source),
              { queryTimezone: persistSourceQueryTimezone(source) },
           );
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
   }
   const bindings = deriveServeBindings(
      entries,
      groupAliasesByName(Object.values(plan)),
      undefined,
      planSourceIds(plan),
   );

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

   const runOnce = async (
      malloy: string,
      givens?: Record<string, GivenValue>,
   ) => {
      const result = await model.runQuery(
         unrecordedAudit(),
         { query: malloy, filterParams: {}, ...(givens ? { givens } : {}) },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      return {
         rows: result.compactResult as unknown as Row[],
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

   const physicalNames = new Map(
      Object.values(entries).map((e) => [e.sourceName, e.physicalTableName]),
   );
   return {
      bindings,
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
         return model.servedCopyProblems();
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
            live: live.rows,
            served: served.rows,
            liveFields: live.fields,
            servedFields: served.fields,
            servedFrom: served.servedFrom,
            servedBy: served.path?.servedBy,
            ...(served.path?.servedBy?.via
               ? { servedVia: served.path.servedBy.via }
               : {}),
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

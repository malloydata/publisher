// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package-level runner for the conformance fixture (ms2data/service#7098),
 * and the library the v1 cutover's conformance runner is built on
 * (`v1_cutover_runner.ts`, plan question 15):
 * a synthetic package goes through the real production path (Environment,
 * Package.create, the package's own build plan, its colocated manifest bind
 * and its `storage=` bind), and every query runs live first, then served with
 * the materialize switch off (today's serve shape) and on (the served copy).
 *
 * The R4 model harness (tests/component/r4/harness.ts) drives one Model with bindings it
 * derives itself; this one lets the package derive them, so it sees what a
 * served package sees: bindings across models, a joined source declared in
 * another file, two models declaring one name, and the serve verdicts.
 */

import { capturedNativeSchema } from "../../service/materialization/run/colocated_native_types";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   type GivenValue,
   MalloyConfig,
} from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../service/access_record";
import {
   __setPackageLoadPoolFactoryForTests,
   __setPackageLoadPoolForTests,
   getPackageLoadWorkerCount,
   PackageLoadPool,
} from "../../package_load/package_load_pool";
import { Settings } from "../../settings/settings";
import { Environment } from "../../service/environment";
import { parsePackageName } from "../../service/identity";
import { QueryPrivileges } from "../../service/model_query_request";
import type { Package } from "../../service/package";
import { installSettings } from "../settings_env";
import { writePackageFiles } from "../temp_package";
import {
   castToDeclaredTypes,
   type DeclaredColumn,
   declaredColumnsOf,
} from "../../service/materialization/stored_column_types";
/** A result row, as `compactResult` carries it. */
export type Row = Record<string, unknown>;

/** `value` with every `reference_id = "…"` (minted per compile) blanked. */
function withoutReferenceIds(value: unknown): unknown {
   if (value === undefined) return value;
   return JSON.parse(
      JSON.stringify(value).replace(/reference_id = \\"[^"\\]*\\"/g, ""),
   );
}

/** The storage destination every `storage=` source in the fixture names. */
export const LAKE = "lake";

/** A served field as the wire carries it: name, type (timeframe included) and annotations. */
export interface WireField {
   name: string;
   type: unknown;
   annotations: string[];
}

/** One query's answer. */
export interface Answer {
   rows: Row[];
   fields: WireField[];
   servedFrom: string | null;
   /** `materialized`, `rollup` or `live`, from the path report. */
   servedBy: string;
   /** `served_copy` when the served copy on `virtual` answered. */
   via?: string;
}

/** What querying a package produced: an answer, or the error it threw. */
export type Outcome = { answer: Answer } | { error: string };

/**
 * The switches a served run is taken under: `off` (`PUBLISHER_MATERIALIZE`
 * unset, the shipped default) and `on` or `cutover` (`PUBLISHER_MATERIALIZE`
 * on). Since the v1 cutover's retirement every one serves through the served
 * copy, the rollup and view legs, or live; `cutover` is kept as a name for
 * the column the fixture declares its expectations under.
 */
export type Switch = "off" | "on" | "cutover";

function settingsFor(materialize: Switch) {
   return {
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: materialize === "off" ? "false" : "true",
      },
   };
}

/**
 * Install the fixture's settings with the switches as given. The router reads
 * them per query, so a loaded package can be queried every way; the v1
 * cutover's bindings are pushed at bind, so a fixture queried under
 * `cutover` is rebound first ({@link LoadedFixture.rebind}).
 */
export function withSwitch(materialize: Switch): () => void {
   return installSettings(settingsFor(materialize)).restore;
}

/**
 * Pools kept for the rest of the run, one per set of settings a pool was made
 * under. Reusing them, rather than spawning a worker per case, keeps a run's
 * worker count down, which makes the run faster. Under Bun 1.3, which kept most
 * of a stopped worker's memory, it also kept Windows CI from running out of
 * memory; Bun 1.4.4 returns that memory.
 */
const poolsBySettings = new Map<string, PackageLoadPool>();

/**
 * The pool for the settings in force now. Called at the first load after
 * {@link freshLoadPool}, so a caller may switch settings before or after it.
 * A pool's worker count, timeouts and cache size are fixed when it is made,
 * and its worker reads the rest at spawn, so the whole settings snapshot is
 * the key.
 */
function poolForCurrentSettings(): PackageLoadPool {
   const key = JSON.stringify(Settings.current().toWorkerData());
   const kept = poolsBySettings.get(key);
   // Another spec may have shut it down through __setPackageLoadPoolForTests.
   if (kept && !kept.isShutDownForTests) return kept;
   const pool = new PackageLoadPool(getPackageLoadWorkerCount());
   poolsBySettings.set(key, pool);
   return pool;
}

/**
 * Drop the current package-load pool, so the next load runs in a pool made
 * under the settings installed then. A worker reads its settings once, at
 * spawn, and the build plan (with `#(materialize)` in it or not) is computed
 * in the worker: a pool made under other settings would plan with those.
 */
export async function freshLoadPool(): Promise<void> {
   __setPackageLoadPoolFactoryForTests(poolForCurrentSettings);
   await __setPackageLoadPoolForTests(null, new Set(poolsBySettings.values()));
}

/** A package loaded, built and bound; queries run against it. */
export interface LoadedFixture {
   pkg: Package;
   /** Load problems: the package's warnings, by code and subject. */
   warnings: { code?: string; subject?: string; message?: string }[];
   /** Each planned source's serve verdict after the bind, `name:verdict[:reason]`. */
   verdicts: string[];
   /** The plan's sources that were built, by name and tier. */
   built: { name: string; tier: "storage" | "colocated" }[];
   /** Problems building a planned source, by name. */
   buildErrors: Record<string, string>;
   /** The `storage=` manifest entries the build bound, by sourceEntityId. */
   stored: Record<string, Record<string, unknown>>;
   /**
    * Bind the built tables again under the settings installed now, and settle
    * every model's served copy: the bindings a switch read at bind (the v1
    * cutover's colocated tables) follow it.
    */
   rebind(): Promise<void>;
   run(
      modelPath: string,
      query: string,
      givens?: Record<string, GivenValue>,
   ): Promise<Outcome>;
   dispose(): Promise<void>;
}

/** How {@link loadFixture}'s `bind` builds the plan. */
export interface BindOptions {
   /**
    * Build the `#(preaggregate)` and `#@ preaggregate` rollups too (default
    * false: most callers serve no rollup, so they build none).
    */
   includeRollups?: boolean;
}

/**
 * Write `files` as a package under a fresh environment, load it, and return a
 * handle that can build and bind it (`bind`) and query it. Queries before
 * `bind` are live; after it, served.
 */
export async function loadFixture(
   /**
    * The package: its files by relative path (the fixture's synthetic
    * packages), or a directory to copy (a real package snapshot).
    */
   files: Record<string, string> | { packageDir: string },
): Promise<{
   run: LoadedFixture["run"];
   bind(options?: BindOptions): Promise<LoadedFixture>;
   dispose(): Promise<void>;
   /** The loaded package, before any bind. */
   pkg: Package;
   /** The temporary directory the environment and the lake live in. */
   root: string;
   /** The storage destination the `storage=` tables are built in. */
   lake: DuckDBConnection;
}> {
   const root = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-conform-"));
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath, { recursive: true });
   if ("packageDir" in files && typeof files.packageDir === "string") {
      fs.cpSync(files.packageDir, path.join(envPath, "pkg"), {
         recursive: true,
      });
   } else {
      writePackageFiles(
         path.join(envPath, "pkg"),
         files as Record<string, string>,
         { manifest: { name: "pkg", description: "conformance fixture" } },
      );
   }
   const lake = new DuckDBConnection(LAKE, path.join(root, `${LAKE}.duckdb`));
   let env: Environment | undefined;
   const dispose = async () => {
      await env?.closeAllConnections().catch(() => {});
      await lake.close().catch(() => {});
      fs.rmSync(root, { recursive: true, force: true });
   };
   let pkg: Package;
   try {
      env = await Environment.create("conform", envPath, []);
      await env.addPackage("pkg");
      pkg = await env.getPackage(parsePackageName("pkg"), false);
   } catch (error) {
      await dispose();
      throw error;
   }
   const lakeConfig = new MalloyConfig({ connections: {} });
   lakeConfig.wrapConnections(
      () => new FixedConnectionMap(new Map([[LAKE, lake]]), LAKE),
   );
   pkg.setServeDestinationConfig(() => lakeConfig);
   // The environment configures no destinations of its own (the lake is wired
   // straight to the package above), so its deployment facts would call `lake`
   // unconfigured and every unbuilt `storage=` source `destination_missing`
   // (P13). Report what this fixture actually wires.
   pkg.setDeploymentFacts({
      destinationNames: () => [LAKE],
      connectionCanHoldTable: () => undefined,
   });

   const run: LoadedFixture["run"] = async (modelPath, query, givens) => {
      const model = pkg.getModel(modelPath);
      if (!model) return { error: `${modelPath} did not load` };
      try {
         const result = await model.runQuery(
            unrecordedAudit(),
            { query, ...(givens ? { givens } : {}) } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         return {
            answer: {
               rows: result.compactResult as unknown as Row[],
               fields: (result.result?.schema?.fields ?? []).map((f) => ({
                  name: f.name,
                  // A nest's fields carry annotations too, with the same
                  // per-compile reference id.
                  type: withoutReferenceIds((f as { type?: unknown }).type),
                  annotations: (
                     (f as { annotations?: { value: string }[] }).annotations ??
                     []
                  ).map((a) =>
                     // A reference id is minted per compile, live or served.
                     a.value.trim().replace(/reference_id = "[^"]*"/g, ""),
                  ),
               })),
               servedFrom: result.servedFrom ?? null,
               servedBy: result.path?.servedBy?.kind ?? "live",
               ...(result.path?.servedBy?.via
                  ? { via: result.path.servedBy.via }
                  : {}),
            },
         };
      } catch (error) {
         return {
            error: error instanceof Error ? error.message : String(error),
         };
      }
   };

   return {
      run,
      dispose,
      pkg,
      root,
      lake,
      async bind(options: BindOptions = {}) {
         const { built, buildErrors, colocated, stored } = await buildAll(
            pkg,
            lake,
            options,
         );
         // The served copy compiles at bind, asynchronously; settle it so its
         // warnings and its first answer are not a race.
         const settle = async () => {
            for (const modelPath of pkg.getModelPaths()) {
               await pkg.getModel(modelPath)?.prepareServedCopy();
            }
         };
         await settle();
         return {
            pkg,
            built,
            buildErrors,
            stored,
            async rebind() {
               // Empty first, so the package derives every binding afresh.
               pkg.bindColocatedServeManifest({});
               pkg.bindColocatedServeManifest(colocated as never);
               pkg.bindStorageServeBindings(stored as never);
               await settle();
            },
            warnings: (pkg.getPackageMetadata().warnings ?? []).map((w) => ({
               code: (w as { code?: string }).code,
               subject: (w as { subject?: string }).subject,
               message: (w as { message?: string }).message,
            })),
            verdicts: pkg
               .getServeVerdicts()
               .filter((v) => v.origin !== "preaggregate")
               .map(
                  (v) =>
                     `${v.sourceName}:${v.verdict}${v.reason ? `:${v.reason}` : ""}`,
               ),
            run,
            dispose,
         };
      },
   };
}

/**
 * Build every planned table as a build would, one per content address:
 * colocated in the origin warehouse (`duckdb`), `storage=` into the lake. A
 * `storage=` table is computed on the origin and copied through Parquet, since
 * the origin reads the package's files by relative path and the lake cannot.
 */
async function buildAll(
   pkg: Package,
   lake: DuckDBConnection,
   options: BindOptions = {},
): Promise<{
   built: LoadedFixture["built"];
   buildErrors: Record<string, string>;
   colocated: Record<string, Record<string, unknown>>;
   stored: Record<string, Record<string, unknown>>;
}> {
   // Each source is built on its own connection, a DuckDB one: the
   // fixture's, or a real package's snapshot (v1_cutover_runner.ts).
   const warehouseFor = async (connectionName: string | undefined) =>
      (await pkg.getMalloyConnection(
         connectionName ?? "duckdb",
      )) as DuckDBConnection;
   const colocated: Record<string, Record<string, unknown>> = {};
   const stored: Record<string, Record<string, unknown>> = {};
   const built: LoadedFixture["built"] = [];
   const buildErrors: Record<string, string> = {};
   const seen = new Set<string>();
   const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "conform-build-"));
   // The declaring source first, so a table is recorded under it rather than
   // an extension that inherits its annotation.
   // Rollups only when asked (`#@ preaggregate`, `#(preaggregate)`), each
   // recorded with its origin, as a build records it.
   const entries = Object.values(pkg.getBuildPlan()?.sources ?? {})
      .filter((e) => options.includeRollups || e.origin !== "preaggregate")
      .sort(
         (a, b) =>
            Number(b.annotationFields?.name === b.name) -
            Number(a.annotationFields?.name === a.name),
      );
   try {
      for (const entry of entries) {
         if (seen.has(entry.sourceEntityId)) continue;
         seen.add(entry.sourceEntityId);
         // The name a build writes: `name=`, else the source's name, so two
         // sources resolving to one name overwrite each other as they would
         // in a real destination. Quoted in DDL; recorded unquoted, as a
         // build records it.
         const table = (entry.annotationFields?.name || entry.name).trim();
         const quoted = `"${table.replace(/"/g, '""')}"`;
         const storage = entry.annotationFields?.storage;
         try {
            const warehouse = await warehouseFor(entry.connectionName);
            if (storage) {
               const file = path.join(
                  scratch,
                  `${entry.sourceEntityId}.parquet`,
               );
               await warehouse.runSQL(
                  `COPY (${entry.sql}) TO '${file.replace(/'/g, "''")}' (FORMAT PARQUET)`,
               );
               const select = `SELECT * FROM read_parquet('${file.replace(/'/g, "''")}')`;
               // Cast to the declared types, as the storage build does.
               const present = new Set(
                  (
                     (
                        await lake.runSQL(`DESCRIBE ${select}`, {
                           rowLimit: 10_000,
                        })
                     ).rows as { column_name: string }[]
                  ).map((r) => r.column_name),
               );
               await lake.runSQL(
                  `CREATE OR REPLACE TABLE ${quoted} AS ${castToDeclaredTypes(
                     select,
                     declaredColumnsFor(pkg, entry).filter((c) =>
                        present.has(c.name),
                     ),
                  )}`,
               );
               const described = await lake.runSQL(`DESCRIBE ${quoted}`, {
                  rowLimit: 10_000,
               });
               stored[entry.sourceEntityId] = {
                  sourceEntityId: entry.sourceEntityId,
                  sourceName: entry.name,
                  physicalTableName: table,
                  connectionName: entry.connectionName,
                  storageDestinationName: storage,
                  realization: "COPY",
                  rowCount: null,
                  ...(entry.origin ? { origin: entry.origin } : {}),
                  schema: (
                     described.rows as {
                        column_name: string;
                        column_type: string;
                     }[]
                  ).map((r) => ({ name: r.column_name, type: r.column_type })),
               };
               built.push({ name: entry.name, tier: "storage" });
            } else {
               await warehouse.runSQL(
                  `CREATE OR REPLACE TABLE ${quoted} AS ${entry.sql}`,
               );
               // The warehouse's own column types, captured as a colocated
               // build captures them (colocated_native_types.ts).
               const nativeSchema = await capturedNativeSchema({
                  run: (sql) => warehouse.runSQL(sql, { rowLimit: 10_000 }),
                  physicalTableName: table,
                  dialect: warehouse.dialectName,
                  sourceName: entry.name,
               });
               colocated[entry.sourceEntityId] = {
                  tableName: table,
                  connectionName: entry.connectionName ?? "duckdb",
                  ...(nativeSchema ? { nativeSchema } : {}),
               };
               built.push({ name: entry.name, tier: "colocated" });
            }
         } catch (error) {
            buildErrors[entry.name] =
               error instanceof Error ? error.message : String(error);
         }
      }
   } finally {
      fs.rmSync(scratch, { recursive: true, force: true });
   }
   pkg.bindColocatedServeManifest(colocated as never);
   pkg.bindStorageServeBindings(stored as never);
   return { built, buildErrors, colocated, stored };
}

/**
 * The scalar dimensions the planned source declares, read from its model's
 * compiled definition: what a storage build casts its columns to. None when
 * the definition cannot be found (the build then runs uncast, as a build does
 * when it cannot read the field list).
 */
function declaredColumnsFor(
   pkg: Package,
   entry: { name: string; modelPath?: string },
): DeclaredColumn[] {
   const def = entry.modelPath
      ? pkg.getModel(entry.modelPath)?.getModelDef()
      : undefined;
   const struct = def?.contents?.[entry.name] as
      | { fields?: { expressionType?: string }[] }
      | undefined;
   return declaredColumnsOf(
      (struct?.fields ?? []).filter(
         (f) => f.expressionType === undefined || f.expressionType === "scalar",
      ),
   );
}

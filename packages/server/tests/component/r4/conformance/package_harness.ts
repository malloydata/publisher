// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package-level runner for the conformance fixture (ms2data/service#7098):
 * a synthetic package goes through the real production path (Environment,
 * Package.create, the package's own build plan, its colocated manifest bind
 * and its `storage=` bind), and every query runs live first, then served with
 * the materialize switch off (today's serve shape) and on (the served copy).
 *
 * The R4 model harness (../harness.ts) drives one Model with bindings it
 * derives itself; this one lets the package derive them, so it sees what a
 * served package sees: bindings across models, a joined source declared in
 * another file, two models declaring one name, and the serve verdicts.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   type GivenValue,
   MalloyConfig,
} from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../../../src/service/access_record";
import { __setPackageLoadPoolForTests } from "../../../../src/package_load/package_load_pool";
import { Environment } from "../../../../src/service/environment";
import { parsePackageName } from "../../../../src/service/identity";
import { QueryPrivileges } from "../../../../src/service/model_query_request";
import type { Package } from "../../../../src/service/package";
import { installSettings } from "../../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../../src/test_helpers/temp_package";
import {
   castToDeclaredTypes,
   type DeclaredColumn,
   declaredColumnsOf,
} from "../../../../src/service/materialization/stored_column_types";
import type { Row } from "../harness";

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

/** The switch a served run is taken under. */
export type Switch = "off" | "on";

function settingsFor(materialize: Switch) {
   return {
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: materialize === "on" ? "true" : "false",
      },
   };
}

/**
 * Install the fixture's settings with the materialize switch as given. The
 * router reads it per query, so a loaded package can be queried both ways.
 */
export function withSwitch(materialize: Switch): () => void {
   return installSettings(settingsFor(materialize)).restore;
}

/**
 * Drop the package-load worker pool, so the next load spawns workers with the
 * settings installed then. A worker reads its settings once, at spawn, and the
 * build plan (with `#(materialize)` in it or not) is computed in the worker:
 * a pool another spec file spawned would plan with that file's settings.
 */
export async function freshLoadPool(): Promise<void> {
   await __setPackageLoadPoolForTests(null);
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
   run(
      modelPath: string,
      query: string,
      givens?: Record<string, GivenValue>,
   ): Promise<Outcome>;
   dispose(): Promise<void>;
}

/**
 * Write `files` as a package under a fresh environment, load it, and return a
 * handle that can build and bind it (`bind`) and query it. Queries before
 * `bind` are live; after it, served.
 */
export async function loadFixture(files: Record<string, string>): Promise<{
   run: LoadedFixture["run"];
   bind(): Promise<LoadedFixture>;
   dispose(): Promise<void>;
}> {
   const root = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-conform-"));
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath, { recursive: true });
   writePackageFiles(path.join(envPath, "pkg"), files, {
      manifest: { name: "pkg", description: "conformance fixture" },
   });
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
                  type: (f as { type?: unknown }).type,
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
      async bind() {
         const { built, buildErrors } = await buildAll(pkg, lake);
         // The served copy compiles at bind, asynchronously; settle it so its
         // warnings and its first answer are not a race.
         for (const modelPath of pkg.getModelPaths()) {
            await pkg.getModel(modelPath)?.prepareServedCopy();
         }
         return {
            pkg,
            built,
            buildErrors,
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
): Promise<{
   built: LoadedFixture["built"];
   buildErrors: Record<string, string>;
}> {
   const warehouse = (await pkg.getMalloyConnection(
      "duckdb",
   )) as DuckDBConnection;
   const colocated: Record<string, Record<string, unknown>> = {};
   const stored: Record<string, Record<string, unknown>> = {};
   const built: LoadedFixture["built"] = [];
   const buildErrors: Record<string, string> = {};
   const seen = new Set<string>();
   const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "conform-build-"));
   // The declaring source first, so a table is recorded under it rather than
   // an extension that inherits its annotation.
   const entries = Object.values(pkg.getBuildPlan()?.sources ?? {})
      .filter((e) => e.origin !== "preaggregate")
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
               colocated[entry.sourceEntityId] = {
                  tableName: table,
                  connectionName: entry.connectionName ?? "duckdb",
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
   return { built, buildErrors };
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

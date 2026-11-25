// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shared setup of the materialization specs: a temporary package
 * directory, a DuckDB connection in it (thread-capped by the test preload,
 * `duckdb_test_threads.ts`, and file-backed, so no other spec file shares its
 * tables), a runtime over an in-memory overlay of model files, and the three
 * things the specs do with them:
 *
 * - `compile`: a model's text, or several files, to its compiled definition;
 * - `plan`: a package's files through the package's own build-plan compile
 *   (`compilePackageBuildPlan`), with `PUBLISHER_MATERIALIZE` on unless the
 *   spec installs other settings;
 * - `servedCopy`: a model's files written as a package's are, so a served
 *   copy (which reads the author's own files) can re-root them.
 *
 * Each spec file takes one fixture with `materializationFixture()`, which
 * closes it after the file, or one plan-package set with `planPackages()`,
 * which installs and restores its settings around each test.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type Model,
   type ModelDef,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeEach } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import type { FinishedBuildPlan } from "../service/materialization/plan/build_plan";
import { compilePackageBuildPlan } from "../service/materialization/plan/build_plan_compile";
import type { MaterializePlan } from "../service/materialization/domain/table_plans";
import { tempBuildPlanPackages } from "./incremental_test_harness";
import { materializePlansIn, sourcesIn } from "./materialization_test_fixtures";
import { installSettings } from "./settings_env";

/** Model files by path, or one model's text (read as `m.malloy`). */
export type ModelFiles = string | Record<string, string>;

function filesOf(files: ModelFiles): Record<string, string> {
   return typeof files === "string" ? { "m.malloy": files } : files;
}

/**
 * A model's files written to a fresh directory, as a package's are: the file
 * URLs to compile from, the directory to hand `setServedCopyContext`, and the
 * text of each file by URL for an in-memory reader.
 */
export interface ServedCopyFiles {
   dir: string;
   /** The directory's URL, with a trailing slash, for `importBaseURL`. */
   root: string;
   urlOf: (file: string) => string;
   /** Each file's text by its URL, for an `InMemoryURLReader`. */
   byUrl: Map<string, string>;
}

/** Write `files` (name to text) to a fresh temporary directory under `parent`. */
function writeServedCopyFiles(
   files: Record<string, string>,
   parent: string,
): ServedCopyFiles {
   const dir = fs.mkdtempSync(path.join(parent, "served-copy-"));
   // pathToFileURL, as Malloy normalizes it, so the copy's location URLs match.
   const urlOf = (file: string) => pathToFileURL(path.join(dir, file)).href;
   const byUrl = new Map<string, string>();
   for (const [file, text] of Object.entries(files)) {
      fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
      fs.writeFileSync(path.join(dir, file), text);
      byUrl.set(urlOf(file), text);
   }
   return { dir, root: `${pathToFileURL(dir).href}/`, urlOf, byUrl };
}

/** Where `compile` and `runtime` place their in-memory files. */
export interface CompileOptions {
   /** The URL the files sit under, with a trailing slash (default `file:///pkg/`). */
   root?: string;
   /** The file to compile (default the first). */
   entry?: string;
}

export interface MaterializationFixture {
   /** The temporary package directory, made on first use. */
   readonly dir: string;
   /** A DuckDB connection named `duckdb`, file-backed in {@link dir}. */
   readonly duckdb: DuckDBConnection;
   /** {@link duckdb} as the only connection, and the default. */
   readonly connections: FixedConnectionMap;
   /** A runtime reading `files` from memory under `root` (default `file:///pkg/`). */
   runtime(files: ModelFiles, root?: string): Runtime;
   /** Compile `files` and return the entry's compiled model. */
   model(files: ModelFiles, options?: CompileOptions): Promise<Model>;
   /** Compile `files` and return the entry's model definition. */
   compile(files: ModelFiles, options?: CompileOptions): Promise<ModelDef>;
   /** Write `files` to a fresh directory in {@link dir}, as a package's are. */
   servedCopy(files: ModelFiles): ServedCopyFiles;
   /** Close the connection and remove the directory. */
   close(): Promise<void>;
}

/** A fixture whose directory and connection are made on first use. */
function openFixture(): MaterializationFixture {
   let dir: string | undefined;
   let duckdb: DuckDBConnection | undefined;
   let connections: FixedConnectionMap | undefined;
   const fixture: MaterializationFixture = {
      get dir() {
         dir ??= fs.mkdtempSync(path.join(os.tmpdir(), "publisher-mz-spec-"));
         return dir;
      },
      get duckdb() {
         duckdb ??= new DuckDBConnection(
            "duckdb",
            path.join(fixture.dir, "duckdb.db"),
         );
         return duckdb;
      },
      get connections() {
         connections ??= new FixedConnectionMap(
            new Map([["duckdb", fixture.duckdb]]),
            "duckdb",
         );
         return connections;
      },
      runtime(files, root = "file:///pkg/") {
         return new Runtime({
            urlReader: new InMemoryURLReader(
               new Map(
                  Object.entries(filesOf(files)).map(([file, text]) => [
                     `${root}${file}`,
                     text,
                  ]),
               ),
            ),
            connections: fixture.connections,
         });
      },
      async model(files, options = {}) {
         const root = options.root ?? "file:///pkg/";
         const all = filesOf(files);
         const entry = options.entry ?? Object.keys(all)[0];
         return fixture
            .runtime(all, root)
            .loadModel(new URL(`${root}${entry}`))
            .getModel();
      },
      async compile(files, options) {
         return (await fixture.model(files, options))._modelDef;
      },
      servedCopy(files) {
         return writeServedCopyFiles(filesOf(files), fixture.dir);
      },
      async close() {
         await duckdb?.close().catch(() => {});
         if (dir) fs.rmSync(dir, { recursive: true, force: true });
         duckdb = undefined;
         connections = undefined;
         dir = undefined;
      },
   };
   return fixture;
}

/**
 * The spec file's fixture, its directory and connection made on first use
 * and closed after the file.
 */
export function materializationFixture(): MaterializationFixture {
   const fixture = openFixture();
   afterAll(() => fixture.close());
   return fixture;
}

/** A package's build plan as `compilePackageBuildPlan` returns it. */
export type CompiledPlan = FinishedBuildPlan;

/** The package a plan was compiled from, with the plan. */
export interface PlannedPackage {
   pkg: Awaited<ReturnType<ReturnType<typeof tempBuildPlanPackages>["create"]>>;
   c: CompiledPlan;
}

export interface PlanPackages {
   /** Write `files` as a package and compile its build plan. */
   plan(
      files: ModelFiles,
      overrides?: Parameters<
         ReturnType<typeof tempBuildPlanPackages>["create"]
      >[1],
   ): Promise<PlannedPackage>;
   /** Write `files` as a package, uncompiled. */
   create: ReturnType<typeof tempBuildPlanPackages>["create"];
   /**
    * Install `env` over `PUBLISHER_MATERIALIZE=true` for the rest of this
    * test, in place of what was installed before it.
    */
   settings(env?: Record<string, string>): void;
}

/**
 * Temporary packages whose build plans the spec compiles, with
 * `PUBLISHER_MATERIALIZE=true` (and `env`) installed before each test and
 * restored, and the packages removed, after it.
 */
export function planPackages(
   env: Record<string, string> = {},
   /** Passed to every `plan`'s compile (see compilePackageBuildPlan). */
   compileOptions: Parameters<typeof compilePackageBuildPlan>[2] = {},
): PlanPackages {
   const packages = tempBuildPlanPackages();
   let restore: (() => void) | undefined;
   const settings = (more: Record<string, string> = env) => {
      restore?.();
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true", ...more },
      }).restore;
   };
   beforeEach(() => settings());
   afterEach(async () => {
      restore?.();
      restore = undefined;
      await packages.cleanup();
   });
   return {
      create: packages.create,
      settings,
      async plan(files, overrides) {
         const pkg = await packages.create(files, overrides);
         return {
            pkg,
            c: await compilePackageBuildPlan(pkg, undefined, compileOptions),
         };
      },
   };
}

/** The `#(materialize)` plan of the source named `name`, if it was planned. */
export function materializePlanNamed(
   c: CompiledPlan,
   name: string,
): MaterializePlan | undefined {
   return Object.values(materializePlansIn(c) ?? {}).find(
      (p) => p.sourceName === name,
   );
}

/** Why `source` is not built (its `materialize-not-built` finding), if it is not. */
export function notBuiltMessage(
   c: CompiledPlan,
   source: string,
): string | undefined {
   return (c.annotationFindings ?? []).find(
      (f) => f.code === "materialize-not-built" && f.subject === source,
   )?.message;
}

/** The planned persist source named `name`. */
export function plannedSourceNamed(c: CompiledPlan, name: string) {
   return Object.values(sourcesIn(c)).find((s) => s.name === name);
}

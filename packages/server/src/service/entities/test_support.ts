// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test-only helpers for the entities extraction specs: a throwaway on-disk
 * package, an in-process DuckDB connection, and a one-call "compile these
 * files and extract their entities" that compiles each file with Malloy the
 * way a load does and hands the compiled models to {@link
 * extractPackageEntities}. Which sources are queryable is asked of a real
 * {@link Package} loaded over the same files, as a load asks it.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   type Connection,
   type InfoConnection,
   type LookupConnection,
   Malloy,
   type Model,
   Runtime,
} from "@malloydata/malloy";
import { readFileSync } from "node:fs";
import {
   mkdir,
   mkdtemp,
   readFile,
   rename,
   rm,
   stat,
   writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { errMessage } from "../../utils";
import { Package } from "../package";
import {
   type EntitiesModelFile,
   type EntitiesQueryRoute,
   extractPackageEntities,
   type PackageEntitiesExtraction,
} from "./extract";
import type { ModelCompileResult } from "./internal_types";
import { FileURLReader } from "./field_helpers";

/**
 * The package's discovery surface, as `publisher.json` resolves it. Both fields
 * optional: a package with no surface keeps every compiled source queryable.
 */
export type PackageManifest = {
   /** Model file paths that form the surface, as `explores` resolves. */
   explores?: string[];
   /** `"declared"` (default) makes the surface a query boundary; `"all"` decouples them. */
   queryableSources?: "declared" | "all";
};

/**
 * The query route of a package that curates nothing: every compiled source
 * runs and no file is read as a report, which is what a {@link Package} with
 * no surface answers too.
 */
export const UNCURATED_QUERY_ROUTE: EntitiesQueryRoute = Object.freeze({
   boundaryActive: false,
   admitsSource: () => true,
   isServedReport: () => false,
   servedSourceOf: () => undefined,
});

/** A single file to write inside a workspace, by workspace-relative path. */
export interface WorkspaceFile {
   relpath: string;
   contents: string;
}

/** A temp workspace plus a cleanup handle. */
export interface Workspace {
   dir: string;
   cleanup: () => Promise<void>;
}

/** Write each file into a fresh temp directory. */
export async function writeWorkspace(
   files: WorkspaceFile[],
   prefix = "entities-spec-",
): Promise<Workspace> {
   const dir = await mkdtemp(path.join(tmpdir(), prefix));
   try {
      for (const file of files) {
         const absolutePath = path.join(dir, file.relpath);
         await mkdir(path.dirname(absolutePath), { recursive: true });
         await writeFile(absolutePath, file.contents, "utf-8");
      }
   } catch (err) {
      await rm(dir, { recursive: true, force: true });
      throw err;
   }
   return {
      dir,
      cleanup: () => rm(dir, { recursive: true, force: true }),
   };
}

/** A package file, by its path relative to the package root. */
export type FilePointer = {
   packageRootPath: string;
   absolutePath: string;
   relativePath: string;
};

/** The files to compile and the package's discovery surface. */
export type CompilePackageTask = {
   modelFiles: FilePointer[];
   manifest?: PackageManifest;
};

/**
 * A lookup serving one in-process DuckDB connection, named `duckdb`, that reads
 * files relative to `workingDirectory`.
 */
export class ConnectionLookup implements LookupConnection<InfoConnection> {
   private readonly duckdb: DuckDBConnection;

   private constructor(workingDirectory?: string) {
      this.duckdb = new DuckDBConnection(
         "duckdb",
         ":memory:",
         workingDirectory,
      );
   }

   /** Mirrors the shape of a lookup built from a host's connection list. */
   public static async getInstance(
      _connections: unknown[],
      workingDirectory?: string,
   ): Promise<ConnectionLookup> {
      return new ConnectionLookup(workingDirectory);
   }

   public async lookupConnection(name?: string): Promise<Connection> {
      if (name === undefined || name === "duckdb") return this.duckdb;
      throw new Error(`No connection named ${name}`);
   }

   public async dispose(): Promise<void> {
      await this.duckdb.close();
   }
}

/** Compile one model file with Malloy, as a load does. */
async function compileModelFile(
   file: FilePointer,
   connections: LookupConnection<InfoConnection>,
): Promise<EntitiesModelFile> {
   try {
      const text = await readFile(file.absolutePath, "utf-8");
      const url = new URL("file://" + file.absolutePath);
      const importBaseURL = new URL(
         "file://" + path.dirname(file.absolutePath) + "/",
      );
      const urlReader = new FileURLReader();
      const model: Model = await Malloy.compile({
         urlReader,
         connections,
         parse: await Malloy.parse({
            url,
            urlReader,
            options: { importBaseURL },
         }),
      });
      return {
         relativePath: file.relativePath,
         absolutePath: file.absolutePath,
         model,
         text,
      };
   } catch (error) {
      return {
         relativePath: file.relativePath,
         absolutePath: file.absolutePath,
         error: errMessage(error),
      };
   }
}

/**
 * The query route of the package at `packageRoot` under `manifest`: a real
 * {@link Package} loaded over the files there, with a `publisher.json` that
 * declares exactly this surface. Without a manifest, the package curates
 * nothing.
 *
 * A package with a model that does not compile does not load, so the files
 * in `uncompiled` are set aside while it loads. They would add nothing to the
 * surface anyway: a file that failed to compile exports nothing.
 */
async function queryRouteFor(
   packageRoot: string,
   manifest: PackageManifest | undefined,
   connections: LookupConnection<InfoConnection>,
   uncompiled: readonly string[],
): Promise<EntitiesQueryRoute> {
   if (!manifest) return UNCURATED_QUERY_ROUTE;
   const setAside = uncompiled.map((relativePath) => {
      const absolutePath = path.join(packageRoot, relativePath);
      return { absolutePath, aside: `${absolutePath}.uncompiled` };
   });
   const publisherJson = path.join(packageRoot, "publisher.json");
   const previous = await stat(publisherJson).then(
      () => readFile(publisherJson, "utf-8"),
      () => undefined,
   );
   const moved: typeof setAside = [];
   try {
      for (const file of setAside) {
         await rename(file.absolutePath, file.aside);
         moved.push(file);
      }
      // An explicit `explores`, so a root index.malloy curates only when the
      // manifest says it does.
      await writeFile(
         publisherJson,
         JSON.stringify({
            name: "entities-spec",
            explores: manifest.explores ?? [],
            ...(manifest.queryableSources !== undefined && {
               queryableSources: manifest.queryableSources,
            }),
         }),
         "utf-8",
      );
      const pkg = await Package.create(
         "entities-spec",
         "entities-spec",
         packageRoot,
         new Map([
            [
               "duckdb",
               // The spec lookup serves a full DuckDB connection.
               (await connections.lookupConnection("duckdb")) as Connection,
            ],
         ]),
      );
      return pkg.entitiesQueryRoute();
   } finally {
      if (previous === undefined) await rm(publisherJson, { force: true });
      else await writeFile(publisherJson, previous, "utf-8");
      for (const file of moved) await rename(file.aside, file.absolutePath);
   }
}

/** Compile every file of the task, then extract the package's entities and findings. */
async function extractPackage(
   task: CompilePackageTask,
   connections: LookupConnection<InfoConnection>,
): Promise<PackageEntitiesExtraction> {
   const modelFiles = await Promise.all(
      task.modelFiles.map((file) => compileModelFile(file, connections)),
   );
   const packageRoot = task.modelFiles[0]?.packageRootPath ?? path.resolve(".");
   return extractPackageEntities({
      packageRoot,
      modelFiles,
      queryRoute: await queryRouteFor(
         packageRoot,
         task.manifest,
         connections,
         modelFiles.flatMap((f) => ("error" in f ? [f.relativePath] : [])),
      ),
      connections,
   });
}

/** {@link extractPackage}'s per-model results. */
async function compilePackageModelFiles(
   task: CompilePackageTask,
   connections: LookupConnection<InfoConnection>,
): Promise<ModelCompileResult[]> {
   return (await extractPackage(task, connections)).results;
}

export const packageCompilationService = {
   compilePackageModelFiles,
   extractPackage,
};

/**
 * Write `files` (models and the data they read) to a temp package, compile
 * every `.malloy` among them as a load does, extract the package's entities,
 * and remove the package.
 */
export async function compileFixture(
   files: Record<string, string>,
   manifest?: PackageManifest,
): Promise<ModelCompileResult[]> {
   return (await extractFixture(files, manifest)).results;
}

/** {@link compileFixture}, with the findings the extraction reported. */
export async function extractFixture(
   files: Record<string, string>,
   manifest?: PackageManifest,
): Promise<PackageEntitiesExtraction> {
   const ws = await writeWorkspace(
      Object.entries(files).map(([relpath, contents]) => ({
         relpath,
         contents,
      })),
   );
   const lookup = await ConnectionLookup.getInstance([], ws.dir);
   try {
      const modelFiles = Object.keys(files)
         .filter((relpath) => relpath.endsWith(".malloy"))
         .sort()
         .map((relpath) => ({
            packageRootPath: ws.dir,
            absolutePath: path.join(ws.dir, relpath),
            relativePath: relpath,
         }));
      return await extractPackage({ modelFiles, manifest }, lookup);
   } finally {
      await lookup.dispose();
      await ws.cleanup();
   }
}

/**
 * Write `files` under `root` and compile `entry` against an in-process DuckDB
 * holding a one-row table `t`, for a test that walks the compiled explores
 * itself.
 */
export async function compileEntryModel(
   root: string,
   files: Record<string, string>,
   entry: string,
): Promise<Model> {
   for (const [relpath, contents] of Object.entries(files)) {
      const absolutePath = path.join(root, relpath);
      await mkdir(path.dirname(absolutePath), { recursive: true });
      await writeFile(absolutePath, contents, "utf-8");
   }
   const connection = new DuckDBConnection("duckdb");
   try {
      await connection.runRawSQL("CREATE OR REPLACE TABLE t AS SELECT 1 as id");
      const runtime = new Runtime({
         urlReader: new FileURLReader(),
         connection,
      });
      return await runtime.getModel(
         new URL("file://" + path.join(root, entry)),
      );
   } finally {
      await connection.close();
   }
}

/** The root model a package with no `explores` surfaces. */
const INDEX_MODEL_NAME = "index.malloy";

/**
 * The discovery surface `publisher.json` declares, read from the package files
 * the way a load resolves it: `explores` as written, `["index.malloy"]` when
 * the key is absent and the package has a root `index.malloy`, no surface
 * otherwise. Undefined when there is no readable `publisher.json`.
 */
function readPackageManifest(
   packageFiles: FilePointer[],
): PackageManifest | undefined {
   const file = packageFiles.find((f) => f.relativePath === "publisher.json");
   if (!file) return undefined;
   let parsed: unknown;
   try {
      parsed = JSON.parse(readFileSync(file.absolutePath, "utf-8"));
   } catch {
      return undefined;
   }
   if (parsed === null) return undefined;
   const raw = parsed as { explores?: unknown; queryableSources?: unknown };
   const queryableSources =
      raw.queryableSources === "all" || raw.queryableSources === "declared"
         ? raw.queryableSources
         : undefined;
   let explores: string[] | undefined;
   if (raw.explores === undefined) {
      explores = packageFiles.some((f) => f.relativePath === INDEX_MODEL_NAME)
         ? [INDEX_MODEL_NAME]
         : undefined;
   } else if (
      Array.isArray(raw.explores) &&
      raw.explores.every((e) => typeof e === "string")
   ) {
      explores = raw.explores as string[];
   } else {
      throw new Error(`Invalid "explores" in publisher.json`);
   }
   return { explores, queryableSources };
}

export const packageManagementService = { readPackageManifest };

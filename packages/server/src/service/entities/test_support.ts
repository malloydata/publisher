// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test-only helpers for the entities extraction specs: a throwaway on-disk
 * package, an in-process DuckDB connection, and a one-call "compile these
 * files and extract their entities" that compiles each file with Malloy the
 * way a load does and hands the compiled models to {@link
 * extractPackageEntities}.
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
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { errMessage } from "../../utils";
import {
   type EntitiesModelFile,
   extractPackageEntities,
   type PackageEntitiesExtraction,
} from "./extract";
import type { ModelCompileResult } from "./internal_types";
import { FileURLReader } from "./field_helpers";
import type { PackageManifest } from "./query_surface";

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
      manifest: task.manifest,
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Generated files: the one way materialization compiles Malloy text it wrote
 * (the Compiler port). Author files and an overlay go in; a compiled model,
 * or a failure traced to what it would have planned, comes out. Nothing here
 * runs SQL.
 *
 * Every generated companion (a rollup, `#(materialize)` or persisted-view
 * build source, a v1 build copy), every serve variant (the served copy, the
 * rollup and view legs' declaring-file variants) and every stand-in compile
 * (a chained build's text on its destination) is compiled through it:
 *
 *  - {@link GeneratedFiles.over} gives a compiler over a package's files
 *    through a runtime factory: a host's (the load worker's, which reads its
 *    own file map), or `package_files.ts`'s, which reads them from disk;
 *  - {@link GeneratedFiles.read} is the one reader of an author file's text
 *    as the compile reads it (a worker's files, an overlay), never the
 *    file on disk behind it;
 *  - {@link GeneratedFiles.blanked} is the author's files with every
 *    source-level `#@` note blanked (`persist_blanking.ts`);
 *  - {@link GeneratedFiles.load} and {@link GeneratedFiles.compile} compile
 *    an entry file over them and an overlay, served at URLs inside the
 *    package so a generated file's `import` of the author's model resolves
 *    while it never appears among the package's models;
 *  - {@link compileInMemory} compiles text that names nothing on disk, over
 *    the connections it is handed;
 *  - {@link companionOmissions} turns a companion's failure into the
 *    `omittedSources` and `omittedFiles` it stands for, one reason per
 *    companion kind.
 *
 * With `package_files.ts`, the module's only caller of `createModelRuntime`
 * and of `new Runtime`. Kept apart from it so the load worker, which
 * compiles through its own factory, never loads the server's model runtime.
 * It is the one writer of an overlay's keys: a caller names each file by its
 * URL however it holds it, and every overlay is keyed here as the compiler
 * asks for the file (`overlay_key.ts`), so no two spellings of one file can
 * both be served.
 */

import * as path from "path";
import { fileURLToPath } from "url";
import {
   InMemoryURLReader,
   Runtime,
   type FixedConnectionMap,
   type Model as MalloyModel,
   type ModelMaterializer,
} from "@malloydata/malloy";
import {
   omittedByID,
   omittedFromFile,
   type Omission,
   type OmittedSourceReason,
} from "../domain/omitted_sources";
import { keyedOverlay } from "../../overlay_key";
import { persistBlankedFiles } from "./persist_blanking";

/**
 * A runtime over the author's package with `overlay`'s files served in place
 * of (or beside) the package's own, by URL.
 */
export type SynthesizedRuntimeFactory = (
   overlay: ReadonlyMap<string, string>,
) => Promise<{ runtime: Runtime; importBaseURL: URL }>;

/** A factory's runtime, with the package model's own URL when it knows it. */
interface PackageRuntime {
   runtime: Runtime;
   importBaseURL: URL;
   modelURL?: URL;
}

/** A loaded entry file: its runtime, and the materializer that compiles it. */
interface LoadedFiles {
   runtime: Runtime;
   importBaseURL: URL;
   materializer: ModelMaterializer;
}

/** A compiled entry file. */
interface CompiledFiles extends LoadedFiles {
   model: MalloyModel;
}

/** A URL reader's answer as text. */
function textOf(read: string | { contents: string }): string {
   return typeof read === "string" ? read : read.contents;
}

/** Compiles generated files over one package's author files. */
export class GeneratedFiles {
   private reader?: Promise<PackageRuntime>;

   private constructor(
      private readonly runtimeFor: (
         overlay: ReadonlyMap<string, string>,
      ) => Promise<PackageRuntime>,
   ) {}

   /**
    * Over a runtime factory: a host's (the load worker's),
    * or a package's files on disk (`package_files.ts`), which also names
    * the package's own model.
    */
   static over(
      factory: (
         overlay: ReadonlyMap<string, string>,
      ) => Promise<PackageRuntime>,
   ): GeneratedFiles {
      return new GeneratedFiles(factory);
   }

   /**
    * Load the package's own model (the `modelPath` of `packageFiles`)
    * over `overlay`. Throws for a compiler over a host's factory, which
    * names no model of its own.
    */
   async loadModel(
      overlay: ReadonlyMap<string, string> = new Map(),
   ): Promise<LoadedFiles> {
      const { runtime, importBaseURL, modelURL } = await this.runtimeFor(
         keyedOverlay(overlay),
      );
      if (!modelURL) throw new Error("no package model to load");
      const materializer = runtime.loadModel(modelURL, { importBaseURL });
      return { runtime, importBaseURL, materializer };
   }

   /** A runtime over the author files and `overlay`. */
   runtime(
      overlay: ReadonlyMap<string, string> = new Map(),
   ): Promise<{ runtime: Runtime; importBaseURL: URL }> {
      return this.runtimeFor(overlay);
   }

   /** The reader author files are read through, made on the first read. */
   private async readerRuntime(): Promise<Runtime> {
      this.reader ??= this.runtimeFor(new Map());
      return (await this.reader).runtime;
   }

   /** An author file's text as the compile reads it. Throws when it cannot be read. */
   async read(url: URL | string): Promise<string> {
      const runtime = await this.readerRuntime();
      return textOf(
         await runtime.urlReader.readURL(
            typeof url === "string" ? new URL(url) : url,
         ),
      );
   }

   /**
    * {@link read}, or undefined when the file cannot be read. A reader that
    * cannot be made at all throws: that is the compiler failing, not a file.
    */
   async readIfAny(url: URL | string): Promise<string | undefined> {
      const runtime = await this.readerRuntime();
      try {
         return textOf(
            await runtime.urlReader.readURL(
               typeof url === "string" ? new URL(url) : url,
            ),
         );
      } catch {
         return undefined;
      }
   }

   /**
    * The files `modelDef` reads with every source-level `#@` note blanked
    * but those in `keep`, by overlay key; undefined when a note cannot be
    * blanked safely (`persist_blanking.ts`).
    */
   blanked(
      modelDef: unknown,
      keep?: ReadonlySet<string>,
   ): Promise<Map<string, string> | undefined> {
      return persistBlankedFiles(modelDef, (url) => this.readIfAny(url), keep);
   }

   /** Load `entry` over the author files and `overlay`; nothing compiled yet. */
   async load(
      entry: URL,
      overlay: ReadonlyMap<string, string> = new Map(),
   ): Promise<LoadedFiles> {
      const { runtime, importBaseURL } = await this.runtimeFor(
         keyedOverlay(overlay),
      );
      const materializer = runtime.loadModel(entry, { importBaseURL });
      return { runtime, importBaseURL, materializer };
   }

   /** Compile `entry` over the author files and `overlay`. Throws on a Malloy error. */
   async compile(
      entry: URL,
      overlay: ReadonlyMap<string, string> = new Map(),
   ): Promise<CompiledFiles> {
      const loaded = await this.load(entry, overlay);
      return { ...loaded, model: await loaded.materializer.getModel() };
   }
}

/**
 * A runtime over `files` alone, by URL, with `connections`: for text that
 * names nothing on disk, such as a build's text on a stand-in for its
 * destination.
 */
export function inMemoryRuntime(
   files: ReadonlyMap<string, string>,
   connections: FixedConnectionMap,
): Runtime {
   return new Runtime({
      urlReader: new InMemoryURLReader(new Map(files)),
      connections,
   });
}

/** Compile `entry` from `files` alone (see {@link inMemoryRuntime}). */
export async function compileInMemory(
   files: ReadonlyMap<string, string>,
   entry: string,
   connections: FixedConnectionMap,
   options: { importBaseURL?: URL } = {},
): Promise<{ runtime: Runtime; model: MalloyModel }> {
   const runtime = inMemoryRuntime(files, connections);
   const model = await runtime
      .loadModel(new URL(entry), options.importBaseURL ? options : undefined)
      .getModel();
   return { runtime, model };
}

/**
 * A generated companion that failed to plan or compile: a publisher bug (the
 * text is generated), which costs its model that companion's tables and
 * nothing else. Returned, never logged here: the plan's caller reports it.
 */
export interface CompanionFailure {
   kind: CompanionKind;
   /** What failed, which the caller logs by its code, never its text. */
   error: unknown;
}

/** {@link CompanionFailure} of `error`. */
export function companionFailure(
   kind: CompanionKind,
   error: unknown,
): CompanionFailure {
   return { kind, error };
}

/** The kinds of generated companion a plan compiles. */
type CompanionKind =
   | "rollup"
   | "rollup_blanking"
   | "materialize"
   | "persisted_view"
   | "v1_build";

/** The omission reason a failed companion of each kind is reported with. */
const COMPANION_FAILED = {
   rollup: "rollup_companion_failed",
   rollup_blanking: "rollup_companion_blanking_failed",
   materialize: "materialize_companion_failed",
   persisted_view: "persisted_view_companion_failed",
   v1_build: "v1_build_companion_failed",
} as const satisfies Record<CompanionKind, OmittedSourceReason>;

/**
 * What a failed companion of `kind` leaves out of the plan: the sources it
 * would have planned, by the names its file declares them under
 * (`<name>@<file>`) or by the author's sourceIDs; or, when planning failed
 * before it could name them, the file standing for all of them.
 */
export function companionOmissions(
   kind: CompanionKind,
   planned:
      | { file: string; names: readonly string[] | undefined }
      | { sourceIDs: Iterable<string> },
): Omission[] {
   const reason = COMPANION_FAILED[kind];
   return "sourceIDs" in planned
      ? omittedByID(planned.sourceIDs, reason)
      : omittedFromFile(planned.file, planned.names, reason);
}

/**
 * Whether a declaring file is `modelFile`, the model a companion plans: a
 * declaration is planned only in the model that declares it, however many
 * models import it. A declaring URL that is not a local file (a host,
 * another scheme) reads as `otherScheme`.
 */
export function declaredInModel(
   modelFile: string,
   otherScheme: boolean,
): (declaringURL: string) => boolean {
   return (declaringURL) => {
      try {
         return path.resolve(fileURLToPath(declaringURL)) === modelFile;
      } catch {
         return otherScheme;
      }
   };
}

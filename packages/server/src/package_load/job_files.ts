// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What one package load shares across the `.malloy` models it compiles
 * (#1566).
 *
 * Each model used to compile in a Runtime of its own, so every file that
 * imports another re-read and re-translated it. A root `index.malloy` that
 * imports every model translated the whole package a second time, and all of
 * it ran at once on the load worker's one thread: the slowest translations
 * of a large package took most of a minute, and the pool's other loads queued
 * behind it.
 *
 * Here one Malloy CacheManager serves every model's imports, so each file is
 * translated once per load, and the models compile in import order so a
 * file's translation is cached before any importer asks for it.
 */
import {
   CacheManager,
   InMemoryModelCache,
   type URLReader,
} from "@malloydata/malloy";
import type { DataStyles } from "@malloydata/render";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { MODEL_FILE_SUFFIX } from "../constants";
import { dataStylesForFile } from "../data_styles";

type TextReader = { readURL: (url: URL) => Promise<string> };

/**
 * A model whose imports come from the cache never reads them, so what a
 * model needs from the files it imports is kept here rather than per model:
 * the text of every file read (a load's files cannot change during it, and
 * the entity plan slices imported definitions out of them), and each file's
 * own data styles.
 */
export interface JobFiles {
   /** Reads each URL once, however many models ask for it at once. */
   reader: TextReader;
   /** Every text read in the load, by URL. */
   texts: Map<string, string>;
   /** Each file's own `--! styles` data styles, by URL. */
   stylesByUrl: Map<string, DataStyles>;
   /** The load's model cache; give each model a ModelCacheView of it. */
   cache: CacheManager;
   /**
    * The URLs of the package's `.malloy` files another package file imports,
    * filled in by compileLevels. Only these are worth keeping in the cache.
    */
   imported: Set<string>;
}

export function jobFiles(inner: TextReader): JobFiles {
   const texts = new Map<string, string>();
   const stylesByUrl = new Map<string, DataStyles>();
   const reads = new Map<string, Promise<string>>();
   const reader: TextReader = {
      readURL: (url: URL): Promise<string> => {
         const key = url.toString();
         let read = reads.get(key);
         if (!read) {
            read = (async () => {
               const contents = await inner.readURL(url);
               texts.set(key, contents);
               stylesByUrl.set(
                  key,
                  await dataStylesForFile(key, contents, inner as URLReader),
               );
               return contents;
            })();
            reads.set(key, read);
            // A failed read is not kept, so the next asker retries it.
            read.catch(() => reads.delete(key));
         }
         return read;
      },
   };
   return {
      reader,
      texts,
      stylesByUrl,
      cache: new CacheManager(new InMemoryModelCache()),
      imported: new Set<string>(),
   };
}

/**
 * The load's model cache as one model's compile sees it: every import comes
 * from the shared cache, but never the model's own file. So each model is
 * translated itself and keeps its own problems (a model Malloy returns from
 * its cache carries none), and the compile order only decides how much is
 * reused, never what a model reports.
 *
 * The cache holds and hands out copies, so no two compiles share an object, as
 * when each model had a Runtime of its own. Both directions matter: the
 * translator writes into the model it imports (it records each source's
 * `#@ persist` flag on the imported model's registry entry), and a model's own
 * result goes on to be served and worked on after its compile.
 *
 * A model no package file imports (a root `index.malloy`, a report) is not
 * kept: nothing would read it, and it is often the package's largest.
 */
export class ModelCacheView extends CacheManager {
   constructor(
      private readonly shared: CacheManager,
      private readonly ownUrl: string,
      private readonly keepOwn = true,
   ) {
      super(new InMemoryModelCache());
   }

   override async getCachedModelDef(
      ...args: Parameters<CacheManager["getCachedModelDef"]>
   ): ReturnType<CacheManager["getCachedModelDef"]> {
      if (args[1] === this.ownUrl) return undefined;
      const hit = await this.shared.getCachedModelDef(...args);
      return hit && { ...hit, modelDef: structuredClone(hit.modelDef) };
   }

   override setCachedModelDef(
      ...[url, cached]: Parameters<CacheManager["setCachedModelDef"]>
   ): ReturnType<CacheManager["setCachedModelDef"]> {
      if (url === this.ownUrl && !this.keepOwn) return Promise.resolve(false);
      return this.shared.setCachedModelDef(url, {
         ...cached,
         modelDef: structuredClone(cached.modelDef),
      });
   }
}

/**
 * A model's data styles as a Runtime of its own accumulated them from every
 * file it read: its own file first, then its imports in the order Malloy
 * read them, later files winning.
 */
export function dataStylesForModel(
   files: JobFiles,
   modelUrl: string,
   dependencies: unknown,
): DataStyles {
   let styles: DataStyles = {};
   for (const url of [modelUrl, ...importReadOrder(dependencies)]) {
      styles = { ...styles, ...(files.stylesByUrl.get(url) ?? {}) };
   }
   return styles;
}

/**
 * The files in a modelDef's `dependencies` tree (`{url: subtree}`) in the
 * order a compile with no cache reads them. Malloy asks for all of a file's
 * unread imports at once, then steps into each import in turn and finishes
 * its whole subtree before the next (`ImportsAndTablesStep` in
 * `lang/parse-malloy`): a pre-order walk that lists a node's imports before
 * descending into them, reading each URL and descending into each file once.
 * This models Malloy's internals; the pool spec pins it against real loads.
 */
export function importReadOrder(tree: unknown): string[] {
   const order: string[] = [];
   const seen = new Set<string>();
   const descended = new Set<string>();
   const visit = (node: unknown): void => {
      if (!node || typeof node !== "object") return;
      const entries = Object.entries(node as Record<string, unknown>);
      for (const [url] of entries) {
         if (!seen.has(url)) {
            seen.add(url);
            order.push(url);
         }
      }
      for (const [url, sub] of entries) {
         if (descended.has(url)) continue;
         descended.add(url);
         visit(sub);
      }
   };
   visit(tree);
   return order;
}

const IMPORT_RE =
   /^\s*import\s+(?:\{[^}]*\}\s+from\s+)?(?:"([^"]+)"|'([^']+)')/gm;

/** The order a load compiles its models in; see compileOrder. */
export interface CompileOrder {
   /**
    * Each model's package imports it waits for before compiling, so their
    * translations are in the cache when it asks: every import in a lower
    * level. Notebooks wait for nothing.
    */
   waitsOn: Map<string, string[]>;
   /** The `.malloy` models by level (imports first), then the notebooks. */
   levels: string[][];
}

/**
 * The package's models in compile order: every package `.malloy` file a
 * model imports sits in an earlier level, and a model waits only for those,
 * so its imports' translations are cached before it asks for them while
 * everything else (warehouse schema fetches included) overlaps as it used
 * to. A model waits only on lower levels, so a cycle cannot hold a load; a
 * cycle, or an import this scan misses, costs only reuse (see
 * ModelCacheView). The URL of every package file another one imports is
 * added to `imported`.
 */
export async function compileOrder(
   packagePath: string,
   modelPaths: string[],
   reader: TextReader,
   imported: Set<string> = new Set(),
): Promise<CompileOrder> {
   const malloy = modelPaths.filter((p) => p.endsWith(MODEL_FILE_SUFFIX));
   const inPackage = new Set(malloy);
   const imports = new Map<string, string[]>();
   for (const modelPath of malloy) {
      const url = pathToFileURL(path.join(packagePath, modelPath));
      let text = "";
      try {
         text = await reader.readURL(url);
      } catch {
         // The model's compile reports it.
      }
      const deps: string[] = [];
      for (const match of text.matchAll(IMPORT_RE)) {
         const target = match[1] ?? match[2];
         if (!target) continue;
         try {
            const resolved = new URL(target, url);
            if (resolved.protocol !== "file:") continue;
            // Spelled as the worker lists model paths: `/` on every platform.
            const rel = path
               .relative(packagePath, fileURLToPath(resolved))
               .replace(/\\/g, "/");
            if (inPackage.has(rel)) {
               deps.push(rel);
               imported.add(resolved.toString());
            }
         } catch {
            // Not a path this scan understands.
         }
      }
      imports.set(modelPath, deps);
   }
   const level = new Map<string, number>();
   const visiting = new Set<string>();
   const levelOf = (modelPath: string): number => {
      const known = level.get(modelPath);
      if (known !== undefined) return known;
      if (visiting.has(modelPath)) return 0; // A cycle; Malloy reports it.
      visiting.add(modelPath);
      let mine = 0;
      for (const dep of imports.get(modelPath) ?? []) {
         mine = Math.max(mine, levelOf(dep) + 1);
      }
      visiting.delete(modelPath);
      level.set(modelPath, mine);
      return mine;
   };
   const levels: string[][] = [];
   const waitsOn = new Map<string, string[]>();
   for (const modelPath of malloy) {
      const mine = levelOf(modelPath);
      (levels[mine] ??= []).push(modelPath);
      waitsOn.set(
         modelPath,
         (imports.get(modelPath) ?? []).filter((dep) => levelOf(dep) < mine),
      );
   }
   const rest = modelPaths.filter((p) => !p.endsWith(MODEL_FILE_SUFFIX));
   for (const modelPath of rest) waitsOn.set(modelPath, []);
   return {
      waitsOn,
      levels: [
         ...levels.filter((l) => l !== undefined && l.length > 0),
         ...(rest.length > 0 ? [rest] : []),
      ],
   };
}

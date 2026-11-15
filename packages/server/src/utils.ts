// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type LogMessage, MalloyError, URLReader } from "@malloydata/malloy";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import { stepsOutside } from "./path_safety";

/**
 * What a package's model is told when it reads a file outside the package.
 * Fixed, with no path in it, and the same whether or not the file exists, so
 * an import cannot be used to learn what is on the server's disk.
 */
export const IMPORT_OUTSIDE_PACKAGE_MESSAGE =
   "imports must stay inside the package";

export class ImportOutsidePackageError extends Error {
   constructor() {
      super(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      this.name = "ImportOutsidePackageError";
   }
}

/**
 * The refusal as the compile problem Malloy reports for an import it cannot
 * read, for a model file that is itself outside the package: it answers as an
 * out-of-package import does (a compile error), not as an outage.
 */
export function importOutsidePackageProblem(): MalloyError {
   const problem = {
      code: "import-error",
      severity: "error",
      message: IMPORT_OUTSIDE_PACKAGE_MESSAGE,
   } as unknown as LogMessage;
   return new MalloyError(IMPORT_OUTSIDE_PACKAGE_MESSAGE, [problem]);
}

// A symlink chain longer than this is refused, as the kernel's ELOOP would.
const MAX_LINK_HOPS = 40;

/** Whether `p` is `root` or inside it, lexically. */
function within(root: string, p: string): boolean {
   return !stepsOutside(path.relative(root, p));
}

/** Where a path leads: `at` is the deepest real point the kernel reaches. */
interface Resolution {
   /** The path to open: real when it exists, else spelled so the open fails as the kernel would. */
   path: string;
   /** The real location to judge containment by. */
   at: string;
}

/** One filesystem question the resolution asks, answered by a driver. */
type Ask = { op: "lstat"; path: string } | { op: "readlink"; path: string };

/** A path's components, as the kernel walks them: no `..` collapsed by spelling. */
function components(p: string): string[] {
   const separators = path.sep === "\\" ? /[\\/]+/ : /\/+/;
   return p.split(separators).filter((c) => c !== "" && c !== ".");
}

/**
 * Where `p` leads, walked one component at a time as the kernel walks it, for
 * a path that may not exist. A `..` steps to the parent of the real directory
 * reached so far, so `link/..` after a symlinked directory goes where the
 * kernel goes (the link target's parent), never where the spelling suggests.
 * A link's text is followed whether or not its target exists. The walk stops
 * at the first component that is missing or cannot be looked at: `at` is that
 * point, and `path` keeps the rest unresolved, so opening it fails there as
 * the kernel would rather than landing on a file the spelling reaches. Throws
 * {@link ImportOutsidePackageError} on a link loop or an unreadable link.
 */
function* resolveSteps(p: string): Generator<Ask, Resolution, unknown> {
   const absolute = path.resolve(p);
   let cur = path.parse(absolute).root;
   let pending = components(absolute.slice(cur.length));
   let hops = 0;
   while (pending.length > 0) {
      const name = pending.shift()!;
      if (name === "..") {
         cur = path.dirname(cur);
         continue;
      }
      const next = path.join(cur, name);
      const stat = (yield { op: "lstat", path: next }) as fs.Stats | undefined;
      if (stat === undefined) {
         return { at: next, path: [next, ...pending].join(path.sep) };
      }
      if (stat.isSymbolicLink()) {
         if (++hops > MAX_LINK_HOPS) throw new ImportOutsidePackageError();
         const text = (yield { op: "readlink", path: next }) as
            | string
            | undefined;
         if (text === undefined) throw new ImportOutsidePackageError();
         if (path.isAbsolute(text)) cur = path.parse(path.resolve(text)).root;
         pending = [...components(text), ...pending];
         continue;
      }
      if (!stat.isDirectory() && pending.length > 0) {
         // A file with more to walk: the kernel stops here (ENOTDIR).
         return { at: next, path: [next, ...pending].join(path.sep) };
      }
      cur = next;
   }
   return { at: cur, path: cur };
}

/** {@link resolveSteps}, asked through the filesystem asynchronously. */
async function resolveLikeKernel(p: string): Promise<Resolution> {
   try {
      const real = await fs.promises.realpath(p);
      return { at: real, path: real };
   } catch {
      // Missing, unreadable or a dangling link: walked below.
   }
   const steps = resolveSteps(p);
   let answer: unknown;
   for (;;) {
      const step = steps.next(answer);
      if (step.done) return step.value;
      const ask = step.value;
      answer =
         ask.op === "lstat"
            ? await fs.promises.lstat(ask.path).catch(() => undefined)
            : await fs.promises.readlink(ask.path).catch(() => undefined);
   }
}

/** {@link resolveSteps}, asked through the filesystem synchronously. */
function resolveLikeKernelSync(p: string): Resolution {
   try {
      const real = fs.realpathSync(p);
      return { at: real, path: real };
   } catch {
      // Missing, unreadable or a dangling link: walked below.
   }
   const steps = resolveSteps(p);
   let answer: unknown;
   for (;;) {
      const step = steps.next(answer);
      if (step.done) return step.value;
      const ask = step.value;
      try {
         answer =
            ask.op === "lstat"
               ? fs.lstatSync(ask.path)
               : fs.readlinkSync(ask.path);
      } catch {
         answer = undefined;
      }
   }
}

/** A `file:` URL's path, or the refusal for any other scheme. */
function filePathOf(url: URL | string): string {
   let parsed: URL;
   try {
      parsed = typeof url === "string" ? new URL(url) : url;
   } catch {
      throw new ImportOutsidePackageError();
   }
   if (parsed.protocol !== "file:") throw new ImportOutsidePackageError();
   return fileURLToPath(parsed);
}

/**
 * Refused before the filesystem is asked about the target when it is outside
 * the package lexically, against both spellings of the root, so the refusal
 * takes the same work whether or not the target exists.
 */
function refuseLexically(
   packageRoot: string,
   realRoot: string,
   filePath: string,
): void {
   const lexical = path.resolve(filePath);
   if (
      !within(path.resolve(packageRoot), lexical) &&
      !within(realRoot, lexical)
   ) {
      throw new ImportOutsidePackageError();
   }
}

/**
 * The realpath of the file `url` names, when it is the package root or inside
 * it; throws {@link ImportOutsidePackageError} otherwise. See
 * {@link readPackageURL}.
 */
export async function confinedPackagePath(
   packageRoot: string,
   url: URL | string,
): Promise<string> {
   const filePath = filePathOf(url);
   let realRoot: string;
   try {
      realRoot = await fs.promises.realpath(packageRoot);
   } catch {
      throw new ImportOutsidePackageError();
   }
   refuseLexically(packageRoot, realRoot, filePath);
   const { at, path: resolved } = await resolveLikeKernel(filePath);
   if (!within(realRoot, at)) throw new ImportOutsidePackageError();
   return resolved;
}

/**
 * Read the file `url` names for a model of the package at `packageRoot` (its
 * version root, for a versioned package). Only a `file:` URL whose realpath
 * is the package root or inside it is read; anything else, a symlink inside
 * the package that leads out of it included, is refused with
 * {@link ImportOutsidePackageError} before the file is opened. Both sides are
 * realpath'd, so a package mounted as a symlink (watch mode) reads its own
 * files. A missing file inside the package fails as a missing file.
 */
export async function readPackageURL(
   packageRoot: string,
   url: URL | string,
): Promise<string> {
   // The resolved path, so the file read is the one just checked.
   return fs.promises.readFile(
      await confinedPackagePath(packageRoot, url),
      "utf8",
   );
}

/** {@link confinedPackagePath}, synchronously. */
export function confinedPackagePathSync(
   packageRoot: string,
   url: URL | string,
): string {
   const filePath = filePathOf(url);
   let realRoot: string;
   try {
      realRoot = fs.realpathSync(packageRoot);
   } catch {
      throw new ImportOutsidePackageError();
   }
   refuseLexically(packageRoot, realRoot, filePath);
   const { at, path: resolved } = resolveLikeKernelSync(filePath);
   if (!within(realRoot, at)) throw new ImportOutsidePackageError();
   return resolved;
}

/** {@link readPackageURL}, synchronously, for the synchronous text readers. */
export function readPackageURLSync(
   packageRoot: string,
   url: URL | string,
): string {
   return fs.readFileSync(confinedPackagePathSync(packageRoot, url), "utf8");
}

/**
 * A synchronous reader of `packageRoot`'s files by URL that answers
 * undefined, never throws, for a file it cannot or may not read: the shape
 * the served copy's and the build's text readers take.
 */
export function packageTextReader(
   packageRoot: string,
): (url: string) => string | undefined {
   return (url) => {
      try {
         return readPackageURLSync(packageRoot, url);
      } catch {
         return undefined;
      }
   };
}

/** A URL in one spelling, for comparing a modelDef's URLs with a reader's. */
function hrefOf(url: string): string | undefined {
   try {
      return new URL(url).href;
   } catch {
      return undefined;
   }
}

/**
 * The file URLs a compiled model was built from: every file its
 * `dependencies` tree and `imports` name, and every file a declaration in it
 * is located in (the model's own file among them). Each was read through the
 * package's reader when the model compiled, so each is inside the package.
 */
export function compiledFileUrls(modelDef: unknown): Set<string> {
   const urls = new Set<string>();
   const add = (url: unknown) => {
      const href = typeof url === "string" ? hrefOf(url) : undefined;
      if (href !== undefined) urls.add(href);
   };
   const def = (modelDef ?? {}) as {
      dependencies?: unknown;
      imports?: { importURL?: unknown }[];
      contents?: Record<string, unknown>;
      sourceRegistry?: Record<string, unknown>;
   };
   const walk = (tree: unknown) => {
      if (!tree || typeof tree !== "object") return;
      for (const [url, sub] of Object.entries(
         tree as Record<string, unknown>,
      )) {
         add(url);
         walk(sub);
      }
   };
   walk(def.dependencies);
   for (const imported of def.imports ?? []) add(imported?.importURL);
   for (const table of [def.contents, def.sourceRegistry]) {
      for (const entry of Object.values(table ?? {})) {
         const located = entry as {
            location?: { url?: unknown };
            entry?: { location?: { url?: unknown } };
         };
         add(located?.location?.url);
         add(located?.entry?.location?.url);
      }
   }
   return urls;
}

/**
 * The direct imports of each file `modelDef` compiled from, by URL (one
 * spelling, see {@link compiledFileUrls}); a file it does not name is the
 * model's own, whose imports are the tree's top level.
 */
export function compiledImports(modelDef: unknown): (url: string) => string[] {
   const byFile = new Map<string, Set<string>>();
   const tree = (modelDef as { dependencies?: unknown } | undefined)
      ?.dependencies;
   const top: string[] = [];
   const walk = (node: unknown, into?: Set<string>) => {
      if (!node || typeof node !== "object") return;
      for (const [url, sub] of Object.entries(
         node as Record<string, unknown>,
      )) {
         const href = hrefOf(url);
         if (href === undefined) continue;
         if (into) into.add(href);
         else top.push(href);
         let own = byFile.get(href);
         if (!own) byFile.set(href, (own = new Set()));
         walk(sub, own);
      }
   };
   walk(tree);
   return (url) => {
      const href = hrefOf(url);
      const own = href === undefined ? undefined : byFile.get(href);
      return own ? [...own] : top;
   };
}

/**
 * A synchronous reader of the files `modelDef` compiled from, and nothing
 * else ({@link compiledFileUrls}); undefined for any other URL or a file it
 * cannot read. With `packageRoot`, each read is confined to the package by
 * realpath too ({@link readPackageURLSync}), so a file swapped for a link out
 * since the compile is not followed.
 */
export function compiledTextReader(
   modelDef: unknown,
   packageRoot?: string,
): (url: string) => string | undefined {
   let allowed: Set<string> | undefined;
   return (url) => {
      allowed ??= compiledFileUrls(modelDef);
      const href = hrefOf(url);
      if (href === undefined || !allowed.has(href)) return undefined;
      try {
         return packageRoot !== undefined
            ? readPackageURLSync(packageRoot, href)
            : fs.readFileSync(fileURLToPath(href), "utf8");
      } catch {
         return undefined;
      }
   };
}

/** The URL reader for the models of the package at `packageRoot`; see {@link readPackageURL}. */
export function packageURLReader(packageRoot: string): URLReader {
   return { readURL: (url: URL) => readPackageURL(packageRoot, url) };
}

/**
 * Every file in the package at `root`, by absolute path, dotfiles and dot
 * directories left out. A directory reached through a symlink is walked only
 * when its realpath is inside the package (each once). Every other link is
 * listed as a file, whatever it leads to (a file or a directory outside the
 * package, or nothing), so nothing outside is listed and the load answers the
 * same for each: the compile reads a model path through the package's reader,
 * which refuses one that leads out.
 */
export async function listPackageTree(root: string): Promise<string[]> {
   const realRoot = await fs.promises.realpath(root);
   const files: string[] = [];
   const walked = new Set<string>([realRoot]);
   const visit = async (dir: string): Promise<void> => {
      for (const entry of await fs.promises.readdir(dir, {
         withFileTypes: true,
      })) {
         if (ignoreDotfiles(entry.name)) continue;
         const full = path.join(dir, entry.name);
         if (entry.isDirectory()) {
            await visit(full);
         } else if (entry.isSymbolicLink()) {
            const target = await fs.promises.stat(full).catch(() => undefined);
            if (!target?.isDirectory()) {
               files.push(full);
               continue;
            }
            const real = await fs.promises.realpath(full);
            if (!within(realRoot, real)) {
               // Listed like any link out, so the load answers the same
               // whatever it leads to: a model path is refused at compile.
               files.push(full);
               continue;
            }
            if (walked.has(real)) continue;
            walked.add(real);
            await visit(full);
         } else {
            files.push(full);
         }
      }
   };
   await visit(root);
   return files;
}

/**
 * Skip dotfiles and dotdirs when walking a package tree: they hold no models
 * or data, and editor/VCS churn makes them vanish mid-scan (spurious ENOENT).
 */
export function ignoreDotfiles(file: string): boolean {
   return path.basename(file).startsWith(".");
}

/** The message of a thrown value, stringifying non-Error throws. */
export function errMessage(err: unknown): string {
   return err instanceof Error ? err.message : String(err);
}

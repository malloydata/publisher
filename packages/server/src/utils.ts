// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { URLReader } from "@malloydata/malloy";
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

// A symlink chain longer than this is refused, as the kernel's ELOOP would.
const MAX_LINK_HOPS = 40;

/**
 * Where `p` leads once every symlink on it is followed, for a path that may
 * not exist: the deepest part that resolves is realpath'd and the rest is
 * appended. A dangling link is followed by its text, so a link to a missing
 * file outside the package resolves outside, exactly like one to a file that
 * is there. Throws {@link ImportOutsidePackageError} on a link loop.
 */
async function resolveThroughLinks(p: string, hops = 0): Promise<string> {
   if (hops > MAX_LINK_HOPS) throw new ImportOutsidePackageError();
   try {
      return await fs.promises.realpath(p);
   } catch {
      // Missing, unreadable, or a dangling link: decided below.
   }
   const stat = await fs.promises.lstat(p).catch(() => undefined);
   if (stat?.isSymbolicLink()) {
      const text = await fs.promises.readlink(p).catch(() => undefined);
      if (text === undefined) throw new ImportOutsidePackageError();
      return resolveThroughLinks(path.resolve(path.dirname(p), text), hops + 1);
   }
   const parent = path.dirname(p);
   if (parent === p) return p;
   return path.join(await resolveThroughLinks(parent, hops), path.basename(p));
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
   url: URL,
): Promise<string> {
   if (url.protocol !== "file:") throw new ImportOutsidePackageError();
   const filePath = fileURLToPath(url);
   let realRoot: string;
   try {
      realRoot = await fs.promises.realpath(packageRoot);
   } catch {
      throw new ImportOutsidePackageError();
   }
   const realFile = await resolveThroughLinks(filePath);
   if (stepsOutside(path.relative(realRoot, realFile))) {
      throw new ImportOutsidePackageError();
   }
   // The resolved path, so the file read is the one just checked.
   return fs.promises.readFile(realFile, "utf8");
}

/** The URL reader for the models of the package at `packageRoot`; see {@link readPackageURL}. */
export function packageURLReader(packageRoot: string): URLReader {
   return { readURL: (url: URL) => readPackageURL(packageRoot, url) };
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A message with every mention of a package's directory on this server, as a
 * path or a `file:` URL, made package-relative: `/srv/data/env/pkg/m.malloy`
 * and `file:///srv/data/env/pkg/m.malloy` both read `m.malloy`, and the
 * directory itself reads `pkg`. A compile error names the file it failed in
 * by the URL it was loaded from, and an API-visible message (a run's failure
 * or recompute reason, a load error) must not tell its reader where the
 * server keeps its files.
 *
 * Every spelling a Windows server produces is found too: backslashes or
 * forward slashes, a drive letter in either case (`C:\…`, `c:/…`), and a
 * `file:///C:/…` URL with the drive's colon plain or encoded (`C%3A`). A
 * relative path is written `/`-separated on every platform, as the package's
 * model paths are.
 *
 * Pure but for the `realpath` of the package directory (macOS's `/var` is
 * `/private/var`; a Windows short name is expanded), read only for the host's
 * own path module. Text that names no part of the directory is returned as it
 * is.
 */

import * as fs from "fs";
import * as path from "path";
import { pathToFileURL } from "url";

/** The parts of a path module this reads; `path.win32` in a test. */
export type PathModule = Pick<
   typeof path,
   "resolve" | "basename" | "sep" | "isAbsolute"
>;

/** `text` with `packagePath` and everything under it named package-relative. */
export function packageRelativeText(
   text: string,
   packagePath: string,
   pathModule: PathModule = path,
): string {
   if (!text || !packagePath) return text;
   const windows = pathModule.sep === "\\";
   const roots = new Set<string>();
   const addRoot = (root: string) => {
      const trimmed = root.replace(/[\\/]+$/, "");
      if (trimmed !== "") roots.add(trimmed);
   };
   addRoot(packagePath);
   addRoot(pathModule.resolve(packagePath));
   if (pathModule === path) {
      for (const real of [fs.realpathSync, fs.realpathSync.native]) {
         try {
            addRoot(real(packagePath));
         } catch {
            // The directory is gone: its spellings as given are all there is.
         }
      }
   }
   const spellings = new Set<string>();
   for (const root of roots) {
      const slashed = root.split("\\").join("/");
      spellings.add(root);
      spellings.add(slashed);
      const urlPath = slashed.startsWith("/") ? slashed : `/${slashed}`;
      // A URL as its reader printed it: encoded, or decoded (a space).
      spellings.add(`file://${urlPath}`);
      spellings.add(`file://${encodeURI(urlPath)}`);
      // A drive's colon, encoded.
      if (/^\/[A-Za-z]:/.test(urlPath)) {
         spellings.add(
            `file://${encodeURI(urlPath).replace(/^\/([A-Za-z]):/, "/$1%3A")}`,
         );
      }
      if (pathModule === path && pathModule.isAbsolute(root)) {
         try {
            spellings.add(pathToFileURL(root).href);
         } catch {
            // Not a path this platform can spell as a URL.
         }
      }
   }
   const name = pathModule.basename(pathModule.resolve(packagePath));
   // A drive letter, and a Windows path's case, are not significant.
   const flags = windows ? "gi" : "g";
   // Longest first, so a URL is taken whole before the path inside it.
   let out = text;
   for (const spelling of [...spellings].sort((a, b) => b.length - a.length)) {
      // Not the tail of a longer path (`/a/srv/pkg` is not `/srv/pkg`).
      const root = `(?<![\\w./\\\\-])${escapeRegExp(spelling)}`;
      // A file under the directory: its path from there, `/`-separated.
      out = out.replace(
         new RegExp(`${root}[\\\\/]([^\\s'"\`<>|*?:]*)`, flags),
         (_whole, rest: string) => rest.split("\\").join("/"),
      );
      // The directory itself, where nothing under it is named: by its name.
      out = out.replace(new RegExp(`${root}(?![\\w.-])`, flags), name);
   }
   return out;
}

function escapeRegExp(text: string): string {
   return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Run `load`, and if it throws, name the package's files package-relative in
 * the error's message before rethrowing it. The one wrapper every package
 * load goes through (a boot, a reload, a published version, a staged
 * publish), so no surface that reports the failure (/status `loadErrors`,
 * the package resource, a publish or reload answer, MCP) can tell its reader
 * where this server keeps its files, and no load path can drift from another.
 */
export async function withPackageRelativeErrors<T>(
   packagePath: string,
   load: () => Promise<T>,
): Promise<T> {
   try {
      return await load();
   } catch (error) {
      if (error instanceof Error) {
         error.message = packageRelativeText(error.message, packagePath);
      }
      throw error;
   }
}

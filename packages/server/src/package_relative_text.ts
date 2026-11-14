// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A message with every mention of a package's directory on this server, as a
 * path or a `file:` URL, made package-relative: `/srv/data/env/pkg/m.malloy`
 * and `file:///srv/data/env/pkg/m.malloy` both read `m.malloy`, and the
 * directory itself reads `pkg`. A compile
 * error names the file it failed in by the URL it was loaded from, and an
 * API-visible message (a run's failure or recompute reason, a load error)
 * must not tell its reader where the server keeps its files.
 *
 * Pure but for one `realpath` of the package directory, so a path the
 * compiler reached through a symlink (macOS's `/var` is `/private/var`) is
 * found too. Text that names no part of the directory is returned as it is.
 */

import * as fs from "fs";
import * as path from "path";
import { pathToFileURL } from "url";

/** `text` with `packagePath` and everything under it named package-relative. */
export function packageRelativeText(text: string, packagePath: string): string {
   if (!text || !packagePath) return text;
   const roots = new Set<string>([path.resolve(packagePath)]);
   try {
      roots.add(fs.realpathSync(packagePath));
   } catch {
      // The directory is gone: its spelling as given is all there is.
   }
   const spellings = new Set<string>();
   for (const root of roots) {
      const trimmed = root.replace(/[\\/]+$/, "");
      if (trimmed === "") continue;
      const url = pathToFileURL(trimmed).href;
      spellings.add(url);
      // A URL some readers print decoded (a space, not %20).
      try {
         spellings.add(decodeURI(url));
      } catch {
         // Not decodable: the encoded form is enough.
      }
      spellings.add(trimmed);
      if (path.sep === "\\") spellings.add(trimmed.split("\\").join("/"));
   }
   // Longest first, so a URL is taken whole before the path inside it.
   let out = text;
   for (const root of [...spellings].sort((a, b) => b.length - a.length)) {
      out = out.split(`${root}/`).join("").split(`${root}\\`).join("");
      // The directory itself, where nothing under it is named: by its name.
      out = out.replace(
         new RegExp(`${escapeRegExp(root)}(?![\\w.-])`, "g"),
         path.basename(path.resolve(packagePath)),
      );
   }
   return out;
}

function escapeRegExp(text: string): string {
   return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import fs from "fs";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";

/**
 * A model's files written to a fresh directory, as a package's are, so a
 * served copy (which reads the author's own files) can re-root them: the
 * file URLs to compile from, the directory to hand `setServedCopyContext`,
 * and the text of each file by URL for an in-memory reader.
 */
export interface ServedCopyFiles {
   dir: string;
   /** The directory's URL, with a trailing slash, for `importBaseURL`. */
   root: string;
   urlOf: (file: string) => string;
   /** Each file's text by its URL, for an `InMemoryURLReader`. */
   byUrl: Map<string, string>;
}

/** Write `files` (name to text) to a fresh temporary directory. */
export function writeServedCopyFiles(
   files: Record<string, string>,
   prefix = "served-copy-",
): ServedCopyFiles {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { URLReader } from "@malloydata/malloy";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

export const URL_READER: URLReader = {
   readURL: (url: URL) => {
      let path = url.toString();
      if (url.protocol == "file:") {
         path = fileURLToPath(url);
      }
      return fs.promises.readFile(path, "utf8");
   },
};

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

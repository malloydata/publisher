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
 * Skip dotfiles/dotdirs (.vscode, .git, .DS_Store, etc.) when walking a
 * package tree. These come from editors/VCS, never contain Malloy models
 * or databases, and have been a source of spurious ENOENTs when their
 * contents disappear mid-scan.
 */
export function ignoreDotfiles(file: string): boolean {
   return path.basename(file).startsWith(".");
}

/** The message of a thrown value, stringifying non-Error throws. */
export function errMessage(err: unknown): string {
   return err instanceof Error ? err.message : String(err);
}

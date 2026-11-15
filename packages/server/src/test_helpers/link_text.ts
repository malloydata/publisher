// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import * as os from "os";
import * as path from "path";

let verbatim: boolean | undefined;

/**
 * Whether a symlink keeps the text it was made with, `..` and all, as on
 * POSIX. On Windows, Node normalizes a link's target when it makes the link
 * (and Windows resolves a reparse point's `..` by name), so `q/../t` is
 * stored as `t`: a link that spells a path out of the package and back
 * never leaves it there, and leads inside whether or not what it spelled
 * through exists. Tests of a path that goes out and comes back expect the
 * refusal where the text is kept and the in-package answer where it is not;
 * the two answers must match either way. Probed once, not keyed on the
 * platform.
 */
export function linkTextKeptVerbatim(): boolean {
   if (verbatim !== undefined) return verbatim;
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "link-text-probe-"));
   try {
      fs.symlinkSync("q/../t", path.join(dir, "l"), "file");
      verbatim = fs.readlinkSync(path.join(dir, "l")) === "q/../t";
   } finally {
      fs.rmSync(dir, { recursive: true, force: true });
   }
   return verbatim;
}

/** `p` with the platform's separators read as `/`, for comparing listings. */
export function slashed(p: string): string {
   return p.split(path.sep).join("/");
}

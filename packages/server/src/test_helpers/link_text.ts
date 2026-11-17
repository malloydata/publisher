// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import * as os from "os";
import * as path from "path";

let verbatim: boolean | undefined;

/**
 * Whether a symlink keeps the text it was made with, exactly, as on POSIX.
 * On Windows it does not, in two different ways (seen on CI): a relative
 * target keeps its `..` but takes backslashes (`q/../t` is stored as
 * `q\..\t`), while an absolute target is resolved when the link is made, so
 * an absolute link that spells a path out of the package and back is stored
 * as the in-package path and never leaves. Tests of a path that goes out and
 * comes back expect the refusal for a kept text or a relative one, and the
 * in-package answer for an absolute one where the text is not kept; the
 * present and absent answers must match either way. Probed once, not keyed on
 * the platform.
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
   // Only Windows is known to normalize. A normalized reading anywhere else
   // (an odd tmp mount, FUSE) would silently relax the refusal assertions
   // this probe gates, so it fails loudly instead.
   if (!verbatim && process.platform !== "win32") {
      throw new Error("symlink text was normalized on a non-Windows runner");
   }
   return verbatim;
}

/** `p` with the platform's separators read as `/`, for comparing listings. */
export function slashed(p: string): string {
   return p.split(path.sep).join("/");
}

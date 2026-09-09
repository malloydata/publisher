// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash, randomUUID } from "crypto";
import fs from "fs";
import path from "path";

/**
 * Hashed in place of a file whose bytes could not be read.
 *
 * Without it an unreadable file and an empty one produce the same digest, so a
 * package that started failing to read a model would look byte-identical to one
 * whose model is legitimately empty. The marker is not valid file content (no
 * path can contain a NUL), so it cannot collide with a file that happens to
 * hold this text.
 */
const UNREADABLE = "\0<unreadable>\0";

/**
 * SHA-256 of the exact bytes a package is serving, keyed by package-relative
 * path.
 *
 * Paths are sorted so the digest is independent of the caller's iteration
 * order, and each path is hashed alongside its content so a rename moves the
 * digest even when the bytes are unchanged.
 *
 * The caller decides what counts as served content by choosing what to pass.
 * Anything omitted is invisible here: a change to a file outside the set moves
 * nothing, which is the whole reason the set is a parameter rather than a walk
 * of the package directory.
 */
export function computeSourceContentSha(
   packagePath: string,
   contentPaths: Iterable<string>,
): string {
   const hash = createHash("sha256");
   for (const relativePath of [...contentPaths].sort()) {
      hash.update(relativePath);
      hash.update("\0");
      const absolute = path.join(packagePath, relativePath);
      try {
         hash.update(fs.readFileSync(absolute));
      } catch {
         hash.update(UNREADABLE);
      }
      hash.update("\0");
   }
   return hash.digest("hex");
}

/**
 * A fresh identifier for one load of a package.
 *
 * Minted per load rather than derived from content, so two loads of identical
 * bytes get different revisions. That makes it the wrong thing to compare when
 * asking "did my edit reach the server" -- use the content sha for that -- and
 * the right thing for identifying which load answered.
 */
export function mintServedRevision(): string {
   return randomUUID();
}

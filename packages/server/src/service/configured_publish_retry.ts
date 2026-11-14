// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import * as fs from "fs";
import * as path from "path";

/**
 * When a refused configured package may be published again. Its first access
 * after boot retries at once (a failure that cleared on its own, such as a
 * table created after boot, recovers on the next request). Each failure after
 * that waits twice as long as the one before, up to {@link MAX_BACKOFF_MS},
 * and a request in between answers the last failure without publishing. A
 * change to the package's model files retries at once whatever the wait.
 *
 * A publish copies the whole package and compiles it, and the routes that
 * trigger one are unauthenticated, so a broken package polled every few
 * seconds must not re-run that every few seconds.
 */
export const FIRST_BACKOFF_MS = 2_000;
export const MAX_BACKOFF_MS = 60_000;

/** The wait after the `failures`-th failed retry (1-based). */
export function backoffAfter(failures: number): number {
   if (failures <= 0) return 0;
   return Math.min(MAX_BACKOFF_MS, FIRST_BACKOFF_MS * 2 ** (failures - 1));
}

/** The files a package's publish reads to decide what it is. */
const FINGERPRINTED = /(\.malloy|\.malloynb|^publisher\.json)$/;
/** Folders that never hold a package's own model files. */
const SKIPPED_DIRS = new Set(["node_modules", ".git", "publisher_data"]);

/**
 * A cheap fingerprint of a local package's model files: each file's path,
 * size and modification time, never its contents. `undefined` when the
 * location cannot be read (the retry then waits out its backoff).
 */
export async function localPackageFingerprint(
   root: string,
): Promise<string | undefined> {
   const lines: string[] = [];
   const walk = async (dir: string): Promise<void> => {
      const entries = await fs.promises.readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
         const full = path.join(dir, entry.name);
         if (entry.isDirectory()) {
            if (!SKIPPED_DIRS.has(entry.name)) await walk(full);
         } else if (entry.isFile() && FINGERPRINTED.test(entry.name)) {
            const stat = await fs.promises.stat(full);
            lines.push(
               `${path.relative(root, full)}\u0000${stat.size}\u0000${stat.mtimeMs}`,
            );
         }
      }
   };
   try {
      await walk(root);
   } catch {
      return undefined;
   }
   lines.sort();
   return createHash("sha256").update(lines.join("\n")).digest("hex");
}

/** One refused configured package's retry state. */
export interface ConfiguredPublishRetry {
   publish: () => Promise<unknown>;
   fingerprint?: () => Promise<string | undefined>;
   /** Failed retries since boot (the boot refusal itself is not counted). */
   failures: number;
   /** Epoch ms before which a retry answers {@link lastError} instead. */
   notBefore: number;
   lastError?: unknown;
   /** The fingerprint the last failed retry ran against. */
   lastFingerprint?: string;
}

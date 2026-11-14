// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash, randomUUID } from "crypto";
import fs from "fs";
import path from "path";
import { compareText } from "../compare_text";

/**
 * Hashed in place of an unreadable file, so it digests differently from an
 * empty one. The NULs keep it from colliding with real file content.
 */
const UNREADABLE = "\0<unreadable>\0";

/**
 * SHA-256 of the exact bytes a package is serving, keyed by package-relative
 * path. Paths are sorted (order-independent) and hashed with their content (a
 * rename moves the digest). The caller chooses the served set, plus `extra`
 * for served text that lives in no file; anything outside it is invisible.
 * A path in `contentShas` is hashed by its already-computed sha256 instead of
 * being read again.
 */
export function computeSourceContentSha(
   packagePath: string,
   contentPaths: Iterable<string>,
   extra?: string,
   contentShas?: ReadonlyMap<string, string>,
): string {
   const hash = createHash("sha256");
   for (const relativePath of [...contentPaths].sort()) {
      hash.update(relativePath);
      hash.update("\0");
      const precomputed = contentShas?.get(relativePath);
      if (precomputed !== undefined) {
         hash.update(`\0sha256\0${precomputed}`);
      } else {
         const absolute = path.join(packagePath, relativePath);
         try {
            hash.update(fs.readFileSync(absolute));
         } catch {
            hash.update(UNREADABLE);
         }
      }
      hash.update("\0");
   }
   if (extra !== undefined) hash.update(`\0extra\0${extra}`);
   return hash.digest("hex");
}

/** JSON with object keys sorted at every depth, so equal values serialize equally. */
export function canonicalJson(value: unknown): string {
   return JSON.stringify(value, (_key, v: unknown) =>
      v !== null && typeof v === "object" && !Array.isArray(v)
         ? Object.fromEntries(
              Object.entries(v).sort(([a], [b]) => compareText(a, b)),
           )
         : v,
   );
}

/** A real `agents` declaration is a few KB; past this it is not read. */
export const MANIFEST_AGENTS_MAX_BYTES = 1024 * 1024;

/**
 * The value, or a marker string when it is too deep or too large to hash and
 * to post across the worker boundary, where a stack overflow is not an
 * author-facing error. The marker is not an object, so the agents reader
 * answers it with its usual "must be an object" warning.
 */
export function boundedManifestValue(value: unknown): unknown {
   try {
      const text = canonicalJson(value);
      if (text === undefined || text.length <= MANIFEST_AGENTS_MAX_BYTES) {
         return value;
      }
   } catch {
      // Too deep to serialize.
   }
   return "[unreadable: too deep or too large]";
}

export function canonicalSha(value: unknown): string {
   return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

/**
 * A fresh identifier for one load of a package. Minted per load, not derived
 * from content: it identifies which load answered; use the content sha to ask
 * whether an edit reached the server.
 */
export function mintServedRevision(): string {
   return randomUUID();
}

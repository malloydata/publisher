// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash, randomUUID } from "crypto";
import { compareText } from "../compare_text";

/**
 * Stands in for the sha of a file the load could not read, so it hashes
 * differently from an empty one. The NULs keep it from colliding with a real
 * sha256, which is hex.
 */
export const UNREADABLE_SHA = "\0<unreadable>\0";

/**
 * SHA-256 over what a package is serving: each path with the sha256 of the
 * exact bytes the load read of it, plus `extra` for served text that lives in
 * no file. Paths are sorted (order-independent) and hashed with their sha (a
 * rename moves the digest). The caller chooses the served set; anything
 * outside it is invisible. Reads no file, so it is safe on the event loop.
 */
export function computeSourceContentSha(
   contentShas: ReadonlyMap<string, string>,
   extra?: string,
): string {
   const hash = createHash("sha256");
   for (const relativePath of [...contentShas.keys()].sort()) {
      hash.update(relativePath);
      hash.update("\0");
      hash.update(`\0sha256\0${contentShas.get(relativePath)}`);
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

/** A real `skillSets` and `workflows` declaration is a few KB; past this together they are not read. */
export const MANIFEST_DECLARATION_MAX_BYTES = 1024 * 1024;

export const UNREADABLE_DECLARATION = "[unreadable: too deep or too large]";

/** The manifest's `skillSets` and `workflows`, unvalidated, as one value; or the marker for a pair too deep or too large to read. */
export type ManifestDeclaration =
   | { skillSets?: unknown; workflows?: unknown }
   | typeof UNREADABLE_DECLARATION;

/**
 * The declaration, `{ skillSets, workflows }` as the manifest wrote them, or a
 * marker string when it is too deep or too large to hash and to hold in the
 * worker, where a stack overflow is not an author-facing error, with the
 * sha256 of its canonical JSON. Both keys are bounded and hashed as one value,
 * and only when either is present: a package with neither has no declaration
 * (`value` undefined, no `sha`). The marker is not an object, so the walk
 * answers it with a "too deeply nested or over 1 MiB" warning of its own.
 */
export function boundedDeclaration(declared: {
   skillSets?: unknown;
   workflows?: unknown;
}): {
   value: ManifestDeclaration | undefined;
   sha?: string;
} {
   if (declared.skillSets === undefined && declared.workflows === undefined) {
      return { value: undefined };
   }
   const value = {
      skillSets: declared.skillSets,
      workflows: declared.workflows,
   };
   try {
      const text = canonicalJson(value);
      if (text.length <= MANIFEST_DECLARATION_MAX_BYTES) {
         return { value, sha: createHash("sha256").update(text).digest("hex") };
      }
   } catch {
      // Too deep to serialize.
   }
   return {
      value: UNREADABLE_DECLARATION,
      sha: canonicalSha(UNREADABLE_DECLARATION),
   };
}

/**
 * The sha of each of `paths` out of `shas`, so the content hash never reads a
 * file. A path without one throws: it must never be hashed as the text
 * "undefined", which would stand in for a file nothing read.
 */
export function shasOf(
   paths: Iterable<string>,
   shas: ReadonlyMap<string, string>,
): Map<string, string> {
   const found = new Map<string, string>();
   for (const p of paths) {
      const sha = shas.get(p);
      if (sha === undefined) {
         throw new Error(
            `no sha was read for '${p}', which the content hash covers`,
         );
      }
      found.set(p, sha);
   }
   return found;
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

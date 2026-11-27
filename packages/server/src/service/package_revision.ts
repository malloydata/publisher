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

/** A real `workflows` declaration is a few KB; past this it is not read. */
export const MANIFEST_WORKFLOWS_MAX_BYTES = 1024 * 1024;

export const UNREADABLE_DECLARATION = "[unreadable: too deep or too large]";

/**
 * The declaration, or a marker string when it is too deep or too large to
 * hash and to post across the worker boundary, where a stack overflow is not
 * an author-facing error, with the sha256 of its canonical JSON (absent for no
 * declaration). The marker is not an object, so the workflows reader answers
 * it with its usual "must be an object" warning.
 */
export function boundedDeclaration(value: unknown): {
   value: unknown;
   sha?: string;
} {
   try {
      const text = canonicalJson(value);
      if (text === undefined) return { value };
      if (text.length <= MANIFEST_WORKFLOWS_MAX_BYTES) {
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

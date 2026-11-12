// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fileURLToPath, pathToFileURL } from "url";

/**
 * The key a `files` overlay holds a file under, for the file at `url` (the
 * URL the compiler asks a reader for) or at an absolute path. Every file URL
 * is put in `pathToFileURL`'s form first: a hand-built `file://${path}`
 * escapes `%`, `#` and `?` differently and does not hold a Windows drive, so
 * the same file would otherwise have two keys.
 */
export function overlayKey(url: URL | string): string {
   const parsed = typeof url === "string" ? new URL(url) : url;
   if (parsed.protocol !== "file:") return parsed.href;
   try {
      return pathToFileURL(fileURLToPath(parsed)).href;
   } catch {
      return parsed.href;
   }
}

/** {@link overlayKey} of the file at absolute path `filePath`. */
export function overlayKeyOfPath(filePath: string): string {
   return pathToFileURL(filePath).href;
}

/** `files` (by URL) as an overlay, each keyed by {@link overlayKey}. */
export function keyedOverlay(
   files: Iterable<readonly [string, string]>,
): Map<string, string> {
   return new Map([...files].map(([url, text]) => [overlayKey(url), text]));
}

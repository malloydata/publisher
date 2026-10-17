// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as path from "path";

import { BadRequestError } from "./errors";

/**
 * Directory-traversal defenses for paths built from request data, in two
 * layers: the `assertSafe*` allowlists reject hostile input (400) at the entry
 * of every public service method, and `safeJoinUnderRoot` resolves and
 * contains at the sink, so a caller that forgets the first still cannot
 * escape. Both are shapes CodeQL's `js/path-injection` recognises.
 */

// One segment. No leading `.`, so internal dirs (`.staging`, `.retired`) and
// editor/VCS dirs cannot be addressed by name.
const SAFE_NAME_RE = /^(?!\.\.?$)(?!\.)[A-Za-z0-9._-]{1,255}$/;

const MAX_MODEL_PATH_LEN = 1024;

// An environment path is server-controlled, but CodeQL treats it as tainted;
// this regex plus the `..`/NUL/length checks is the barrier it recognises.
// Printable ASCII, absolute POSIX or Windows.
const SAFE_ENVIRONMENT_PATH_RE = /^(?:\/|[A-Za-z]:[\\/])[\x20-\x7E]*$/;
const MAX_ENVIRONMENT_PATH_LEN = 4096;

/** Reject anything that is not a plausible single-segment package name. */
export function assertSafePackageName(
   packageName: unknown,
): asserts packageName is string {
   if (typeof packageName !== "string" || !SAFE_NAME_RE.test(packageName)) {
      throw new BadRequestError(
         `Invalid package name: must be 1-255 characters of letters, digits, "-", "_", or "." and must not start with "."`,
      );
   }
}

/**
 * Reject anything that is not a relative model path inside a package: forward
 * slashes only, no absolute path, NUL, empty, dot or dot-prefixed segment.
 */
export function assertSafeRelativeModelPath(
   modelPath: unknown,
): asserts modelPath is string {
   if (
      typeof modelPath !== "string" ||
      modelPath.length === 0 ||
      modelPath.length > MAX_MODEL_PATH_LEN ||
      modelPath.includes("\0") ||
      modelPath.includes("\\") ||
      path.isAbsolute(modelPath) ||
      modelPath.startsWith("/")
   ) {
      throw new BadRequestError(`Invalid model path`);
   }

   const segments = modelPath.split("/");
   for (const segment of segments) {
      if (segment === "" || segment === "." || segment === "..") {
         throw new BadRequestError(`Invalid model path`);
      }
      if (segment.startsWith(".")) {
         throw new BadRequestError(`Invalid model path`);
      }
   }
}

/**
 * Reject anything that does not look like a server-controlled absolute path.
 * Applied at the `Environment` constructor so every downstream join sees an
 * allowlisted value.
 */
export function assertSafeEnvironmentPath(
   environmentPath: unknown,
): asserts environmentPath is string {
   if (typeof environmentPath !== "string") {
      throw new BadRequestError(`Invalid environment path: must be a string`);
   }
   if (
      environmentPath.length === 0 ||
      environmentPath.length > MAX_ENVIRONMENT_PATH_LEN
   ) {
      throw new BadRequestError(`Invalid environment path: bad length`);
   }
   if (environmentPath.indexOf("\0") !== -1) {
      throw new BadRequestError(`Invalid environment path: contains NUL byte`);
   }
   // This exact shape is what CodeQL recognises as a traversal guard.
   if (environmentPath.indexOf("..") !== -1) {
      throw new BadRequestError(
         `Invalid environment path: contains ".." traversal segment`,
      );
   }
   if (!SAFE_ENVIRONMENT_PATH_RE.test(environmentPath)) {
      throw new BadRequestError(
         `Invalid environment path: must be an absolute path of printable ASCII characters`,
      );
   }
}

/**
 * Resolve `root` joined with `segments` and throw `BadRequestError` unless the
 * result is `root` or inside it. Lexical only (symlinks are not resolved), and
 * the second line of defense: validate user segments first.
 */
export function safeJoinUnderRoot(root: string, ...segments: string[]): string {
   const resolvedRoot = path.resolve(root);
   const joined = path.resolve(resolvedRoot, ...segments);
   const rootWithSep = resolvedRoot.endsWith(path.sep)
      ? resolvedRoot
      : resolvedRoot + path.sep;
   if (joined !== resolvedRoot && !joined.startsWith(rootWithSep)) {
      throw new BadRequestError(`Resolved path is outside of root`);
   }
   return joined;
}

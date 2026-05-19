import * as path from "path";

import { BadRequestError } from "./errors";

/**
 * Path-safety helpers used by `Environment` (and any other service that
 * builds an on-disk path from request data) to defend against directory
 * traversal. The intent is two-fold:
 *
 *  1. **Source-side allowlist**: `assertSafePackageName` /
 *     `assertSafeRelativeModelPath` reject hostile inputs (`..`, leading
 *     `/`, `\`, NUL, dotfiles) at the entry of every public service
 *     method before any path-construction happens. These throw
 *     `BadRequestError` so the controller layer's error mapper returns
 *     HTTP 400.
 *
 *  2. **Sink-side containment**: `safeJoinUnderRoot` joins, resolves,
 *     and verifies the result is strictly within the supplied root.
 *     Even if a future caller forgets the source-side check, the sink
 *     refuses to hand back an escaping path. This is the standard
 *     "resolve-and-contain" pattern that CodeQL's `js/path-injection`
 *     query recognises as a sanitizer.
 */

// Single path segment: ASCII letters, digits, `-`, `_`, `.`. No leading
// `.` so internal sibling dirs (`.staging`, `.retired`) and editor /
// VCS dirs can't be addressed by name from outside.
const SAFE_NAME_RE = /^(?!\.\.?$)(?!\.)[A-Za-z0-9._-]{1,255}$/;

const MAX_MODEL_PATH_LEN = 1024;

/**
 * Reject anything that isn't a plausible single-segment package name.
 * The allowlist is deliberately conservative — every existing test and
 * production package name we've seen fits within it, and tightening
 * here costs nothing.
 */
export function assertSafePackageName(packageName: unknown): void {
   if (typeof packageName !== "string" || !SAFE_NAME_RE.test(packageName)) {
      throw new BadRequestError(
         `Invalid package name: must be 1-255 characters of letters, digits, "-", "_", or "." and must not start with "."`,
      );
   }
}

/**
 * Reject anything that isn't a plausible *relative* path to a model
 * file inside a package directory. Forward slashes are allowed (models
 * live in subdirectories like `models/foo.malloy`); backslashes,
 * absolute paths, NUL bytes, and `..` / `.` segments are not.
 */
export function assertSafeRelativeModelPath(modelPath: unknown): void {
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
 * Resolve `path.join(root, ...segments)` and verify the result lives
 * strictly inside `root` (or is `root` itself). Throws
 * `BadRequestError` if the resolved path escapes the root via `..`,
 * absolute segments, or symlink-style trickery in the input.
 *
 * Callers should still run `assertSafePackageName` / similar on
 * user-controlled segments first — this helper is the second line of
 * defense, not the first.
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

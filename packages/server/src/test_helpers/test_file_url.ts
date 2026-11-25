// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An absolute file URL, its path, and a source ID declared there, on every
 * platform, for specs that spell a model's location.
 *
 * A hand-spelled `file:///pkg/m.malloy` has no drive letter. On POSIX that is
 * an absolute path, but on Windows it names no path at all: `fileURLToPath`
 * throws, `path.resolve` puts it on another root, and a cache keyed by path
 * misses. A spec that passes such a URL to code that turns it into a path
 * then fails only on Windows. These build the URL the way Publisher does
 * (`pathToFileURL` over an absolute path), so on Windows it carries the
 * current drive: `testFileURL("pkg", "m.malloy")` is `file:///pkg/m.malloy`
 * on POSIX and `file:///C:/pkg/m.malloy` on Windows.
 *
 * The lint rule `publisher/no-driveless-file-url` (eslint.config.mjs) refuses
 * a new drive-less `file:///…` in a spec's or test helper's string, at its
 * start or inside it (a source ID, `m@file:///…`), and points here.
 */

import * as path from "node:path";
import { pathToFileURL } from "node:url";

/**
 * The absolute path `segments` name under the filesystem root (the current
 * drive's, on Windows). A trailing `/` on the last segment is kept, so the
 * result can name a directory.
 */
export function testFilePath(...segments: string[]): string {
   const resolved = path.resolve("/", ...segments);
   const dir = segments.at(-1)?.endsWith("/") && !resolved.endsWith(path.sep);
   return dir ? `${resolved}${path.sep}` : resolved;
}

/**
 * The `file:` URL of `testFilePath(...segments)`, as a string. A trailing `/`
 * on the last segment is kept, so the URL can be a root that `${root}m.malloy`
 * or `new URL("m.malloy", root)` extends.
 */
export function testFileURL(...segments: string[]): string {
   return pathToFileURL(testFilePath(...segments)).href;
}

/**
 * A source ID as Malloy spells one, `<name>@<file URL>`, for a source declared
 * in `testFileURL(...segments)`: `testSourceID("m", "pkg", "m.malloy")` is
 * `m@file:///pkg/m.malloy` on POSIX and `m@file:///C:/pkg/m.malloy` on
 * Windows, so code that reads the declaring file back as a path finds it.
 */
export function testSourceID(name: string, ...segments: string[]): string {
   return `${name}@${testFileURL(...segments)}`;
}

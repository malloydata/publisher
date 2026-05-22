import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { BadRequestError } from "../errors";
import { deleteDuckLakeConnectionFile } from "./connection";
import { Environment } from "./environment";

/**
 * Integration tests for the path-injection sanitizer barriers added
 * to address CodeQL `js/path-injection` alerts.
 *
 * Each block targets a public entry point that previously fed
 * request-derived strings into `fs.*` without shape validation. The
 * tests pin the *wiring* — `path_safety.spec.ts` already covers the
 * helpers themselves exhaustively. Here we prove that every gate now
 * rejects traversal-bearing inputs with `BadRequestError` (which the
 * controller layer maps to HTTP 400) before any filesystem access.
 *
 * The watch-mode controller boundary is covered indirectly via the
 * `assertSafeEnvironmentName` allowlist test (in
 * `src/path_safety.spec.ts`) — exercising the controller end-to-end
 * here would import `EnvironmentStore`, which transitively pulls in
 * `instrumentation.ts` (uses `perf_hooks.monitorEventLoopDelay`, not
 * yet implemented in the Bun runtime used for unit tests).
 */

const TRAVERSAL_NAMES = [
   "../etc/passwd",
   "..\\windows",
   "foo/bar",
   "a\0b",
   ".staging",
   "",
];

describe("path-injection boundary: deleteDuckLakeConnectionFile", () => {
   const envDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "publisher-ducklake-traversal-"),
   );

   it.each(TRAVERSAL_NAMES)(
      "rejects unsafe connection name %p before touching fs",
      async (badName) => {
         await expect(
            deleteDuckLakeConnectionFile(badName, envDir),
         ).rejects.toThrow(BadRequestError);
      },
   );

   it.each([
      ["relative path", "publisher_data"],
      ["traversal", "/var/publisher/../etc"],
      ["empty", ""],
      ["null byte", "/tmp/foo\0bar"],
   ])(
      "rejects unsafe environment path (%s) before touching fs",
      async (_label, badPath) => {
         await expect(
            deleteDuckLakeConnectionFile("conn", badPath),
         ).rejects.toThrow(BadRequestError);
      },
   );

   it("accepts a well-formed name + absolute env path (and no-ops on missing file)", async () => {
      // The file doesn't exist; helper logs and returns. Asserting no
      // throw is the win — the validators must not reject this shape.
      await expect(
         deleteDuckLakeConnectionFile("bigquery", envDir),
      ).resolves.toBeUndefined();
   });
});

describe("path-injection boundary: Environment.create", () => {
   it.each(TRAVERSAL_NAMES)(
      "rejects unsafe environment name %p",
      async (badName) => {
         await expect(
            Environment.create(badName, "/tmp/env-fake-path", []),
         ).rejects.toThrow(BadRequestError);
      },
   );

   it.each([
      ["relative", "publisher_data"],
      ["traversal", "/var/../etc"],
      ["empty", ""],
   ])("rejects unsafe environment path (%s)", async (_label, badPath) => {
      await expect(Environment.create("env", badPath, [])).rejects.toThrow(
         BadRequestError,
      );
   });
});

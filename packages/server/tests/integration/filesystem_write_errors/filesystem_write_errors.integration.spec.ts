// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A package add whose write the server cannot make must say so.
 *
 * Since 0.9.0 the image runs as uid 1000 (#1273), so every mount the server
 * writes to has to be writable by that user. When one is not, the server
 * answers `{"code":500,"message":"Internal server error."}` and the EACCES that
 * explains it reaches only the server log. A runtime add that fails this way is
 * not in `/status` either, so the log is the only record of it.
 *
 * The first block reproduces S3 of the Docker smoke script: a zip `location` in
 * a directory the server can read but not write. A zip location is extracted into
 * a sibling directory beside the zip, so a package mount is a write mount. A
 * read-only directory owned by the test user stands in for a root-owned bind,
 * which needs no Docker and no root. The Docker smoke test in build.yml covers
 * the real uid-1000 image against a root-owned volume.
 *
 * The assertions name the errno and refuse the generic body. They do not pin
 * the wording, or whether the path is named, which is the fix's call.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { chmodSync, mkdirSync, rmSync, writeFileSync } from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const GENERIC_INTERNAL_MESSAGE = "Internal server error.";
const ENV_NAME = `fs-write-errors-env-${Date.now()}`;
const SEED_PACKAGE = "seed";
const ZIP_PACKAGE = "tiny";

// Root ignores directory permissions, so a read-only directory proves nothing
// there; Windows has no POSIX mode bits.
const canRevokeWrite = process.platform !== "win32" && process.getuid?.() !== 0;
const hasZip =
   process.platform !== "win32" &&
   Bun.spawnSync(["which", "zip"]).exitCode === 0;

function writeTinyPackage(dir: string, name: string): void {
   mkdirSync(dir, { recursive: true });
   writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name, description: "one-model DuckDB package" }),
   );
   writeFileSync(
      path.join(dir, "model.malloy"),
      `source: s is duckdb.sql("SELECT 1 AS x")\n`,
   );
}

describe.skipIf(!canRevokeWrite || !hasZip)(
   "a filesystem write the server cannot make (E2E)",
   () => {
      let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
      let baseUrl: string;
      let workDir: string;
      let readOnlyDir: string;

      beforeAll(async () => {
         workDir = path.join(
            os.tmpdir(),
            `publisher-fs-write-errors-${process.pid}-${Date.now()}`,
         );
         // The seed package lives somewhere writable, so the environment
         // itself loads cleanly and only the add under test fails.
         writeTinyPackage(path.join(workDir, SEED_PACKAGE), SEED_PACKAGE);

         // A real zip of the tiny package, in a directory the server can read
         // but not write: a root-owned shared package mount.
         const source = path.join(workDir, "zip-src");
         writeTinyPackage(source, ZIP_PACKAGE);
         readOnlyDir = path.join(workDir, "read-only");
         mkdirSync(readOnlyDir, { recursive: true });
         const zipped = Bun.spawnSync(
            [
               "zip",
               "-q",
               "-r",
               path.join(readOnlyDir, `${ZIP_PACKAGE}.zip`),
               ".",
            ],
            { cwd: source },
         );
         if (zipped.exitCode !== 0) {
            throw new Error(`zip failed: ${zipped.stderr.toString()}`);
         }
         chmodSync(readOnlyDir, 0o555);

         env = await startRestE2E();
         baseUrl = env.baseUrl;

         const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: ENV_NAME,
               packages: [
                  {
                     name: SEED_PACKAGE,
                     location: path.join(workDir, SEED_PACKAGE),
                  },
               ],
               connections: [],
            }),
         });
         if (!createRes.ok) {
            throw new Error(
               `Failed to seed test environment (${createRes.status}): ${await createRes.text()}`,
            );
         }
      });

      afterAll(async () => {
         if (baseUrl) {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            }).catch(() => {
               /* best-effort cleanup */
            });
         }
         await env?.stop();
         env = null;
         if (readOnlyDir) chmodSync(readOnlyDir, 0o755);
         if (workDir) rmSync(workDir, { recursive: true, force: true });
      });

      it("answers a zip add into a read-only directory with the errno, not a bare 500", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: ZIP_PACKAGE,
                  location: path.join(readOnlyDir, `${ZIP_PACKAGE}.zip`),
               }),
            },
         );
         const body = (await res.json()) as { code: number; message: string };

         expect(res.ok).toBe(false);
         expect(body.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
         expect(body.message).toMatch(/EACCES|permission denied/i);
      });

      it("records the failed runtime add in /status loadErrors", async () => {
         const res = await fetch(`${baseUrl}/api/v0/status`);
         expect(res.ok).toBe(true);
         const status = (await res.json()) as {
            loadErrors?: Array<{
               environment: string;
               package?: string;
               message: string;
            }>;
         };
         const entry = status.loadErrors?.find(
            (e) => e.environment === ENV_NAME && e.package === ZIP_PACKAGE,
         );

         expect(entry).toBeDefined();
         expect(entry?.message).toMatch(/EACCES|permission denied/i);
      });

      it("still serves the environment's other package", async () => {
         // The failure is the add's alone: the seed package keeps serving, so
         // the fix must not take the environment down to report it.
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${SEED_PACKAGE}`,
         );
         expect(res.status).toBe(200);
      });
   },
);

/**
 * Writes into the server's own data directory, which a publisher_data volume
 * left root-owned by a root-run 0.8.x denies. Each case revokes access on a
 * directory the server created, runs one request, and restores it, so the
 * cases are independent. Three of them also pin that the errno survives a
 * write site that re-wraps it: the README and publisher.json writes throw a
 * fresh Error with no `cause`, and a manifest that cannot be stat'ed is
 * reported as one that does not exist.
 */
describe.skipIf(!canRevokeWrite)(
   "a write into the server's own data directory it cannot make (E2E)",
   () => {
      const DATA_ENV = `fs-data-errors-env-${Date.now()}`;
      let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
      let baseUrl: string;
      let workDir: string;
      let environmentPath: string;

      // Revoke `mode` on `target` for the duration of `run`, always restoring.
      async function withMode<T>(
         target: string,
         mode: number,
         run: () => Promise<T>,
      ): Promise<T> {
         chmodSync(target, mode);
         try {
            return await run();
         } finally {
            chmodSync(target, target.endsWith(".json") ? 0o644 : 0o755);
         }
      }

      async function json(res: Response) {
         return (await res.json()) as { code?: number; message: string };
      }

      beforeAll(async () => {
         workDir = path.join(
            os.tmpdir(),
            `publisher-fs-data-errors-${process.pid}-${Date.now()}`,
         );
         writeTinyPackage(path.join(workDir, SEED_PACKAGE), SEED_PACKAGE);
         writeTinyPackage(path.join(workDir, "second"), "second");

         env = await startRestE2E();
         baseUrl = env.baseUrl;
         const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: DATA_ENV,
               packages: [
                  {
                     name: SEED_PACKAGE,
                     location: path.join(workDir, SEED_PACKAGE),
                  },
               ],
               connections: [],
            }),
         });
         if (!createRes.ok) {
            throw new Error(
               `Failed to seed test environment (${createRes.status}): ${await createRes.text()}`,
            );
         }
         environmentPath = ((await createRes.json()) as { location: string })
            .location;
      });

      afterAll(async () => {
         if (baseUrl) {
            await fetch(`${baseUrl}/api/v0/environments/${DATA_ENV}`, {
               method: "DELETE",
            }).catch(() => {
               /* best-effort cleanup */
            });
         }
         await env?.stop();
         env = null;
         if (workDir) rmSync(workDir, { recursive: true, force: true });
      });

      it("names the errno when a runtime install cannot stage into the environment", async () => {
         const body = await withMode(environmentPath, 0o555, async () =>
            json(
               await fetch(
                  `${baseUrl}/api/v0/environments/${DATA_ENV}/packages`,
                  {
                     method: "POST",
                     headers: { "Content-Type": "application/json" },
                     body: JSON.stringify({
                        name: "second",
                        location: path.join(workDir, "second"),
                     }),
                  },
               ),
            ),
         );
         expect(body.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
         expect(body.message).toMatch(/EACCES|permission denied/i);

         const status = (await (
            await fetch(`${baseUrl}/api/v0/status`)
         ).json()) as {
            loadErrors?: Array<{
               environment: string;
               package?: string;
               message: string;
            }>;
         };
         const entry = status.loadErrors?.find(
            (e) => e.environment === DATA_ENV && e.package === "second",
         );
         expect(entry?.message).toMatch(/EACCES|permission denied/i);
      });

      it("names the errno when the environment README cannot be written", async () => {
         const res = await withMode(environmentPath, 0o555, async () =>
            fetch(`${baseUrl}/api/v0/environments/${DATA_ENV}`, {
               method: "PATCH",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: DATA_ENV,
                  readme: "# Updated\n",
               }),
            }),
         );
         const body = await json(res);
         expect(res.ok).toBe(false);
         expect(body.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
         expect(body.message).toMatch(/EACCES|permission denied/i);
      });

      it("names the errno when a package's publisher.json cannot be written", async () => {
         const manifest = path.join(
            environmentPath,
            SEED_PACKAGE,
            "publisher.json",
         );
         const res = await withMode(manifest, 0o444, async () =>
            fetch(
               `${baseUrl}/api/v0/environments/${DATA_ENV}/packages/${SEED_PACKAGE}`,
               {
                  method: "PATCH",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                     name: SEED_PACKAGE,
                     description: "updated description",
                  }),
               },
            ),
         );
         const body = await json(res);
         expect(res.ok).toBe(false);
         expect(body.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
         expect(body.message).toMatch(/EACCES|permission denied/i);
      });

      it("does not report an unreadable package as one that does not exist", async () => {
         // A 404 "does not exist" sends the operator looking for a missing
         // file when the file is there and only its permissions are wrong.
         const packageDir = path.join(environmentPath, SEED_PACKAGE);
         const res = await withMode(packageDir, 0o000, async () =>
            fetch(
               `${baseUrl}/api/v0/environments/${DATA_ENV}/packages/${SEED_PACKAGE}?reload=true`,
            ),
         );
         const body = await json(res);
         expect(res.ok).toBe(false);
         expect(body.message).not.toMatch(/does not exist/i);
         expect(body.message).toMatch(/EACCES|permission denied/i);
      });
   },
);

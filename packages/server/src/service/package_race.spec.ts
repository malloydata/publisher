import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { Environment } from "./environment";

/**
 * Race-condition regression tests for the package-directory pipeline.
 *
 * Three tests, all deterministic without timing-based flakiness:
 *
 *  1. **Behavioral race repro** — concurrently install (rewrite the
 *     package directory) and read (`getModelFileText`); assert no
 *     `ENOENT` is observed. On the pre-fix code, the read would fail
 *     mid-rewrite. With the per-package mutex now covering both paths,
 *     all reads succeed.
 *
 *  2. **Mutex coverage** — manually hold `withPackageLock` and assert
 *     that a concurrent reader is pending until released. Pins the
 *     invariant that readers actually take the lock.
 *
 *  3. **Download does not block compile** — start an `installPackage`
 *     whose downloader never resolves on its own, then assert that
 *     `getModelFileText` resolves promptly. This pins the Phase 1 /
 *     Phase 2 split — if a future regression accidentally moves the
 *     download inside the lock, this test fails.
 */
describe("package directory race", () => {
   let rootDir: string;
   let envPath: string;
   let fixtureDir: string;

   const PUBLISHER_JSON = JSON.stringify({
      name: "pkg",
      description: "race-test fixture",
   });
   const MODEL_MALLOY = `source: ones is duckdb.sql("SELECT 1 as x")\n`;

   async function writeFixture(targetDir: string): Promise<void> {
      await fs.mkdir(targetDir, { recursive: true });
      await fs.writeFile(
         path.join(targetDir, "publisher.json"),
         PUBLISHER_JSON,
      );
      await fs.writeFile(path.join(targetDir, "model.malloy"), MODEL_MALLOY);
   }

   async function copyDir(src: string, dst: string): Promise<void> {
      await fs.mkdir(dst, { recursive: true });
      await fs.cp(src, dst, { recursive: true });
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-race-"));
      envPath = path.join(rootDir, "env");
      fixtureDir = path.join(rootDir, "fixture");
      await fs.mkdir(envPath, { recursive: true });
      await writeFixture(fixtureDir);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("(A) concurrent installs and reads never observe a half-rewritten tree", async () => {
      const env = await Environment.create("testEnv", envPath, []);

      // Initial install to populate the canonical path.
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixtureDir, stagingPath),
      );

      const ITERATIONS = 30;
      const errors: unknown[] = [];
      let mutatorDone = false;

      // Mutator loop: re-install the package over and over. Each iteration
      // exercises the full Phase 1 (no-lock) + Phase 2 (locked) swap.
      const mutator = (async () => {
         try {
            for (let i = 0; i < ITERATIONS; i++) {
               try {
                  await env.installPackage("pkg", (stagingPath) =>
                     copyDir(fixtureDir, stagingPath),
                  );
               } catch (err) {
                  errors.push({ kind: "install", err });
               }
            }
         } finally {
            mutatorDone = true;
         }
      })();

      // Reader loop: hammer `getModelFileText` while installs run. On the
      // pre-fix code (no lock on reads), the read would sometimes hit ENOENT
      // because the canonical dir was momentarily missing during the rename
      // window. With the per-package mutex covering reads as well, this
      // window is never observable.
      const reader = (async () => {
         while (!mutatorDone) {
            try {
               const text = await env.getModelFileText("pkg", "model.malloy");
               expect(text).toBe(MODEL_MALLOY);
            } catch (err) {
               errors.push({ kind: "read", err });
            }
         }
      })();

      await mutator;
      await reader;

      // Any error here means the lock wasn't actually covering one of the
      // sides — that's the regression we're guarding against.
      if (errors.length > 0) {
         throw new Error(
            `Observed ${errors.length} race-window error(s): ${JSON.stringify(
               errors.slice(0, 3),
               (_k, v) => (v instanceof Error ? `${v.name}: ${v.message}` : v),
            )}`,
         );
      }
   }, 60_000);

   it("(B) compile-time disk reads queue behind withPackageLock", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixtureDir, stagingPath),
      );

      const lockEntered = defer<void>();
      const releaseLock = defer<void>();

      // Hold the per-package mutex from "outside" — simulates a mutator
      // (install / delete / writePackageManifest) being in flight.
      const lockHolder = env.withPackageLock("pkg", async () => {
         lockEntered.resolve();
         await releaseLock.promise;
      });

      await lockEntered.promise;

      // While the lock is held, the reader must NOT make progress.
      const readPromise = env.getModelFileText("pkg", "model.malloy");
      const TIMEOUT_SENTINEL = Symbol("timeout");
      const raced = await Promise.race([
         readPromise,
         new Promise<typeof TIMEOUT_SENTINEL>((resolve) =>
            setTimeout(() => resolve(TIMEOUT_SENTINEL), 50),
         ),
      ]);
      expect(raced).toBe(TIMEOUT_SENTINEL);

      // Release the lock; the reader must now complete.
      releaseLock.resolve();
      await lockHolder;
      const text = await readPromise;
      expect(text).toBe(MODEL_MALLOY);
   }, 15_000);

   it("(C) a slow download does not block concurrent reads", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      // Initial install to make the package present.
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixtureDir, stagingPath),
      );

      const downloadGate = defer<void>();

      // Kick off an install whose Phase 1 downloader stalls until we open
      // the gate. Phase 2 (the brief locked swap) cannot run until then.
      const slowInstall = env.installPackage("pkg", async (stagingPath) => {
         await downloadGate.promise;
         await copyDir(fixtureDir, stagingPath);
      });

      // The reader must resolve well before we open the gate, proving the
      // per-package mutex is NOT held during Phase 1.
      const readStart = Date.now();
      const text = await env.getModelFileText("pkg", "model.malloy");
      const readElapsedMs = Date.now() - readStart;

      expect(text).toBe(MODEL_MALLOY);
      // 1s is generous; in practice this resolves in single-digit ms.
      expect(readElapsedMs).toBeLessThan(1_000);

      // Now open the gate and let the install complete.
      downloadGate.resolve();
      await slowInstall;
   }, 15_000);
});

interface Deferred<T> {
   promise: Promise<T>;
   resolve: (value: T) => void;
   reject: (reason?: unknown) => void;
}

function defer<T>(): Deferred<T> {
   let resolve!: (value: T) => void;
   let reject!: (reason?: unknown) => void;
   const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
   });
   return { promise, resolve, reject };
}

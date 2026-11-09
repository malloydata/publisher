// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { installTestPackage } from "../test_helpers/install_test_package";
import { Environment } from "./environment";

/**
 * Race-condition regression test for the package-directory pipeline.
 *
 * **Mutex coverage** — manually hold `withPackageLock` and assert that a
 * concurrent reader is pending until released. Pins the invariant that
 * readers actually take the lock, so a mutator holding it (delete, a model
 * file write) never shows them a half-rewritten tree.
 */
describe("package directory race", () => {
   let rootDir: string;
   let envPath: string;

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

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-race-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("compile-time disk reads queue behind withPackageLock", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      await installTestPackage(env, "pkg", writeFixture);

      const lockEntered = defer<void>();
      const releaseLock = defer<void>();

      // Hold the per-package mutex from "outside" — simulates a mutator
      // (delete / a model file write) being in flight.
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

// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import {
   existsSync,
   mkdirSync,
   readFileSync,
   rmSync,
   symlinkSync,
   writeFileSync,
} from "fs";
import * as path from "path";
import { TEMP_DIR_PATH } from "../constants";
import { BadRequestError } from "../errors";

const workDir = path.join(TEMP_DIR_PATH, "unzip-environment-spec");
const outsideDir = path.join(TEMP_DIR_PATH, "unzip-environment-spec-outside");

mock.module("../storage/StorageManager", () => ({
   StorageManager: class MockStorageManager {
      async initialize(): Promise<void> {}
      getRepository() {
         return {
            listEnvironments: async () => [],
            listPackages: async () => [],
            listConnections: async () => [],
         };
      }
   },
   StorageConfig: {} as Record<string, unknown>,
}));

const { EnvironmentStore } = await import("./environment_store");

// Zips `entries` of `sourceDir` in the order given, with `zip -y`, which stores
// a symlink as a symlink entry rather than following it.
function zipDir(
   sourceDir: string,
   archivePath: string,
   entries: string[] = ["."],
): void {
   const result = Bun.spawnSync(
      ["zip", "-q", "-y", "-r", archivePath, ...entries],
      { cwd: sourceDir },
   );
   if (result.exitCode !== 0) {
      throw new Error(`zip failed: ${result.stderr.toString()}`);
   }
}

const hasZip =
   process.platform !== "win32" &&
   Bun.spawnSync(["which", "zip"]).exitCode === 0;

describe.skipIf(!hasZip)("unzipEnvironment", () => {
   beforeEach(() => {
      for (const dir of [workDir, outsideDir]) {
         rmSync(dir, { recursive: true, force: true });
         mkdirSync(dir, { recursive: true });
      }
   });

   afterEach(() => {
      for (const dir of [workDir, outsideDir]) {
         rmSync(dir, { recursive: true, force: true });
      }
   });

   it("extracts a plain archive beside itself", async () => {
      const source = path.join(workDir, "src");
      mkdirSync(path.join(source, "models"), { recursive: true });
      writeFileSync(path.join(source, "publisher.json"), '{"name":"pkg"}');
      writeFileSync(path.join(source, "models", "m.malloy"), "source: s is x");
      const archive = path.join(workDir, "pkg.zip");
      zipDir(source, archive);

      const store = new EnvironmentStore(workDir);
      const extracted = await store.unzipEnvironment(archive);

      expect(extracted).toBe(path.join(workDir, "pkg"));
      expect(
         readFileSync(path.join(extracted, "models", "m.malloy"), "utf-8"),
      ).toBe("source: s is x");
   });

   it("refuses an archive containing a symlink and leaves nothing extracted", async () => {
      const source = path.join(workDir, "src");
      mkdirSync(source, { recursive: true });
      writeFileSync(path.join(source, "publisher.json"), '{"name":"pkg"}');
      symlinkSync(outsideDir, path.join(source, "escape"));
      const archive = path.join(workDir, "pkg.zip");
      // A regular file first, so the rejection has a partial extract to remove.
      zipDir(source, archive, ["publisher.json", "escape"]);

      const store = new EnvironmentStore(workDir);
      const attempt = store.unzipEnvironment(archive);

      await expect(attempt).rejects.toBeInstanceOf(BadRequestError);
      await expect(attempt).rejects.toThrow(/"escape" is a symbolic link/);
      expect(existsSync(path.join(workDir, "pkg"))).toBe(false);
   });
});

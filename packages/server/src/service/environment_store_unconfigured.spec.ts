// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "fs";
import * as path from "path";
import { getUnresolvedPublisherConfigPath } from "../config";
import { TEMP_DIR_PATH } from "../constants";
import { logger } from "../logger";

/**
 * A server that loads nothing reports `serving` with `environments=0
 * packages=0 load_errors=0`, and every one of those numbers is correct:
 * nothing failed because nothing was configured. Before this notice existed,
 * no output named the config path that had been checked, so a Docker image
 * booted without its config mount looked healthy and served an empty catalog.
 *
 * These specs assert REPORTING rather than resolution: the helper's return
 * value is easy to get right and still reach nobody. The parser-level cases
 * live in the first describe, the boot-path ones in the second.
 *
 * `../config` is deliberately NOT mocked: environment_store.spec.ts mocks it in
 * a beforeEach and these cases need the real resolver.
 *
 * Storage IS mocked, and it has to be. `mock.module` is process-wide and every
 * spec file shares one bun process, so without a mock of its own this file
 * inherits whichever StorageManager the previously-run file installed.
 * environment_store_clone.spec.ts installs one whose `initialize` throws its
 * redaction fixture, which makes construction fail, `PUBLISHER_INIT_FAILED`
 * fire, and the success tail that carries the notice never run. Every boot case
 * below then reports the notice missing -- including "stays quiet", which
 * passes for that reason rather than the one it names. Measured: running this
 * file alone passed 6/6; running it after the clone spec failed the two
 * positive cases and left the negative one green.
 *
 * Hence `assertInitialized`. A notice-absent assertion is only meaningful once
 * the boot it describes actually happened, so each boot case pins the success
 * line that immediately precedes the notice.
 */

/**
 * Environments the mocked database holds. Mutable because the interesting boot
 * is the one where the database knows an environment the filesystem no longer
 * has.
 */
let storedEnvironments: Array<Record<string, unknown>> = [];

mock.module("../storage/StorageManager", () => ({
   StorageManager: class MockStorageManager {
      async initialize(): Promise<void> {}
      getRepository() {
         return {
            listEnvironments: async () => storedEnvironments,
            getEnvironmentByName: async () => null,
            createEnvironment: async (data: Record<string, unknown>) => ({
               id: "env-id",
               name: data.name,
               path: data.path,
            }),
            listPackages: async () => [],
            getPackageByName: async () => null,
            createPackage: async (data: Record<string, unknown>) => ({
               id: "pkg-id",
               name: data.name,
            }),
            listConnections: async () => [],
            // Without this the runtime-creation case logs "Error syncing
            // storage destinations" on every run and passes anyway, which is
            // the shape of thing this file exists to catch.
            listStorageDestinations: async () => [],
         };
      }
   },
   StorageConfig: {} as Record<string, unknown>,
}));

// After the mock, so the binding is the mock rather than the real module.
const { EnvironmentStore } = await import("./environment_store");

const serverRootPath = path.join(TEMP_DIR_PATH, "unconfigured-spec-root");
const missingEnvPath = path.join(serverRootPath, "publisher_data", "gone");

const resetRoot = () => {
   if (existsSync(serverRootPath)) {
      rmSync(serverRootPath, { recursive: true, force: true });
   }
   mkdirSync(serverRootPath, { recursive: true });
};

describe("getUnresolvedPublisherConfigPath", () => {
   const priorConfigPath = process.env.PUBLISHER_CONFIG_PATH;

   beforeEach(() => {
      resetRoot();
      delete process.env.PUBLISHER_CONFIG_PATH;
   });

   afterEach(() => {
      rmSync(serverRootPath, { recursive: true, force: true });
      if (priorConfigPath === undefined) {
         delete process.env.PUBLISHER_CONFIG_PATH;
      } else {
         process.env.PUBLISHER_CONFIG_PATH = priorConfigPath;
      }
   });

   it("names the path it looked for when no config is present", () => {
      expect(getUnresolvedPublisherConfigPath(serverRootPath)).toBe(
         path.join(serverRootPath, "publisher.config.json"),
      );
   });

   it("returns null once a config exists at the server root", () => {
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({ frozenConfig: false, environments: [] }),
      );
      expect(getUnresolvedPublisherConfigPath(serverRootPath)).toBeNull();
   });

   it("stays quiet for an explicit --config, even a missing one", () => {
      // getPublisherConfig already logs that case at error. Reporting the same
      // mistake twice in two different shapes helps nobody.
      process.env.PUBLISHER_CONFIG_PATH = path.join(
         serverRootPath,
         "nowhere.json",
      );
      expect(getUnresolvedPublisherConfigPath(serverRootPath)).toBeNull();
   });
});

describe("unconfigured boot notice", () => {
   let infoSpy: ReturnType<typeof spyOn>;
   let infoLines: string[];

   const noticeLines = () =>
      infoLines.filter((line) => line.includes("Serving with no environments"));

   // The notice is logged from initialize()'s success tail, one line after
   // this one. If the boot failed instead both are absent, and a
   // notice-absent assertion then passes without the boot it describes ever
   // having happened.
   const assertInitialized = () =>
      expect(
         infoLines.filter((line) =>
            line.includes("Environment store successfully initialized"),
         ),
      ).toHaveLength(1);

   beforeEach(() => {
      resetRoot();
      delete process.env.PUBLISHER_CONFIG_PATH;
      storedEnvironments = [];
      infoLines = [];
      infoSpy = spyOn(logger, "info").mockImplementation((message: unknown) => {
         infoLines.push(String(message));
         return logger;
      });
   });

   afterEach(() => {
      infoSpy.mockRestore();
      rmSync(serverRootPath, { recursive: true, force: true });
   });

   it("names the missing config once when nothing loaded", async () => {
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      assertInitialized();
      const notices = noticeLines();
      expect(notices).toHaveLength(1);
      expect(notices[0]).toContain(
         path.join(serverRootPath, "publisher.config.json"),
      );
      // The two ways out, both of which a reader needs to act on it.
      expect(notices[0]).toContain("--config");
      expect(notices[0]).toContain("runtime");
   });

   it("can still create an environment at runtime, as the notice claims", async () => {
      // The notice, the code comment beside it and docs/deployment.md all tell
      // the reader that an unconfigured server is a supported way to run
      // because environments can be created afterwards. That claim rests on
      // `frozenConfig` defaulting to false when no config file exists, which
      // nothing else pins. If a future change made runtime creation require a
      // config file, all three would quietly become wrong.
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      assertInitialized();
      expect(noticeLines()).toHaveLength(1);

      await store.addEnvironment({ name: "created-at-runtime" });

      const environments = await store.listEnvironments();
      expect(environments.map((environment) => environment.name)).toContain(
         "created-at-runtime",
      );
   });

   it("stays quiet when the database held an environment that did not load", async () => {
      // Reachable exactly the way this notice's own last sentence invites:
      // boot unconfigured, create an environment over the API, then restart
      // onto a root where publisher_data/<env> is gone. The environment is
      // skipped with a logged error and no failedEnvironments entry, so
      // `environments.size === 0` while a config file was never the problem.
      storedEnvironments = [
         { id: "env-id", name: "created-at-runtime", path: missingEnvPath },
      ];

      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      assertInitialized();
      expect(noticeLines()).toHaveLength(0);
   });

   it("stays quiet when a config resolved, however empty", async () => {
      // An operator who configured no environments meant it. The notice is for
      // the reader who thinks they configured some and did not.
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({ frozenConfig: false, environments: [] }),
      );

      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      assertInitialized();
      expect(noticeLines()).toHaveLength(0);
   });
});
